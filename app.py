<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <title>PELÉE AI | Address Search</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
import os
import io
import json
import csv
import zipfile
import re

from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    redirect,
    url_for,
    session,
    send_file,
)
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


# ----------------- CONFIG -----------------

load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-2"))
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
# per-county enriched files, e.g. merged_with_tracts_acs/tx/harris-with-values-income.geojson
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")

# how many address rows to show in UI
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# in-memory caches
_STATE_KEYS_CACHE = {}
_CITY_TO_COUNTY_CACHE = {}


# ----------------- LOOKUP TABLES -----------------

US_STATES = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
}

# allow detection of 2-letter codes
STATE_CODES = {v: v for v in US_STATES.values()}

# minimal ZIP → (STATE, COUNTY) mapping for now
ZIP_TO_STATE_COUNTY = {
    # Rocky River / ZIP 44116
    "44116": ("OH", "cuyahoga"),
}

# minimal city → county mapping for known special cases
CITY_TO_COUNTY = {
    "wa": {
        # city name (lowercase) -> (county_name, Pretty City Name)
        "vancouver": ("clark", "Vancouver"),
    },
    # you can append more here over time
}


# ----------------- UTILITIES -----------------


def normalize_text(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def canonicalize_county_name(name: str) -> str:
    """
    normalize "Wakulla", "Wakulla County" etc → "wakulla"
    """
    if not name:
        return ""
    s = name.lower().replace("_", " ").replace("-", " ")
    for word in [" county", " parish"]:
        if word in s:
            s = s.replace(word, "")
    return " ".join(s.split())


def detect_state(raw_query: str):
    """
    Detect a US state from the *raw* query string.

    Rules:
      1) Look for ALL-CAPS 2-letter tokens that match a state code
         (so 'TX', 'WA', 'OH' work, but 'me' in 'give me' is ignored).
      2) Fall back to full state names like 'florida', 'ohio', etc.
    """
    raw_query = raw_query or ""
    tokens_raw = raw_query.split()

    # 1) ALL-CAPS two-letter tokens, e.g. TX, WA
    for tok in tokens_raw:
        t = tok.strip(",. ")
        if len(t) == 2 and t.isalpha() and t.isupper():
            code = t.lower()
            if code in STATE_CODES:
                return t.upper()

    # 2) Full state names
    lower_q = raw_query.lower()
    for name, code in US_STATES.items():
        if name in lower_q:
            return code.upper()

    return None


def extract_zip(query: str):
    """
    Return the first 5-digit ZIP code found, or None.
    """
    m = re.search(r"\b(\d{5})\b", query or "")
    return m.group(1) if m else None


def parse_numeric_filters(query: str):
    """
    Extract simple 'over X' / 'above X' for incomes or values.
    """
    q = (query or "").lower()
    income_min = None
    value_min = None

    tokens = q.replace("$", "").replace(",", "").split()
    for i, t in enumerate(tokens):
        if t in {"over", "above"} and i + 1 < len(tokens):
            raw = tokens[i + 1].strip(".,)")
            mult = 1
            if raw.endswith("k"):
                mult = 1000
                raw = raw[:-1]
            elif raw.endswith("m"):
                mult = 1_000_000
                raw = raw[:-1]

            try:
                num = float(raw) * mult
            except ValueError:
                continue

            window = " ".join(tokens[max(0, i - 3): i + 6])
            if "income" in window or "household" in window:
                income_min = num
            elif any(w in window for w in ["value", "home", "homes", "properties", "price"]):
                value_min = num
            else:
                # default this numeric to income if we don't see a clear hint
                if income_min is None:
                    income_min = num

    return income_min, value_min


def parse_location_and_filters(query: str):
    """
    Parse state, county, city, ZIP and income/value filters from a query.

    Supports things like:
      - "Give me residential addresses in Harris County TX"
      - "addresses in Wakulla County Florida"
      - "addresses in Vancouver WA over 600k"
      - "ZIP 44116 residential addresses"
      - "homes in Strongsville Ohio with incomes over 300k"
    """
    raw = query or ""
    q = normalize_text(raw)
    tokens = q.split()

    income_min, value_min = parse_numeric_filters(raw)
    zip_code = extract_zip(raw)
    state = detect_state(raw)
    county = None
    city = None

    # ZIP override if we know the mapping
    if zip_code and zip_code in ZIP_TO_STATE_COUNTY:
        zip_state, zip_county = ZIP_TO_STATE_COUNTY[zip_code]
        if not state:
            state = zip_state
        county = zip_county

    # If we still don't have a state, look for 2-letter lowercase codes,
    # but ignore common words that collide with codes (in, me, or, etc.)
    if not state:
        AMBIGUOUS = {"in", "me", "or", "as", "is", "us"}
        for t in tokens:
            if t in AMBIGUOUS:
                continue
            if len(t) == 2 and t in STATE_CODES:
                state = t.upper()
                break

    # 1) County: "... Harris County TX"
    if not county and "county" in tokens:
        idx = tokens.index("county")
        j = idx - 1
        STOP = {
            "in",
            "of",
            "all",
            "any",
            "properties",
            "homes",
            "households",
            "residential",
            "addresses",
            "parcels",
        }
        county_tokens_rev = []
        while j >= 0:
            t = tokens[j]
            if t in STOP:
                break
            county_tokens_rev.append(t)
            j -= 1
        county_tokens = list(reversed(county_tokens_rev))
        if county_tokens:
            county = " ".join(county_tokens)

    # 2) Special handling for Ohio: "Strongsville Ohio" → city=Strongsville, county=Cuyahoga
    if not county and state == "OH":
        state_idx = None
        for i, t in enumerate(tokens):
            if t in STATE_CODES or t in US_STATES:
                state_idx = i
                break
        if state_idx is not None and state_idx > 0:
            j = state_idx - 1
            STOP = {"in", "ohio", "oh"}
            city_tokens_rev = []
            while j >= 0 and tokens[j] not in STOP:
                city_tokens_rev.append(tokens[j])
                j -= 1
            city_tokens = list(reversed(city_tokens_rev))
            if city_tokens:
                city = " ".join(city_tokens)
        # known that Strongsville / Berea etc live in Cuyahoga County
        county = "cuyahoga"

    # 3) City → County mapping for things like Vancouver, WA
    if state and not county:
        st_key = state.lower()
        city_map = CITY_TO_COUNTY.get(st_key, {})
        for city_name, (city_county, pretty_city) in city_map.items():
            if city_name in q:
                county = city_county
                city = pretty_city
                break

    # 4) Fallback: basic "in <City> <State>" parsing if we still have no county
    if state and not county:
        try:
            in_idx = tokens.index("in")
        except ValueError:
            in_idx = None

        if in_idx is not None:
            city_tokens = []
            for t in tokens[in_idx + 1:]:
                if t in STATE_CODES or t in US_STATES:
                    break
                if t == "county":
                    break
                city_tokens.append(t)
            if city_tokens:
                city = " ".join(city_tokens)

    return state, county, city, zip_code, income_min, value_min


def list_state_keys(state: str):
    """
    Return all GeoJSON keys under S3_PREFIX/<state>/, cached in memory.
    """
    state = (state or "").lower()
    if not state:
        return []

    if state in _STATE_KEYS_CACHE:
        return _STATE_KEYS_CACHE[state]

    prefix = f"{S3_PREFIX}/{state}/"
    keys = []
    token = None

    while True:
        kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        try:
            resp = s3_client.list_objects_v2(**kwargs)
        except ClientError as e:
            app.logger.error(f"[S3] list_objects_v2 failed for state={state}: {e}")
            break

        for obj in resp.get("Contents", []):
            key = obj.get("Key")
            if key and key.endswith(".geojson"):
                keys.append(key)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    _STATE_KEYS_CACHE[state] = keys
    app.logger.info(f"[S3] Cached {len(keys)} keys for state={state}")
    return keys


def resolve_dataset_key(state: str, county: str):
    """
    Given state code (e.g., 'TX') and county name (e.g., 'Harris'),
    pick the best matching S3 key.

    Preference:
      1) exact county + '-with-values-income'
      2) exact county + '.geojson'
      3) fuzzy with '-with-values-income'
      4) fuzzy raw '.geojson'
    """
    if not state or not county:
        return None

    state = state.lower()
    county_clean = canonicalize_county_name(county)

    keys = list_state_keys(state)
    if not keys:
        return None

    enriched_exact = []
    raw_exact = []
    enriched_fuzzy = []
    raw_fuzzy = []

    for key in keys:
        fname = key.split("/")[-1]
        if not fname.endswith(".geojson"):
            continue
        base = fname[:-len(".geojson")]
        enriched = "with-values-income" in base
        base_no_suffix = base.replace("-with-values-income", "")
        base_canon = canonicalize_county_name(base_no_suffix)

        if base_canon == county_clean:
            (enriched_exact if enriched else raw_exact).append(key)
        elif base_canon.startswith(county_clean) or county_clean.startswith(base_canon):
            (enriched_fuzzy if enriched else raw_fuzzy).append(key)

    for bucket in (enriched_exact, raw_exact, enriched_fuzzy, raw_fuzzy):
        if bucket:
            return sorted(bucket, key=len)[0]

    return None


def load_geojson_from_s3(key: str):
    """
    Simple S3 load: read the whole object and json.loads it.
    """
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        body = resp["Body"].read()
        return json.loads(body)
    except Exception as e:
        app.logger.error(f"[S3] load failed for {key}: {e}")
        raise


def filter_features(features, income_min=None, value_min=None, zip_code=None, city=None):
    """
    Filter by income, value, optional ZIP, and optional city.
    """
    if (income_min is None) and (value_min is None) and not zip_code and not city:
        return features

    income_keys = ["DP03_0062E", "median_income", "income", "household_income"]
    value_keys = ["DP04_0089E", "median_value", "home_value", "value"]

    def read_val(props, keys):
        for k in keys:
            if k in props and props[k] not in ("", None):
                try:
                    return float(props[k])
                except Exception:
                    pass
        return None

    out = []
    city_norm = city.lower() if city else None

    for f in features:
        p = f.get("properties", {})
        keep = True

        # ZIP filter
        if zip_code is not None:
            pc = str(p.get("postcode") or "").strip()
            if pc != zip_code:
                keep = False

        # City filter (when we detected a city name)
        if keep and city_norm:
            prop_city = str(p.get("city") or p.get("CITY") or "").strip().lower()
            if prop_city:
                # loose match: either substring direction
                if city_norm not in prop_city and prop_city not in city_norm:
                    keep = False

        # Income filter
        if keep and income_min is not None:
            v = read_val(p, income_keys)
            if v is None or v < income_min:
                keep = False

        # Value filter
        if keep and value_min is not None:
            v = read_val(p, value_keys)
            if v is None or v < value_min:
                keep = False

        if keep:
            out.append(f)

    return out


def feature_to_address_obj(feat):
    """
    Convert a GeoJSON feature into the object your front-end expects.
    """
    props = feat.get("properties", {})
    geom = feat.get("geometry", {}) or {}
    coords = geom.get("coordinates") or [None, None]

    number = props.get("number") or props.get("house_number") or ""
    street = props.get("street") or props.get("road") or ""
    unit = props.get("unit") or ""

    city = props.get("city") or props.get("CITY") or ""
    region = props.get("region") or props.get("STUSPS") or ""
    postcode = props.get("postcode") or ""

    income = (
        props.get("DP03_0062E")
        or props.get("median_income")
        or props.get("income")
    )
    value = (
        props.get("DP04_0089E")
        or props.get("median_value")
        or props.get("home_value")
        or props.get("value")
    )

    return {
        "address": " ".join(x for x in [str(number), street, unit] if x),
        "city": city,
        "state": region,
        "zip": postcode,
        "income": income,
        "value": value,
        "lat": coords[1],
        "lon": coords[0],
    }


# ----------------- ROUTES -----------------


@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == PASSWORD:
            session["authed"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Invalid password.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def index():
    if not session.get("authed"):
        return redirect(url_for("login"))
    return render_template("index.html")


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    try:
        if request.is_json:
            data = request.get_json(silent=True) or {}
            query = (data.get("query") or "").strip()
        else:
            query = (request.form.get("query") or "").strip()

        if not query:
            return jsonify({"ok": False, "error": "Please enter a query."})

        (
            state,
            county,
            city,
            zip_code,
            income_min,
            value_min,
        ) = parse_location_and_filters(query)

        app.logger.info(
            f"[search] query={query!r} → state={state}, county={county}, city={city}, zip={zip_code}, "
            f"income_min={income_min}, value_min={value_min}"
        )

        if not state:
            return jsonify({"ok": False, "error": "No state detected."})
        if not county:
            return jsonify({"ok": False, "error": "No county detected."})

        key = resolve_dataset_key(state, county)
        app.logger.info(f"[search] resolved dataset_key={key}")

        if not key:
            return jsonify(
                {
                    "ok": False,
                    "error": f"No dataset found for {county} County, {state}.",
                }
            )

        try:
            gj = load_geojson_from_s3(key)
        except Exception as e:
            return jsonify(
                {
                    "ok": False,
                    "error": f"Failed loading {key}: {str(e)}",
                }
            )

        feats = gj.get("features", [])
        feats = filter_features(
            feats,
            income_min=income_min,
            value_min=value_min,
            zip_code=zip_code,
            city=city,
        )

        total = len(feats)
        feats = feats[:MAX_RESULTS]
        addresses = [feature_to_address_obj(f) for f in feats]

        return jsonify(
            {
                "ok": True,
                "query": query,
                "state": state,
                "county": county,
                "city": city,
                "zip": zip_code,
                "dataset_key": key,
                "total": total,
                "shown": len(addresses),
                "results": addresses,
            }
        )
    except Exception as e:
        app.logger.exception(f"[search] unexpected error: {e}")
        return jsonify({"ok": False, "error": "Server error (search failed)."}), 500


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    if request.is_json:
        data = request.get_json(silent=True) or {}
        query = (data.get("query") or "").strip()
    else:
        query = (request.form.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Missing query for export."}), 400

    state, county, city, zip_code, income_min, value_min = parse_location_and_filters(query)

    if not state or not county:
        return jsonify({"ok": False, "error": "Need a state + county to export."}), 400

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify(
            {
                "ok": False,
                "error": f"No dataset found for export for {county} County, {state}.",
            }
        ), 400

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify(
            {"ok": False, "error": f"Failed to load dataset for export: {str(e)}"}
        ), 500

    feats = gj.get("features", [])
    feats = filter_features(
        feats,
        income_min=income_min,
        value_min=value_min,
        zip_code=zip_code,
        city=city,
    )

    rows = []
    fieldnames = set()

    for f in feats:
        props = f.get("properties", {}).copy()
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates") or [None, None]
        props["lon"] = coords[0]
        props["lat"] = coords[1]
        rows.append(props)
        fieldnames.update(props.keys())

    fieldnames = sorted(fieldnames)

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
        zf.writestr("addresses.csv", buf.getvalue())

    mem.seek(0)
    fname = f"pelee_export_{state}_{canonicalize_county_name(county).replace(' ', '_')}.zip"

    return send_file(
        mem,
        mimetype="application/zip",
        as_attachment=True,
        download_name=fname,
    )


# ----------------- MAIN -----------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

    <style>
        :root {
            --bg: #050816;
            --card: #0b1220;
            --accent: #38bdf8;
            --accent-soft: rgba(56, 189, 248, 0.18);
            --text-main: #e5e7eb;
            --text-muted: #9ca3af;
            --danger: #f97373;
        }

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            font-family: system-ui, -apple-system, BlinkMacSystemFont, "Inter", sans-serif;
            background: radial-gradient(circle at top, #111827 0, #020617 55%, #000 100%);
            color: var(--text-main);
        }

        header {
            padding: 18px 32px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            border-bottom: 1px solid rgba(148, 163, 184, 0.2);
            backdrop-filter: blur(16px);
            background: linear-gradient(to right, rgba(15,23,42,0.9), rgba(15,23,42,0.7));
            position: sticky;
            top: 0;
            z-index: 10;
        }

        .logo {
            font-weight: 700;
            font-size: 22px;
            letter-spacing: 0.08em;
        }

        .logo span {
            color: var(--accent);
        }

        .header-right a {
            color: var(--text-muted);
            text-decoration: none;
            font-size: 13px;
            opacity: 0.8;
        }

        .header-right a:hover {
            opacity: 1;
            text-decoration: underline;
        }

        .container {
            max-width: 960px;
            margin: 40px auto 60px;
            padding: 0 16px;
        }

        .hero {
            text-align: left;
            margin-bottom: 24px;
        }

        .hero-title {
            font-size: 32px;
            font-weight: 700;
            letter-spacing: 0.04em;
            margin-bottom: 8px;
            text-transform: uppercase;
        }

        .hero-subtitle {
            font-size: 16px;
            color: var(--text-muted);
        }

        .pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            font-size: 12px;
            padding: 4px 10px;
            border-radius: 999px;
            background: rgba(15,23,42,0.9);
            border: 1px solid rgba(148,163,184,0.45);
            margin-bottom: 10px;
            color: var(--text-muted);
        }

        .pill-dot {
            width: 6px;
            height: 6px;
            border-radius: 999px;
            background: #22c55e;
        }

        .search-card {
            background: radial-gradient(circle at top left, rgba(56,189,248,0.12), transparent 60%),
                        linear-gradient(to bottom right, rgba(15,23,42,0.96), rgba(15,23,42,0.98));
            border-radius: 18px;
            padding: 20px 20px 18px;
            border: 1px solid rgba(148,163,184,0.3);
            box-shadow: 0 18px 60px rgba(15,23,42,0.8);
        }

        .search-form {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            align-items: center;
        }

        .search-input {
            flex: 1 1 220px;
            background: rgba(15,23,42,0.9);
            border-radius: 999px;
            border: 1px solid rgba(148,163,184,0.7);
            padding: 10px 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .search-input input {
            flex: 1;
            background: transparent;
            border: none;
            outline: none;
            color: var(--text-main);
            font-size: 15px;
        }

        .search-input input::placeholder {
            color: rgba(148,163,184,0.9);
        }

        .search-button {
            border-radius: 999px;
            border: none;
            padding: 10px 18px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            background: linear-gradient(to right, #0ea5e9, #22c55e);
            color: #0b1120;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }

        .search-button:hover {
            filter: brightness(1.05);
        }

        .examples {
            margin-top: 10px;
            font-size: 13px;
            color: var(--text-muted);
        }

        .examples span {
            color: var(--text-main);
        }

        .results-wrap {
            margin-top: 28px;
        }

        .summary {
            padding: 12px 14px;
            border-radius: 10px;
            border: 1px solid rgba(148,163,184,0.4);
            background: radial-gradient(circle at top left, var(--accent-soft), rgba(15,23,42,0.96));
            font-size: 13px;
            line-height: 1.6;
        }

        .summary strong {
            color: var(--accent);
        }

        .error-box {
            padding: 12px 14px;
            border-radius: 10px;
            border: 1px solid rgba(248,113,113,0.7);
            background: rgba(127,29,29,0.65);
            color: #fecaca;
            font-size: 13px;
            margin-top: 12px;
        }

        .export-row {
            margin-top: 10px;
            display: flex;
            justify-content: flex-end;
        }

        .export-btn {
            border-radius: 999px;
            border: 1px solid rgba(56,189,248,0.6);
            padding: 7px 12px;
            font-size: 12px;
            background: rgba(15,23,42,0.9);
            color: var(--accent);
            cursor: pointer;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }

        .export-btn:hover {
            background: rgba(15,23,42,1);
        }

        .results-list {
            margin-top: 18px;
            display: grid;
            grid-template-columns: 1fr;
            gap: 10px;
        }

        @media (min-width: 800px) {
            .results-list {
                grid-template-columns: 1fr 1fr;
            }
        }

        .result-card {
            background: linear-gradient(to bottom right, #020617, #020617);
            border-radius: 12px;
            padding: 12px 12px 11px;
            border: 1px solid rgba(148,163,184,0.3);
            box-shadow: 0 10px 30px rgba(15,23,42,0.8);
            font-size: 13px;
            line-height: 1.5;
        }

        .result-address {
            font-weight: 600;
            margin-bottom: 4px;
        }

        .result-meta {
            color: var(--text-muted);
        }

        .pill-badge {
            display: inline-block;
            font-size: 11px;
            padding: 2px 7px;
            border-radius: 999px;
            background: rgba(15,23,42,0.9);
            border: 1px solid rgba(148,163,184,0.7);
            margin-right: 6px;
            margin-top: 4px;
        }

        .loading {
            opacity: 0.85;
        }
    </style>
</head>
<body>

<header>
    <div class="logo"><span>PELÉE</span> AI</div>
    <div class="header-right">
        <a href="/logout">Logout</a>
    </div>
</header>

<div class="container">
    <div class="hero">
        <div class="pill">
            <span class="pill-dot"></span>
            Internal Data Tool · Residential + Commercial
        </div>
        <div class="hero-title">AI-powered address discovery</div>
        <div class="hero-subtitle">
            Pull targeted addresses by county, city, ZIP, income bands, and home values in one natural-language query.
        </div>
    </div>

    <div class="search-card">
        <form id="searchForm" class="search-form">
            <div class="search-input">
                <span>🔍</span>
                <input
                    id="query"
                    type="text"
                    placeholder="e.g. homes in Strongsville Ohio with incomes over 150k"
                    autocomplete="off"
                />
            </div>
            <button class="search-button" type="submit">
                <span>Search</span> ⏎
            </button>
        </form>

        <div class="examples">
            Examples:
            <span>“addresses in Wakulla County Florida”</span> ·
            <span>“homes in Teton County WY with incomes over 200k”</span> ·
            <span>“ZIP 44116 residential addresses”</span>
        </div>

        <div id="results-container" class="results-wrap"></div>
    </div>
</div>

<script>
const form = document.getElementById("searchForm");
const queryInput = document.getElementById("query");
const resultsDiv = document.getElementById("results-container");

function escapeHtml(str) {
    if (str === null || str === undefined) return "";
    return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
}

form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const q = (queryInput.value || "").trim();
    if (!q) {
        resultsDiv.innerHTML = "<div class='error-box'>Please enter a query.</div>";
        return;
    }

    resultsDiv.innerHTML = `
        <div class="summary loading">
            Searching…<br>
            <span style="color: var(--text-muted); font-size: 12px;">(${escapeHtml(q)})</span>
        </div>
    `;

    try {
        const resp = await fetch("/search", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: q })
        });

        if (!resp.ok) {
            const text = await resp.text();
            resultsDiv.innerHTML = `
                <div class="error-box">
                    Server error (${resp.status}).<br>
                    <div style="opacity:0.8; margin-top:4px; font-size:11px;">${escapeHtml(text || "No details provided.")}</div>
                </div>
            `;
            return;
        }

        const data = await resp.json();

        if (data.error) {
            resultsDiv.innerHTML = `<div class="error-box">${escapeHtml(data.error)}</div>`;
            return;
        }

        renderResults(q, data);
    } catch (err) {
        resultsDiv.innerHTML = `
            <div class="error-box">
                Network or server error. Try again.<br>
                <div style="opacity:0.8; margin-top:4px; font-size:11px;">${escapeHtml(err)}</div>
            </div>
        `;
    }
});

function renderResults(query, data) {
    const hasResults = (data.results || []).length > 0;

    const locationParts = [];
    if (data.city) locationParts.push(data.city);
    if (data.county) locationParts.push(capitalize(data.county) + " County");
    if (data.state) locationParts.push(data.state);

    const locationStr = locationParts.length ? locationParts.join(", ") : "—";

    const messageLine = data.message
        ? `<br><span style="color: var(--text-muted); font-size: 12px;">${escapeHtml(data.message)}</span>`
        : "";

    const summaryHtml = `
        <div class="summary">
            <strong>Query</strong>: ${escapeHtml(data.query)}<br>
            <strong>Location</strong>: ${escapeHtml(locationStr)}<br>
            <strong>Dataset</strong>: <span style="color:#cbd5f5">${escapeHtml(data.dataset_key || "n/a")}</span><br>
            <strong>Results</strong>: Showing ${data.shown || 0} of ${data.total || 0}
            ${messageLine}
        </div>
    `;

    let exportHtml = "";
    if (hasResults) {
        exportHtml = `
            <div class="export-row">
                <button class="export-btn" onclick="exportResults('${encodeURIComponent(query)}')">
                    ⬇️ Export full CSV
                </button>
            </div>
        `;
    }

    let cards = "";
    if (hasResults) {
        cards = data.results.map((r) => {
            const addr = r.address || "";
            const city = r.city || "";
            const st = r.state || "";
            const zip = r.zip || "";
            const income = r.income ?? "N/A";
            const value = r.value ?? "N/A";
            const lat = (r.lat !== null && r.lat !== undefined) ? r.lat : "—";
            const lon = (r.lon !== null && r.lon !== undefined) ? r.lon : "—";

            return `
                <div class="result-card">
                    <div class="result-address">${escapeHtml(addr)}</div>
                    <div class="result-meta">
                        ${escapeHtml(city)}${city ? ", " : ""}${escapeHtml(st)}${zip ? " " + escapeHtml(zip) : ""}
                    </div>
                    <div class="result-meta">
                        <span class="pill-badge">Income: ${escapeHtml(income)}</span>
                        <span class="pill-badge">Value: ${escapeHtml(value)}</span>
                    </div>
                    <div class="result-meta" style="margin-top:4px;">
                        Lat: ${escapeHtml(lat)} · Lon: ${escapeHtml(lon)}
                    </div>
                </div>
            `;
        }).join("");
    } else {
        cards = `
            <div class="error-box" style="margin-top:12px;">
                No matching addresses found for that filter.<br>
                Try lowering the income/value threshold or adjusting the city / county / ZIP.
            </div>
        `;
    }

    resultsDiv.innerHTML = summaryHtml + exportHtml + `<div class="results-list">${cards}</div>`;
}

async function exportResults(qEncoded) {
    const q = decodeURIComponent(qEncoded);
    try {
        const resp = await fetch("/export", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ query: q })
        });

        if (!resp.ok) {
            alert("Export failed (" + resp.status + ").");
            return;
        }

        const blob = await resp.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "pelee_export.zip";
        document.body.appendChild(a);
        a.click();
        a.remove();
        window.URL.revokeObjectURL(url);
    } catch (err) {
        alert("Export failed: " + err);
    }
}

function capitalize(s) {
    if (!s) return "";
    s = String(s);
    return s.charAt(0).toUpperCase() + s.slice(1);
}
</script>

</body>
</html>


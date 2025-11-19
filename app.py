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

# per-county enriched files like: merged_with_tracts_acs/fl/wakulla-with-values-income.geojson
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")

# how many address rows to show in UI
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# in-memory cache of S3 keys by state
_STATE_KEYS_CACHE = {}

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

# allow detection of 2-letter codes like "fl", "oh", "tx"
STATE_CODES = {v: v for v in US_STATES.values()}

# minimal ZIP → (STATE, COUNTY) mapping for what you’ve tested so far
ZIP_TO_STATE_COUNTY = {
    # Rocky River / ZIP 44116
    "44116": ("OH", "cuyahoga"),
    # add more as needed
}

# minimal city → county mapping for things like Vancouver, WA
CITY_TO_COUNTY = {
    "wa": {
        # city name (lowercase) -> (county_name, Pretty City Name)
        "vancouver": ("clark", "Vancouver"),
    },
    # you can add more states / cities over time
    # "oh": {"berea": ("cuyahoga", "Berea"), "strongsville": ("cuyahoga", "Strongsville")},
}


# ----------------- UTILITIES -----------------


def normalize_text(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def normalize_city(s: str) -> str:
    return normalize_text(s)


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


def detect_state(q: str):
    """
    Try to detect a 2-letter state or full state name from the query.
    NO AI here; purely deterministic.
    """
    q = (q or "").lower()
    tokens = q.split()

    # look for 2-letter codes first (tx, oh, fl, etc.)
    for t in tokens:
        t_clean = t.strip(",.").lower()
        # don't treat the word "in" as Indiana
        if t_clean == "in":
            continue
        if t_clean in STATE_CODES:
            return t_clean.upper()

    # look for full names like "florida", "washington"
    for name, code in US_STATES.items():
        if name in q:
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
            if any(x in window for x in ["income", "household"]):
                income_min = num
            elif any(x in window for x in ["value", "home", "homes", "properties"]):
                value_min = num
            else:
                # default to income if ambiguous
                if income_min is None:
                    income_min = num

    return income_min, value_min


def parse_location_and_filters(query: str):
    """
    Purely deterministic parsing of state, county, city, ZIP and income/value filters.

    Supports examples like:
      - "addresses in Wakulla County Florida"
      - "ZIP 44116 residential addresses"
      - "addresses in vancouver wa"
      - "homes in Strongsville Ohio with incomes over 300k"
      - "Give me residential addresses in Harris County TX"
    """
    raw = query or ""
    q = normalize_text(raw)
    tokens = q.split()

    # numeric filters
    income_min, value_min = parse_numeric_filters(raw)

    # 1) ZIP detection
    zip_code = extract_zip(raw)
    state = detect_state(q)
    county = None
    city = None

    # ZIP -> state + county if we know it
    if zip_code and zip_code in ZIP_TO_STATE_COUNTY:
        zip_state, zip_county = ZIP_TO_STATE_COUNTY[zip_code]
        if not state:
            state = zip_state
        county = zip_county

    # 2) County by "X County" pattern
    if "county" in tokens:
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

    # 3) Generic city detection: "<City> <ST>"
    # Works for things like "Berea OH", "Vancouver WA", "Strongsville Ohio"
    if state:
        state_token_positions = []
        state_lc = state.lower()
        full_state_name = None
        for name, code in US_STATES.items():
            if code.upper() == state:
                full_state_name = name
                break

        for i, t in enumerate(tokens):
            t_clean = t.strip(",.")
            if t_clean == state_lc or (full_state_name and t_clean == full_state_name):
                state_token_positions.append(i)

        if state_token_positions:
            idx = state_token_positions[0]
            j = idx - 1
            STOP = {"in", "of", full_state_name or "", state_lc}
            city_tokens_rev = []
            while j >= 0 and tokens[j] not in STOP:
                city_tokens_rev.append(tokens[j])
                j -= 1
            city_tokens = list(reversed(city_tokens_rev))
            if city_tokens and not city:
                city = " ".join(city_tokens)

    # 4) Ohio special-case: if we have a city and state == OH but no county → assume Cuyahoga
    if state == "OH" and city and not county:
        county = "cuyahoga"

    # 5) City → County mapping for things like Vancouver, WA
    if state:
        st_key = state.lower()
        city_map = CITY_TO_COUNTY.get(st_key, {})
        for city_name, (city_county, pretty_city) in city_map.items():
            if city_name in q:
                if not county:
                    county = city_county
                if not city:
                    city = pretty_city
                break

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
    Given state code (e.g., 'FL') and county name (e.g., 'Wakulla'),
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


def filter_features(features, income_min=None, value_min=None, zip_code=None, city_name=None):
    """
    Filter by income, value, optional ZIP, and optional city.

    - ZIP filter: exact match on postcode
    - City filter: exact match on normalized city string, if provided
    """
    if (
        income_min is None
        and value_min is None
        and not zip_code
        and not city_name
    ):
        return features

    income_keys = ["DP03_0062E", "median_income", "income", "household_income"]
    value_keys = ["DP04_0089E", "median_value", "home_value", "value"]

    city_norm = normalize_city(city_name) if city_name else None

    def read_val(props, keys):
        for k in keys:
            if k in props and props[k] not in ("", None):
                try:
                    return float(props[k])
                except Exception:
                    pass
        return None

    out = []
    for f in features:
        p = f.get("properties", {})
        keep = True

        # ZIP filter
        if zip_code is not None:
            pc = str(p.get("postcode") or "").strip()
            if pc != zip_code:
                keep = False

        # City filter (only if we have a city name and no explicit ZIP filter)
        if keep and city_norm and zip_code is None:
            feat_city_norm = normalize_city(p.get("city") or "")
            if feat_city_norm != city_norm:
                keep = False

        if keep and income_min is not None:
            v = read_val(p, income_keys)
            if v is None or v < income_min:
                keep = False

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

    city = props.get("city") or ""
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

        feats_all = gj.get("features", [])

        # First pass: apply city/ZIP/income/value filters
        feats_filtered = filter_features(
            feats_all,
            income_min=income_min,
            value_min=value_min,
            zip_code=zip_code,
            city_name=city,
        )

        message = None

        # If we asked for a city but got zero matches while the county has data,
        # fall back to county-wide results and tell the user.
        if city and zip_code is None and len(feats_filtered) == 0 and len(feats_all) > 0:
            app.logger.info(
                f"[search] city filter for {city} returned 0; falling back to county-level"
            )
            feats_filtered = filter_features(
                feats_all,
                income_min=income_min,
                value_min=value_min,
                zip_code=None,
                city_name=None,
            )
            message = f"No records matched city '{city}'. Showing county-wide results instead."

        total = len(feats_filtered)
        feats_limited = feats_filtered[:MAX_RESULTS]

        addresses = [feature_to_address_obj(f) for f in feats_limited]

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
                "message": message,
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

    feats_all = gj.get("features", [])

    feats_filtered = filter_features(
        feats_all,
        income_min=income_min,
        value_min=value_min,
        zip_code=zip_code,
        city_name=city,
    )

    # same fallback behavior as search
    if city and zip_code is None and len(feats_filtered) == 0 and len(feats_all) > 0:
        feats_filtered = filter_features(
            feats_all,
            income_min=income_min,
            value_min=value_min,
            zip_code=None,
            city_name=None,
        )

    rows = []
    fieldnames = set()

    for f in feats_filtered:
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

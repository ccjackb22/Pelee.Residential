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

# IMPORTANT: enriched ACS data
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")

MAX_RESULTS = 500  # how many addresses to show in UI

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# simple in-memory cache of S3 keys per state
_STATE_KEYS_CACHE = {}

# optional local index (for ZIP/city → state/county)
S3_INDEX = None
try:
    if os.path.exists("s3_address_index.json"):
        with open("s3_address_index.json", "r", encoding="utf-8") as f:
            S3_INDEX = json.load(f)
        app.logger.info("[INDEX] Loaded s3_address_index.json")
    else:
        app.logger.info("[INDEX] s3_address_index.json not found (ZIP/city lookup disabled)")
except Exception as e:
    app.logger.error(f"[INDEX] Failed to load s3_address_index.json: {e}")
    S3_INDEX = None

ZIP_RE = re.compile(r"\b\d{5}\b")


# ----------------- UTILITIES -----------------

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

STATE_CODES = {v: v for v in US_STATES.values()}


def normalize_text(s: str) -> str:
    return " ".join(s.strip().lower().split())


def canonicalize_county_name(name: str) -> str:
    """
    Make county names comparable:
    - lowercase
    - replace '_' and '-' with spaces
    - remove 'county' / 'parish'
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
    Detect 2-letter state codes or full names.

    FIX: ignore the preposition 'in' so we don't treat it as 'IN' (Indiana).
    """
    q = q.lower()
    tokens = q.split()

    # 1) look for 2-letter codes
    for t in tokens:
        t_clean = t.strip(",.").lower()
        if t_clean == "in":  # don't treat "in" as Indiana
            continue
        if t_clean in STATE_CODES:
            return STATE_CODES[t_clean].upper()

    # 2) look for full names
    for name, code in US_STATES.items():
        if name in q:
            return code.upper()

    return None


def guess_from_zip(zip_code: str):
    """
    Try to deduce (state, county) from s3_address_index.json.

    This is defensive: it supports a few possible shapes of the index
    and silently fails if it can't figure it out.
    """
    if not S3_INDEX:
        return None, None

    try:
        # Case 1: {"by_zip": {"44116": {...}}}
        if isinstance(S3_INDEX, dict) and "by_zip" in S3_INDEX:
            by_zip = S3_INDEX["by_zip"]
            if isinstance(by_zip, dict) and zip_code in by_zip:
                entry = by_zip[zip_code]
                state = (
                    entry.get("state")
                    or entry.get("state_code")
                    or entry.get("STATE")
                    or entry.get("st")
                )
                county = (
                    entry.get("county")
                    or entry.get("county_name")
                    or entry.get("COUNTY")
                )
                return state, county

        # Case 2: top-level zip -> entry
        if isinstance(S3_INDEX, dict) and zip_code in S3_INDEX:
            entry = S3_INDEX[zip_code]
            state = (
                entry.get("state")
                or entry.get("state_code")
                or entry.get("STATE")
                or entry.get("st")
            )
            county = (
                entry.get("county")
                or entry.get("county_name")
                or entry.get("COUNTY")
            )
            return state, county

        # Case 3: list of entries
        if isinstance(S3_INDEX, list):
            for entry in S3_INDEX:
                z = str(
                    entry.get("zip")
                    or entry.get("ZIP")
                    or entry.get("zipcode")
                    or entry.get("postal_code")
                    or ""
                )
                if z == zip_code:
                    state = (
                        entry.get("state")
                        or entry.get("state_code")
                        or entry.get("STATE")
                        or entry.get("st")
                    )
                    county = (
                        entry.get("county")
                        or entry.get("county_name")
                        or entry.get("COUNTY")
                    )
                    return state, county
    except Exception as e:
        app.logger.warning(f"[ZIP] lookup failed for {zip_code}: {e}")

    return None, None


def parse_location(query: str):
    """
    Parse state + county + city from a free-form query.

    Priority:
    - if 'county' present → extract county tokens before 'county'
    - else, handle special OH city-only queries (default Cuyahoga)
    - else, try ZIP fallback via s3_address_index.json (if present)
    """
    q = normalize_text(query)
    tokens = q.split()

    state = detect_state(q)
    county = None
    city = None

    # county pattern "... X Y county STATE ..."
    if "county" in tokens:
        idxs = [i for i, t in enumerate(tokens) if t == "county"]
        idx = idxs[0]
        j = idx - 1
        county_tokens_rev = []
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
            "zip",
        }
        while j >= 0:
            t = tokens[j]
            if t in STOP:
                break
            # don't cross explicit state mentions
            if t.upper() in STATE_CODES or t in US_STATES:
                break
            county_tokens_rev.append(t)
            j -= 1
        county_tokens = list(reversed(county_tokens_rev))
        if county_tokens:
            county = " ".join(county_tokens)

    # Ohio city-only queries → default Cuyahoga
    if not county and state == "OH":
        state_idx = None
        for i, t in enumerate(tokens):
            if (t.lower() in STATE_CODES) or (t in US_STATES):
                state_idx = i
                break
        if state_idx is not None and state_idx > 0:
            j = state_idx - 1
            city_tokens_rev = []
            STOP = {"in", "ohio", "oh"}
            while j >= 0 and tokens[j] not in STOP:
                city_tokens_rev.append(tokens[j])
                j -= 1
            city_tokens = list(reversed(city_tokens_rev))
            if city_tokens:
                city = " ".join(city_tokens)
        county = "cuyahoga"

    # ZIP-only / ZIP-first queries: try to infer from index
    if not state:
        m = ZIP_RE.search(q)
        if m:
            zip_code = m.group(0)
            z_state, z_county = guess_from_zip(zip_code)
            if z_state:
                state = z_state.upper()
            if z_county and not county:
                county = z_county

    return state, county, city


def parse_numeric_filters(query: str):
    """
    Extract simple 'over X' / 'above X' for incomes or values.
    """
    q = query.lower()
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
            elif "value" in window or "home" in window or "homes" in window or "properties" in window:
                value_min = num
            else:
                if income_min is None:
                    income_min = num

    return income_min, value_min


def list_state_keys(state_code: str):
    """
    Cache + return all keys under S3_PREFIX/{state_code}/
    """
    state_code = state_code.lower()
    if state_code in _STATE_KEYS_CACHE:
        return _STATE_KEYS_CACHE[state_code]

    prefix = f"{S3_PREFIX}/{state_code}/"
    keys = []
    continuation = None

    while True:
        try:
            if continuation:
                resp = s3_client.list_objects_v2(
                    Bucket=S3_BUCKET,
                    Prefix=prefix,
                    ContinuationToken=continuation,
                )
            else:
                resp = s3_client.list_objects_v2(
                    Bucket=S3_BUCKET,
                    Prefix=prefix,
                )
        except ClientError as e:
            app.logger.error(f"[S3] list_objects_v2 failed for state={state_code}: {e}")
            break

        contents = resp.get("Contents", [])
        for obj in contents:
            key = obj.get("Key")
            if key and key.endswith(".geojson"):
                keys.append(key)

        if resp.get("IsTruncated"):
            continuation = resp.get("NextContinuationToken")
        else:
            break

    _STATE_KEYS_CACHE[state_code] = keys
    app.logger.info(f"[S3] Cached {len(keys)} keys for state={state_code}")
    return keys


def resolve_dataset_key(state: str, county: str):
    """
    Given a state code (e.g. 'FL') and county name (e.g. 'Palm Beach'),
    pick the best matching S3 key.

    Preference order:
    1) exact county match + '-with-values-income'
    2) exact county match + '.geojson'
    3) fuzzy (starts/contains) with '-with-values-income'
    4) fuzzy with '.geojson'
    """
    if not state or not county:
        return None

    state_code = state.lower()
    county_clean = canonicalize_county_name(county)

    keys = list_state_keys(state_code)
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
        base = fname[:-len(".geojson")]  # strip .geojson
        has_values = "with-values-income" in base
        base_no_suffix = base.replace("-with-values-income", "")
        base_canon = canonicalize_county_name(base_no_suffix)

        if not base_canon:
            continue

        if base_canon == county_clean:
            if has_values:
                enriched_exact.append(key)
            else:
                raw_exact.append(key)
        elif base_canon.startswith(county_clean) or county_clean.startswith(base_canon):
            if has_values:
                enriched_fuzzy.append(key)
            else:
                raw_fuzzy.append(key)

    for bucket in (enriched_exact, raw_exact, enriched_fuzzy, raw_fuzzy):
        if bucket:
            return sorted(bucket, key=len)[0]

    return None


def load_geojson_from_s3(key: str):
    """
    Simplified: read full object. For your county-level files this is fine
    and avoids the streaming weirdness that was hitting worker timeouts.
    """
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        body = resp["Body"].read()
        return json.loads(body)
    except Exception as e:
        app.logger.error(f"[S3] get_object failed for {key}: {e}")
        raise


def filter_features(features, income_min=None, value_min=None):
    """
    Filter features by optional 'median_income' / 'median_value' fields (or similar).
    We'll look for several possible property names.
    """
    if income_min is None and value_min is None:
        return features

    income_keys = ["DP03_0062E", "median_income", "income", "household_income"]
    value_keys = ["DP04_0089E", "median_value", "home_value", "value"]

    def get_first(props, keys):
        for k in keys:
            if k in props and props[k] not in (None, ""):
                try:
                    return float(props[k])
                except (TypeError, ValueError):
                    continue
        return None

    filtered = []
    for feat in features:
        props = feat.get("properties", {})
        keep = True

        if income_min is not None:
            inc = get_first(props, income_keys)
            if inc is None or inc < income_min:
                keep = False

        if keep and value_min is not None:
            val = get_first(props, value_keys)
            if val is None or val < value_min:
                keep = False

        if keep:
            filtered.append(feat)

    return filtered


def feature_to_address_obj(feat):
    props = feat.get("properties", {})
    geom = feat.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None]

    number = props.get("number") or props.get("house_number") or ""
    street = props.get("street") or props.get("road") or ""
    unit = props.get("unit") or ""

    city = props.get("city") or ""
    region = props.get("region") or props.get("STUSPS") or ""
    postcode = props.get("postcode") or ""

    income = props.get("DP03_0062E") or props.get("median_income")
    value = props.get("DP04_0089E") or props.get("median_value")

    address_line = " ".join(x for x in [str(number), street, unit] if x)

    return {
        "address": address_line,
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
    env_ok = bool(AWS_REGION) and bool(S3_BUCKET)
    return jsonify({"ok": True, "env_s3": env_ok})


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


def _get_query_from_request():
    """
    Support both form POST and JSON POST without blowing up.
    """
    if request.is_json:
        data = request.get_json(silent=True) or {}
        return (data.get("query") or "").strip()
    return (request.form.get("query") or "").strip()


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    try:
        query = _get_query_from_request()
        app.logger.debug(f"/search query={query!r}")

        if not query:
            return jsonify({"ok": False, "error": "Please enter a query."})

        state, county, city = parse_location(query)
        income_min, value_min = parse_numeric_filters(query)

        app.logger.debug(f"parse_location → state={state}, county={county}, city={city}")
        app.logger.debug(f"parse_numeric_filters → income_min={income_min}, value_min={value_min}")

        if not state:
            return jsonify({"ok": False, "error": "No state detected."})

        if not county:
            return jsonify({"ok": False, "error": "No county detected."})

        key = resolve_dataset_key(state, county)
        app.logger.debug(f"resolve_dataset_key → key={key}")

        if not key:
            return jsonify(
                {
                    "ok": False,
                    "error": f"No dataset found for {county} County, {state}.",
                }
            )

        gj = load_geojson_from_s3(key)
        features = gj.get("features", [])
        app.logger.debug(f"Loaded {len(features)} features from {key}")

        features = filter_features(features, income_min=income_min, value_min=value_min)
        total = len(features)
        features = features[:MAX_RESULTS]

        results = [feature_to_address_obj(f) for f in features]

        return jsonify(
            {
                "ok": True,
                "query": query,
                "state": state,
                "county": county,
                "dataset_key": key,
                "total": total,
                "shown": len(results),
                "results": results,
            }
        )
    except Exception as e:
        app.logger.exception(f"/search internal error: {e}")
        # IMPORTANT: return 200 with ok=False so the frontend does NOT show a raw 500 page
        return jsonify({"ok": False, "error": "Internal error while processing your query."})


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    try:
        query = _get_query_from_request()
        if not query:
            return jsonify({"ok": False, "error": "Missing query for export."})

        state, county, _ = parse_location(query)
        income_min, value_min = parse_numeric_filters(query)

        if not state or not county:
            return jsonify({"ok": False, "error": "Need a recognisable state and county to export."})

        key = resolve_dataset_key(state, county)
        if not key:
            return jsonify(
                {
                    "ok": False,
                    "error": f"No dataset found for export for {county} County, {state}.",
                }
            )

        gj = load_geojson_from_s3(key)
        features = gj.get("features", [])
        features = filter_features(features, income_min=income_min, value_min=value_min)

        rows = []
        fieldnames = set()

        for feat in features:
            props = feat.get("properties", {}).copy()
            geom = feat.get("geometry") or {}
            coords = geom.get("coordinates") or [None, None]
            props["lon"] = coords[0]
            props["lat"] = coords[1]
            fieldnames.update(props.keys())
            rows.append(props)

        fieldnames = sorted(fieldnames)

        memfile = io.BytesIO()
        with zipfile.ZipFile(memfile, "w", zipfile.ZIP_DEFLATED) as zf:
            csv_buf = io.StringIO()
            writer = csv.DictWriter(csv_buf, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
            zf.writestr("addresses.csv", csv_buf.getvalue())

        memfile.seek(0)
        filename = f"pelee_export_{state}_{canonicalize_county_name(county).replace(' ', '_')}.zip"

        return send_file(
            memfile,
            mimetype="application/zip",
            as_attachment=True,
            download_name=filename,
        )
    except Exception as e:
        app.logger.exception(f"/export internal error: {e}")
        return jsonify({"ok": False, "error": "Internal error while preparing export."})


# ----------------- MAIN -----------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

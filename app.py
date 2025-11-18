import os
import io
import json
import csv
import zipfile

from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    send_file,
)
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ----------------- CONFIG -----------------

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-2"))
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")

MAX_RESULTS = 500  # how many rows to show in UI

app = Flask(__name__)

s3_client = boto3.client("s3", region_name=AWS_REGION)
_STATE_KEYS_CACHE = {}

# ----------------- STATE / COUNTY HELPERS -----------------

US_STATES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct", "delaware": "de",
    "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id",
    "illinois": "il", "indiana": "in", "iowa": "ia", "kansas": "ks",
    "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md",
    "massachusetts": "ma", "michigan": "mi", "minnesota": "mn", "mississippi": "ms",
    "missouri": "mo", "montana": "mt", "nebraska": "ne", "nevada": "nv",
    "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm", "new york": "ny",
    "north carolina": "nc", "north dakota": "nd", "ohio": "oh", "oklahoma": "ok",
    "oregon": "or", "pennsylvania": "pa", "rhode island": "ri", "south carolina": "sc",
    "south dakota": "sd", "tennessee": "tn", "texas": "tx", "utah": "ut",
    "vermont": "vt", "virginia": "va", "washington": "wa", "west virginia": "wv",
    "wisconsin": "wi", "wyoming": "wy",
}

STATE_CODES = {v: v for v in US_STATES.values()}


def normalize_text(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def canonicalize_county_name(name: str) -> str:
    if not name:
        return ""
    s = name.lower().replace("_", " ").replace("-", " ")
    for word in [" county", " parish"]:
        if word in s:
            s = s.replace(word, "")
    return " ".join(s.split())


def detect_state(q: str):
    q = (q or "").lower()
    tokens = q.split()

    # 2-letter codes, but ignore "in" → Indiana problem
    for t in tokens:
        t_clean = t.strip(",.").lower()
        if t_clean == "in":
            continue
        if t_clean in STATE_CODES:
            return t_clean.upper()

    # full names
    for name, code in US_STATES.items():
        if name in q:
            return code.upper()

    return None


def parse_location(query: str):
    """
    Extract state + county (and optional city).
    """
    q = normalize_text(query)
    tokens = q.split()

    state = detect_state(q)
    county = None
    city = None

    # pattern: "... <tokens> county <state>"
    if "county" in tokens:
        idx = tokens.index("county")
        j = idx - 1
        STOP = {
            "in", "of", "all", "any",
            "properties", "homes", "households", "residential", "addresses", "parcels",
        }
        county_rev = []
        while j >= 0:
            t = tokens[j]
            if t in STOP:
                break
            county_rev.append(t)
            j -= 1
        county_tokens = list(reversed(county_rev))
        if county_tokens:
            county = " ".join(county_tokens)

    # OH fallback: "Strongsville OH" → default to Cuyahoga
    if not county and state == "OH":
        state_idx = None
        for i, t in enumerate(tokens):
            if t.lower() in STATE_CODES or t in US_STATES:
                state_idx = i
                break
        if state_idx is not None and state_idx > 0:
            j = state_idx - 1
            STOP = {"in", "ohio", "oh"}
            city_rev = []
            while j >= 0 and tokens[j] not in STOP:
                city_rev.append(tokens[j])
                j -= 1
            city_tokens = list(reversed(city_rev))
            if city_tokens:
                city = " ".join(city_tokens)
        county = "cuyahoga"

    return state, county, city


def parse_numeric_filters(query: str):
    """
    Very simple parser:
    - "... over 200k income"
    - "... above 800000 home value"
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
            elif "value" in window or "home" in window or "homes" in window or "properties" in window:
                value_min = num
            else:
                if income_min is None:
                    income_min = num

    return income_min, value_min


# ----------------- S3 HELPERS -----------------


def list_state_keys(state: str):
    state = (state or "").lower()
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
            k = obj.get("Key")
            if k and k.endswith(".geojson"):
                keys.append(k)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    _STATE_KEYS_CACHE[state] = keys
    app.logger.info(f"[S3] Cached {len(keys)} keys for state={state}")
    return keys


def resolve_dataset_key(state: str, county: str):
    """
    Choose the best matching GeoJSON key for a given state+county.
    Preference:
      1) exact match + with-values-income
      2) exact match
      3) fuzzy + with-values-income
      4) fuzzy
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
    Stream the S3 object to avoid timeouts on large county files.
    """
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        stream = resp["Body"]
        chunks = []
        for chunk in iter(lambda: stream.read(2 * 1024 * 1024), b""):
            chunks.append(chunk)
        data = b"".join(chunks)
        return json.loads(data)
    except Exception as e:
        app.logger.error(f"[S3] load failed for {key}: {e}")
        raise


# ----------------- FILTERING & SHAPING -----------------


def filter_features(features, income_min=None, value_min=None):
    """
    Filter by ACS-like numeric fields if thresholds are present.
    """
    if income_min is None and value_min is None:
        return features

    income_keys = ["DP03_0062E", "median_income", "income", "household_income"]
    value_keys = ["DP04_0089E", "median_value", "home_value", "value"]

    def read_val(props, keys):
        for k in keys:
            if k in props and props[k] not in ("", None):
                try:
                    return float(props[k])
                except (TypeError, ValueError):
                    continue
        return None

    out = []
    for f in features:
        props = f.get("properties", {}) or {}
        keep = True

        if income_min is not None:
            v = read_val(props, income_keys)
            if v is None or v < income_min:
                keep = False

        if keep and value_min is not None:
            v = read_val(props, value_keys)
            if v is None or v < value_min:
                keep = False

        if keep:
            out.append(f)

    return out


def feature_to_address_obj(feat):
    """
    Convert a GeoJSON feature into a structured address row for the table.
    """
    props = feat.get("properties", {}) or {}
    geom = feat.get("geometry", {}) or {}
    coords = geom.get("coordinates") or [None, None]

    number = props.get("number") or props.get("house_number") or ""
    street = props.get("street") or props.get("road") or ""
    unit = props.get("unit") or ""

    city = props.get("city") or ""
    region = props.get("region") or props.get("STUSPS") or ""
    postcode = props.get("postcode") or ""

    income = props.get("DP03_0062E") or props.get("median_income")
    value = props.get("DP04_0089E") or props.get("median_value")

    address = " ".join(x for x in [str(number), street, unit] if x)

    lat = None
    lon = None
    if isinstance(coords, (list, tuple)) and len(coords) >= 2:
        lon, lat = coords[0], coords[1]

    return {
        "address": address or "(no address)",
        "city": city,
        "state": region,
        "zip": postcode,
        "income": income,
        "value": value,
        "lat": lat,
        "lon": lon,
    }


# ----------------- ROUTES -----------------


@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search", methods=["POST"])
def search():
    # accept both JSON and form-encoded
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        query = (payload.get("query") or "").strip()
    else:
        query = (request.form.get("query") or "").strip()

    if not query:
        return jsonify({"error": "Enter a query."}), 400

    state, county, city = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    app.logger.info(f"[search] query={query!r}, state={state}, county={county}, city={city}, "
                    f"income_min={income_min}, value_min={value_min}")

    if not state:
        return jsonify({"error": "No state detected in your query."}), 400
    if not county:
        return jsonify({"error": "No county detected in your query."}), 400

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset found for {county} County, {state}."}), 404

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify({"error": f"Failed loading {key}: {str(e)}"}), 500

    feats = gj.get("features", []) or []
    feats = filter_features(feats, income_min, value_min)

    total = len(feats)
    feats = feats[:MAX_RESULTS]
    rows = [feature_to_address_obj(f) for f in feats]

    return jsonify(
        {
            "ok": True,
            "query": query,
            "state": state,
            "county": county,
            "dataset_key": key,
            "total": total,
            "shown": len(rows),
            "results": rows,
        }
    )


@app.route("/export", methods=["POST"])
def export():
    # accept both JSON and form-encoded
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        query = (payload.get("query") or "").strip()
    else:
        query = (request.form.get("query") or "").strip()

    if not query:
        return jsonify({"error": "Missing query for export."}), 400

    state, county, _ = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state or not county:
        return jsonify({"error": "Need a recognizable state and county to export."}), 400

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset found for {county} County, {state}."}), 404

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify({"error": f"Failed to load dataset for export: {str(e)}"}), 500

    feats = gj.get("features", []) or []
    feats = filter_features(feats, income_min, value_min)

    # Flatten to CSV
    rows = []
    fieldnames = set()
    for f in feats:
        props = f.get("properties", {}).copy()
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates") or [None, None]
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            props["lon"] = coords[0]
            props["lat"] = coords[1]
        rows.append(props)
        fieldnames.update(props.keys())

    fieldnames = sorted(fieldnames)

    memfile = io.BytesIO()
    with zipfile.ZipFile(memfile, "w", zipfile.ZIP_DEFLATED) as zf:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
        zf.writestr("addresses.csv", buf.getvalue())

    memfile.seek(0)
    filename = f"pelee_export_{state}_{canonicalize_county_name(county).replace(' ', '_')}.zip"

    return send_file(
        memfile,
        mimetype="application/zip",
        as_attachment=True,
        download_name=filename,
    )


# ----------------- MAIN -----------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

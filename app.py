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
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")

MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

_STATE_KEYS_CACHE = {}

# ----------------- LOOKUP HINTS -----------------

US_STATES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar", "california": "ca",
    "colorado": "co", "connecticut": "ct", "delaware": "de", "florida": "fl", "georgia": "ga",
    "hawaii": "hi", "idaho": "id", "illinois": "il", "indiana": "in", "iowa": "ia",
    "kansas": "ks", "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md",
    "massachusetts": "ma", "michigan": "mi", "minnesota": "mn", "mississippi": "ms", "missouri": "mo",
    "montana": "mt", "nebraska": "ne", "nevada": "nv", "new hampshire": "nh", "new jersey": "nj",
    "new mexico": "nm", "new york": "ny", "north carolina": "nc", "north dakota": "nd", "ohio": "oh",
    "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa", "rhode island": "ri", "south carolina": "sc",
    "south dakota": "sd", "tennessee": "tn", "texas": "tx", "utah": "ut", "vermont": "vt",
    "virginia": "va", "washington": "wa", "west virginia": "wv", "wisconsin": "wi", "wyoming": "wy",
}

STATE_CODES = {v: v for v in US_STATES.values()}

# Specific city → county hints for the flows you actually care about
CITY_TO_COUNTY_HINT = {
    ("OH", "strongsville"): "cuyahoga",
    ("OH", "berea"): "cuyahoga",
    ("OH", "rocky river"): "cuyahoga",
    ("FL", "crawfordville"): "wakulla",
    ("FL", "panacea"): "wakulla",
    ("FL", "sopchoppy"): "wakulla",
    ("WA", "vancouver"): "clark",
}

# Specific ZIP → (state, county) hints for common examples
ZIP_HINTS = {
    "44116": ("OH", "cuyahoga"),
    "44136": ("OH", "cuyahoga"),
    "44130": ("OH", "cuyahoga"),
    "32327": ("FL", "wakulla"),
    "32346": ("FL", "wakulla"),
    "32358": ("FL", "wakulla"),
}


def normalize_text(s: str) -> str:
    return " ".join(s.strip().lower().split())


def canonicalize_county_name(name: str) -> str:
    if not name:
        return ""
    s = name.lower().replace("_", " ").replace("-", " ")
    for word in [" county", " parish"]:
        if word in s:
            s = s.replace(word, "")
    return " ".join(s.split())


def detect_state(q: str):
    q = q.lower()
    tokens = q.split()

    # 2-letter codes
    for t in tokens:
        t_clean = t.strip(",.").lower()
        if t_clean == "in":  # don't treat "in" as Indiana
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
    Returns (state, county, city, zipcode_or_none).

    Priority:
      1) explicit 'X County, ST'
      2) ZIP-based hints
      3) city + state → CITY_TO_COUNTY_HINT
      4) OH fallback: any 'city ohio' → cuyahoga
    """
    q = normalize_text(query)
    tokens = q.split()

    # find any 5-digit thing that looks like ZIP
    zipcode = None
    for t in tokens:
        if len(t) == 5 and t.isdigit():
            zipcode = t
            break

    state = detect_state(q)
    county = None
    city = None

    # --- 1) explicit county ---
    if "county" in tokens:
        idx = tokens.index("county")
        j = idx - 1
        STOP = {"in", "of", "all", "any", "properties", "homes", "households", "residential", "addresses", "parcels"}
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

    # --- 2) ZIP-based hints (if we still don't have county) ---
    if not county and zipcode and zipcode in ZIP_HINTS:
        hint_state, hint_county = ZIP_HINTS[zipcode]
        if not state:
            state = hint_state
        county = hint_county

    # --- 3) city + state → hints ---
    if not county and state:
        # attempt to read "in CITY STATE" or "CITY, STATE"
        state_idx = None
        for i, t in enumerate(tokens):
            if t.lower() in STATE_CODES or t in US_STATES:
                state_idx = i
                break

        if state_idx is not None and state_idx > 0:
            # everything before state, after 'in' or start, is city chunk
            j = state_idx - 1
            city_tokens_rev = []
            STOP = {"in", "city", "zip", "addresses", "homes", "properties", "county"}
            while j >= 0 and tokens[j] not in STOP:
                city_tokens_rev.append(tokens[j])
                j -= 1
            city_tokens = list(reversed(city_tokens_rev))
            if city_tokens:
                city = " ".join(city_tokens)
                key = (state, city.lower())
                if key in CITY_TO_COUNTY_HINT:
                    county = CITY_TO_COUNTY_HINT[key]

    # --- 4) OH fallback: city-only queries default to Cuyahoga ---
    if not county and state == "OH":
        county = "cuyahoga"

    return state, county, city, zipcode


def parse_numeric_filters(query: str):
    q = query.lower()
    income_min = None
    value_min = None

    tokens = q.replace("$", "").replace(",", "").split()
    for i, t in enumerate(tokens):
        if t in {"over", "above"} and i + 1 < len(tokens):
            raw = tokens[i + 1].strip(".,)")
            mult = 1.0
            if raw.endswith("k"):
                mult = 1_000
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


def list_state_keys(state: str):
    state = state.lower()
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
            app.logger.error(f"[S3] list_objects_v2 failed for {state}: {e}")
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
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        stream = resp["Body"]
        chunks = []
        for chunk in iter(lambda: stream.read(2 * 1024 * 1024), b""):
            chunks.append(chunk)
        return json.loads(b"".join(chunks))
    except Exception as e:
        app.logger.error(f"[S3] load failed for {key}: {e}")
        raise


def filter_by_income_value(features, income_min=None, value_min=None):
    if income_min is None and value_min is None:
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
    for f in features:
        p = f.get("properties", {})
        keep = True

        if income_min is not None:
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


def filter_by_city_zip(features, city=None, zipcode=None):
    if not city and not zipcode:
        return features

    city_norm = city.lower() if city else None
    out = []

    for f in features:
        p = f.get("properties", {})
        if zipcode:
            pc = str(p.get("postcode") or "").strip()
            if pc != zipcode:
                continue
        if city_norm:
            c = str(p.get("city") or "").strip().lower()
            if c != city_norm:
                continue
        out.append(f)

    return out


def feature_to_address_obj(feat):
    props = feat.get("properties", {})
    geom = feat.get("geometry", {})
    coords = geom.get("coordinates") or [None, None]

    number = props.get("number") or props.get("house_number") or ""
    street = props.get("street") or props.get("road") or ""
    unit = props.get("unit") or ""

    city = props.get("city") or ""
    region = props.get("region") or props.get("STUSPS") or ""
    postcode = props.get("postcode") or ""

    income = props.get("DP03_0062E") or props.get("median_income")
    value = props.get("DP04_0089E") or props.get("median_value")

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


def _get_query_text():
    if request.is_json:
        data = request.get_json(silent=True) or {}
        return (data.get("query") or "").strip()
    # form-encoded
    return (request.form.get("query") or "").strip()


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"error": "Unauthorized"}), 401

    query = _get_query_text()
    if not query:
        return jsonify({"error": "Enter a query."})

    state, county, city, zipcode = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state:
        return jsonify({"error": "I couldn't detect a state in your query. Try adding 'Ohio', 'FL', etc."})
    if not county:
        pieces = [f"state={state}"] if state else []
        if city:
            pieces.append(f"city={city}")
        if zipcode:
            pieces.append(f"zip={zipcode}")
        detail = ", ".join(pieces) or "nothing parsed"
        return jsonify({
            "error": (
                "Could not determine a county from your query "
                f"(parsed: {detail}). Try 'Wakulla County FL' or 'Clark County WA'."
            )
        })

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset found for {county} County, {state}."})

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify({"error": f"Failed loading {key}: {str(e)}"})

    feats = gj.get("features", [])

    # apply filters
    feats = filter_by_income_value(feats, income_min, value_min)
    feats = filter_by_city_zip(feats, city=city, zipcode=zipcode)

    total = len(feats)
    feats = feats[:MAX_RESULTS]
    results = [feature_to_address_obj(f) for f in feats]

    if total == 0:
        msg = "No matching addresses found for that filter. "
        msg += "Try lowering the income/value threshold" if (income_min or value_min) else "Try adjusting the city/county/ZIP."
        return jsonify({
            "ok": True,
            "query": query,
            "state": state,
            "county": county,
            "dataset_key": key,
            "total": 0,
            "shown": 0,
            "results": [],
            "message": msg,
        })

    return jsonify({
        "ok": True,
        "query": query,
        "state": state,
        "county": county,
        "dataset_key": key,
        "total": total,
        "shown": len(results),
        "results": results,
    })


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"error": "Unauthorized"}), 401

    query = _get_query_text()
    if not query:
        return jsonify({"error": "Missing query for export."}), 400

    state, county, city, zipcode = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state or not county:
        return jsonify({"error": "Need a recognisable state and county to export."}), 400

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset found for export for {county} County, {state}."}), 400

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify({"error": f"Failed to load dataset for export: {str(e)}"}), 500

    feats = gj.get("features", [])
    feats = filter_by_income_value(feats, income_min, value_min)
    feats = filter_by_city_zip(feats, city=city, zipcode=zipcode)

    rows = []
    fieldnames = set()

    for f in feats:
        props = f.get("properties", {}).copy()
        geom = f.get("geometry", {})
        coords = geom.get("coordinates") or [None, None]
        props["lon"] = coords[0]
        props["lat"] = coords[1]
        rows.append(props)
        fieldnames.update(props.keys())

    fieldnames = sorted(fieldnames)

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
        zf.writestr("addresses.csv", buf.getvalue())

    mem.seek(0)
    fname = f"pelee_export_{state}_{canonicalize_county_name(county).replace(' ', '_')}.zip"

    return send_file(mem, mimetype="application/zip", as_attachment=True, download_name=fname)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

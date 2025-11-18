import os
import io
import json
import csv
import zipfile
from functools import lru_cache

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

# IMPORTANT: point to enriched ACS data
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")

MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

_STATE_KEYS_CACHE = {}


# ----------------- UTILITIES -----------------

US_STATES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct", "delaware": "de",
    "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id",
    "illinois": "il", "indiana": "in", "iowa": "ia", "kansas": "ks",
    "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md",
    "massachusetts": "ma", "michigan": "mi", "minnesota": "mn",
    "mississippi": "ms", "missouri": "mo", "montana": "mt", "nebraska": "ne",
    "nevada": "nv", "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm",
    "new york": "ny", "north carolina": "nc", "north dakota": "nd", "ohio": "oh",
    "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa", "rhode island": "ri",
    "south carolina": "sc", "south dakota": "sd", "tennessee": "tn", "texas": "tx",
    "utah": "ut", "vermont": "vt", "virginia": "va", "washington": "wa",
    "west virginia": "wv", "wisconsin": "wi", "wyoming": "wy",
}

STATE_CODES = {v: v for v in US_STATES.values()}


def normalize_text(s: str) -> str:
    return " ".join(s.strip().lower().split())


def canonicalize_county_name(name: str) -> str:
    if not name:
        return ""
    s = name.lower().replace("_", " ").replace("-", " ")
    for word in [" county", " parish"]:
        s = s.replace(word, "")
    return " ".join(s.split())


def detect_state(query: str):
    q = query.lower()
    tokens = q.split()

    # 2-letter codes, but ignore the word "in"
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
    q = normalize_text(query)
    tokens = q.split()

    state = detect_state(q)
    county = None
    city = None

    if "county" in tokens:
        idx = tokens.index("county")
        j = idx - 1
        elems = []
        STOP = {"in", "of", "all", "any", "homes", "households", "residential", "addresses"}
        while j >= 0:
            t = tokens[j]
            if t in STOP:
                break
            if t.upper() in STATE_CODES:
                break
            elems.append(t)
            j -= 1
        elems.reverse()
        if elems:
            county = " ".join(elems)

    # If city-only in Ohio → default to Cuyahoga
    if not county and state == "OH":
        county = "cuyahoga"

    return state, county, city


def parse_numeric_filters(query: str):
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
            except:
                continue

            window = " ".join(tokens[max(0, i - 3): i + 6])
            if "income" in window:
                income_min = num
            elif "value" in window or "home" in window:
                value_min = num
            else:
                if income_min is None:
                    income_min = num

    return income_min, value_min


def list_state_keys(state_code: str):
    state_code = state_code.lower()
    if state_code in _STATE_KEYS_CACHE:
        return _STATE_KEYS_CACHE[state_code]

    prefix = f"{S3_PREFIX}/{state_code}/"
    keys = []
    token = None

    while True:
        args = dict(Bucket=S3_BUCKET, Prefix=prefix)
        if token:
            args["ContinuationToken"] = token

        try:
            resp = s3_client.list_objects_v2(**args)
        except ClientError as e:
            app.logger.error(f"[S3] list failed for {state_code}: {e}")
            break

        for o in resp.get("Contents", []):
            k = o.get("Key")
            if k and k.endswith(".geojson"):
                keys.append(k)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    _STATE_KEYS_CACHE[state_code] = keys
    return keys


def resolve_dataset_key(state: str, county: str):
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
        base = fname[:-8]  # strip .geojson
        enriched = "with-values-income" in base
        base_clean = canonicalize_county_name(base.replace("-with-values-income", ""))

        if base_clean == county_clean:
            if enriched: enriched_exact.append(key)
            else: raw_exact.append(key)
        elif base_clean.startswith(county_clean) or county_clean.startswith(base_clean):
            if enriched: enriched_fuzzy.append(key)
            else: raw_fuzzy.append(key)

    for bucket in (enriched_exact, raw_exact, enriched_fuzzy, raw_fuzzy):
        if bucket:
            return sorted(bucket, key=len)[0]

    return None


# ----------------- FIXED FUNCTION (NO TIMEOUTS) -----------------

def load_geojson_from_s3(key: str):
    """
    Fast one-shot loader to avoid Render worker timeouts.
    """
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        body = resp["Body"].read()

        # Safety limit
        if len(body) > 500_000_000:
            raise Exception(f"File too large: {key}")

        return json.loads(body)

    except Exception as e:
        app.logger.error(f"[S3] load_geojson_from_s3 FAILED for {key}: {e}")
        raise


def filter_features(features, income_min=None, value_min=None):
    if income_min is None and value_min is None:
        return features

    income_keys = ["DP03_0062E", "median_income", "income"]
    value_keys = ["DP04_0089E", "median_value", "value"]

    def first(props, keys):
        for k in keys:
            if k in props:
                try: return float(props[k])
                except: pass
        return None

    out = []
    for f in features:
        p = f.get("properties", {})
        keep = True

        if income_min is not None:
            inc = first(p, income_keys)
            if inc is None or inc < income_min:
                keep = False

        if keep and value_min is not None:
            val = first(p, value_keys)
            if val is None or val < value_min:
                keep = False

        if keep:
            out.append(f)

    return out


def feature_to_address_line(f):
    p = f.get("properties", {})
    number = p.get("number") or ""
    street = p.get("street") or ""
    unit = p.get("unit") or ""
    city = p.get("city") or ""
    region = p.get("region") or p.get("STUSPS") or ""
    postcode = p.get("postcode") or ""

    line1 = " ".join(x for x in [str(number), street, unit] if x)
    loc = ", ".join(x for x in [city, region, postcode] if x)

    if line1 and loc:
        return line1 + "\n" + loc
    return line1 or loc or "(no address)"


# ----------------- ROUTES -----------------

@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
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
        return jsonify({"error": "Unauthorized"}), 401

    if request.is_json:
        query = request.json.get("query", "")
    else:
        query = request.form.get("query", "")
    query = query.strip()

    if not query:
        return jsonify({"error": "Empty query."})

    state, county, city = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state:
        return jsonify({"error": "Could not detect a state."})
    if not county:
        return jsonify({"error": "Could not detect a county."})

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset found for {county} County, {state}."})

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify({"error": f"Failed to load {key}: {e}"}), 500

    feats = gj.get("features", [])
    feats = filter_features(feats, income_min, value_min)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    addrs = [feature_to_address_line(f) for f in feats]

    return jsonify({
        "ok": True,
        "query": query,
        "state": state,
        "county": county,
        "dataset_key": key,
        "total": total,
        "shown": len(addrs),
        "results": addrs,
    })


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"error": "Unauthorized"}), 401

    if request.is_json:
        query = request.json.get("query", "")
    else:
        query = request.form.get("query", "")
    query = query.strip()

    if not query:
        return jsonify({"error": "Missing query."})

    state, county, city = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state or not county:
        return jsonify({"error": "Need state + county."}), 400

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset for {county} County, {state}."}), 400

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify({"error": f"Failed to load dataset: {e}"}), 500

    feats = filter_features(gj.get("features", []), income_min, value_min)

    rows = []
    fieldnames = set()

    for f in feats:
        p = f.get("properties", {}).copy()
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            p["lon"] = coords[0]
            p["lat"] = coords[1]
        fieldnames.update(p.keys())
        rows.append(p)

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
    name = f"pelee_export_{state}_{canonicalize_county_name(county)}.zip"

    return send_file(
        mem,
        mimetype="application/zip",
        as_attachment=True,
        download_name=name,
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

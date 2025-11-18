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

S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

_STATE_KEYS_CACHE = {}

# ----------------- UTILITIES -----------------

US_STATES = {
    "alabama": "al","alaska": "ak","arizona": "az","arkansas": "ar","california": "ca",
    "colorado": "co","connecticut": "ct","delaware": "de","florida": "fl","georgia": "ga",
    "hawaii": "hi","idaho": "id","illinois": "il","indiana": "in","iowa": "ia",
    "kansas": "ks","kentucky": "ky","louisiana": "la","maine": "me","maryland": "md",
    "massachusetts": "ma","michigan": "mi","minnesota": "mn","mississippi": "ms","missouri": "mo",
    "montana": "mt","nebraska": "ne","nevada": "nv","new hampshire": "nh","new jersey": "nj",
    "new mexico": "nm","new york": "ny","north carolina": "nc","north dakota": "nd","ohio": "oh",
    "oklahoma": "ok","oregon": "or","pennsylvania": "pa","rhode island": "ri","south carolina": "sc",
    "south dakota": "sd","tennessee": "tn","texas": "tx","utah": "ut","vermont": "vt",
    "virginia": "va","washington": "wa","west virginia": "wv","wisconsin": "wi","wyoming": "wy",
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

def detect_state(q: str):
    q = q.lower()
    tokens = q.split()

    for t in tokens:
        clean = t.strip(",.").lower()
        if clean == "in":
            continue
        if clean in STATE_CODES:
            return clean.upper()

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
    zipcode = None

    # ZIP detection
    for t in tokens:
        if len(t) == 5 and t.isdigit():
            zipcode = t

    # COUNTY
    if "county" in tokens:
        idx = tokens.index("county")
        j = idx - 1
        STOP = {"in", "of", "all", "any", "addresses", "homes", "residential", "properties"}
        buf = []
        while j >= 0 and tokens[j] not in STOP:
            buf.append(tokens[j])
            j -= 1
        if buf:
            county = " ".join(reversed(buf))

    # CITY fallback
    if not county and state:
        for name in US_STATES:
            if name in q:
                continue
        # crude city detection — you wanted basic city support
        if " in " in q:
            after = q.split(" in ", 1)[1]
            parts = after.split()
            if parts and parts[0] not in US_STATES:
                city = parts[0]

    # OHIO special-case
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
            elif "value" in window:
                value_min = num
            else:
                if income_min is None:
                    income_min = num

    return income_min, value_min

# -------- STATE KEY LOOKUP --------

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

        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".geojson"):
                keys.append(key)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    _STATE_KEYS_CACHE[state] = keys
    return keys

def resolve_dataset_key(state: str, county: str):
    if not state or not county:
        return None

    county_clean = canonicalize_county_name(county)
    keys = list_state_keys(state)
    if not keys:
        return None

    enriched = []
    raw = []

    for key in keys:
        fname = key.split("/")[-1]
        base = fname.replace(".geojson", "")
        is_acs = "with-values-income" in base
        base_clean = canonicalize_county_name(base.replace("-with-values-income", ""))

        if base_clean == county_clean:
            (enriched if is_acs else raw).append(key)

    if enriched:
        return sorted(enriched, key=len)[0]
    if raw:
        return sorted(raw, key=len)[0]

    return None

# -------- LOAD + FILTER --------

def load_geojson_from_s3(key: str):
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        data = resp["Body"].read()
        return json.loads(data)
    except Exception as e:
        raise RuntimeError(f"S3 load failed for {key}: {e}")

def filter_features(features, income_min=None, value_min=None, zipcode=None):
    out = []
    for f in features:
        p = f.get("properties", {})

        if zipcode:
            if str(p.get("postcode", "")) != zipcode:
                continue

        if income_min is not None:
            inc = p.get("DP03_0062E") or p.get("median_income")
            try:
                if inc is None or float(inc) < income_min:
                    continue
            except:
                continue

        if value_min is not None:
            val = p.get("DP04_0089E") or p.get("median_value")
            try:
                if val is None or float(val) < value_min:
                    continue
            except:
                continue

        out.append(f)
    return out

def feature_to_address_obj(f):
    p = f.get("properties", {})
    geom = f.get("geometry", {}).get("coordinates") or [None, None]

    num = p.get("number", "") or p.get("house_number", "")
    street = p.get("street", "") or p.get("road", "")
    unit = p.get("unit", "")
    city = p.get("city", "")
    state = p.get("region", "") or p.get("STUSPS", "")
    zipc = p.get("postcode", "")

    income = p.get("DP03_0062E") or p.get("median_income")
    value = p.get("DP04_0089E") or p.get("median_value")

    return {
        "address": " ".join(x for x in [num, street, unit] if x),
        "city": city,
        "state": state,
        "zip": zipc,
        "income": income,
        "value": value,
        "lat": geom[1],
        "lon": geom[0],
    }

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

    # ******** FIXED JSON HANDLING (NO MORE 500) ********
    if request.is_json:
        j = request.get_json(silent=True) or {}
        data = j.get("query", "")
    else:
        data = request.form.get("query", "")

    query = data.strip()
    if not query:
        return jsonify({"error": "Enter a query."})

    state, county, city, zipcode = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state:
        return jsonify({"error": "No state detected."})
    if not county:
        return jsonify({"error": "No county detected."})

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset found for {county} County, {state}."})

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify({"error": str(e)})

    feats = gj.get("features", [])
    feats = filter_features(feats, income_min, value_min, zipcode)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    out = [feature_to_address_obj(f) for f in feats]

    return jsonify({
        "ok": True,
        "query": query,
        "state": state,
        "county": county,
        "dataset_key": key,
        "zipcode": zipcode,
        "total": total,
        "shown": len(out),
        "results": out
    })

@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"error": "Unauthorized"}), 401

    if request.is_json:
        j = request.get_json(silent=True) or {}
        data = j.get("query", "")
    else:
        data = request.form.get("query", "")

    query = data.strip()
    state, county, _, zipcode = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state or not county:
        return jsonify({"error": "Need a state + county."})

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset for {county} County, {state}."})

    gj = load_geojson_from_s3(key)
    feats = filter_features(gj.get("features", []), income_min, value_min, zipcode)

    rows = []
    fieldnames = set()

    for f in feats:
        p = f.get("properties", {}).copy()
        geom = f.get("geometry", {}).get("coordinates") or [None, None]
        p["lon"] = geom[0]
        p["lat"] = geom[1]
        rows.append(p)
        fieldnames.update(p.keys())

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
    fname = f"export_{state}_{county.replace(' ', '_')}.zip"

    return send_file(mem, mimetype="application/zip", as_attachment=True, download_name=fname)

# --------------- MAIN -----------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)

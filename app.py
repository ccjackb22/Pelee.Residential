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

# IMPORTANT: this MUST match your actual enriched folder in S3
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
    "massachusetts": "ma", "michigan": "mi", "minnesota": "mn", "mississippi": "ms",
    "missouri": "mo", "montana": "mt", "nebraska": "ne", "nevada": "nv",
    "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm",
    "new york": "ny", "north carolina": "nc", "north dakota": "nd",
    "ohio": "oh", "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa",
    "rhode island": "ri", "south carolina": "sc", "south dakota": "sd",
    "tennessee": "tn", "texas": "tx", "utah": "ut", "vermont": "vt",
    "virginia": "va", "washington": "wa", "west virginia": "wv",
    "wisconsin": "wi", "wyoming": "wy"
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

    # ignore "in"
    for t in tokens:
        if t.lower() == "in":
            continue
        if t.lower() in STATE_CODES:
            return STATE_CODES[t.lower()].upper()

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
        rev = []
        STOP = {"in", "of", "all", "any", "properties", "homes", "households", "residential", "addresses", "parcels"}
        while j >= 0:
            t = tokens[j]
            if t in STOP or t.lower() in STATE_CODES or t in US_STATES:
                break
            rev.append(t)
            j -= 1
        if rev:
            county = " ".join(reversed(rev))

    # OH city-only fallback
    if not county and state == "OH":
        state_idx = None
        for i, t in enumerate(tokens):
            if t.lower() in STATE_CODES or t in US_STATES:
                state_idx = i
                break
        if state_idx and state_idx > 0:
            j = state_idx - 1
            rev = []
            while j >= 0 and tokens[j] not in {"in", "ohio", "oh"}:
                rev.append(tokens[j])
                j -= 1
            if rev:
                city = " ".join(reversed(rev))
        county = "cuyahoga"

    return state, county, city

def parse_numeric_filters(query: str):
    q = query.lower()
    tokens = q.replace("$", "").replace(",", "").split()
    income_min = None
    value_min = None

    for i, t in enumerate(tokens):
        if t in {"over", "above"} and i + 1 < len(tokens):
            raw = tokens[i + 1].strip(".,)")
            mult = 1_000 if raw.endswith("k") else (1_000_000 if raw.endswith("m") else 1)
            if raw[-1].lower() in {"k", "m"}:
                raw = raw[:-1]
            try:
                num = float(raw) * mult
            except:
                continue

            window = " ".join(tokens[max(0, i - 3): i + 6])

            if "income" in window:
                income_min = num
            elif "value" in window or "home" in window or "properties" in window:
                value_min = num
            else:
                income_min = income_min or num

    return income_min, value_min

def list_state_keys(state_code: str):
    state_code = state_code.lower()
    if state_code in _STATE_KEYS_CACHE:
        return _STATE_KEYS_CACHE[state_code]

    prefix = f"{S3_PREFIX}/{state_code}/"
    keys = []
    token = None

    while True:
        try:
            resp = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=prefix,
                ContinuationToken=token
            ) if token else s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=prefix
            )
        except ClientError as e:
            app.logger.error(f"[S3] list failed: {e}")
            break

        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".geojson"):
                keys.append(obj["Key"])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    _STATE_KEYS_CACHE[state_code] = keys
    return keys

def resolve_dataset_key(state: str, county: str):
    if not state or not county:
        return None

    st = state.lower()
    cc = canonicalize_county_name(county)
    keys = list_state_keys(st)

    enriched_exact = []
    raw_exact = []
    enriched_fuzzy = []
    raw_fuzzy = []

    for k in keys:
        fname = k.split("/")[-1]
        base = fname[:-8]  # remove .geojson
        enriched = "with-values-income" in base
        base2 = canonicalize_county_name(base.replace("-with-values-income", ""))

        if base2 == cc:
            (enriched_exact if enriched else raw_exact).append(k)
        elif base2.startswith(cc) or cc.startswith(base2):
            (enriched_fuzzy if enriched else raw_fuzzy).append(k)

    for group in (enriched_exact, raw_exact, enriched_fuzzy, raw_fuzzy):
        if group:
            return sorted(group, key=len)[0]

    return None

# ----------------- FIXED STREAMING LOADER -----------------

def load_geojson_from_s3(key: str):
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        stream = resp["Body"]

        chunks = []
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            chunks.append(chunk)

        data = b"".join(chunks)
        return json.loads(data)

    except Exception as e:
        app.logger.error(f"[S3] load failed for {key}: {e}")
        raise

# ----------------- FILTERING -----------------

def filter_features(features, income_min=None, value_min=None):
    if income_min is None and value_min is None:
        return features

    income_fields = ["DP03_0062E", "median_income", "income"]
    value_fields = ["DP04_0089E", "median_value", "home_value"]

    def first(props, keys):
        for k in keys:
            if k in props:
                try:
                    return float(props[k])
                except:
                    pass
        return None

    out = []
    for feat in features:
        props = feat.get("properties", {})
        ok = True

        if income_min is not None:
            inc = first(props, income_fields)
            if inc is None or inc < income_min:
                ok = False

        if ok and value_min is not None:
            val = first(props, value_fields)
            if val is None or val < value_min:
                ok = False

        if ok:
            out.append(feat)

    return out

def feature_to_address_line(f):
    p = f.get("properties", {})
    n = p.get("number", "") or p.get("house_number", "")
    s = p.get("street", "") or p.get("road", "")
    u = p.get("unit", "")
    c = p.get("city", "")
    r = p.get("region", "") or p.get("STUSPS", "")
    z = p.get("postcode", "")

    line1 = " ".join(x for x in [str(n), s, u] if x)
    line2 = ", ".join(x for x in [c, r, z] if x)

    return (line1 + "\n" + line2).strip()

# ----------------- ROUTES -----------------

@app.route("/health")
def health():
    return jsonify({"ok": True})

@app.route("/login", methods=["GET","POST"])
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
        return jsonify({"error":"Unauthorized"}), 401

    q = request.form.get("query","") or (request.json.get("query","") if request.is_json else "")
    q = q.strip()

    if not q:
        return jsonify({"error":"Please enter a query."})

    state, county, city = parse_location(q)
    income_min, value_min = parse_numeric_filters(q)

    if not state:
        return jsonify({"error":"Could not detect a state."})
    if not county:
        return jsonify({"error":"Could not detect a county."})

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error":f"No dataset found for {county} County, {state}."})

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify({"error":f"Failed to load: {key}: {e}"}), 500

    feats = gj.get("features", [])
    feats = filter_features(feats, income_min, value_min)
    total = len(feats)

    feats = feats[:MAX_RESULTS]
    addrs = [feature_to_address_line(f) for f in feats]

    return jsonify({
        "ok":True,
        "query":q,
        "state":state,
        "county":county,
        "dataset_key":key,
        "total":total,
        "shown":len(addrs),
        "results":addrs
    })

@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"error":"Unauthorized"}),401

    q = request.form.get("query","") or (request.json.get("query","") if request.is_json else "")
    q = q.strip()

    state, county, _ = parse_location(q)
    income_min, value_min = parse_numeric_filters(q)

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error":f"No dataset found for export"}),400

    gj = load_geojson_from_s3(key)
    feats = filter_features(gj.get("features",[]), income_min, value_min)

    rows = []
    fieldnames = set()

    for f in feats:
        p = f.get("properties",{}).copy()
        geom = f.get("geometry",{})
        c = geom.get("coordinates")
        if isinstance(c,(list,tuple)) and len(c)>=2:
            p["lon"]=c[0]
            p["lat"]=c[1]
        fieldnames.update(p.keys())
        rows.append(p)

    fn = sorted(fieldnames)

    mem = io.BytesIO()
    with zipfile.ZipFile(mem,"w",zipfile.ZIP_DEFLATED) as z:
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=fn)
        w.writeheader()
        for r in rows:
            w.writerow(r)
        z.writestr("addresses.csv", buf.getvalue())

    mem.seek(0)
    fname = f"pelee_export_{state}_{canonicalize_county_name(county)}.zip"

    return send_file(mem, mimetype="application/zip",
                     as_attachment=True,
                     download_name=fname)

# ----------------- MAIN -----------------

if __name__ == "__main__":
    port = int(os.getenv("PORT","5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

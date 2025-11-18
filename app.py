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

# IMPORTANT: point to the enriched ACS data
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
        if word in s:
            s = s.replace(word, "")
    return " ".join(s.split())


def detect_state(q: str):
    q = q.lower()
    tokens = q.split()

    for t in tokens:
        t_clean = t.strip(",.").lower()
        if t_clean == "in":  # don't treat "in" as Indiana
            continue
        if t_clean in STATE_CODES:
            return t_clean.upper()

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

    if not county and state == "OH":
        state_idx = None
        for i, t in enumerate(tokens):
            if t.lower() in STATE_CODES or t in US_STATES:
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
            key = obj.get("Key")
            if key and key.endswith(".geojson"):
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
        base = fname[:-8]  # remove .geojson
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


def filter_features(features, income_min=None, value_min=None):
    if income_min is None and value_min is None:
        return features

    income_keys = ["DP03_0062E", "median_income", "income"]
    value_keys = ["DP04_0089E", "median_value", "value"]

    def read_val(props, keys):
        for k in keys:
            if k in props and props[k] not in ("", None):
                try:
                    return float(props[k])
                except:
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


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.form.get("query") or (request.json.get("query") if request.is_json else "")
    query = (data or "").strip()
    if not query:
        return jsonify({"error": "Enter a query."})

    state, county, city = parse_location(query)
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
        return jsonify({"error": f"Failed loading {key}: {str(e)}"})

    feats = gj.get("features", [])
    feats = filter_features(feats, income_min, value_min)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    addresses = [feature_to_address_obj(f) for f in feats]

    return jsonify({
        "ok": True,
        "query": query,
        "state": state,
        "county": county,
        "dataset_key": key,
        "total": total,
        "shown": len(addresses),
        "results": addresses
    })


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.form.get("query") or (request.json.get("query") if request.is_json else "")
    query = (data or "").strip()

    state, county, _ = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state or not county:
        return jsonify({"error": "Need a state + county."})

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset found for {county} County, {state}."})

    gj = load_geojson_from_s3(key)
    feats = gj.get("features", [])
    feats = filter_features(feats, income_min, value_min)

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


# ----------------- MAIN -----------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)

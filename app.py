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
S3_PREFIX = "merged_with_tracts"

MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ----------------- LOAD MASTER INDEX -----------------

INDEX_PATH = os.path.join(os.path.dirname(__file__), "data", "master_file_index.json")
with open(INDEX_PATH, "r") as f:
    MASTER_INDEX = json.load(f)

# ----------------- UTILITIES -----------------

US_STATES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct", "delaware": "de",
    "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id",
    "illinois": "il", "indiana": "in", "iowa": "ia", "kansas": "ks",
    "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md",
    "massachusetts": "ma", "michigan": "mi", "minnesota": "mn",
    "mississippi": "ms", "missouri": "mo", "montana": "mt", "nebraska": "ne",
    "nevada": "nv", "new hampshire": "nh", "new jersey": "nj",
    "new mexico": "nm", "new york": "ny", "north carolina": "nc",
    "north dakota": "nd", "ohio": "oh", "oklahoma": "ok", "oregon": "or",
    "pennsylvania": "pa", "rhode island": "ri", "south carolina": "sc",
    "south dakota": "sd", "tennessee": "tn", "texas": "tx", "utah": "ut",
    "vermont": "vt", "virginia": "va", "washington": "wa",
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
        if word in s:
            s = s.replace(word, "")
    return " ".join(s.split())


def detect_state(query: str):
    q = query.lower()
    tokens = q.split()

    for t in tokens:
        if t in STATE_CODES:
            return STATE_CODES[t].upper()

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
        county_tokens_rev = []
        STOP = {"in", "of", "all", "any", "properties", "homes", "residential", "addresses", "parcels"}
        while j >= 0:
            t = tokens[j]
            if t in STOP:
                break
            if t.upper() in STATE_CODES:
                break
            county_tokens_rev.append(t)
            j -= 1
        if county_tokens_rev:
            county = " ".join(reversed(county_tokens_rev))

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
            raw = tokens[i + 1].strip(".,")
            mult = 1
            if raw.endswith("k"):
                mult = 1000
                raw = raw[:-1]
            elif raw.endswith("m"):
                mult = 1000000
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
                if income_min is None:
                    income_min = num

    return income_min, value_min


# ----------------- THE FIX: USE MASTER INDEX -----------------

def resolve_dataset_key(state: str, county: str):
    """
    100% deterministic dataset lookup.
    No AWS listing. No fuzzy logic.
    """
    if not state or not county:
        return None

    state = state.lower()
    county_clean = canonicalize_county_name(county)

    for fips, entry in MASTER_INDEX.items():
        if entry["state"] == state and entry["county"] == county_clean:
            if entry["enriched_key"]:
                return entry["enriched_key"]
            return entry["raw_key"]

    return None


# ----------------- LOAD + FILTER -----------------

def load_geojson_from_s3(key: str):
    resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(resp["Body"].read())


def filter_features(features, income_min=None, value_min=None):
    if income_min is None and value_min is None:
        return features

    income_keys = ["DP03_0062E", "median_income", "income", "household_income"]
    value_keys = ["DP04_0089E", "median_value", "home_value", "value"]

    def get(props, keys):
        for k in keys:
            if k in props and props[k] not in (None, ""):
                try:
                    return float(props[k])
                except:
                    pass
        return None

    out = []
    for feat in features:
        props = feat.get("properties", {})
        keep = True

        if income_min is not None:
            inc = get(props, income_keys)
            if inc is None or inc < income_min:
                keep = False

        if keep and value_min is not None:
            val = get(props, value_keys)
            if val is None or val < value_min:
                keep = False

        if keep:
            out.append(feat)

    return out


def feature_to_address_line(feat):
    props = feat.get("properties", {})
    number = props.get("number", "")
    street = props.get("street", "")
    unit = props.get("unit", "")
    city = props.get("city", "")
    postcode = props.get("postcode", "")
    region = props.get("region", props.get("STUSPS", ""))

    parts = []
    line1 = " ".join([str(x).strip() for x in [number, street, unit] if x])
    if line1:
        parts.append(line1)
    loc = ", ".join(p for p in [city, region, postcode] if p)
    if loc:
        parts.append(loc)

    return "\n".join(parts) if parts else "(no address)"


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

    query = request.form.get("query", "") or request.json.get("query", "")
    query = query.strip()

    state, county, city = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state:
        return jsonify({"error": "Could not detect a state."})
    if not county:
        return jsonify({"error": "Could not detect a county."})

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify({"error": f"No dataset found for {county} County, {state}."})

    gj = load_geojson_from_s3(key)
    features = gj.get("features", [])

    features = filter_features(features, income_min, value_min)
    total = len(features)
    features = features[:MAX_RESULTS]

    addresses = [feature_to_address_line(f) for f in features]

    return jsonify({
        "ok": True,
        "query": query,
        "state": state,
        "county": county,
        "dataset_key": key,
        "total": total,
        "shown": len(addresses),
        "results": addresses,
    })


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"error": "Unauthorized"}), 401

    query = request.form.get("query", "") or request.json.get("query", "")
    query = query.strip()

    state, county, city = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)
    key = resolve_dataset_key(state, county)

    if not key:
        return jsonify({"error": f"No dataset for export for {county}, {state}."}), 400

    gj = load_geojson_from_s3(key)
    features = filter_features(gj.get("features", []), income_min, value_min)

    rows = []
    fieldnames = set()

    for feat in features:
        props = feat.get("properties", {}).copy()
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
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

    filename = f"pelee_export_{state}_{canonicalize_county_name(county)}.zip"

    return send_file(
        memfile,
        mimetype="application/zip",
        as_attachment=True,
        download_name=filename,
    )


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

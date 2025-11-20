import os
import io
import json
import csv
import re
from flask import (
    Flask, render_template, request, jsonify,
    redirect, url_for, session, send_file
)
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# *** YOU CONFIRMED THE CORRECT LOCATION ***
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts")

MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

_STATE_KEYS_CACHE = {}

# ---------------------------------------------------------
# LOOKUPS
# ---------------------------------------------------------

US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}

STATE_CODES = {code.lower(): code for code in US_STATES.values()}

CITY_TO_COUNTY = {
    "wa": {
        "vancouver": ("clark", "Vancouver"),
    },
    "oh": {
        "strongsville": ("cuyahoga", "Strongsville"),
        "berea": ("cuyahoga", "Berea"),
        "rocky river": ("cuyahoga", "Rocky River"),
        "lakewood": ("cuyahoga", "Lakewood"),
        "westlake": ("cuyahoga", "Westlake"),
        "bay village": ("cuyahoga", "Bay Village"),
        "avon": ("lorain", "Avon"),
        "avon lake": ("lorain", "Avon Lake"),
    },
    "tx": {
        # if you want city-based detection for Corpus Christi etc later,
        # we can map them here to Nueces County
        # "corpus christi": ("nueces", "Corpus Christi"),
    },
}

# ---------------------------------------------------------
# COUNTY BOUNDING BOXES (to kill cross-county junk)
# ---------------------------------------------------------
# (state_code, canonical_county) -> bounding box in degrees
# These are intentionally a little generous but still exclude Dallas/Austin etc.
BOUNDING_BOXES = {
    ("tx", "harris"): {
        # Covers greater Houston / Harris County, excludes Dallas/Austin
        "min_lat": 28.5,
        "max_lat": 30.5,
        "min_lon": -96.5,
        "max_lon": -94.5,
    },
    ("tx", "nueces"): {
        # Nueces County (Corpus Christi area) bounding box
        # Based on published parcel extents and county location :contentReference[oaicite:0]{index=0}
        "min_lat": 27.55,
        "max_lat": 28.05,
        "min_lon": -98.05,
        "max_lon": -97.0,
    },
}

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def norm(s):
    return " ".join((s or "").lower().strip().split())

def canonical_county(name: str):
    if not name:
        return ""
    name = (
        name.lower()
        .replace("_", " ")
        .replace("-", " ")
        .replace(" county", "")
        .replace(" parish", "")
    )
    name = " ".join(name.split())

    # Fix known common typos / variants
    if name == "neuces":
        name = "nueces"

    return name

def detect_state(query: str):
    q_lower = query.lower()

    for fullname, code in US_STATES.items():
        if re.search(r"\b" + re.escape(fullname) + r"\b", q_lower):
            return code

    for m in re.finditer(r"\b([A-Z]{2})\b", query):
        t = m.group(1).lower()
        if t in STATE_CODES:
            return STATE_CODES[t]

    return None

def detect_zip(query: str):
    m = re.search(r"\b(\d{5})\b", query)
    return m.group(1) if m else None

def parse_filters(q: str):
    q = q.lower().replace("$", "").replace(",", "")
    toks = q.split()

    income_min = None
    value_min = None

    for i, t in enumerate(toks):
        if t in ("over", "above") and i + 1 < len(toks):
            raw = toks[i + 1]
            mult = 1
            if raw.endswith("k"):
                mult = 1000
                raw = raw[:-1]
            if raw.endswith("m"):
                mult = 1_000_000
                raw = raw[:-1]
            try:
                val = float(raw) * mult
            except Exception:
                continue

            window = " ".join(toks[max(0, i - 5): i + 5])
            if "income" in window:
                income_min = val
                continue
            if "value" in window or "home" in window:
                value_min = val
                continue

            if income_min is None:
                income_min = val

    return income_min, value_min

def parse_location(query: str):
    q = norm(query)
    tokens = q.split()

    state = detect_state(query)
    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    county = None
    city = None

    if "county" in tokens:
        idx = tokens.index("county")
        if idx > 0:
            county = tokens[idx - 1]

    if state:
        st_key = state.lower()
        for cname, (c_county, pretty) in CITY_TO_COUNTY.get(st_key, {}).items():
            if cname in q:
                county = c_county
                city = pretty
                break

    if not county and state == "OH":
        for i, t in enumerate(tokens):
            if t in ("oh", "ohio") and i > 0:
                city = tokens[i - 1]
                county = "cuyahoga"
                break

    return state, county, city, zip_code, income_min, value_min

def list_keys(state: str):
    if not state:
        return []

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
        except ClientError:
            break

        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".geojson"):
                keys.append(obj["Key"])

        if not resp.get("IsTruncated"):
            break

        token = resp.get("NextContinuationToken")

    _STATE_KEYS_CACHE[state] = keys
    return keys

# ---------------------------------------------------------
# NEW IMPROVED DATASET PICKER
# ---------------------------------------------------------

def pick_dataset(state: str, county: str):
    """
    Picks correct dataset for <county>.
    Handles:
      - county.geojson
      - county-with-values-income.geojson
      - county-with-tracts-acs-with-values-income.geojson
      - ANY variant containing "-with..."
    Always prefers enriched datasets.
    """
    if not state or not county:
        return None

    state = state.lower()
    county_clean = canonical_county(county)

    keys = list_keys(state)
    if not keys:
        return None

    matches = []

    for k in keys:
        base = os.path.basename(k).replace(".geojson", "").lower()
        base_root = re.split(r"-with.*", base)[0]
        base_clean = canonical_county(base_root)

        if base_clean == county_clean:
            matches.append(k)

    if not matches:
        return None

    for m in matches:
        if "with-values" in m or "with-value" in m:
            return m

    return matches[0]

# ---------------------------------------------------------
# LOAD + FILTER
# ---------------------------------------------------------

def load_geojson(key: str):
    data = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(data)

def feature_to_obj(f):
    p = f.get("properties", {})
    geom = f.get("geometry", {}).get("coordinates", [None, None])

    return {
        "address": " ".join(
            str(x) for x in [
                p.get("number") or p.get("house_number") or "",
                p.get("street") or p.get("road") or "",
                p.get("unit") or "",
            ] if x
        ),
        "city": p.get("city") or "",
        "state": p.get("region") or p.get("STUSPS") or "",
        "zip": p.get("postcode") or "",
        "income": p.get("DP03_0062E") or p.get("median_income") or p.get("income"),
        "value": p.get("DP04_0089E") or p.get("median_value") or p.get("home_value"),
        "lat": geom[1],
        "lon": geom[0],
    }

def apply_filters(features, income_min, value_min, zip_code, state=None, county=None):
    out = []
    county_key = None
    bbox = None

    if state and county:
        county_key = (state.lower(), canonical_county(county))
        bbox = BOUNDING_BOXES.get(county_key)

    for f in features:
        p = f.get("properties", {})

        # 1) County/geometry sanity check: enforce bounding box if defined
        if bbox:
            coords = f.get("geometry", {}).get("coordinates")
            if not coords or len(coords) < 2:
                continue
            lon, lat = coords[0], coords[1]
            if not (
                bbox["min_lat"] <= lat <= bbox["max_lat"]
                and bbox["min_lon"] <= lon <= bbox["max_lon"]
            ):
                # Outside the county bounding box -> drop
                continue

        # 2) ZIP filter
        if zip_code:
            if str(p.get("postcode")) != str(zip_code):
                continue

        # 3) Income filter
        if income_min:
            v = p.get("DP03_0062E") or p.get("median_income") or p.get("income")
            try:
                if v is None or float(v) < income_min:
                    continue
            except Exception:
                continue

        # 4) Home value filter
        if value_min:
            v = p.get("DP04_0089E") or p.get("median_value") or p.get("home_value")
            try:
                if v is None or float(v) < value_min:
                    continue
            except Exception:
                continue

        out.append(f)
    return out

# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------

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
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    state, county, city, zip_code, income_min, value_min = parse_location(query)

    if not state:
        return jsonify({"ok": False, "error": "No state detected."})
    if not county:
        return jsonify({"ok": False, "error": "No county detected."})

    key = pick_dataset(state, county)
    if not key:
        return jsonify({"ok": False, "error": f"No dataset found for {county} County, {state}."})

    try:
        gj = load_geojson(key)
    except Exception:
        return jsonify({"ok": False, "error": "Failed loading dataset."}), 500

    feats = gj.get("features", [])
    feats = apply_filters(feats, income_min, value_min, zip_code, state=state, county=county)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    return jsonify({
        "ok": True,
        "query": query,
        "state": state,
        "county": county,
        "city": city,
        "zip": zip_code,
        "dataset_key": key,
        "total": total,
        "shown": len(feats),
        "results": [feature_to_obj(f) for f in feats],
    })

# ---------------------------------------------------------
# EXPORT
# ---------------------------------------------------------

@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Missing query."})

    state, county, city, zip_code, income_min, value_min = parse_location(query)

    if not state or not county:
        return jsonify({"ok": False, "error": "Invalid query."})

    key = pick_dataset(state, county)
    if not key:
        return jsonify({"ok": False, "error": "Dataset not found."})

    try:
        gj = load_geojson(key)
    except Exception:
        return jsonify({"ok": False, "error": "Failed loading dataset."}), 500

    feats = apply_filters(
        gj.get("features", []),
        income_min,
        value_min,
        zip_code,
        state=state,
        county=county,
    )

    out = io.StringIO()
    w = csv.writer(out)

    w.writerow([
        "address_number",
        "street",
        "unit",
        "city",
        "state",
        "zip",
        "income",
        "value",
        "lat",
        "lon"
    ])

    for f in feats:
        p = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [None, None])

        num = p.get("number") or p.get("house_number") or ""
        street = p.get("street") or p.get("road") or ""
        unit = p.get("unit") or ""

        city = p.get("city") or ""
        st = p.get("region") or p.get("STUSPS") or ""
        zipc = p.get("postcode") or ""

        income = p.get("DP03_0062E") or p.get("median_income") or p.get("income")
        value = p.get("DP04_0089E") or p.get("median_value") or p.get("home_value")

        w.writerow([
            num, street, unit, city, st, zipc, income, value, coords[1], coords[0]
        ])

    mem = io.BytesIO(out.getvalue().encode("utf-8"))
    mem.seek(0)

    filename = f"pelee_export_{county}_{state}.csv"

    return send_file(mem,
                     mimetype="text/csv",
                     as_attachment=True,
                     download_name=filename)

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

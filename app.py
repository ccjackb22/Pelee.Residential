import os
import json
import logging
from functools import wraps
from io import StringIO
import csv

from flask import (
    Flask,
    request,
    jsonify,
    render_template,
    redirect,
    url_for,
    session,
    Response,
)
from dotenv import load_dotenv

import boto3
import httpx
from openai import OpenAI

# -----------------------------------------------------------------------------
# ENV + CONFIG
# -----------------------------------------------------------------------------
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "CaLuna")
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# -----------------------------------------------------------------------------
# FIX FOR RENDER PROXIES BUG
# -----------------------------------------------------------------------------
# Render injects http_proxy/https_proxy env vars that break httpx
# → The OpenAI client uses httpx under the hood → we must disable proxies manually.
session_client = httpx.Client(proxies=None)

client = OpenAI(api_key=OPENAI_API_KEY, http_client=session_client)

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# FLASK
# -----------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "super_secret_key")

# -----------------------------------------------------------------------------
# AWS SESSION
# -----------------------------------------------------------------------------
session_aws = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION,
)
s3 = session_aws.client("s3")

# -----------------------------------------------------------------------------
# AUTH DECORATOR
# -----------------------------------------------------------------------------
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "logged_in" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper

# -----------------------------------------------------------------------------
# LOGIN ROUTES
# -----------------------------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == ADMIN_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("home"))
        else:
            error = "Incorrect password"
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def normalize_str(x):
    return str(x).strip().lower() if x else ""

def get_first(props, keys):
    for k in keys:
        if k in props and props[k]:
            return props[k]
    return ""

def get_numeric_prop(props, keys):
    for k in keys:
        val = props.get(k)
        if val not in (None, ""):
            try:
                return float(str(val).replace(",", ""))
            except:
                continue
    return None

INCOME_FIELDS = ["median_income", "MEDIAN_INCOME", "DP03_0062E", "income"]
HOME_VALUE_FIELDS = ["median_home_value", "MEDIAN_HOME_VALUE", "DP04_0089E", "home_value"]

def build_label(props, state):
    num = get_first(props, ["NUMBER", "number"])
    street = get_first(props, ["STREET", "street"])
    unit = get_first(props, ["unit", "UNIT", "apt"])
    city = get_first(props, ["CITY", "city"])
    zipc = get_first(props, ["ZIP", "zip", "POSTCODE", "postcode"])

    parts = []
    if num: parts.append(num)
    if street: parts.append(street)
    if unit: parts.append(unit)
    line1 = " ".join(parts)

    full = ", ".join([p for p in [line1, city, state, zipc] if p])
    return full

def feature_to_result(feature, state, county):
    props = feature.get("properties", {})
    geom = feature.get("geometry", {})
    coords = geom.get("coordinates", [None, None])

    return {
        "address": build_label(props, state),
        "city": get_first(props, ["CITY", "city"]),
        "state": state,
        "zip": get_first(props, ["ZIP", "zip", "POSTCODE"]),
        "county": county,
        "lat": coords[1],
        "lon": coords[0],
        "income": get_numeric_prop(props, INCOME_FIELDS),
        "home_value": get_numeric_prop(props, HOME_VALUE_FIELDS),
    }

# -----------------------------------------------------------------------------
# LOAD COUNTY FROM S3
# -----------------------------------------------------------------------------
def load_county_geojson(state_abbr, county_slug):
    state = state_abbr.lower()
    county = county_slug.lower().replace(" county", "").replace(" ", "_")

    candidates = [
        f"{state}/{county}-with-values-income.geojson",
        f"{state}/{county}-addresses-county.geojson",
        f"{state}/{county}.geojson",
    ]

    for key in candidates:
        try:
            log.info(f"Trying S3 key: {key}")
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            text = obj["Body"].read().decode("utf-8", errors="replace").strip()

            try:
                parsed = json.loads(text)
                if "features" in parsed:
                    return parsed
            except:
                pass

            feats = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    o = json.loads(line)
                    if o.get("type") == "Feature":
                        feats.append(o)
                except:
                    pass
            if feats:
                return {"type": "FeatureCollection", "features": feats}

        except Exception as e:
            log.warning(f"Failed {key}: {e}")

    raise FileNotFoundError(f"No county file found for {state}/{county}")

# -----------------------------------------------------------------------------
# ZIP INFERENCE
# -----------------------------------------------------------------------------
def infer_zip_from_city_state(city, state):
    if not city or not state:
        return None

    prompt = f"""
    City: {city}
    State: {state}
    Return ONLY a single 5-digit ZIP.
    """

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        text = resp.choices[0].message.content.strip()
        digits = "".join(c for c in text if c.isdigit())
        return digits if len(digits) == 5 else None
    except:
        return None

# -----------------------------------------------------------------------------
# PARSE QUERY
# -----------------------------------------------------------------------------
@app.route("/parse_query", methods=["POST"])
@login_required
def parse_query():
    text = request.json.get("query", "").strip()

    prompt = f"""
    Extract filters from this query: "{text}"

    Return JSON with:
    - state
    - county
    - city
    - zip
    - query
    - min_income
    - max_income
    - min_value
    - max_value
    - limit
    """

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        blob = resp.choices[0].message.content
        start = blob.find("{")
        end = blob.rfind("}")
        filters = json.loads(blob[start:end+1])

        if not filters.get("zip") and filters.get("city") and filters.get("state"):
            inferred = infer_zip_from_city_state(filters["city"], filters["state"])
            if inferred:
                filters["zip"] = inferred

        log.info(filters)
        return jsonify({"ok": True, "filters": filters})

    except Exception as e:
        log.exception("parse_query error")
        return jsonify({"ok": False, "error": str(e)}), 500

# -----------------------------------------------------------------------------
# RUN SEARCH
# -----------------------------------------------------------------------------
def run_search(filters):
    state = filters.get("state")
    county = filters.get("county")
    city = normalize_str(filters.get("city"))
    zipc = normalize_str(filters.get("zip"))
    query_text = normalize_str(filters.get("query"))
    limit = int(filters.get("limit") or 5000)

    if not county:
        raise ValueError("County required")

    data = load_county_geojson(state, county)
    features = data.get("features", [])
    results = []

    def base_pass(f):
        props = f.get("properties", {})
        inc = get_numeric_prop(props, INCOME_FIELDS)
        val = get_numeric_prop(props, HOME_VALUE_FIELDS)

        if filters.get("min_income") and inc and inc < filters["min_income"]:
            return False
        if filters.get("max_income") and inc and inc > filters["max_income"]:
            return False
        if filters.get("min_value") and val and val < filters["min_value"]:
            return False
        if filters.get("max_value") and val and val > filters["max_value"]:
            return False
        return True

    def prop_zip(f):
        return normalize_str(get_first(
            f.get("properties", {}),
            ["ZIP", "zip", "POSTCODE", "postcode"],
        ))

    def prop_city(f):
        return normalize_str(get_first(
            f.get("properties", {}),
            ["CITY", "city", "PlaceName"],
        ))

    def full_text(f):
        p = f.get("properties", {})
        parts = [
            get_first(p, ["NUMBER", "number"]),
            get_first(p, ["STREET", "street"]),
            get_first(p, ["UNIT", "unit"]),
            get_first(p, ["CITY", "city"]),
            get_first(p, ["ZIP", "zip"]),
        ]
        return normalize_str(" ".join(str(x) for x in parts if x))

    # ZIP-first search
    if zipc:
        for f in features:
            if prop_zip(f).startswith(zipc) and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    return results

    # City search
    if not results and city:
        for f in features:
            if city in prop_city(f) and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    return results

    # Query search
    if not results and query_text:
        for f in features:
            if query_text in full_text(f) and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    return results

    # Fallback
    for f in features[:limit]:
        if base_pass(f):
            results.append(feature_to_result(f, state, county))

    return results

# -----------------------------------------------------------------------------
# API ROUTES
# -----------------------------------------------------------------------------
@app.route("/search_advanced", methods=["POST"])
@login_required
def search_advanced():
    try:
        results = run_search(request.json)
        return jsonify({"ok": True, "count": len(results), "results": results})
    except Exception as e:
        log.exception("search error")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/download_csv", methods=["POST"])
@login_required
def download_csv():
    results = run_search(request.json)

    fieldnames = [
        "address", "city", "state", "zip",
        "county", "lat", "lon", "income", "home_value"
    ]
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in results:
        writer.writerow(r)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=addresses.csv"},
    )

# -----------------------------------------------------------------------------
# PAGE ROUTES
# -----------------------------------------------------------------------------
@app.route("/")
def root():
    if session.get("logged_in"):
        return redirect(url_for("home"))
    return redirect(url_for("login"))

@app.route("/home")
@login_required
def home():
    return render_template("index.html")

@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "openai": bool(OPENAI_API_KEY),
        "aws": bool(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY),
    })

# -----------------------------------------------------------------------------
# RUN
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Pelée running → http://127.0.0.1:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)

<<<<<<< HEAD
<<<<<<< HEAD
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
from botocore.exceptions import ClientError
from openai import OpenAI

# --------------------------------------------------------------------------
# ENV + CONFIG
# --------------------------------------------------------------------------
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "CaLuna")

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

client = OpenAI(api_key=OPENAI_API_KEY)

# --------------------------------------------------------------------------
# LOGGING
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# FLASK APP
# --------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "super_secret_key")

# --------------------------------------------------------------------------
# AWS SESSION
# --------------------------------------------------------------------------
session_aws = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION,
)
s3 = session_aws.client("s3")

# --------------------------------------------------------------------------
# AUTH DECORATOR
# --------------------------------------------------------------------------
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "logged_in" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper

# --------------------------------------------------------------------------
# LOGIN ROUTES
# --------------------------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == ADMIN_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("app_page"))
        else:
            error = "Incorrect password"
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# --------------------------------------------------------------------------
# SMALL HELPERS
# --------------------------------------------------------------------------
INCOME_FIELDS = [
    "median_income",
    "MEDIAN_INCOME",
    "DP03_0062E",
    "INCOME",
    "income",
]

HOME_VALUE_FIELDS = [
    "median_home_value",
    "MEDIAN_HOME_VALUE",
    "DP04_0089E",
    "HOME_VALUE",
    "home_value",
]

def normalize_str(x):
    if x is None:
        return ""
    return str(x).strip().lower()

def get_first(props, keys):
    for k in keys:
        if k in props and props[k] not in (None, ""):
            return props[k]
    return ""

def get_numeric_prop(props, keys):
    for k in keys:
        if k in props and props[k] not in (None, ""):
            try:
                return float(str(props[k]).replace(",", ""))
            except (TypeError, ValueError):
                continue
    return None

def build_address_label(props, state_abbr):
    number = get_first(
        props,
        ["address", "NUMBER", "number", "addr:housenumber", "housenumber"],
    )
    street = get_first(
        props,
        ["STREET", "street", "addr:street", "road", "ROAD"],
    )
    unit = get_first(
        props,
        ["UNIT", "unit", "apt", "APT", "suite", "SUITE"],
    )
    city = get_first(
        props,
        ["CITY", "city", "PlaceName", "place_name", "town", "TOWN", "municipality"],
    )
    postcode = get_first(
        props,
        ["POSTCODE", "postcode", "ZIP", "zip", "ZIPCODE", "postal_code"],
    )

    if "address" in props and props["address"]:
        line1 = str(props["address"]).strip()
    else:
        parts = []
        if number:
            parts.append(str(number).strip())
        if street:
            parts.append(str(street).strip())
        if unit:
            parts.append(str(unit).strip())
        line1 = " ".join(parts)

    chunks = []
    if line1:
        chunks.append(line1)
    if city:
        chunks.append(str(city).strip())
    if state_abbr:
        chunks.append(state_abbr.upper())
    if postcode:
        chunks.append(str(postcode).strip())

    return ", ".join(chunks)

def feature_to_result(feature, state_abbr, county_slug):
    props = feature.get("properties", {}) or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None]

    number = get_first(
        props,
        ["NUMBER", "number", "addr:housenumber", "housenumber"],
    )
    street = get_first(
        props,
        ["STREET", "street", "addr:street", "road", "ROAD"],
    )
    unit = get_first(
        props,
        ["UNIT", "unit", "apt", "APT", "suite", "SUITE"],
    )
    city = get_first(
        props,
        ["CITY", "city", "PlaceName", "place_name", "town", "TOWN", "municipality"],
    )
    state_val = get_first(
        props,
        ["REGION", "region", "STATE", "state"],
    ) or state_abbr.upper()
    postcode = get_first(
        props,
        ["POSTCODE", "postcode", "ZIP", "zip", "ZIPCODE", "postal_code"],
    )

    income_val = get_numeric_prop(props, INCOME_FIELDS)
    home_val = get_numeric_prop(props, HOME_VALUE_FIELDS)

    label = build_address_label(props, state_abbr)

    return {
        "address": label,
        "label": label,
        "number": number,
        "street": street,
        "unit": unit,
        "city": city,
        "state": state_val,
        "zip": postcode,
        "county": county_slug.lower(),
        "lat": coords[1] if len(coords) > 1 else None,
        "lon": coords[0] if len(coords) > 0 else None,
        "income": income_val,
        "home_value": home_val,
        "properties": props,
    }

# --------------------------------------------------------------------------
# S3 LOADER — GEOJSON OR NDJSON
# --------------------------------------------------------------------------
def load_county_geojson_from_s3(state_abbr: str, county_slug: str):
    state = state_abbr.lower()
    county = (
        county_slug.lower()
        .replace(" county", "")
        .replace("county", "")
        .strip()
        .replace(" ", "_")
    )

    candidate_keys = [
        f"{state}/{county}-with-values-income.geojson",
        f"{state}/{county}-addresses-county.geojson",
        f"{state}/{county}.geojson",
    ]

    last_error = None

    for key in candidate_keys:
        try:
            log.info(f"Trying S3 key: {key}")
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            raw = obj["Body"].read().decode("utf-8", errors="replace").strip()

            # Try FeatureCollection first
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and "features" in parsed:
                    log.info(f"Loaded FeatureCollection from {key}")
                    return parsed, key
            except json.JSONDecodeError:
                pass

            # Otherwise treat as NDJSON
            features = []
            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj_line = json.loads(line)
                    if isinstance(obj_line, dict) and obj_line.get("type") == "Feature":
                        features.append(obj_line)
                except Exception:
                    continue

            if features:
                log.info(f"Loaded NDJSON with {len(features)} features from {key}")
                return {"type": "FeatureCollection", "features": features}, key

        except Exception as e:
            last_error = e
            log.warning(f"Failed loading {key}: {e}")

    raise FileNotFoundError(
        f"No GeoJSON/NDJSON found for {state}/{county}. Tried: {candidate_keys}. Last error: {last_error}"
    )

# --------------------------------------------------------------------------
# ZIP INFERENCE — CITY + STATE → PRIMARY ZIP
# --------------------------------------------------------------------------
def infer_zip_from_city_state(city, state):
    city_norm = (city or "").strip()
    state_norm = (state or "").strip()
    if not city_norm or not state_norm:
        return None

    prompt = f"""
    You are a precise US ZIP code lookup engine.

    City: "{city_norm}"
    State: "{state_norm}" (2-letter code or full state name)

    Respond with a SINGLE 5-digit ZIP code only.
    Example: 44017
    """

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        content = resp.choices[0].message.content.strip()
        digits = "".join(ch for ch in content if ch.isdigit())
        if len(digits) == 5:
            return digits
    except Exception as e:
        log.warning(f"ZIP inference failed for {city_norm}, {state_norm}: {e}")

    return None

# --------------------------------------------------------------------------
# NLP PARSER — NATURAL LANGUAGE → FILTERS
# --------------------------------------------------------------------------
@app.route("/parse_query", methods=["POST"])
@login_required
def parse_query():
    data = request.get_json(force=True)
    text = data.get("query", "").strip()

    if not text:
        return jsonify({"ok": False, "error": "Empty query"}), 400

    prompt = f"""
    You are a parser for an address-search engine.

    From this user query, extract:

    - state: 2-letter code (e.g. "OH")
    - county: county name only, no "County" word. (infer from city if needed)
    - city
    - zip: 5-digit ZIP or null
    - query: street or address text (e.g. "Heatherwood Ct") or "" if not specified
    - min_income: numeric or null
    - max_income: numeric or null
    - min_value: numeric or null
    - max_value: numeric or null
    - limit: 5000

    Return ONLY valid JSON.
    Example:

    {{
      "state": "OH",
      "county": "cuyahoga",
      "city": "berea",
      "zip": "44017",
      "query": "heatherwood ct",
      "min_income": null,
      "max_income": null,
      "min_value": null,
      "max_value": null,
      "limit": 5000
    }}

    User query: "{text}"
    """

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )

        content = resp.choices[0].message.content.strip()
        start = content.find("{")
        end = content.rfind("}")
        if start == -1 or end == -1:
            raise ValueError(f"LLM did not return JSON: {content}")

        json_str = content[start : end + 1]
        filters = json.loads(json_str)

        # Defaults
        filters.setdefault("state", "OH")
        filters.setdefault("county", "")
        filters.setdefault("city", "")
        filters.setdefault("zip", None)
        filters.setdefault("query", "")
        filters.setdefault("min_income", None)
        filters.setdefault("max_income", None)
        filters.setdefault("min_value", None)
        filters.setdefault("max_value", None)
        filters.setdefault("limit", 5000)

        # Cleanup county
        if filters.get("county"):
            c = filters["county"].lower().strip()
            if c.endswith(" county"):
                c = c[:-7]
            filters["county"] = c

        # Infer ZIP if we have city + state but no zip
        if not filters.get("zip") and filters.get("city") and filters.get("state"):
            inferred = infer_zip_from_city_state(filters["city"], filters["state"])
            if inferred:
                filters["zip"] = inferred

        log.info(f"Parsed filters: {filters}")
        return jsonify({"ok": True, "filters": filters})

    except Exception as e:
        log.exception("Error in /parse_query")
        return jsonify({"ok": False, "error": str(e)}), 500

# --------------------------------------------------------------------------
# CORE SEARCH — ZIP-FIRST WITH FALLBACKS
# --------------------------------------------------------------------------
def run_search(filters):
    state = (filters.get("state") or "OH").strip()
    county = (filters.get("county") or "").strip()
    city = (filters.get("city") or "").strip()
    zip_code = str(filters.get("zip")).strip() if filters.get("zip") not in (None, "") else ""
    query_text = (filters.get("query") or "").strip()

    def parse_num(v):
        if v in (None, ""):
            return None
        try:
            return float(str(v).replace(",", ""))
        except (TypeError, ValueError):
            return None

    min_income = parse_num(filters.get("min_income"))
    max_income = parse_num(filters.get("max_income"))
    min_value = parse_num(filters.get("min_value"))
    max_value = parse_num(filters.get("max_value"))

    limit = filters.get("limit") or 5000
    try:
        limit = min(int(limit), 50000)
    except (TypeError, ValueError):
        limit = 5000

    if not county:
        raise ValueError("County is required")

    geojson, key_used = load_county_geojson_from_s3(state, county)
    features = geojson.get("features", [])
    log.info(f"Filtering {len(features)} features from {key_used}")

    city_norm = normalize_str(city)
    query_norm = normalize_str(query_text)
    zip_norm = normalize_str(zip_code)

    def base_pass(f):
        props = f.get("properties", {}) or {}
        income_val = get_numeric_prop(props, INCOME_FIELDS)
        value_val = get_numeric_prop(props, HOME_VALUE_FIELDS)

        if min_income is not None or max_income is not None:
            if income_val is not None:
                if min_income is not None and income_val < min_income:
                    return False
                if max_income is not None and income_val > max_income:
                    return False

        if min_value is not None or max_value is not None:
            if value_val is not None:
                if min_value is not None and value_val < min_value:
                    return False
                if max_value is not None and value_val > max_value:
                    return False

        return True

    def get_zip(props):
        return normalize_str(
            get_first(
                props,
                ["POSTCODE", "postcode", "ZIP", "zip", "ZIPCODE", "postal_code"],
            )
        )

    def get_city(props):
        return normalize_str(
            get_first(
                props,
                ["CITY", "city", "PlaceName", "place_name", "town", "TOWN", "municipality"],
            )
        )

    def full_text(props):
        parts = [
            get_first(props, ["address", "NUMBER", "number", "addr:housenumber", "housenumber"]),
            get_first(props, ["STREET", "street", "addr:street", "road", "ROAD"]),
            get_first(props, ["UNIT", "unit", "apt", "APT", "suite", "SUITE"]),
            get_first(props, ["CITY", "city", "PlaceName", "place_name", "town", "TOWN", "municipality"]),
            get_first(props, ["POSTCODE", "postcode", "ZIP", "zip", "ZIPCODE", "postal_code"]),
        ]
        return normalize_str(" ".join(str(p) for p in parts if p))

    results = []

    # 1) ZIP-first
    if zip_norm:
        log.info(f"Stage 1: ZIP-first search for ZIP {zip_norm}")
        for f in features:
            props = f.get("properties", {}) or {}
            f_zip = get_zip(props)
            if f_zip.startswith(zip_norm) and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    break

    # 2) City substring
    if not results and city_norm:
        log.info(f"Stage 2: city-substring search for city '{city_norm}'")
        for f in features:
            props = f.get("properties", {}) or {}
            f_city = get_city(props)
            if city_norm in f_city and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    break

    # 3) Query substring
    if not results and query_norm:
        log.info(f"Stage 3: query-substring search for '{query_norm}'")
        for f in features:
            props = f.get("properties", {}) or {}
            ft = full_text(props)
            if query_norm in ft and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    break

    # 4) Fallback: first N
    if not results:
        log.info("Stage 4: fallback — returning first N features unfiltered")
        for f in features[:limit]:
            if base_pass(f):
                results.append(feature_to_result(f, state, county))

    log.info(f"Returning {len(results)} results")
    return results

# --------------------------------------------------------------------------
# ADVANCED SEARCH — JSON API
# --------------------------------------------------------------------------
@app.route("/search_advanced", methods=["POST"])
@login_required
def search_advanced():
    data = request.get_json(force=True) or {}
    try:
        results = run_search(data)
        return jsonify(
            {
                "ok": True,
                "count": len(results),
                "results": results,
            }
        )
    except Exception as e:
        log.exception("Error in /search_advanced")
        return jsonify({"ok": False, "error": str(e)}), 500

# --------------------------------------------------------------------------
# CSV DOWNLOAD
# --------------------------------------------------------------------------
@app.route("/download_csv", methods=["POST"])
@login_required
def download_csv():
    data = request.get_json(force=True) or {}
    try:
        results = run_search(data)

        fieldnames = [
            "address",
            "number",
            "street",
            "unit",
            "city",
            "state",
            "zip",
            "county",
            "lat",
            "lon",
            "income",
            "home_value",
        ]

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for r in results:
            writer.writerow(
                {
                    "address": r.get("address", ""),
                    "number": r.get("number", ""),
                    "street": r.get("street", ""),
                    "unit": r.get("unit", ""),
                    "city": r.get("city", ""),
                    "state": r.get("state", ""),
                    "zip": r.get("zip", ""),
                    "county": r.get("county", ""),
                    "lat": r.get("lat", ""),
                    "lon": r.get("lon", ""),
                    "income": r.get("income", ""),
                    "home_value": r.get("home_value", ""),
                }
            )

        csv_data = output.getvalue()
        output.close()

        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=addresses.csv"},
        )
    except Exception as e:
        log.exception("Error in /download_csv")
        return jsonify({"ok": False, "error": str(e)}), 500

# --------------------------------------------------------------------------
# PAGE ROUTES
# --------------------------------------------------------------------------
@app.route("/")
def index():
    if session.get("logged_in"):
        return redirect(url_for("app_page"))
    return redirect(url_for("login"))

@app.route("/app")
@login_required
def app_page():
    return render_template("app.html")

@app.route("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "env_openai": bool(OPENAI_API_KEY),
            "env_aws": bool(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY),
        }
    )

# --------------------------------------------------------------------------
# RUN
# --------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Pelée running → http://127.0.0.1:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
=======
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
from botocore.exceptions import ClientError
from openai import OpenAI

# --------------------------------------------------------------------------
# ENV + CONFIG
# --------------------------------------------------------------------------
load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "CaLuna")

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

client = OpenAI(api_key=OPENAI_API_KEY)

# --------------------------------------------------------------------------
# LOGGING
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# FLASK APP
# --------------------------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "super_secret_key")

# --------------------------------------------------------------------------
# AWS SESSION
# --------------------------------------------------------------------------
session_aws = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION,
)
s3 = session_aws.client("s3")

# --------------------------------------------------------------------------
# AUTH DECORATOR
# --------------------------------------------------------------------------
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "logged_in" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper

# --------------------------------------------------------------------------
# LOGIN ROUTES
# --------------------------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == ADMIN_PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("app_page"))
        else:
            error = "Incorrect password"
    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# --------------------------------------------------------------------------
# SMALL HELPERS
# --------------------------------------------------------------------------
INCOME_FIELDS = [
    "median_income",
    "MEDIAN_INCOME",
    "DP03_0062E",
    "INCOME",
    "income",
]

HOME_VALUE_FIELDS = [
    "median_home_value",
    "MEDIAN_HOME_VALUE",
    "DP04_0089E",
    "HOME_VALUE",
    "home_value",
]

def normalize_str(x):
    if x is None:
        return ""
    return str(x).strip().lower()

def get_first(props, keys):
    for k in keys:
        if k in props and props[k] not in (None, ""):
            return props[k]
    return ""

def get_numeric_prop(props, keys):
    for k in keys:
        if k in props and props[k] not in (None, ""):
            try:
                return float(str(props[k]).replace(",", ""))
            except (TypeError, ValueError):
                continue
    return None

def build_address_label(props, state_abbr):
    number = get_first(
        props,
        ["address", "NUMBER", "number", "addr:housenumber", "housenumber"],
    )
    street = get_first(
        props,
        ["STREET", "street", "addr:street", "road", "ROAD"],
    )
    unit = get_first(
        props,
        ["UNIT", "unit", "apt", "APT", "suite", "SUITE"],
    )
    city = get_first(
        props,
        ["CITY", "city", "PlaceName", "place_name", "town", "TOWN", "municipality"],
    )
    postcode = get_first(
        props,
        ["POSTCODE", "postcode", "ZIP", "zip", "ZIPCODE", "postal_code"],
    )

    if "address" in props and props["address"]:
        line1 = str(props["address"]).strip()
    else:
        parts = []
        if number:
            parts.append(str(number).strip())
        if street:
            parts.append(str(street).strip())
        if unit:
            parts.append(str(unit).strip())
        line1 = " ".join(parts)

    chunks = []
    if line1:
        chunks.append(line1)
    if city:
        chunks.append(str(city).strip())
    if state_abbr:
        chunks.append(state_abbr.upper())
    if postcode:
        chunks.append(str(postcode).strip())

    return ", ".join(chunks)

def feature_to_result(feature, state_abbr, county_slug):
    props = feature.get("properties", {}) or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or [None, None]

    number = get_first(
        props,
        ["NUMBER", "number", "addr:housenumber", "housenumber"],
    )
    street = get_first(
        props,
        ["STREET", "street", "addr:street", "road", "ROAD"],
    )
    unit = get_first(
        props,
        ["UNIT", "unit", "apt", "APT", "suite", "SUITE"],
    )
    city = get_first(
        props,
        ["CITY", "city", "PlaceName", "place_name", "town", "TOWN", "municipality"],
    )
    state_val = get_first(
        props,
        ["REGION", "region", "STATE", "state"],
    ) or state_abbr.upper()
    postcode = get_first(
        props,
        ["POSTCODE", "postcode", "ZIP", "zip", "ZIPCODE", "postal_code"],
    )

    income_val = get_numeric_prop(props, INCOME_FIELDS)
    home_val = get_numeric_prop(props, HOME_VALUE_FIELDS)

    label = build_address_label(props, state_abbr)

    return {
        "address": label,
        "label": label,
        "number": number,
        "street": street,
        "unit": unit,
        "city": city,
        "state": state_val,
        "zip": postcode,
        "county": county_slug.lower(),
        "lat": coords[1] if len(coords) > 1 else None,
        "lon": coords[0] if len(coords) > 0 else None,
        "income": income_val,
        "home_value": home_val,
        "properties": props,
    }

# --------------------------------------------------------------------------
# S3 LOADER — GEOJSON OR NDJSON
# --------------------------------------------------------------------------
def load_county_geojson_from_s3(state_abbr: str, county_slug: str):
    state = state_abbr.lower()
    county = (
        county_slug.lower()
        .replace(" county", "")
        .replace("county", "")
        .strip()
        .replace(" ", "_")
    )

    candidate_keys = [
        f"{state}/{county}-with-values-income.geojson",
        f"{state}/{county}-addresses-county.geojson",
        f"{state}/{county}.geojson",
    ]

    last_error = None

    for key in candidate_keys:
        try:
            log.info(f"Trying S3 key: {key}")
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            raw = obj["Body"].read().decode("utf-8", errors="replace").strip()

            # Try FeatureCollection first
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and "features" in parsed:
                    log.info(f"Loaded FeatureCollection from {key}")
                    return parsed, key
            except json.JSONDecodeError:
                pass

            # Otherwise treat as NDJSON
            features = []
            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj_line = json.loads(line)
                    if isinstance(obj_line, dict) and obj_line.get("type") == "Feature":
                        features.append(obj_line)
                except Exception:
                    continue

            if features:
                log.info(f"Loaded NDJSON with {len(features)} features from {key}")
                return {"type": "FeatureCollection", "features": features}, key

        except Exception as e:
            last_error = e
            log.warning(f"Failed loading {key}: {e}")

    raise FileNotFoundError(
        f"No GeoJSON/NDJSON found for {state}/{county}. Tried: {candidate_keys}. Last error: {last_error}"
    )

# --------------------------------------------------------------------------
# ZIP INFERENCE — CITY + STATE → PRIMARY ZIP
# --------------------------------------------------------------------------
def infer_zip_from_city_state(city, state):
    city_norm = (city or "").strip()
    state_norm = (state or "").strip()
    if not city_norm or not state_norm:
        return None

    prompt = f"""
    You are a precise US ZIP code lookup engine.

    City: "{city_norm}"
    State: "{state_norm}" (2-letter code or full state name)

    Respond with a SINGLE 5-digit ZIP code only.
    Example: 44017
    """

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        content = resp.choices[0].message.content.strip()
        digits = "".join(ch for ch in content if ch.isdigit())
        if len(digits) == 5:
            return digits
    except Exception as e:
        log.warning(f"ZIP inference failed for {city_norm}, {state_norm}: {e}")

    return None

# --------------------------------------------------------------------------
# NLP PARSER — NATURAL LANGUAGE → FILTERS
# --------------------------------------------------------------------------
@app.route("/parse_query", methods=["POST"])
@login_required
def parse_query():
    data = request.get_json(force=True)
    text = data.get("query", "").strip()

    if not text:
        return jsonify({"ok": False, "error": "Empty query"}), 400

    prompt = f"""
    You are a parser for an address-search engine.

    From this user query, extract:

    - state: 2-letter code (e.g. "OH")
    - county: county name only, no "County" word. (infer from city if needed)
    - city
    - zip: 5-digit ZIP or null
    - query: street or address text (e.g. "Heatherwood Ct") or "" if not specified
    - min_income: numeric or null
    - max_income: numeric or null
    - min_value: numeric or null
    - max_value: numeric or null
    - limit: 5000

    Return ONLY valid JSON.
    Example:

    {{
      "state": "OH",
      "county": "cuyahoga",
      "city": "berea",
      "zip": "44017",
      "query": "heatherwood ct",
      "min_income": null,
      "max_income": null,
      "min_value": null,
      "max_value": null,
      "limit": 5000
    }}

    User query: "{text}"
    """

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )

        content = resp.choices[0].message.content.strip()
        start = content.find("{")
        end = content.rfind("}")
        if start == -1 or end == -1:
            raise ValueError(f"LLM did not return JSON: {content}")

        json_str = content[start : end + 1]
        filters = json.loads(json_str)

        # Defaults
        filters.setdefault("state", "OH")
        filters.setdefault("county", "")
        filters.setdefault("city", "")
        filters.setdefault("zip", None)
        filters.setdefault("query", "")
        filters.setdefault("min_income", None)
        filters.setdefault("max_income", None)
        filters.setdefault("min_value", None)
        filters.setdefault("max_value", None)
        filters.setdefault("limit", 5000)

        # Cleanup county
        if filters.get("county"):
            c = filters["county"].lower().strip()
            if c.endswith(" county"):
                c = c[:-7]
            filters["county"] = c

        # Infer ZIP if we have city + state but no zip
        if not filters.get("zip") and filters.get("city") and filters.get("state"):
            inferred = infer_zip_from_city_state(filters["city"], filters["state"])
            if inferred:
                filters["zip"] = inferred

        log.info(f"Parsed filters: {filters}")
        return jsonify({"ok": True, "filters": filters})

    except Exception as e:
        log.exception("Error in /parse_query")
        return jsonify({"ok": False, "error": str(e)}), 500

# --------------------------------------------------------------------------
# CORE SEARCH — ZIP-FIRST WITH FALLBACKS
# --------------------------------------------------------------------------
def run_search(filters):
    state = (filters.get("state") or "OH").strip()
    county = (filters.get("county") or "").strip()
    city = (filters.get("city") or "").strip()
    zip_code = str(filters.get("zip")).strip() if filters.get("zip") not in (None, "") else ""
    query_text = (filters.get("query") or "").strip()

    def parse_num(v):
        if v in (None, ""):
            return None
        try:
            return float(str(v).replace(",", ""))
        except (TypeError, ValueError):
            return None

    min_income = parse_num(filters.get("min_income"))
    max_income = parse_num(filters.get("max_income"))
    min_value = parse_num(filters.get("min_value"))
    max_value = parse_num(filters.get("max_value"))

    limit = filters.get("limit") or 5000
    try:
        limit = min(int(limit), 50000)
    except (TypeError, ValueError):
        limit = 5000

    if not county:
        raise ValueError("County is required")

    geojson, key_used = load_county_geojson_from_s3(state, county)
    features = geojson.get("features", [])
    log.info(f"Filtering {len(features)} features from {key_used}")

    city_norm = normalize_str(city)
    query_norm = normalize_str(query_text)
    zip_norm = normalize_str(zip_code)

    def base_pass(f):
        props = f.get("properties", {}) or {}
        income_val = get_numeric_prop(props, INCOME_FIELDS)
        value_val = get_numeric_prop(props, HOME_VALUE_FIELDS)

        if min_income is not None or max_income is not None:
            if income_val is not None:
                if min_income is not None and income_val < min_income:
                    return False
                if max_income is not None and income_val > max_income:
                    return False

        if min_value is not None or max_value is not None:
            if value_val is not None:
                if min_value is not None and value_val < min_value:
                    return False
                if max_value is not None and value_val > max_value:
                    return False

        return True

    def get_zip(props):
        return normalize_str(
            get_first(
                props,
                ["POSTCODE", "postcode", "ZIP", "zip", "ZIPCODE", "postal_code"],
            )
        )

    def get_city(props):
        return normalize_str(
            get_first(
                props,
                ["CITY", "city", "PlaceName", "place_name", "town", "TOWN", "municipality"],
            )
        )

    def full_text(props):
        parts = [
            get_first(props, ["address", "NUMBER", "number", "addr:housenumber", "housenumber"]),
            get_first(props, ["STREET", "street", "addr:street", "road", "ROAD"]),
            get_first(props, ["UNIT", "unit", "apt", "APT", "suite", "SUITE"]),
            get_first(props, ["CITY", "city", "PlaceName", "place_name", "town", "TOWN", "municipality"]),
            get_first(props, ["POSTCODE", "postcode", "ZIP", "zip", "ZIPCODE", "postal_code"]),
        ]
        return normalize_str(" ".join(str(p) for p in parts if p))

    results = []

    # 1) ZIP-first
    if zip_norm:
        log.info(f"Stage 1: ZIP-first search for ZIP {zip_norm}")
        for f in features:
            props = f.get("properties", {}) or {}
            f_zip = get_zip(props)
            if f_zip.startswith(zip_norm) and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    break

    # 2) City substring
    if not results and city_norm:
        log.info(f"Stage 2: city-substring search for city '{city_norm}'")
        for f in features:
            props = f.get("properties", {}) or {}
            f_city = get_city(props)
            if city_norm in f_city and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    break

    # 3) Query substring
    if not results and query_norm:
        log.info(f"Stage 3: query-substring search for '{query_norm}'")
        for f in features:
            props = f.get("properties", {}) or {}
            ft = full_text(props)
            if query_norm in ft and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    break

    # 4) Fallback: first N
    if not results:
        log.info("Stage 4: fallback — returning first N features unfiltered")
        for f in features[:limit]:
            if base_pass(f):
                results.append(feature_to_result(f, state, county))

    log.info(f"Returning {len(results)} results")
    return results

# --------------------------------------------------------------------------
# ADVANCED SEARCH — JSON API
# --------------------------------------------------------------------------
@app.route("/search_advanced", methods=["POST"])
@login_required
def search_advanced():
    data = request.get_json(force=True) or {}
    try:
        results = run_search(data)
        return jsonify(
            {
                "ok": True,
                "count": len(results),
                "results": results,
            }
        )
    except Exception as e:
        log.exception("Error in /search_advanced")
        return jsonify({"ok": False, "error": str(e)}), 500

# --------------------------------------------------------------------------
# CSV DOWNLOAD
# --------------------------------------------------------------------------
@app.route("/download_csv", methods=["POST"])
@login_required
def download_csv():
    data = request.get_json(force=True) or {}
    try:
        results = run_search(data)

        fieldnames = [
            "address",
            "number",
            "street",
            "unit",
            "city",
            "state",
            "zip",
            "county",
            "lat",
            "lon",
            "income",
            "home_value",
        ]

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for r in results:
            writer.writerow(
                {
                    "address": r.get("address", ""),
                    "number": r.get("number", ""),
                    "street": r.get("street", ""),
                    "unit": r.get("unit", ""),
                    "city": r.get("city", ""),
                    "state": r.get("state", ""),
                    "zip": r.get("zip", ""),
                    "county": r.get("county", ""),
                    "lat": r.get("lat", ""),
                    "lon": r.get("lon", ""),
                    "income": r.get("income", ""),
                    "home_value": r.get("home_value", ""),
                }
            )

        csv_data = output.getvalue()
        output.close()

        return Response(
            csv_data,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=addresses.csv"},
        )
    except Exception as e:
        log.exception("Error in /download_csv")
        return jsonify({"ok": False, "error": str(e)}), 500

# --------------------------------------------------------------------------
# PAGE ROUTES
# --------------------------------------------------------------------------
@app.route("/")
def index():
    if session.get("logged_in"):
        return redirect(url_for("app_page"))
    return redirect(url_for("login"))

@app.route("/app")
@login_required
def app_page():
    return render_template("app.html")

@app.route("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "env_openai": bool(OPENAI_API_KEY),
            "env_aws": bool(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY),
        }
    )

# --------------------------------------------------------------------------
# RUN
# --------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Pelée running → http://127.0.0.1:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
>>>>>>> 78dabbb23441e21978acaf863cfa16a314765349
=======
import os
import json
from flask import Flask, request, jsonify, render_template
import boto3
import logging

# -----------------------------
# App setup
# -----------------------------
app = Flask(__name__)

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# -----------------------------
# Environment variables (Render sets these)
# -----------------------------
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
S3_BUCKET = os.environ.get("S3_BUCKET", "residential-data-jack")

# -----------------------------
# S3 Client
# -----------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
)

# -----------------------------
# Helper: load county JSON from S3
# -----------------------------
def load_county_json(state, county):
    key = f"{state.lower()}/{county.lower()}-addresses-county.geojson"
    try:
        logging.info(f"Trying S3 key: {key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.load(obj['Body'])
        logging.info(f"Loaded NDJSON with {len(data['features'])} features from {key}")
        return data
    except Exception as e:
        logging.warning(f"Failed loading {key}: {e}")
        return None

# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/search_advanced", methods=["POST"])
def search_advanced():
    filters = request.get_json()
    state = filters.get("state")
    county = filters.get("county")
    city = filters.get("city", "").lower()
    zip_code = filters.get("zip", "").strip()
    limit = int(filters.get("limit", 20))

    county_data = load_county_json(state, county)
    if not county_data:
        return jsonify({"ok": False, "error": f"No data found for {county}, {state}"}), 404

    results = []
    for feature in county_data["features"]:
        props = feature["properties"]
        addr_city = props.get("city", "").lower()
        addr_zip = props.get("postcode", "").strip()

        if city and city not in addr_city:
            continue
        if zip_code and zip_code != addr_zip:
            continue

        results.append({
            "address": f"{props.get('number','')} {props.get('street','')}".strip(),
            "city": props.get("city",""),
            "state": props.get("region",""),
            "zip": props.get("postcode","")
        })
        if len(results) >= limit:
            break

    logging.info(f"Returning {len(results)} results")
    return jsonify({"ok": True, "count": len(results), "results": results})

@app.route("/parse_query", methods=["POST"])
def parse_query():
    # Placeholder for OpenAI parsing integration
    data = request.get_json()
    query = data.get("query", "")
    logging.info(f"Parsing query: {query}")

    # For now, naive parsing (example: "Berea, OH" → state=OH, city=Berea)
    state = "OH" if "OH" in query else ""
    city = query.split(",")[0] if "," in query else query
    county = ""
    zip_code = ""

    filters = {
        "state": state,
        "county": county,
        "city": city.lower(),
        "zip": zip_code,
        "query": "",
        "min_income": None,
        "max_income": None,
        "min_value": None,
        "max_value": None,
        "limit": 20
    }

    logging.info(f"Parsed filters: {filters}")
    return jsonify({"ok": True, "filters": filters})

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
>>>>>>> d68ade903b57e8df11271b0f2df5ea588734bbb5

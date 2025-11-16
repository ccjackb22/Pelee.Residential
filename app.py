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
        # If you want to disable login, just return f(*args, **kwargs) here.
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
            return redirect(url_for("home"))
        else:
            error = "Incorrect password"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# --------------------------------------------------------------------------
# HELPERS
# --------------------------------------------------------------------------
def normalize_str(x):
    return str(x).strip().lower() if x is not None else ""


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
            except Exception:
                continue
    return None


INCOME_FIELDS = ["median_income", "MEDIAN_INCOME", "DP03_0062E", "income"]
HOME_VALUE_FIELDS = [
    "median_home_value",
    "MEDIAN_HOME_VALUE",
    "DP04_0089E",
    "home_value",
]


def build_label(props, state):
    num = get_first(props, ["NUMBER", "number"])
    street = get_first(props, ["STREET", "street"])
    unit = get_first(props, ["unit", "UNIT", "apt"])
    city = get_first(props, ["CITY", "city"])
    zipc = get_first(props, ["ZIP", "zip", "POSTCODE", "postcode"])

    parts = []
    if num:
        parts.append(str(num))
    if street:
        parts.append(str(street))
    if unit:
        parts.append(str(unit))
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
        "zip": get_first(props, ["ZIP", "zip", "POSTCODE", "postcode"]),
        "county": county,
        "lat": coords[1],
        "lon": coords[0],
        "income": get_numeric_prop(props, INCOME_FIELDS),
        "home_value": get_numeric_prop(props, HOME_VALUE_FIELDS),
    }

# --------------------------------------------------------------------------
# LOAD COUNTY FILE FROM S3
# --------------------------------------------------------------------------
def load_county_geojson(state_abbr, county_slug):
    """
    Load a single county GeoJSON from S3.

    It will try (in order):
      {state}/{county}-with-values-income.geojson
      {state}/{county}-addresses-county.geojson
      {state}/{county}.geojson
    """
    state = (state_abbr or "").lower()
    county = (county_slug or "").lower().replace(" county", "").replace(" ", "_")

    if not state or not county:
        raise ValueError("State and county are required to load county GeoJSON")

    candidates = [
        f"{state}/{county}-with-values-income.geojson",
        f"{state}/{county}-addresses-county.geojson",
        f"{state}/{county}.geojson",
    ]

    last_error = None

    for key in candidates:
        try:
            log.info(f"Trying S3 key: {key}")
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            text = obj["Body"].read().decode("utf-8", errors="replace").strip()

            # Try standard GeoJSON FeatureCollection
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict) and "features" in parsed:
                    return parsed
            except Exception:
                pass

            # Fallback: NDJSON
            feats = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    o = json.loads(line)
                    if o.get("type") == "Feature":
                        feats.append(o)
                except Exception:
                    continue

            if feats:
                return {"type": "FeatureCollection", "features": feats}
        except Exception as e:
            last_error = e
            log.warning(f"Failed {key}: {e}")

    raise FileNotFoundError(
        f"No county file found in S3 for {state}/{county}. Last error: {last_error}"
    )

# --------------------------------------------------------------------------
# ZIP & COUNTY INFERENCE
# --------------------------------------------------------------------------
def infer_zip_from_city_state(city, state):
    if not city or not state:
        return None

    prompt = (
        "Given the following US city and state, return ONLY a single 5-digit ZIP code.\n"
        "If there are many ZIP codes, choose the most central / representative one.\n\n"
        f"City: {city}\nState: {state}\n\n"
        "Output: just the 5 digit ZIP, no other text."
    )

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        text = resp.choices[0].message.content.strip()
        digits = "".join(c for c in text if c.isdigit())
        return digits if len(digits) == 5 else None
    except Exception as e:
        log.warning(f"infer_zip_from_city_state failed: {e}")
        return None


def infer_county_from_city_state(city, state):
    """
    Use the model to infer the county name from a city+state.
    Returns lowercase county name WITHOUT the word 'county'
    (e.g., 'cuyahoga', 'pinellas').
    """
    if not city or not state:
        return None

    prompt = (
        "You are a US geography assistant.\n"
        "Given a city (or town, village, or suburb) and a state, "
        "return ONLY the name of the county it is located in.\n\n"
        "Rules:\n"
        "- Output just the county name.\n"
        "- Do NOT include the word 'County' or anything else.\n"
        "- Use standard English county name, e.g. 'cuyahoga', 'los angeles', 'pinellas'.\n\n"
        f"City: {city}\nState: {state}\n\n"
        "Output: county name only."
    )

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        text = resp.choices[0].message.content.strip()
        # Normalize
        text = text.replace("County", "").replace("county", "").strip()
        text = text.replace(".", "").strip()
        return text.lower() if text else None
    except Exception as e:
        log.warning(f"infer_county_from_city_state failed: {e}")
        return None

# --------------------------------------------------------------------------
# PARSE QUERY (USED BY CHAT UI)
# --------------------------------------------------------------------------
@app.route("/parse_query", methods=["POST"])
@login_required
def parse_query():
    """
    Takes raw natural language in {query: "..."} and returns structured filters.
    Your front-end calls this first from the chat UI.
    """
    text = (request.json or {}).get("query", "").strip()
    if not text:
        return jsonify({"ok": False, "error": "Empty query"}), 400

    prompt = f"""
You are a strict JSON API. You parse natural-language targeting queries for US address data.

User query:
{text}

Return a JSON object with EXACTLY these keys:

{{
  "state": "two-letter USPS code, lowercase, e.g. 'oh' or 'fl'. If unknown, null.",
  "county": "county name in lowercase without the word 'county', e.g. 'cuyahoga'. If unknown, null.",
  "city": "city or place name as written by the user, or null",
  "zip": "5-digit ZIP or null",
  "query": "original query text",
  "min_income": "minimum household income as a number, or null",
  "max_income": "maximum household income as a number, or null",
  "min_value": "minimum home value as a number, or null",
  "max_value": "maximum home value as a number, or null",
  "limit": 5000
}}

Important:
- Use your knowledge of US geography to infer the county from city+state when possible.
- If the user mentions a city and state (e.g. 'Strongsville OH'), fill:
  state = 'oh'
  county = 'cuyahoga'
  city = 'Strongsville'
- All numbers (income, values) must be plain numbers, no '$' or commas.
- If you are unsure, use null.

Output ONLY the JSON, no explanation.
"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        blob = resp.choices[0].message.content

        # Extract JSON object from text
        start = blob.find("{")
        end = blob.rfind("}")
        if start == -1 or end == -1:
            raise ValueError(f"Model did not return JSON: {blob}")

        filters = json.loads(blob[start : end + 1])

        # Normalize basics
        filters["query"] = text
        state = filters.get("state")
        city = filters.get("city")
        county = filters.get("county")

        # Ensure lowercase state / county
        if isinstance(state, str):
            filters["state"] = state.lower().strip()
        if isinstance(county, str):
            filters["county"] = county.lower().replace(" county", "").strip()

        # Try to infer county if missing but we have city+state
        if (not filters.get("county")) and city and filters.get("state"):
            inferred_county = infer_county_from_city_state(city, filters["state"])
            if inferred_county:
                filters["county"] = inferred_county
                log.info(f"Inferred county={inferred_county} from {city}, {filters['state']}")

        # Try to infer ZIP if missing but city+state present
        if (not filters.get("zip")) and city and filters.get("state"):
            inferred_zip = infer_zip_from_city_state(city, filters["state"])
            if inferred_zip:
                filters["zip"] = inferred_zip
                log.info(f"Inferred zip={inferred_zip} from {city}, {filters['state']}")

        log.info(f"Parsed filters: {filters}")
        return jsonify({"ok": True, "filters": filters})

    except Exception as e:
        log.exception("parse_query error")
        return jsonify({"ok": False, "error": str(e)}), 500

# --------------------------------------------------------------------------
# RUN SEARCH
# --------------------------------------------------------------------------
def run_search(filters):
    """
    Core search over a single county file.
    filters is a dict like the object returned by /parse_query.
    """
    state = filters.get("state")
    county = filters.get("county")
    city = normalize_str(filters.get("city"))
    zipc = normalize_str(filters.get("zip"))
    query_text = normalize_str(filters.get("query"))
    limit = int(filters.get("limit") or 5000)

    # Normalize numeric bounds
    def to_float_or_none(x):
        if x in (None, "", "null"):
            return None
        try:
            return float(x)
        except Exception:
            return None

    min_income = to_float_or_none(filters.get("min_income"))
    max_income = to_float_or_none(filters.get("max_income"))
    min_value = to_float_or_none(filters.get("min_value"))
    max_value = to_float_or_none(filters.get("max_value"))

    if not state or not county:
        raise ValueError("State and county are required for search (could not infer from query).")

    data = load_county_geojson(state, county)
    features = data.get("features", [])
    results = []

    def base_pass(f):
        props = f.get("properties", {})
        inc = get_numeric_prop(props, INCOME_FIELDS)
        val = get_numeric_prop(props, HOME_VALUE_FIELDS)

        if min_income is not None and inc is not None and inc < min_income:
            return False
        if max_income is not None and inc is not None and inc > max_income:
            return False
        if min_value is not None and val is not None and val < min_value:
            return False
        if max_value is not None and val is not None and val > max_value:
            return False
        return True

    def prop_zip(f):
        return normalize_str(
            get_first(
                f.get("properties", {}),
                ["ZIP", "zip", "POSTCODE", "postcode"],
            )
        )

    def prop_city(f):
        return normalize_str(
            get_first(
                f.get("properties", {}),
                ["CITY", "city", "PlaceName"],
            )
        )

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

    # 1) ZIP-first
    if zipc:
        for f in features:
            if prop_zip(f).startswith(zipc) and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    return results

    # 2) City match
    if not results and city:
        for f in features:
            if city in prop_city(f) and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    return results

    # 3) Free text match
    if not results and query_text:
        for f in features:
            if query_text in full_text(f) and base_pass(f):
                results.append(feature_to_result(f, state, county))
                if len(results) >= limit:
                    return results

    # 4) Fallback: first N that pass numeric filters
    for f in features:
        if base_pass(f):
            results.append(feature_to_result(f, state, county))
            if len(results) >= limit:
                break

    return results

# --------------------------------------------------------------------------
# API ROUTES (USED BY CHAT UI)
# --------------------------------------------------------------------------
@app.route("/search_advanced", methods=["POST"])
@login_required
def search_advanced():
    """
    Expects JSON filters (what /parse_query returns in 'filters').
    Your chat JS calls this AFTER /parse_query.
    """
    try:
        filters = request.get_json(force=True)
        if not isinstance(filters, dict):
            raise ValueError("Invalid filters payload")

        results = run_search(filters)
        return jsonify(
            {
                "ok": True,
                "count": len(results),
                "results": results,
            }
        )
    except Exception as e:
        log.exception("search_advanced error")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/download_csv", methods=["POST"])
@login_required
def download_csv():
    """
    Optional CSV export endpoint that accepts the same filters JSON
    as /search_advanced, runs run_search, and streams CSV back.
    """
    try:
        filters = request.get_json(force=True)
        if not isinstance(filters, dict):
            raise ValueError("Invalid filters payload")

        rows = run_search(filters)

        fieldnames = [
            "address",
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
        for r in rows:
            writer.writerow(r)

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=addresses.csv"},
        )
    except Exception as e:
        log.exception("download_csv error")
        return jsonify({"ok": False, "error": str(e)}), 500

# --------------------------------------------------------------------------
# PAGE ROUTES
# --------------------------------------------------------------------------
@app.route("/")
def root():
    # If you want to bypass login completely, just:
    # return render_template("index.html")
    if session.get("logged_in"):
        return redirect(url_for("home"))
    return redirect(url_for("login"))


@app.route("/home")
@login_required
def home():
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "openai": bool(OPENAI_API_KEY),
            "aws": bool(AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY),
        }
    )

# --------------------------------------------------------------------------
# RUN LOCAL
# --------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Pelée running → http://127.0.0.1:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)

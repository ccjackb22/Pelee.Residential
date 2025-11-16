import os
import json
import csv
import io

import boto3
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    session,
    jsonify,
    make_response,
)
from botocore.config import Config
from openai import OpenAI

# ----------------------------
# CONFIG
# ----------------------------
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

s3 = boto3.client(
    "s3",
    region_name="us-east-2",
    config=Config(signature_version="s3v4"),
)

BUCKET = "residential-data-jack"
PASSWORD = "CaLuna"

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret")

# Just for the /loading endpoint (front-end is only showing a CSS bar on submit)
loading_state = {"active": False}


# ----------------------------
# LOGIN SYSTEM
# ----------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        else:
            return render_template("login.html", error="Incorrect password")
    return render_template("login.html")


@app.before_request
def require_login():
    # allow login page and static files without auth
    if request.endpoint in ("login", "static"):
        return
    if not session.get("logged_in"):
        return redirect("/login")


# ----------------------------
# GPT: QUERY → FILTERS
# ----------------------------
def parse_query(prompt: str) -> dict:
    """
    Ask GPT to turn a natural-language query into structured filters.
    Expected JSON fields:
      city, state, county, street,
      min_income, max_income, min_value, max_value
    """
    system_msg = (
        "Convert user real-estate queries into structured filters.\n"
        "Return JSON ONLY (no prose, no code fences).\n"
        "Allowed keys: city, state, county, min_income, max_income, "
        "min_value, max_value, street.\n"
        "If city is mentioned, output city and state.\n"
        "If only county is mentioned, output county and state.\n"
        "Income/value fields should be numeric.\n"
        "Never include comments or explanations, JSON only."
    )

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": prompt},
        ],
    )

    content = resp.choices[0].message.content
    try:
        data = json.loads(content)
    except Exception:
        return {}

    # Coerce numerics where possible
    for key in ("min_income", "max_income", "min_value", "max_value"):
        if key in data and data[key] is not None:
            try:
                data[key] = float(data[key])
            except Exception:
                data[key] = None

    # Normalize strings
    for key in ("city", "state", "county", "street"):
        if key in data and data[key] is not None:
            data[key] = str(data[key]).strip()

    return data


# ----------------------------
# GPT: CITY+STATE → COUNTY
# ----------------------------
def lookup_county(city: str, state: str) -> str | None:
    """
    Ask GPT for 'Cuyahoga County' style answer, then we normalize it.
    """
    prompt = f"Return ONLY the county name (no extra words) for: {city}, {state}."
    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": "Reply with the county name only, e.g. 'Cuyahoga County'."},
            {"role": "user", "content": prompt},
        ],
    )
    raw = resp.choices[0].message.content.strip()
    return raw or None


# ----------------------------
# S3 LOADER
# ----------------------------
def _slugify_county(name: str) -> str:
    """
    Convert 'Cuyahoga County' → 'cuyahoga'
    'Palm Beach County' → 'palm_beach'
    """
    n = name.strip().lower()
    # Remove common suffixes
    for suf in (" county", " parish", " borough", " census area", " city"):
        if n.endswith(suf):
            n = n[: -len(suf)]
            break
    n = n.replace("&", "and")
    n = n.replace("'", "")
    n = n.replace(".", "")
    n = n.replace("-", "_")
    n = n.replace(" ", "_")
    return n


def load_county_file(state: str | None, county_name: str | None):
    """
    Try multiple S3 key patterns so we can support:
      - cuyahoga-with-values-income.geojson
      - cuyahoga.geojson
      - merged_with_tracts/pinellas-addresses-county-with-tracts.geojson
      - merged_with_tracts/pa/union-with-values-income.geojson
    """
    if not county_name:
        return None

    slug = _slugify_county(county_name)
    state_slug = (state or "").strip().lower()

    candidate_keys: list[str] = []

    # Root-level enriched files (Ohio-style)
    candidate_keys.append(f"{slug}-with-values-income.geojson")
    candidate_keys.append(f"{slug}.geojson")

    # State subfolder variants
    if state_slug:
        candidate_keys.append(f"{state_slug}/{slug}-with-values-income.geojson")
        candidate_keys.append(f"{state_slug}/{slug}.geojson")

    # merged_with_tracts, both with and without state subfolder
    candidate_keys.extend(
        [
            f"merged_with_tracts/{slug}-addresses-county-with-tracts.geojson",
            f"merged_with_tracts/{slug}-parcels-county-with-tracts.geojson",
        ]
    )
    if state_slug:
        candidate_keys.extend(
            [
                f"merged_with_tracts/{state_slug}/{slug}-with-values-income.geojson",
                f"merged_with_tracts/{state_slug}/{slug}-addresses-county-with-tracts.geojson",
                f"merged_with_tracts/{state_slug}/{slug}-parcels-county-with-tracts.geojson",
            ]
        )

    # Some FL-style: palm_beach_county-*
    candidate_keys.extend(
        [
            f"merged_with_tracts/{slug}_county-addresses-county-with-tracts.geojson",
            f"merged_with_tracts/{slug}_county-parcels-county-with-tracts.geojson",
        ]
    )

    for key in candidate_keys:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            return json.loads(body)
        except Exception:
            continue

    return None


# ----------------------------
# FILTER LOGIC
# ----------------------------
def matches(feature: dict, f: dict) -> bool:
    props = feature.get("properties") or {}

    # Street contains (very literal)
    street_filter = f.get("street")
    if street_filter:
        if street_filter.lower() not in (props.get("street") or "").lower():
            return False

    # City: allow sloppy matches ("Berea" vs "Berea City")
    city_filter = f.get("city")
    if city_filter:
        cf = city_filter.lower()
        pc = (props.get("city") or "").lower()
        if cf not in pc and pc not in cf:
            return False

    # Min income (DP03_0062E)
    min_income = f.get("min_income")
    if min_income is not None:
        raw = props.get("DP03_0062E")
        if raw is None:
            return False
        try:
            if float(raw) < float(min_income):
                return False
        except Exception:
            return False

    # Min home value (DP04_0089E)
    min_value = f.get("min_value")
    if min_value is not None:
        raw = props.get("DP04_0089E")
        if raw is None:
            return False
        try:
            if float(raw) < float(min_value):
                return False
        except Exception:
            return False

    return True


# ----------------------------
# ROUTES
# ----------------------------
@app.route("/")
def index():
    # index.html handles how results/errors are rendered into the chat box
    return render_template("index.html")


@app.route("/loading")
def loading():
    return jsonify(loading_state)


@app.route("/search", methods=["POST"])
def search():
    global loading_state
    loading_state["active"] = True

    query = request.form.get("query", "").strip()
    if not query:
        loading_state["active"] = False
        return render_template("index.html", error="Please enter a query.", results=[])

    # 1) NLP → filters
    filters = parse_query(query)
    if not filters:
        loading_state["active"] = False
        return render_template(
            "index.html",
            error="I couldn't understand that query.",
            results=[],
        )

    state = filters.get("state")
    county = filters.get("county")

    # 2) If we have city+state but no county, ask GPT for the county
    if not county and filters.get("city") and state:
        county_name = lookup_county(filters["city"], state)
        if county_name:
            filters["county"] = county_name
            county = county_name

    if not state or not county:
        loading_state["active"] = False
        return render_template(
            "index.html",
            error="State and county could not be determined from the query.",
            results=[],
        )

    # 3) Load dataset from S3
    dataset = load_county_file(state, county)
    if not dataset:
        loading_state["active"] = False
        # show a clean error in the right-hand box
        err_msg = f"Dataset not found for {county}, {state}."
        return render_template("index.html", error=err_msg, results=[])

    # 4) Filter features
    results: list[dict] = []
    for feat in dataset.get("features", []):
        if matches(feat, filters):
            # Properties only — index.html expects r.number, r.street, etc.
            props = feat.get("properties") or {}
            results.append(props)
            if len(results) >= 500:
                break

    loading_state["active"] = False

    if not results:
        return render_template(
            "index.html",
            error="No matching addresses found.",
            results=[],
        )

    return render_template("index.html", results=results)


@app.route("/export")
def export():
    """
    Very simple export placeholder.
    Right now it just tells the user to re-run via a backend script.
    We are not wiring full CSV export yet to avoid blowing up cookie sessions.
    """
    return "Export not wired yet. (Safe placeholder so the link doesn't 404.)", 200


# ----------------------------
# RUN (local dev)
# ----------------------------
if __name__ == "__main__":
    # For local testing only; Render uses gunicorn
    app.run(host="0.0.0.0", port=5000, debug=True)

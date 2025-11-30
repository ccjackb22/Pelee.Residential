import os
import csv
import io
import re
import logging
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file
)
from dotenv import load_dotenv
import boto3
import requests

# -------------------------------------------------------------------
# BASIC SETUP
# -------------------------------------------------------------------

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev_secret_key")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -------------------------------------------------------------------
# AWS S3 CONFIG
# -------------------------------------------------------------------

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

S3_BUCKET = "residential-data-jack"
S3_PREFIX = "merged_with_tracts_acs_clean"

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

COUNTY_INDEX = {}  # {("ohio","cuyahoga"): "merged_with_tracts_acs_clean/ohio/cuyahoga.csv"}

# -------------------------------------------------------------------
# GOOGLE PLACES CONFIG
# -------------------------------------------------------------------

GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def norm(x):
    return x.strip().lower().replace(".", "").replace(",", "") if x else ""

STATE_ALIASES = {
    "oh": "ohio",
    "fl": "florida",
    "tx": "texas",
    "nc": "north carolina",
    "sc": "south carolina",
    "ga": "georgia",
    "ca": "california",
    "ny": "new york",
    "pa": "pennsylvania",
    "mi": "michigan",
    "wa": "washington",
    "or": "oregon",
    "il": "illinois",
    "in": "indiana",
    "va": "virginia",
}

def canonical_state(x):
    x = norm(x)
    return STATE_ALIASES.get(x, x)

def extract_county_state(query):
    """
    Extract a (county, state) like:
    'residential addresses in Cuyahoga County Ohio'
    """
    q = norm(query)
    tokens = q.split()

    if "county" not in tokens:
        return None, None

    idx = tokens.index("county")
    if idx == 0:
        return None, None

    county = " ".join(tokens[idx - 1:idx])
    if idx + 1 < len(tokens):
        state = canonical_state(" ".join(tokens[idx+1:]))
    else:
        state = None

    if not county or not state:
        return None, None

    county = county.replace(" county", "").strip()
    return county, state

# -------------------------------------------------------------------
# RESIDENTIAL SEARCH
# -------------------------------------------------------------------

def load_county_csv(key):
    """Load CSV from S3 and return rows as dicts."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"].read().decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(body))
    return list(reader)

def run_residential_search(query):
    county, state = extract_county_state(query)
    if not county or not state:
        return None, None, None, "Couldn't resolve a (county, state). Use: 'residential addresses in Cuyahoga County Ohio'."

    key = COUNTY_INDEX.get((state, county))
    if not key:
        return None, None, None, f"County dataset not found: {county.title()} County, {state.title()}."

    rows = load_county_csv(key)
    total = len(rows)

    # preview rows (first 20)
    preview = rows[:20]

    # normalize output
    cleaned = []
    for r in preview:
        cleaned.append({
            "address": r.get("address", ""),
            "number": r.get("number", ""),
            "street": r.get("street", ""),
            "city": r.get("city", ""),
            "state": r.get("state", ""),
            "zip": r.get("zip", ""),
            "income": float(r["income"]) if r.get("income") else None,
            "home_value": float(r["home_value"]) if r.get("home_value") else None,
        })

    return cleaned, total, f"{county.title()} County, {state.title()}", None

# -------------------------------------------------------------------
# COMMERCIAL SEARCH (GOOGLE PLACES)
# -------------------------------------------------------------------

def run_commercial_search(query):
    logging.info(f"[COMMERCIAL] Google query: {query}")

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": GOOGLE_PLACES_API_KEY}

    resp = requests.get(url, params=params)
    data = resp.json()

    if "results" not in data:
        return [], "Invalid response from Google Places."

    final = []
    for place in data["results"][:15]:
        place_id = place.get("place_id")

        # pull details
        d_url = "https://maps.googleapis.com/maps/api/place/details/json"
        d_params = {
            "place_id": place_id,
            "fields": "international_phone_number,website,opening_hours",
            "key": GOOGLE_PLACES_API_KEY
        }
        d_resp = requests.get(d_url, params=d_params).json()
        det = d_resp.get("result", {})

        final.append({
            "Name": place.get("name", ""),
            "Address": place.get("formatted_address", ""),
            "Phone": det.get("international_phone_number", "N/A"),
            "Website": det.get("website", "N/A"),
            "Hours": ", ".join(det.get("opening_hours", {}).get("weekday_text", []))
                        if det.get("opening_hours") else "N/A",
        })

    return final, None

# -------------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------------

@app.route("/")
def home():
    mode = request.args.get("mode", "residential")
    return render_template(
        "index.html",
        mode=mode,
        query="",
        results=None,
        commercial_results=None,
        location=None,
        shown=0,
        total=0,
        download_available=False,
        error=None,
        commercial_error=None,
    )

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("home"))

@app.route("/search", methods=["POST"])
def search():
    mode = request.form.get("mode", "residential")
    query = (request.form.get("query") or "").strip()

    logging.info(f"[SEARCH] mode={mode} query='{query}'")

    # -------------------------------------------------------------------
    # COMMERCIAL MODE
    # -------------------------------------------------------------------
    if mode == "commercial":
        commercial_results, commercial_error = run_commercial_search(query)

        return render_template(
            "index.html",
            mode="commercial",
            query=query,
            commercial_results=commercial_results,
            commercial_error=commercial_error,
            results=None,
            location=None,
            shown=0,
            total=0,
            download_available=False,
            error=None,
        )

    # -------------------------------------------------------------------
    # RESIDENTIAL MODE
    # -------------------------------------------------------------------
    results, total, location, error = run_residential_search(query)

    shown = len(results) if results else 0

    return render_template(
        "index.html",
        mode="residential",
        query=query,
        results=results,
        total=total if total else 0,
        shown=shown,
        location=location,
        error=error,
        commercial_results=None,
        commercial_error=None,
        download_available=(shown > 0),
    )

@app.route("/download_csv", methods=["POST"])
def download_csv():
    query = request.form.get("query")
    results, total, location, error = run_residential_search(query)

    if error or not results:
        return "No data to download", 400

    # fetch full dataset again from S3 for entire CSV
    county, state = extract_county_state(query)
    key = COUNTY_INDEX.get((state, county))
    rows = load_county_csv(key)

    # create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(rows[0].keys())
    for row in rows:
        writer.writerow(row.values())

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"{county}_{state}.csv",
    )

# -------------------------------------------------------------------
# MANUAL BOOT INITIALIZATION (Flask 3 compatible)
# -------------------------------------------------------------------

with app.app_context():
    logging.info("[BOOT] Indexing county CSVs...")

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue

            parts = key.split("/")
            if len(parts) != 3:
                continue

            _, state, file = parts
            if not file.endswith(".csv"):
                continue

            county = file.replace(".csv", "").strip()
            COUNTY_INDEX[(norm(state), norm(county))] = key

    logging.info(f"[BOOT] Indexed {len(COUNTY_INDEX)} datasets.")

# -------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000, debug=True)

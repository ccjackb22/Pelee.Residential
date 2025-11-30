import os
import csv
import io
import logging
import boto3
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file
)
from dotenv import load_dotenv
import requests

load_dotenv()

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

SECRET_KEY = os.getenv("SECRET_KEY", "default-secret")
ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET")

GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# ------------------------------------------------------------
# STATE LOOKUP
# ------------------------------------------------------------

STATE_ALIASES = {
    "alabama": "al", "al": "al",
    "alaska": "ak", "ak": "ak",
    "arizona": "az", "az": "az",
    "arkansas": "ar", "ar": "ar",
    "california": "ca", "ca": "ca",
    "colorado": "co", "co": "co",
    "connecticut": "ct", "ct": "ct",
    "delaware": "de", "de": "de",
    "florida": "fl", "fl": "fl",
    "georgia": "ga", "ga": "ga",
    "hawaii": "hi", "hi": "hi",
    "idaho": "id", "id": "id",
    "illinois": "il", "il": "il",
    "indiana": "in", "in": "in",
    "iowa": "ia", "ia": "ia",
    "kansas": "ks", "ks": "ks",
    "kentucky": "ky", "ky": "ky",
    "louisiana": "la", "la": "la",
    "maine": "me", "me": "me",
    "maryland": "md", "md": "md",
    "massachusetts": "ma", "ma": "ma",
    "michigan": "mi", "mi": "mi",
    "minnesota": "mn", "mn": "mn",
    "mississippi": "ms", "ms": "ms",
    "missouri": "mo", "mo": "mo",
    "montana": "mt", "mt": "mt",
    "nebraska": "ne", "ne": "ne",
    "nevada": "nv", "nv": "nv",
    "new hampshire": "nh", "nh": "nh",
    "new jersey": "nj", "nj": "nj",
    "new mexico": "nm", "nm": "nm",
    "new york": "ny", "ny": "ny",
    "north carolina": "nc", "nc": "nc",
    "north dakota": "nd", "nd": "nd",
    "ohio": "oh", "oh": "oh",
    "oklahoma": "ok", "ok": "ok",
    "oregon": "or", "or": "or",
    "pennsylvania": "pa", "pa": "pa",
    "rhode island": "ri", "ri": "ri",
    "south carolina": "sc", "sc": "sc",
    "south dakota": "sd", "sd": "sd",
    "tennessee": "tn", "tn": "tn",
    "texas": "tx", "tx": "tx",
    "utah": "ut", "ut": "ut",
    "vermont": "vt", "vt": "vt",
    "virginia": "va", "va": "va",
    "washington": "wa", "wa": "wa",
    "west virginia": "wv", "wv": "wv",
    "wisconsin": "wi", "wi": "wi",
    "wyoming": "wy", "wy": "wy",
}

# ------------------------------------------------------------
# AWS CLIENT
# ------------------------------------------------------------

s3 = boto3.client(
    "s3",
    region_name=AWS_DEFAULT_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# ------------------------------------------------------------
# RESIDENTIAL DATA INDEXING
# ------------------------------------------------------------

COUNTY_DATASETS = {}   # ("cuyahoga", "oh") → "path/in/bucket.csv"

def index_s3():
    """
    Scan bucket and index all CSVs.
    """
    global COUNTY_DATASETS
    logging.info("[BOOT] Indexing county CSVs...")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET)

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"].lower()
            if not key.endswith(".csv"):
                continue

            # Expect format: state/county.csv
            parts = key.split("/")
            if len(parts) != 2:
                continue

            state = parts[0]
            county_file = parts[1]
            county = county_file.replace(".csv", "")

            COUNTY_DATASETS[(county, state)] = key

    logging.info(f"[BOOT] Indexed {len(COUNTY_DATASETS)} datasets")


# ------------------------------------------------------------
# FLASK APP
# ------------------------------------------------------------

app = Flask(__name__)
app.secret_key = SECRET_KEY


@app.before_first_request
def startup():
    index_s3()


# ------------------------------------------------------------
# LOGIN
# ------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("login.html")

    password = request.form.get("password", "")
    if password == ACCESS_PASSWORD:
        session["authed"] = True
        return redirect(url_for("index"))
    return render_template("login.html", error="Incorrect password.")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ------------------------------------------------------------
# HOME PAGE
# ------------------------------------------------------------

@app.route("/")
def index():
    if not session.get("authed"):
        return redirect(url_for("login"))

    mode = request.args.get("mode", "residential")
    return render_template("index.html", mode=mode)


# ------------------------------------------------------------
# SEARCH ENDPOINT
# ------------------------------------------------------------

@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return redirect(url_for("login"))

    mode = request.form.get("mode", "residential").lower()
    query = request.form.get("query", "").strip()

    logging.info(f"[SEARCH] mode={mode} query='{query}'")

    # -------------------------
    # COMMERCIAL SEARCH (Google)
    # -------------------------
    if mode == "commercial":
        url = (
            "https://maps.googleapis.com/maps/api/place/textsearch/json"
            f"?query={query}&key={GOOGLE_API_KEY}"
        )

        resp = requests.get(url).json()
        results = []

        if "results" in resp:
            for place in resp["results"]:
                place_id = place["place_id"]
                details_url = (
                    "https://maps.googleapis.com/maps/api/place/details/json"
                    f"?place_id={place_id}&fields=international_phone_number,website,opening_hours&key={GOOGLE_API_KEY}"
                )
                d = requests.get(details_url).json()

                phone = d.get("result", {}).get("international_phone_number", "N/A")
                website = d.get("result", {}).get("website", "N/A")
                hours = d.get("result", {}).get("opening_hours", {}).get("weekday_text", [])
                hours_str = "; ".join(hours) if hours else "N/A"

                results.append({
                    "Name": place.get("name", "N/A"),
                    "Address": place.get("formatted_address", "N/A"),
                    "Phone": phone,
                    "Website": website,
                    "Hours": hours_str,
                })

        return render_template(
            "index.html",
            mode="commercial",
            query=query,
            commercial_results=results,
        )

    # -------------------------
    # RESIDENTIAL SEARCH (AWS CSV)
    # -------------------------

    words = query.lower().split()
    if "county" not in words:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            error="Couldn't resolve a county. Include BOTH the county name and 'County' and the full state name."
        )

    # Extract county name
    try:
        idx = words.index("county")
        county = words[idx - 1]
    except:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            error="Couldn't parse the county name."
        )

    # Extract last word as state name
    state_full = words[-1]
    if state_full not in STATE_ALIASES:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            error="Couldn't parse state name."
        )

    state = STATE_ALIASES[state_full]

    key = COUNTY_DATASETS.get((county, state))
    if not key:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            error="County dataset not found."
        )

    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    csv_bytes = obj["Body"].read().decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(csv_bytes))

    results = list(reader)
    shown = results[:20]
    total = len(results)

    return render_template(
        "index.html",
        mode="residential",
        query=query,
        results=shown,
        shown=len(shown),
        total=total,
        location=f"{county.title()} County, {state.upper()}",
        download_available=True,
    )


# ------------------------------------------------------------
# CSV EXPORT
# ------------------------------------------------------------

@app.route("/download_csv", methods=["POST"])
def download_csv():
    if not session.get("authed"):
        return redirect(url_for("login"))

    query = request.form.get("query", "")
    words = query.lower().split()
    idx = words.index("county")
    county = words[idx - 1]
    state = STATE_ALIASES[words[-1]]

    key = COUNTY_DATASETS[(county, state)]
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)

    return send_file(
        io.BytesIO(obj["Body"].read()),
        mimetype="text/csv",
        download_name=f"{county}_{state}.csv",
        as_attachment=True,
    )


# ------------------------------------------------------------
# RUN
# ------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

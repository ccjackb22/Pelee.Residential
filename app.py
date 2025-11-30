import os
import csv
import io
import json
import logging
import re
import time
from datetime import datetime

import boto3
from botocore.config import Config
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, Response
)
from dotenv import load_dotenv


# -----------------------------------------------------------
# ENV + LOGGING
# -----------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# IMPORTANT: prefix must be empty — your working version had no prefix.
S3_PREFIX = ""   

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Charlotte69")
SECRET_KEY = os.getenv("SECRET_KEY") or os.urandom(24).hex()

app = Flask(__name__)
app.secret_key = SECRET_KEY

boto_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
s3 = boto_session.client("s3", config=Config(max_pool_connections=10))

COUNTY_INDEX = {}  # state -> county -> s3_key


# -----------------------------------------------------------
# NORMALIZATION HELPERS
# -----------------------------------------------------------

STATE_ALIASES = {
    "alabama":"al","al":"al",
    "alaska":"ak","ak":"ak",
    "arizona":"az","az":"az",
    "arkansas":"ar","ar":"ar",
    "california":"ca","ca":"ca",
    "colorado":"co","co":"co",
    "connecticut":"ct","ct":"ct",
    "delaware":"de","de":"de",
    "florida":"fl","fl":"fl",
    "georgia":"ga","ga":"ga",
    "hawaii":"hi","hi":"hi",
    "idaho":"id","id":"id",
    "illinois":"il","il":"il",
    "indiana":"in","in":"in",
    "iowa":"ia","ia":"ia",
    "kansas":"ks","ks":"ks",
    "kentucky":"ky","ky":"ky",
    "louisiana":"la","la":"la",
    "maine":"me","me":"me",
    "maryland":"md","md":"md",
    "massachusetts":"ma","ma":"ma",
    "michigan":"mi","mi":"mi",
    "minnesota":"mn","mn":"mn",
    "mississippi":"ms","ms":"ms",
    "missouri":"mo","mo":"mo",
    "montana":"mt","mt":"mt",
    "nebraska":"ne","ne":"ne",
    "nevada":"nv","nv":"nv",
    "new hampshire":"nh","nh":"nh",
    "new jersey":"nj","nj":"nj",
    "new mexico":"nm","nm":"nm",
    "new york":"ny","ny":"ny",
    "north carolina":"nc","nc":"nc",
    "north dakota":"nd","nd":"nd",
    "ohio":"oh","oh":"oh",
    "oklahoma":"ok","ok":"ok",
    "oregon":"or","or":"or",
    "pennsylvania":"pa","pa":"pa",
    "rhode island":"ri","ri":"ri",
    "south carolina":"sc","sc":"sc",
    "south dakota":"sd","sd":"sd",
    "tennessee":"tn","tn":"tn",
    "texas":"tx","tx":"tx",
    "utah":"ut","ut":"ut",
    "vermont":"vt","vt":"vt",
    "virginia":"va","va":"va",
    "washington":"wa","wa":"wa",
    "west virginia":"wv","wv":"wv",
    "wisconsin":"wi","wi":"wi",
    "wyoming":"wy","wy":"wy",
    "district of columbia":"dc","dc":"dc",
}


def normalize_state(token: str) -> str | None:
    if not token:
        return None
    t = token.strip().lower().replace(",", "")
    return STATE_ALIASES.get(t)


def normalize_county(name: str) -> str:
    n = name.lower().strip()
    for suf in [" county", " parish", " borough", " census area"]:
        if n.endswith(suf):
            n = n.replace(suf, "")
    return n.strip()


# -----------------------------------------------------------
# BUILD COUNTY INDEX (CSV)
# -----------------------------------------------------------

def build_county_index():
    global COUNTY_INDEX

    logging.info("[BOOT] Scanning S3 for CSV datasets...")

    paginator = s3.get_paginator("list_objects_v2")

    total = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".csv"):
                continue

            parts = key.split("/")
            if len(parts) != 2:
                continue

            state = parts[0].lower()
            county = parts[1].replace(".csv", "").lower()

            COUNTY_INDEX.setdefault(state, {})
            COUNTY_INDEX[state][county] = key
            total += 1

    logging.info(f"[BOOT] Indexed {total} CSV county files.")


build_county_index()


# -----------------------------------------------------------
# QUERY PARSING
# -----------------------------------------------------------

def parse_query(query: str):
    """
    Extract county + state from natural-language text.
    """
    q = query.lower()

    # Example: "homes in cuyahoga county ohio"
    m = re.search(r"in (.*?) county ([a-z ]+)", q)
    if not m:
        return None, None

    county = normalize_county(m.group(1))
    state = normalize_state(m.group(2))

    return state, county


# -----------------------------------------------------------
# LOGIN
# -----------------------------------------------------------

def require_login():
    return session.get("authenticated") is True


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == ACCESS_PASSWORD:
            session["authenticated"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Incorrect password.")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# -----------------------------------------------------------
# INDEX PAGE
# -----------------------------------------------------------

@app.route("/")
def index():
    if not require_login():
        return redirect(url_for("login"))

    return render_template(
        "index.html",
        query="",
        results=[],
        error=None,
        location=None,
        total=None,
        shown=0,
        download_available=False,
    )


# -----------------------------------------------------------
# SEARCH
# -----------------------------------------------------------

@app.route("/search", methods=["POST"])
def search():
    if not require_login():
        return redirect(url_for("login"))

    query = request.form.get("query", "").strip()
    if not query:
        return render_template(
            "index.html", query=query, results=[], error="Enter a search query."
        )

    state, county = parse_query(query)
    if not state or not county:
        return render_template(
            "index.html", query=query, results=[], error="Couldn't detect county + state."
        )

    if state not in COUNTY_INDEX or county not in COUNTY_INDEX[state]:
        return render_template(
            "index.html",
            query=query,
            results=[],
            error="County dataset not found.",
        )

    key = COUNTY_INDEX[state][county]

    # load CSV
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    text = obj["Body"].read().decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))

    results = []
    shown = 0
    max_preview = 25

    for row in reader:
        if shown < max_preview:
            results.append({
                "address": f"{row.get('NUMBER','')} {row.get('STREET','')}".strip(),
                "city": row.get("CITY",""),
                "state": row.get("REGION",""),
                "zip": row.get("POSTCODE",""),
                "income": row.get("median_income"),
                "home_value": row.get("median_home_value"),
            })
            shown += 1

    return render_template(
        "index.html",
        query=query,
        results=results,
        location=f"{county.title()} County, {state.upper()}",
        error=None,
        total="?",
        shown=shown,
        download_available=True,
    )


# -----------------------------------------------------------
# CSV EXPORT
# -----------------------------------------------------------

@app.route("/download", methods=["POST"])
def download():
    if not require_login():
        return redirect(url_for("login"))

    query = request.form.get("query", "")
    state, county = parse_query(query)

    key = COUNTY_INDEX[state][county]

    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    csv_bytes = obj["Body"].read()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{county}_{state}_{ts}.csv"

    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True)

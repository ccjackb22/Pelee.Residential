import os
import json
import logging
import time
import re
import csv
import io
from datetime import datetime

import boto3
from botocore.config import Config
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    jsonify,
    Response,
)
import requests
from dotenv import load_dotenv

# -------------------------------------------------------------------
# ENV + LOGGING SETUP
# -------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = (
    os.getenv("AWS_DEFAULT_REGION")
    or os.getenv("AWS_REGION")
    or "us-east-2"
)

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip()

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

# (state, normalized_county) -> {key, size, raw_name}
COUNTY_INDEX: dict[str, dict[str, dict]] = {}

# -------------------------------------------------------------------
# STATE + COUNTY NORMALIZATION
# -------------------------------------------------------------------

STATE_ALIASES = {
    "al": "al","ak":"ak","az":"az","ar":"ar","ca":"ca","co":"co","ct":"ct","de":"de","fl":"fl",
    "ga":"ga","hi":"hi","id":"id","il":"il","in":"in","ia":"ia","ks":"ks","ky":"ky","la":"la",
    "me":"me","md":"md","ma":"ma","mi":"mi","mn":"mn","ms":"ms","mo":"mo","mt":"mt","ne":"ne",
    "nv":"nv","nh":"nh","nj":"nj","nm":"nm","ny":"ny","nc":"nc","nd":"nd","oh":"oh","ok":"ok",
    "or":"or","pa":"pa","ri":"ri","sc":"sc","sd":"sd","tn":"tn","tx":"tx","ut":"ut","vt":"vt",
    "va":"va","wa":"wa","wv":"wv","wi":"wi","wy":"wy",
    "alabama":"al","alaska":"ak","arizona":"az","arkansas":"ar","california":"ca",
    "colorado":"co","connecticut":"ct","delaware":"de","florida":"fl","georgia":"ga",
    "hawaii":"hi","idaho":"id","illinois":"il","indiana":"in","iowa":"ia","kansas":"ks",
    "kentucky":"ky","louisiana":"la","maine":"me","maryland":"md","massachusetts":"ma",
    "michigan":"mi","minnesota":"mn","mississippi":"ms","missouri":"mo","montana":"mt",
    "nebraska":"ne","nevada":"nv","new hampshire":"nh","new jersey":"nj","new mexico":"nm",
    "new york":"ny","north carolina":"nc","north dakota":"nd","ohio":"oh","oklahoma":"ok",
    "oregon":"or","pennsylvania":"pa","rhode island":"ri","south carolina":"sc",
    "south dakota":"sd","tennessee":"tn","texas":"tx","utah":"ut","vermont":"vt",
    "virginia":"va","washington":"wa","west virginia":"wv","wisconsin":"wi","wyoming":"wy",
    "district of columbia":"dc","puerto rico":"pr",
}

def normalize_state_name(text):
    if not text:
        return None
    t = text.lower().strip(",. ")
    return STATE_ALIASES.get(t)

def normalize_county_name(name):
    if not name:
        return None
    n = name.lower().replace("_", " ").strip()
    for suf in [" county", " parish", " borough", " census area", " municipality", " city"]:
        if n.endswith(suf):
            n = n[:-len(suf)]
    return n.strip()

# -------------------------------------------------------------------
# BOOT: BUILD COUNTY INDEX
# -------------------------------------------------------------------

def build_county_index():
    global COUNTY_INDEX
    paginator = s3.get_paginator("list_objects_v2")

    prefix = S3_PREFIX
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue

            rel = key[len(prefix):] if prefix else key
            parts = rel.split("/")
            if len(parts) != 2:
                continue

            state = parts[0].lower()
            filename = parts[1]
            county = filename.replace(".csv", "").lower()

            norm = normalize_county_name(county) or county
            COUNTY_INDEX.setdefault(state, {})
            COUNTY_INDEX[state][norm] = {
                "key": key,
                "size": obj["Size"],
                "raw_name": county,
            }

build_county_index()

# -------------------------------------------------------------------
# AUTH HELPERS
# -------------------------------------------------------------------

def require_login():
    return session.get("authenticated") is True

# -------------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------------

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


@app.route("/", methods=["GET"])
def index():
    if not require_login():
        return redirect(url_for("login"))
    return render_template("index.html")

# -------------------------------------------------------------------
# RESIDENTIAL SEARCH (S3)
# -------------------------------------------------------------------

def parse_location(q):
    q = q.lower()
    tokens = q.split()

    if "county" not in tokens:
        return None, None

    idx = tokens.index("county")
    county_tokens = tokens[max(0, idx-2):idx]
    county = normalize_county_name(" ".join(county_tokens))
    if not county:
        return None, None

    state = None
    for t in tokens[idx+1:]:
        st = normalize_state_name(t)
        if st:
            state = st
            break

    return state, county

@app.route("/search", methods=["POST"])
def search_residential():
    data = request.get_json()
    query = data.get("query", "").strip()

    state, county = parse_location(query)
    if not state or not county:
        return jsonify({"ok": False, "error": "Couldn't resolve a (county, state)."}), 200

    state_map = COUNTY_INDEX.get(state)
    if not state_map:
        return jsonify({"ok": False, "error": "State not found."})

    meta = state_map.get(county)
    if not meta:
        return jsonify({"ok": False, "error": "County not found."})

    obj = s3.get_object(Bucket=S3_BUCKET, Key=meta["key"])
    text = obj["Body"].read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    rows = []
    for r in reader:
        rows.append({
            "address": r.get("full_address") or f"{r.get('NUMBER','')} {r.get('STREET','')}",
            "city": r.get("CITY"),
            "state": r.get("REGION"),
            "zip": r.get("POSTCODE"),
            "income": r.get("median_income"),
            "home_value": r.get("median_home_value"),
        })
        if len(rows) >= 20:
            break

    return jsonify({"ok": True, "results": rows})

# -------------------------------------------------------------------
# COMMERCIAL SEARCH (GOOGLE)
# -------------------------------------------------------------------

@app.route("/commercial_search", methods=["POST"])
def commercial_query():
    data = request.get_json()
    query = data.get("query", "")

    url = (
        "https://maps.googleapis.com/maps/api/place/textsearch/json"
        f"?query={query}&key={GOOGLE_API_KEY}"
    )
    r = requests.get(url)
    js = r.json()

    if "results" not in js:
        return jsonify({"ok": False, "error": "No results"}), 200

    out = []
    for p in js["results"][:20]:
        out.append({
            "Name": p.get("name"),
            "Address": p.get("formatted_address"),
            "Phone": "N/A",
            "Website": "N/A",
            "Hours": "N/A"
        })

    return jsonify({"ok": True, "results": out})

# -------------------------------------------------------------------
# CSV DOWNLOAD
# -------------------------------------------------------------------

@app.route("/download", methods=["POST"])
def download_csv():
    if not require_login():
        return redirect(url_for("login"))

    query = request.form.get("query", "")
    state, county = parse_location(query)

    if not state or not county:
        return "Invalid", 400

    meta = COUNTY_INDEX[state][county]
    key = meta["key"]

    def generate():
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        text = obj["Body"].read().decode("utf-8")
        yield text

    filename = f"{county}_{state}_{int(time.time())}.csv"

    return Response(
        generate(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )

# -------------------------------------------------------------------
# RUN
# -------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

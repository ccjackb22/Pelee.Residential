import os
import json
import logging
import time
import csv
import io
import re
from datetime import datetime

import boto3
from botocore.config import Config
from flask import (
    Flask, render_template, request, redirect,
    url_for, session, jsonify, send_file
)
from dotenv import load_dotenv

# ------------------------------------------------------------
# ENV + LOGGING
# ------------------------------------------------------------

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

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs_clean/")

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

# (state, county_norm) -> key,size
COUNTY_INDEX = {}

# ------------------------------------------------------------
# STATE / COUNTY NORMALIZATION
# ------------------------------------------------------------

STATE_ALIASES = {
    # abbreviations
    "al":"al","ak":"ak","az":"az","ar":"ar","ca":"ca","co":"co","ct":"ct","de":"de",
    "fl":"fl","ga":"ga","hi":"hi","id":"id","il":"il","in":"in","ia":"ia","ks":"ks",
    "ky":"ky","la":"la","me":"me","md":"md","ma":"ma","mi":"mi","mn":"mn","ms":"ms",
    "mo":"mo","mt":"mt","ne":"ne","nv":"nv","nh":"nh","nj":"nj","nm":"nm","ny":"ny",
    "nc":"nc","nd":"nd","oh":"oh","ok":"ok","or":"or","pa":"pa","ri":"ri","sc":"sc",
    "sd":"sd","tn":"tn","tx":"tx","ut":"ut","vt":"vt","va":"va","wa":"wa","wv":"wv",
    "wi":"wi","wy":"wy","dc":"dc","pr":"pr",
    # full names
    "alabama":"al","alaska":"ak","arizona":"az","arkansas":"ar","california":"ca",
    "colorado":"co","connecticut":"ct","delaware":"de","florida":"fl","georgia":"ga",
    "hawaii":"hi","idaho":"id","illinois":"il","indiana":"in","iowa":"ia",
    "kansas":"ks","kentucky":"ky","louisiana":"la","maine":"me","maryland":"md",
    "massachusetts":"ma","michigan":"mi","minnesota":"mn","mississippi":"ms",
    "missouri":"mo","montana":"mt","nebraska":"ne","nevada":"nv",
    "new hampshire":"nh","new jersey":"nj","new mexico":"nm","new york":"ny",
    "north carolina":"nc","north dakota":"nd","ohio":"oh","oklahoma":"ok",
    "oregon":"or","pennsylvania":"pa","rhode island":"ri","south carolina":"sc",
    "south dakota":"sd","tennessee":"tn","texas":"tx","utah":"ut","vermont":"vt",
    "virginia":"va","washington":"wa","west virginia":"wv","wisconsin":"wi",
    "wyoming":"wy","district of columbia":"dc","puerto rico":"pr",
}

def normalize_state(text):
    if not text: return None
    t = text.strip().lower().rstrip(",.")
    return STATE_ALIASES.get(t)

def normalize_county(text):
    if not text: return None
    t = text.lower().strip()
    t = t.replace("_"," ")
    t = re.sub(r"[-]+"," ",t)

    remove = [" county"," parish"," borough"," census area"," municipality"," city"]
    for r in remove:
        if t.endswith(r):
            t = t[:-len(r)]
    return t.strip()

# ------------------------------------------------------------
# BUILD COUNTY INDEX
# ------------------------------------------------------------

def build_index():
    global COUNTY_INDEX
    logging.info("[BOOT] Building index…")
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents",[]):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            rel = key[len(S3_PREFIX):]     # e.g. fl/wakulla-clean.geojson
            parts = rel.split("/")
            if len(parts) != 2:
                continue

            state = parts[0].lower()
            filename = parts[1]
            base = filename[:-len("-clean.geojson")]

            county_norm = normalize_county(base)
            if not county_norm:
                continue

            COUNTY_INDEX.setdefault(state,{})
            COUNTY_INDEX[state][county_norm] = {
                "key": key,
                "size": obj["Size"],
                "raw_name": base,
            }

    logging.info("[BOOT] Index built. %d counties loaded.", sum(len(v) for v in COUNTY_INDEX.values()))

build_index()

# ------------------------------------------------------------
# QUERY PARSING
# ------------------------------------------------------------

def parse_location(query):
    q = query.lower()
    tokens = q.split()

    county_idx = None
    for i,tok in enumerate(tokens):
        if tok in ("county","parish","borough"):
            county_idx = i
            break
    if county_idx is None:
        return None,None

    # find "in"
    last_in = None
    for i in range(county_idx):
        if tokens[i] == "in":
            last_in = i
    if last_in is None:
        return None,None

    county_phrase = " ".join(tokens[last_in+1:county_idx])
    county_norm = normalize_county(county_phrase)

    # find state
    state_code = None
    for t in tokens[county_idx+1:]:
        st = normalize_state(t)
        if st:
            state_code = st
            break

    return state_code, county_norm

# ------------------------------------------------------------
# LOAD COUNTY
# ------------------------------------------------------------

def load_features(state, county_norm):
    state_map = COUNTY_INDEX.get(state)
    if not state_map:
        return None,None
    meta = state_map.get(county_norm)
    if not meta:
        # fuzzy
        for c,m in state_map.items():
            if county_norm in c or c in county_norm:
                meta = m
                break
    if not meta:
        return None,None

    key = meta["key"]
    body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    data = json.loads(body)
    feats = data.get("features",[])
    return feats, meta

# ------------------------------------------------------------
# AUTH
# ------------------------------------------------------------

def require_login():
    return bool(session.get("authenticated"))

# ------------------------------------------------------------
# ROUTES
# ------------------------------------------------------------

@app.get("/health")
def health():
    return jsonify({"ok":True})

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method=="POST":
        pw = request.form.get("password","")
        if pw == ACCESS_PASSWORD:
            session["authenticated"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Incorrect password.")
    return render_template("login.html")

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.get("/")
def index():
    if not require_login():
        return redirect(url_for("login"))
    return render_template("index.html")

# ------------------------------------------------------------
# SEARCH → CSV DOWNLOAD
# ------------------------------------------------------------

@app.post("/search")
def search():
    if not require_login():
        return redirect(url_for("login"))

    query = (request.form.get("query") or "").strip()

    state, county = parse_location(query)
    if not state or not county:
        return render_template(
            "index.html",
            error="Could not resolve a county + state. Use: 'addresses in Cuyahoga County Ohio'."
        )

    feats, meta = load_features(state, county)
    if feats is None:
        return render_template(
            "index.html",
            error=f"No cleaned dataset found for {county.title()} County, {state.upper()}."
        )

    # -------------------------
    # BUILD CSV IN MEMORY
    # -------------------------
    output = io.StringIO()
    writer = None

    for f in feats:
        props = f.get("properties", {})
        if writer is None:
            writer = csv.DictWriter(output, fieldnames=list(props.keys()))
            writer.writeheader()
        writer.writerow(props)

    output.seek(0)

    filename = f"{county}-{state}.csv"

    return send_file(
        io.BytesIO(output.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename
    )

# ------------------------------------------------------------
# ENTRYPOINT
# ------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

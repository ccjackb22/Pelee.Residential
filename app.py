import os
import io
import json
import csv
import re

from flask import (
    Flask, render_template, request, jsonify,
    redirect, url_for, session, Response
)

import boto3
from dotenv import load_dotenv

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
CLEAN_PREFIX = "merged_with_tracts_acs_clean"

MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3 = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATE NORMALIZATION
# ---------------------------------------------------------

STATE_CODES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct", "delaware": "de",
    "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id",
    "illinois": "il", "indiana": "in", "iowa": "ia", "kansas": "ks",
    "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md",
    "massachusetts": "ma", "michigan": "mi", "minnesota": "mn",
    "mississippi": "ms", "missouri": "mo", "montana": "mt",
    "nebraska": "ne", "nevada": "nv", "new hampshire": "nh",
    "new jersey": "nj", "new mexico": "nm", "new york": "ny",
    "north carolina": "nc", "north dakota": "nd", "ohio": "oh",
    "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa",
    "rhode island": "ri", "south carolina": "sc", "south dakota": "sd",
    "tennessee": "tn", "texas": "tx", "utah": "ut", "vermont": "vt",
    "virginia": "va", "washington": "wa", "west virginia": "wv",
    "wisconsin": "wi", "wyoming": "wy"
}

# Reverse lookup for “OH → ohio”
STATE_NAMES = {v: k for k, v in STATE_CODES.items()}

# ---------------------------------------------------------
# LOAD DATASETS FROM S3
# ---------------------------------------------------------

ALL_DATASETS = {}      # (state_code, county) → s3_key
COUNTIES_BY_STATE = {} # state_code → [county, county, ...]

def normalize(name: str):
    """lowercase, remove punctuation, replace underscores with spaces"""
    name = name.lower()
    name = name.replace("_", " ")
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def scan_available_datasets():
    global ALL_DATASETS, COUNTIES_BY_STATE
    ALL_DATASETS = {}
    COUNTIES_BY_STATE = {}

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{CLEAN_PREFIX}/")

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            # Example key: merged_with_tracts_acs_clean/oh/cuyahoga-clean.geojson
            parts = key.split("/")
            if len(parts) != 3:
                continue

            _, state, filename = parts
            county = filename.replace("-clean.geojson", "")

            ALL_DATASETS[(state, county)] = key
            COUNTIES_BY_STATE.setdefault(state, []).append(county)

scan_available_datasets()

# ---------------------------------------------------------
# QUERY PARSING
# ---------------------------------------------------------

def detect_state(query: str):
    q = normalize(query)

    # full state name
    for name, code in STATE_CODES.items():
        if normalize(name) in q:
            return code

    # state code (e.g. "oh" or "ohio")
    for code in STATE_CODES.values():
        if re.search(rf"\b{code}\b", q):
            return code

    return None

def detect_county(query: str, state_code: str):
    if not state_code:
        return None

    q = normalize(query)
    q = q.replace("county", "").strip()

    for county in COUNTIES_BY_STATE.get(state_code, []):
        if normalize(county) in q:
            return county

    return None


def resolve_location(query: str):
    state_code = detect_state(query)
    if not state_code:
        return None, None, None

    county = detect_county(query, state_code)
    if not county:
        return None, None, None

    key = ALL_DATASETS.get((state_code, county))
    return state_code, county, key

def detect_zip(query: str):
    m = re.search(r"\b(\d{5})\b", query)
    return m.group(1) if m else None

def parse_filters(q: str):
    q = q.lower().replace("$", "").replace(",", "")
    toks = q.split()

    income_min = None
    value_min = None

    for i, t in enumerate(toks):
        if t in ("over", "above", "greater", ">", "atleast", "at_least"):
            if i+1 < len(toks):
                raw = toks[i+1]
                mult = 1
                if raw.endswith("k"):
                    raw = raw[:-1]; mult = 1000
                if raw.endswith("m"):
                    raw = raw[:-1]; mult = 1_000_000
                try:
                    val = float(raw) * mult
                except:
                    continue

                window = " ".join(toks[max(0, i-5): i+5])
                if "income" in window:
                    income_min = val
                elif "value" in window or "home" in window or "house" in window:
                    value_min = val
                else:
                    if income_min is None:
                        income_min = val
    return income_min, value_min

def load_geojson(key: str):
    data = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(data)

def feature_to_obj(f):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}
    coords = geom.get("coordinates") or [None, None]

    if isinstance(coords[0], (list, tuple)):
        try:
            lon, lat = coords[0]
        except:
            lon, lat = None, None
    else:
        try:
            lon, lat = coords[:2]
        except:
            lon, lat = None, None

    num = p.get("number") or p.get("NUMBER") or ""
    street = p.get("street") or p.get("STREET") or ""
    unit = p.get("unit") or ""

    return {
        "address": " ".join(x for x in [num, street, unit] if x),
        "city": p.get("city") or "",
        "state": p.get("region") or p.get("STUSPS") or "",
        "zip": p.get("postcode") or p.get("ZCTA5CE20") or "",
        "income": p.get("median_income"),
        "value": p.get("median_value"),
        "lat": lat,
        "lon": lon,
    }

def apply_filters(features, income_min, value_min, zip_code):
    out = []
    for f in features:
        p = f.get("properties", {}) or {}

        if zip_code:
            if str(p.get("postcode") or "") != zip_code:
                continue

        if income_min:
            v = p.get("median_income")
            try:
                if v is None or float(v) < income_min:
                    continue
            except:
                continue

        if value_min:
            v = p.get("median_value")
            try:
                if v is None or float(v) < value_min:
                    continue
            except:
                continue

        out.append(f)
    return out

# ---------------------------------------------------------
# AUTH
# ---------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
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

# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------

@app.route("/")
def index():
    if not session.get("authed"):
        return redirect(url_for("login"))
    return render_template("index.html")

@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "states_loaded": len(COUNTIES_BY_STATE),
        "datasets": len(ALL_DATASETS)
    })

@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    state, county, key = resolve_location(query)
    if not key:
        return jsonify({"ok": False, "error": "No cleaned dataset found for that location."})

    gj = load_geojson(key)
    feats = gj.get("features", [])

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)
    feats = apply_filters(feats, income_min, value_min, zip_code)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    return jsonify({
        "ok": True,
        "state": state.upper(),
        "county": county.title(),
        "query": query,
        "total": total,
        "shown": len(feats),
        "dataset_key": key,
        "results": [feature_to_obj(f) for f in feats]
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

    state, county, key = resolve_location(query)
    if not key:
        return jsonify({"ok": False, "error": "Dataset not found."})

    gj = load_geojson(key)
    feats = gj.get("features", [])

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)
    feats = apply_filters(feats, income_min, value_min, zip_code)

    filename = f"pelee_export_{county}_{state}.csv"

    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        writer.writerow(["address", "city", "state", "zip", "income", "value", "lat", "lon"])
        yield buffer.getvalue()
        buffer.seek(0); buffer.truncate(0)

        for f in feats:
            r = feature_to_obj(f)
            writer.writerow([
                r["address"], r["city"], r["state"], r["zip"],
                r["income"], r["value"], r["lat"], r["lon"]
            ])
            chunk = buffer.getvalue()
            yield chunk
            buffer.seek(0); buffer.truncate(0)

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

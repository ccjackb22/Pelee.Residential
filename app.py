import os
import io
import json
import csv
import re

from flask import (
    Flask, render_template, request, jsonify, redirect,
    url_for, session, Response
)

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# CLEANED dataset prefix
CLEAN_PREFIX = "merged_with_tracts_acs_clean"

MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# LOAD AVAILABLE CLEANED DATASETS
# ---------------------------------------------------------

STATE_CODES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct", "delaware": "de",
    "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id",
    "illinois": "il", "indiana": "in", "iowa": "ia", "kansas": "ks",
    "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md",
    "massachusetts": "ma", "michigan": "mi", "minnesota": "mn", "mississippi": "ms",
    "missouri": "mo", "montana": "mt", "nebraska": "ne", "nevada": "nv",
    "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm",
    "new york": "ny", "north carolina": "nc", "north dakota": "nd",
    "ohio": "oh", "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa",
    "rhode island": "ri", "south carolina": "sc", "south dakota": "sd",
    "tennessee": "tn", "texas": "tx", "utah": "ut", "vermont": "vt",
    "virginia": "va", "washington": "wa", "west virginia": "wv",
    "wisconsin": "wi", "wyoming": "wy",
}

ALL_DATASETS = {}  # (state, county) → s3_key

def scan_available_datasets():
    """
    Scans S3 for all cleaned datasets and builds a lookup
      ALL_DATASETS[("oh", "cuyahoga")] = "merged_with_tracts_acs_clean/oh/cuyahoga-clean.geojson"
    """
    global ALL_DATASETS
    ALL_DATASETS = {}

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=CLEAN_PREFIX + "/")

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            parts = key.split("/")
            if len(parts) != 3:
                continue

            _, state, fname = parts
            if state not in STATE_CODES.values():
                continue

            county = fname.replace("-clean.geojson", "")
            ALL_DATASETS[(state, county)] = key

scan_available_datasets()

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def detect_zip(query: str):
    m = re.search(r"\b(\d{5})\b", query)
    return m.group(1) if m else None


def parse_filters(q: str):
    q = q.lower().replace("$", "").replace(",", "")
    toks = q.split()

    income_min = None
    value_min = None

    for i, t in enumerate(toks):
        if t in ("over","above","greater",">","atleast","at_least") and i+1 < len(toks):
            raw = toks[i+1]
            mult = 1
            if raw.endswith("k"):
                mult = 1000; raw = raw[:-1]
            if raw.endswith("m"):
                mult = 1_000_000; raw = raw[:-1]

            try:
                val = float(raw) * mult
            except:
                continue

            window = " ".join(toks[max(0, i-5): i+5])
            if "income" in window:
                income_min = val; continue
            if "value" in window or "home" in window or "house" in window:
                value_min = val; continue

            if income_min is None:
                income_min = val

    return income_min, value_min


def resolve_any_county(query: str):
    """
    Resolve ANY county + state from cleaned S3 datasets.
    """
    q = query.lower()

    # Detect state
    state_code = None
    for name, code in STATE_CODES.items():
        if name in q or f" {code} " in q or q.endswith(code):
            state_code = code
            break

    if not state_code:
        return None, None, None

    # Detect county (remove "county")
    county_guess = None
    for (_, counties) in {}.items():
        pass  # we build dynamically below

    # Check all counties for that state
    for (s, county) in ALL_DATASETS.keys():
        if s != state_code:
            continue

        if county.replace("_", " ") in q or county in q:
            return state_code, county, ALL_DATASETS[(s, county)]

    # If they said “Cuyahoga County”
    for (s, county) in ALL_DATASETS.keys():
        if s == state_code and f"{county} county" in q:
            return s, county, ALL_DATASETS[(s, county)]

    return None, None, None


def load_geojson(key: str):
    data = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(data)


def feature_to_obj(f):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}

    coords = geom.get("coordinates") or [None, None]
    if isinstance(coords[0], (list, tuple)):
        try:
            lon, lat = coords[0][0], coords[0][1]
        except:
            lon, lat = None, None
    else:
        lon, lat = coords[0], coords[1] if len(coords) >= 2 else (None, None)

    return {
        "address": " ".join([str(x) for x in [
            p.get("number") or p.get("NUMBER") or "",
            p.get("street") or p.get("STREET") or "",
            p.get("unit") or ""
        ] if x]),
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
# CORE ROUTES
# ---------------------------------------------------------

@app.route("/")
def index():
    if not session.get("authed"):
        return redirect(url_for("login"))
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify({"ok": True, "datasets": len(ALL_DATASETS)})


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    state, county, key = resolve_any_county(query)
    if not key:
        return jsonify({"ok": False, "error": "No cleaned dataset found for that location."})

    try:
        gj = load_geojson(key)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed loading dataset {key}", "details": str(e)})

    feats = gj.get("features", [])

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    feats = apply_filters(feats, income_min, value_min, zip_code)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    return jsonify({
        "ok": True,
        "state": state.upper(),
        "county": county.replace("_", " ").title(),
        "query": query,
        "total": total,
        "shown": len(feats),
        "dataset_key": key,
        "results": [feature_to_obj(f) for f in feats],
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

    state, county, key = resolve_any_county(query)
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

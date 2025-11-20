import os
import io
import json
import csv
import re
from flask import (
    Flask, render_template, request, jsonify,
    redirect, url_for, session, send_file
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

# We still use this prefix, but for Harris/Nueces we hard-code keys
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts")

MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def detect_zip(query: str):
    m = re.search(r"\b(\d{5})\b", query)
    return m.group(1) if m else None


def parse_filters(q: str):
    """
    Parse phrases like:
      - "over 200k income"
      - "homes above 500000"
      - "value over 800k"
    Returns (income_min, value_min)
    """
    q = q.lower().replace("$", "").replace(",", "")
    toks = q.split()

    income_min = None
    value_min = None

    for i, t in enumerate(toks):
        if t in ("over", "above", "greater", ">", "atleast", "at_least") and i + 1 < len(toks):
            raw = toks[i + 1]
            mult = 1
            if raw.endswith("k"):
                mult = 1000
                raw = raw[:-1]
            if raw.endswith("m"):
                mult = 1_000_000
                raw = raw[:-1]
            try:
                val = float(raw) * mult
            except Exception:
                continue

            window = " ".join(toks[max(0, i - 5): i + 5])
            if "income" in window:
                income_min = val
                continue
            if "value" in window or "home" in window or "house" in window:
                value_min = val
                continue

            # If no keyword, assume income first
            if income_min is None:
                income_min = val

    return income_min, value_min


def resolve_tx_county_dataset(query: str):
    """
    HARD-CODED resolver for exactly what you care about:
      - Harris County, TX
      - Nueces County, TX (incl. "Corpus Christi")
    Returns (state, county, dataset_key) or (None, None, None) if unsupported.
    """
    q = query.lower()

    state = "TX"  # we only support Texas here

    if "harris" in q:
        county = "harris"
        key = f"{S3_PREFIX}/tx/harris-with-values-income.geojson"
        return state, county, key

    if "nueces" in q or "corpus christi" in q:
        county = "nueces"
        key = f"{S3_PREFIX}/tx/nueces-with-values-income.geojson"
        return state, county, key

    return None, None, None


def load_geojson(key: str):
    data = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(data)


def feature_to_obj(f):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}
    coords = geom.get("coordinates") or [None, None]

    # Some GeoJSON can nest coords (e.g. MultiPoint, weird structures); handle len==2 case only
    if isinstance(coords[0], (list, tuple)):
        # try first point
        try:
            lon, lat = coords[0][0], coords[0][1]
        except Exception:
            lon, lat = None, None
    else:
        try:
            lon, lat = coords[0], coords[1]
        except Exception:
            lon, lat = None, None

    return {
        "address": " ".join(
            str(x) for x in [
                p.get("number") or p.get("house_number") or "",
                p.get("street") or p.get("road") or "",
                p.get("unit") or "",
            ] if x
        ),
        "city": p.get("city") or "",
        "state": p.get("region") or p.get("STUSPS") or "TX",
        "zip": p.get("postcode") or "",
        # prefer our enriched fields, but fall back to old if present
        "income": (
            p.get("median_income")
            or p.get("B19013_001E")
            or p.get("DP03_0062E")
            or p.get("income")
        ),
        "value": (
            p.get("median_value")
            or p.get("B25077_001E")
            or p.get("DP04_0089E")
            or p.get("home_value")
        ),
        "lat": lat,
        "lon": lon,
    }


def apply_filters(features, income_min, value_min, zip_code):
    out = []
    for f in features:
        p = f.get("properties", {}) or {}

        # ZIP filter
        if zip_code:
            if str(p.get("postcode")) != str(zip_code):
                continue

        # income filter
        if income_min:
            v = (
                p.get("median_income")
                or p.get("B19013_001E")
                or p.get("DP03_0062E")
                or p.get("income")
            )
            try:
                if v is None or float(v) < income_min:
                    continue
            except Exception:
                continue

        # value filter
        if value_min:
            v = (
                p.get("median_value")
                or p.get("B25077_001E")
                or p.get("DP04_0089E")
                or p.get("home_value")
            )
            try:
                if v is None or float(v) < value_min:
                    continue
            except Exception:
                continue

        out.append(f)
    return out


# ---------------------------------------------------------
# AUTH ROUTES
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
    # Simple health endpoint similar to what you've used before
    return jsonify({
        "ok": True,
        "env_aws_access_key": bool(os.getenv("AWS_ACCESS_KEY_ID")),
        "env_openai": bool(os.getenv("OPENAI_API_KEY")),
    })


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    q_lower = query.lower()

    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    # Only care about Harris + Nueces right now
    state, county, dataset_key = resolve_tx_county_dataset(q_lower)
    if not state or not county or not dataset_key:
        return jsonify({
            "ok": False,
            "error": "Right now, this tool only supports Harris County, TX and Nueces County, TX (Corpus Christi)."
        })

    # Load dataset
    try:
        gj = load_geojson(dataset_key)
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"Failed loading dataset: {dataset_key}",
            "details": str(e),
        }), 500

    feats = gj.get("features", [])

    # Parse filters
    zip_code = detect_zip(q_lower)
    income_min, value_min = parse_filters(q_lower)

    feats = apply_filters(feats, income_min, value_min, zip_code)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    return jsonify({
        "ok": True,
        "query": query,
        "state": state,
        "county": county,
        "city": None,
        "zip": zip_code,
        "dataset_key": dataset_key,
        "total": total,
        "shown": len(feats),
        "results": [feature_to_obj(f) for f in feats],
    })


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    q_lower = query.lower()

    if not query:
        return jsonify({"ok": False, "error": "Missing query."})

    state, county, dataset_key = resolve_tx_county_dataset(q_lower)
    if not state or not county or not dataset_key:
        return jsonify({
            "ok": False,
            "error": "Export currently only supported for Harris County, TX and Nueces County, TX."
        })

    try:
        gj = load_geojson(dataset_key)
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"Failed loading dataset: {dataset_key}",
            "details": str(e),
        }), 500

    feats = gj.get("features", [])

    zip_code = detect_zip(q_lower)
    income_min, value_min = parse_filters(q_lower)
    feats = apply_filters(feats, income_min, value_min, zip_code)

    # Build CSV
    out = io.StringIO()
    w = csv.writer(out)

    w.writerow([
        "address_number",
        "street",
        "unit",
        "city",
        "state",
        "zip",
        "income",
        "value",
        "lat",
        "lon",
    ])

    for f in feats:
        p = f.get("properties", {}) or {}
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates") or [None, None]

        if isinstance(coords[0], (list, tuple)):
            try:
                lon, lat = coords[0][0], coords[0][1]
            except Exception:
                lon, lat = None, None
        else:
            try:
                lon, lat = coords[0], coords[1]
            except Exception:
                lon, lat = None, None

        num = p.get("number") or p.get("house_number") or ""
        street = p.get("street") or p.get("road") or ""
        unit = p.get("unit") or ""

        city = p.get("city") or ""
        st = p.get("region") or p.get("STUSPS") or "TX"
        zipc = p.get("postcode") or ""

        income = (
            p.get("median_income")
            or p.get("B19013_001E")
            or p.get("DP03_0062E")
            or p.get("income")
        )
        value = (
            p.get("median_value")
            or p.get("B25077_001E")
            or p.get("DP04_0089E")
            or p.get("home_value")
        )

        w.writerow([num, street, unit, city, st, zipc, income, value, lat, lon])

    mem = io.BytesIO(out.getvalue().encode("utf-8"))
    mem.seek(0)

    filename = f"pelee_export_{county}_{state}.csv"

    return send_file(
        mem,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

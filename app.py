import os
import io
import json
import csv
import re
from typing import Dict, Tuple, Optional

from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    redirect,
    url_for,
    session,
    Response,
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

# Prefix where CLEANED GeoJSONs live (what your scripts wrote to)
# e.g. s3://residential-data-jack/merged_with_tracts_acs_clean/oh/cuyahoga-clean.geojson
CLEAN_PREFIX = os.getenv("CLEAN_PREFIX", "merged_with_tracts_acs_clean")

# Hard cap on how big a single GeoJSON file we allow to fully load (bytes)
# ~350MB is a compromise so Cuyahoga-sized monsters don't kill the worker.
MAX_GEOJSON_BYTES = int(os.getenv("MAX_GEOJSON_BYTES", "350000000"))

# Max features to show on-screen (export streams everything)
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)


# ---------------------------------------------------------
# LOGGING HELPER
# ---------------------------------------------------------
def log(msg: str):
    print(msg, flush=True)


# ---------------------------------------------------------
# STATE & DATASET INDEX
# ---------------------------------------------------------

STATE_CODES: Dict[str, str] = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
}

# (state_code, county_key) -> s3_key
ALL_DATASETS: Dict[Tuple[str, str], str] = {}

# (state_code, normalized_synonym) -> (state_code, county_key, s3_key)
SEARCH_INDEX: Dict[Tuple[str, str], Tuple[str, str, str]] = {}


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def scan_available_datasets():
    """
    Scan S3 for all cleaned county datasets.
    We do NOT assume anything about county names beyond:
      CLEAN_PREFIX/<state>/<county>-clean.geojson
    """
    global ALL_DATASETS, SEARCH_INDEX
    ALL_DATASETS = {}
    SEARCH_INDEX = {}

    log(f"[BOOT] Scanning S3 bucket={S3_BUCKET} prefix={CLEAN_PREFIX}/ for *-clean.geojson ...")

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=CLEAN_PREFIX + "/")

        count = 0
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith("-clean.geojson"):
                    continue

                # Expect at least CLEAN_PREFIX/state/file
                parts = key.split("/")
                if len(parts) < 3:
                    continue

                # parts[0] = CLEAN_PREFIX, parts[1] = state code, parts[-1] = filename
                state = parts[1].lower()
                fname = parts[-1]

                if state not in STATE_CODES.values():
                    continue

                if not fname.endswith("-clean.geojson"):
                    continue

                county = fname[:-len("-clean.geojson")]  # strip suffix
                ALL_DATASETS[(state, county)] = key
                count += 1

        log(f"[BOOT] Indexed {count} cleaned county datasets.")

        # Build search synonyms
        for (state, county), key in ALL_DATASETS.items():
            base = county.replace("_", " ")
            syns = {
                base,
                f"{base} county",
            }

            # Saint / St. normalization
            if "saint " in base:
                syns.add(base.replace("saint ", "st "))
                syns.add(base.replace("saint ", "st ") + " county")
            if "st " in base:
                syns.add(base.replace("st ", "saint "))
                syns.add(base.replace("st ", "saint ") + " county")

            for s in syns:
                norm_syn = _norm(s)
                SEARCH_INDEX[(state, norm_syn)] = (state, county, key)

        log(f"[BOOT] Built search index with {len(SEARCH_INDEX)} (state, county) synonyms.")

    except Exception as e:
        log(f"[ERROR] Failed to scan cleaned datasets: {e}")
        ALL_DATASETS = {}
        SEARCH_INDEX = {}


scan_available_datasets()


# ---------------------------------------------------------
# QUERY HELPERS
# ---------------------------------------------------------

def detect_zip(query: str) -> Optional[str]:
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

            if income_min is None:
                income_min = val

    return income_min, value_min


def detect_state_code(q: str) -> Optional[str]:
    """
    1) Prefer full state names ("ohio", "texas").
    2) Fallback to safe 2-letter codes like "tx", "oh" where they don't collide with common words.
    """
    q = q.lower()

    # Full names first
    for name, code in STATE_CODES.items():
        if name in q:
            return code

    # Safe two-letter codes
    ambiguous = {"in", "or", "me"}  # common words
    safe_codes = {c for c in STATE_CODES.values() if c not in ambiguous}

    for code in safe_codes:
        if re.search(rf"\b{code}\b", q):
            return code

    return None


def resolve_any_county(query: str):
    """
    Resolve ANY (state, county, s3_key) from the text using SEARCH_INDEX.
    """
    if not ALL_DATASETS:
        log("[RESOLVE] ALL_DATASETS is empty. No cleaned datasets loaded.")
        return None, None, None

    q_norm = _norm(query)
    state_code = detect_state_code(q_norm)

    if not state_code:
        log(f"[RESOLVE] No state found in query: {query!r}")
        return None, None, None

    best_match = None
    best_len = 0

    # Try to match longest synonym substring to avoid partial collisions
    for (state, syn), info in SEARCH_INDEX.items():
        if state != state_code:
            continue
        if syn in q_norm and len(syn) > best_len:
            best_match = info
            best_len = len(syn)

    if not best_match:
        log(f"[RESOLVE] No county match for state={state_code} in query={query!r}")
        return None, None, None

    s, county, key = best_match
    log(f"[RESOLVE] Matched state={s}, county={county}, key={key}")
    return s, county, key


# ---------------------------------------------------------
# GEOJSON HELPERS
# ---------------------------------------------------------

def load_geojson(key: str):
    """
    Load a GeoJSON object from S3, with a size guard so we don't OOM on huge counties.
    """
    try:
        head = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        size = head.get("ContentLength", 0)
        log(f"[S3] head_object key={key} size={size} bytes")

        if size and size > MAX_GEOJSON_BYTES:
            raise RuntimeError(
                f"Dataset {key} is too large to load in-memory ({size} bytes). "
                f"Max allowed is {MAX_GEOJSON_BYTES} bytes."
            )

        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj["Body"].read()
        log(f"[S3] Loaded {len(data)} bytes from {key}")
        return json.loads(data)
    except ClientError as ce:
        log(f"[ERROR] S3 error loading {key}: {ce}")
        raise
    except Exception as e:
        log(f"[ERROR] Failed to load or parse GeoJSON {key}: {e}")
        raise


def feature_to_obj(f):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}

    coords = geom.get("coordinates") or [None, None]

    if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (list, tuple)):
        # e.g. Polygon / MultiPolygon – just grab first vertex
        try:
            lon, lat = coords[0][0], coords[0][1]
        except Exception:
            lon, lat = None, None
    else:
        try:
            lon = coords[0]
            lat = coords[1] if len(coords) > 1 else None
        except Exception:
            lon, lat = None, None

    num = p.get("number") or p.get("NUMBER") or p.get("house_number") or ""
    street = p.get("street") or p.get("STREET") or p.get("road") or ""
    unit = p.get("unit") or p.get("UNIT") or ""

    city = p.get("city") or p.get("CITY") or ""
    st = (
        p.get("region")
        or p.get("REGION")
        or p.get("STUSPS")
        or ""
    )
    zipc = (
        p.get("postcode")
        or p.get("POSTCODE")
        or p.get("ZCTA5CE20")
        or ""
    )

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

    return {
        "address": " ".join(str(x) for x in [num, street, unit] if x),
        "number": num,
        "street": street,
        "unit": unit,
        "city": city,
        "state": st,
        "zip": zipc,
        "income": income,
        "value": value,
        "lat": lat,
        "lon": lon,
    }


def apply_filters(features, income_min, value_min, zip_code):
    out = []
    for f in features:
        p = f.get("properties", {}) or {}

        # ZIP filter
        if zip_code:
            feature_zip = (
                p.get("postcode")
                or p.get("POSTCODE")
                or p.get("ZCTA5CE20")
                or ""
            )
            if str(feature_zip) != str(zip_code):
                continue

        # Income filter
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

        # Value filter
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
# AUTH
# ---------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
            session["authed"] = True
            log("[AUTH] Login success")
            return redirect(url_for("index"))
        log("[AUTH] Login failed")
        return render_template("login.html", error="Invalid password.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    log("[AUTH] Logged out")
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
    return jsonify({
        "ok": True,
        "bucket": S3_BUCKET,
        "clean_prefix": CLEAN_PREFIX,
        "dataset_count": len(ALL_DATASETS),
        "search_index_entries": len(SEARCH_INDEX),
        "aws_region": AWS_REGION,
    })


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    log(f"[SEARCH] Incoming query: {query!r}")

    try:
        state, county, key = resolve_any_county(query)
        if not key:
            return jsonify({
                "ok": False,
                "error": "No cleaned dataset found for that location."
            })

        gj = load_geojson(key)
        feats = gj.get("features", [])

        zip_code = detect_zip(query)
        income_min, value_min = parse_filters(query)

        log(f"[SEARCH] Filters zip={zip_code}, income_min={income_min}, value_min={value_min}")

        feats = apply_filters(feats, income_min, value_min, zip_code)

        total = len(feats)
        feats = feats[:MAX_RESULTS]

        log(f"[SEARCH] state={state}, county={county}, key={key}, total={total}, shown={len(feats)}")

        return jsonify({
            "ok": True,
            "query": query,
            "state": state.upper(),
            "county": county.replace("_", " ").title(),
            "zip": zip_code,
            "dataset_key": key,
            "total": total,
            "shown": len(feats),
            "results": [feature_to_obj(f) for f in feats],
        })

    except RuntimeError as re_err:
        # e.g. file too big, explicit guard
        log(f"[ERROR] Runtime error for query={query!r}: {re_err}")
        return jsonify({
            "ok": False,
            "error": str(re_err),
        }), 500
    except Exception as e:
        log(f"[ERROR] Unhandled error during /search: {e}")
        return jsonify({
            "ok": False,
            "error": "Server error during search.",
            "details": str(e),
        }), 500


# ---------------------------------------------------------
# EXPORT
# ---------------------------------------------------------

@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Missing query."})

    log(f"[EXPORT] Incoming export query: {query!r}")

    try:
        state, county, key = resolve_any_county(query)
        if not key:
            return jsonify({"ok": False, "error": "Dataset not found for that location."}), 400

        gj = load_geojson(key)
        feats = gj.get("features", [])

        zip_code = detect_zip(query)
        income_min, value_min = parse_filters(query)
        feats = apply_filters(feats, income_min, value_min, zip_code)

        log(f"[EXPORT] state={state}, county={county}, key={key}, rows={len(feats)}")

        filename = f"pelee_export_{county}_{state}.csv"

        def generate():
            buffer = io.StringIO()
            writer = csv.writer(buffer)

            # header
            writer.writerow([
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
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

            for f in feats:
                r = feature_to_obj(f)
                writer.writerow([
                    r["number"],
                    r["street"],
                    r["unit"],
                    r["city"],
                    r["state"],
                    r["zip"],
                    r["income"],
                    r["value"],
                    r["lat"],
                    r["lon"],
                ])
                chunk = buffer.getvalue()
                if chunk:
                    yield chunk
                    buffer.seek(0)
                    buffer.truncate(0)

        return Response(
            generate(),
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

    except RuntimeError as re_err:
        log(f"[ERROR] Runtime error during /export: {re_err}")
        return jsonify({
            "ok": False,
            "error": str(re_err),
        }), 500
    except Exception as e:
        log(f"[ERROR] Unhandled error during /export: {e}")
        return jsonify({
            "ok": False,
            "error": "Server error during export.",
            "details": str(e),
        }), 500


# ---------------------------------------------------------
# MAIN (local dev)
# ---------------------------------------------------------

if __name__ == "__main__":
    log("[BOOT] Starting Flask dev server on 0.0.0.0:5000")
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

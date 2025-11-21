import os
import io
import json
import csv
import re
import logging

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
# BASIC LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# CLEANED dataset prefix
CLEAN_PREFIX = os.getenv("CLEAN_PREFIX", "merged_with_tracts_acs_clean")

# Hard cap for GeoJSON file size (bytes) to avoid OOM on Render
MAX_GEOJSON_BYTES = int(os.getenv("MAX_GEOJSON_BYTES", "350000000"))

# On-screen max results
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATES & DATASET INDEX
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

STATE_NAME_BY_CODE = {code: name for name, code in STATE_CODES.items()}

# DATASETS: (state_code, county_slug) -> s3_key
DATASETS = {}

# COUNTY_SYNONYMS[state_code][phrase] = county_slug
COUNTY_SYNONYMS = {}


def _add_synonym(state: str, county: str, phrase: str):
    """Register a lowercase phrase → county slug mapping for a given state."""
    phrase = phrase.strip().lower()
    if not phrase:
        return
    COUNTY_SYNONYMS.setdefault(state, {})
    # We allow overwrites; collisions are rare and not catastrophic for our use.
    COUNTY_SYNONYMS[state][phrase] = county


def scan_available_datasets():
    """
    Scan S3 for all cleaned county-level datasets and build:
      - DATASETS[(state, county)] = key
      - COUNTY_SYNONYMS[state][phrase] = county
    Assumes keys like:
      merged_with_tracts_acs_clean/oh/cuyahoga-clean.geojson
      merged_with_tracts_acs_clean/tx/harris-clean.geojson
    """
    global DATASETS, COUNTY_SYNONYMS
    DATASETS = {}
    COUNTY_SYNONYMS = {}

    prefix = CLEAN_PREFIX.rstrip("/") + "/"

    log.info(
        "[BOOT] Scanning S3 bucket=%s prefix=%s for *-clean.geojson ...",
        S3_BUCKET,
        prefix,
    )

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    count_keys = 0
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            parts = key.split("/")
            # Expect: [merged_with_tracts_acs_clean, state, filename]
            if len(parts) != 3:
                continue

            _, state, fname = parts
            state = state.lower().strip()

            if state not in STATE_NAME_BY_CODE:
                # Skip states outside our mapping (e.g. territories, DC)
                continue

            county_slug = fname.replace("-clean.geojson", "").lower().strip()
            DATASETS[(state, county_slug)] = key
            count_keys += 1

            # Build synonyms for county names
            county_words = county_slug.replace("_", " ")
            state_name = STATE_NAME_BY_CODE[state]

            # Raw county name
            _add_synonym(state, county_slug, county_words)
            # "harris county"
            _add_synonym(state, county_slug, f"{county_words} county")
            # "harris county tx"
            _add_synonym(state, county_slug, f"{county_words} county {state}")
            _add_synonym(state, county_slug, f"{county_words} county {state.upper()}")
            # "harris county texas"
            _add_synonym(state, county_slug, f"{county_words} county {state_name}")
            # "harris, tx"
            _add_synonym(state, county_slug, f"{county_words}, {state}")
            # "harris, texas"
            _add_synonym(state, county_slug, f"{county_words}, {state_name}")

    total_synonyms = sum(len(v) for v in COUNTY_SYNONYMS.values())
    log.info("[BOOT] Indexed %d cleaned county datasets.", count_keys)
    log.info("[BOOT] Built search index with %d (state, county) synonyms.", total_synonyms)


scan_available_datasets()

# ---------------------------------------------------------
# TEXT PARSING HELPERS
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

            if income_min is None:
                income_min = val

    return income_min, value_min


def resolve_state(query: str):
    """
    Try to detect a state code from the query using both full name and 2-letter code.
    """
    q = query.lower()

    # 1) Full state names
    for name, code in STATE_CODES.items():
        if name in q:
            return code

    # 2) 2-letter codes with word boundaries / punctuation
    for code in STATE_NAME_BY_CODE.keys():
        pattern = r"(^|[\s,])" + re.escape(code) + r"([\s,.]|$)"
        if re.search(pattern, q):
            return code

    return None


def resolve_county(query: str, state_code: str):
    """
    Use COUNTY_SYNONYMS to find the county for a given state_code.
    - First, match the longest synonym phrase that appears in the query.
    - If nothing matches, fallback to any county whose slug or 'slug with spaces'
      is a substring of the query.
    """
    q = query.lower()

    # 1) Synonym-based: longest phrase first
    synonyms = COUNTY_SYNONYMS.get(state_code, {})
    best_match = None
    best_len = 0

    for phrase, county in synonyms.items():
        if phrase in q and len(phrase) > best_len:
            best_match = county
            best_len = len(phrase)

    if best_match:
        return best_match

    # 2) Fallback: check every county slug
    candidates = [
        county for (st, county) in DATASETS.keys() if st == state_code
    ]

    for county in candidates:
        words = county.replace("_", " ")
        if words in q or county in q:
            return county

    return None


def resolve_any_county(query: str):
    """
    Full resolver:
      - detect state
      - detect county within that state
      - return (state_code, county_slug, s3_key)
    """
    state_code = resolve_state(query)
    if not state_code:
        log.info("[RESOLVE] No state match in query='%s'", query)
        return None, None, None

    county_slug = resolve_county(query, state_code)
    if not county_slug:
        log.info(
            "[RESOLVE] No county match for state=%s in query='%s'",
            state_code,
            query,
        )
        return state_code, None, None

    key = DATASETS.get((state_code, county_slug))
    if not key:
        log.info(
            "[RESOLVE] State %s, county %s not found in DATASETS",
            state_code,
            county_slug,
        )
        return state_code, county_slug, None

    log.info(
        "[RESOLVE] Matched state=%s, county=%s, key=%s",
        state_code,
        county_slug,
        key,
    )
    return state_code, county_slug, key


# ---------------------------------------------------------
# GEOJSON LOAD & FILTER HELPERS
# ---------------------------------------------------------


def load_geojson_limited(key: str):
    """
    Head the object to check size, then load only if below MAX_GEOJSON_BYTES.
    """
    try:
        head = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        raise RuntimeError(f"Failed to head_object for {key}: {e}")

    size = head.get("ContentLength", 0)
    log.info("[S3] head_object key=%s size=%d bytes", key, size)

    if size > MAX_GEOJSON_BYTES:
        raise RuntimeError(
            f"Dataset {key} is too large to load in-memory ({size} bytes). "
            f"Max allowed is {MAX_GEOJSON_BYTES} bytes."
        )

    try:
        body = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(body)
    except Exception as e:
        raise RuntimeError(f"Failed to load/parse GeoJSON {key}: {e}")


def get_zip_from_props(p: dict):
    return (
        p.get("postcode")
        or p.get("POSTCODE")
        or p.get("ZCTA5CE20")
        or ""
    )


def feature_to_obj(f):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}

    coords = geom.get("coordinates") or [None, None]
    lat, lon = None, None
    try:
        if isinstance(coords[0], (list, tuple)):
            lon, lat = coords[0][0], coords[0][1]
        else:
            lon, lat = coords[0], coords[1]
    except Exception:
        lat, lon = None, None

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
    zipc = get_zip_from_props(p)

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
            feature_zip = str(get_zip_from_props(p))
            if feature_zip != str(zip_code):
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
            log.info("[AUTH] Login success")
            return redirect(url_for("index"))
        log.info("[AUTH] Login failed")
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
    datasets_count = len(DATASETS)
    synonyms_count = sum(len(v) for v in COUNTY_SYNONYMS.values())
    return jsonify({
        "ok": True,
        "datasets": datasets_count,
        "synonyms": synonyms_count,
        "max_geojson_bytes": MAX_GEOJSON_BYTES,
    })


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    log.info("[SEARCH] Incoming query: '%s'", query)
    try:
        state, county, key = resolve_any_county(query)
        if not state:
            return jsonify({
                "ok": False,
                "error": "Could not detect a state in that query. Try 'Franklin County Ohio' or 'Harris County TX'."
            })
        if not county:
            return jsonify({
                "ok": False,
                "error": f"No cleaned dataset found for that county in {state.upper()}."
            })
        if not key:
            return jsonify({
                "ok": False,
                "error": f"No cleaned dataset found for {county.replace('_', ' ').title()}, {state.upper()}."
            })

        # Load dataset with size guard
        gj = load_geojson_limited(key)
        feats = gj.get("features", [])

        zip_code = detect_zip(query)
        income_min, value_min = parse_filters(query)

        feats = apply_filters(feats, income_min, value_min, zip_code)
        total = len(feats)
        feats = feats[:MAX_RESULTS]

        log.info(
            "[RESULTS] %s, %s → total=%d, shown=%d (key=%s)",
            county,
            state,
            total,
            len(feats),
            key,
        )

        state_name = STATE_NAME_BY_CODE.get(state, state.upper())
        return jsonify({
            "ok": True,
            "query": query,
            "state": state.upper(),
            "state_name": state_name,
            "county": county.replace("_", " ").title(),
            "zip": zip_code,
            "dataset_key": key,
            "total": total,
            "shown": len(feats),
            "results": [feature_to_obj(f) for f in feats],
            "message": None,
        })

    except RuntimeError as e:
        # Includes "dataset too large" messages
        log.error("[ERROR] Runtime error for query='%s': %s", query, e)
        return jsonify({"ok": False, "error": str(e)}), 500
    except Exception as e:
        log.exception("[ERROR] Unexpected error for query='%s'", query)
        return jsonify({"ok": False, "error": "Unexpected server error."}), 500


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

    log.info("[EXPORT] Incoming export query: '%s'", query)

    try:
        state, county, key = resolve_any_county(query)
        if not state or not county or not key:
            return jsonify({"ok": False, "error": "Dataset not found for that location."})

        # Use same size guard for now (you can relax this later if you want)
        gj = load_geojson_limited(key)
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
            buffer.seek(0)
            buffer.truncate(0)

            for f in feats:
                r = feature_to_obj(f)
                writer.writerow([
                    r["address"],
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

        log.info(
            "[EXPORT] Streaming %d rows for %s, %s (key=%s)",
            len(feats),
            county,
            state,
            key,
        )

        return Response(
            generate(),
            mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except RuntimeError as e:
        log.error("[ERROR] Runtime error during export for query='%s': %s", query, e)
        return jsonify({"ok": False, "error": str(e)}), 500
    except Exception as e:
        log.exception("[ERROR] Unexpected export error for query='%s'", query)
        return jsonify({"ok": False, "error": "Unexpected server error during export."}), 500


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

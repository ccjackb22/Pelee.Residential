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
# LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# CLEANED dataset prefix (your nationwide cleaned files)
CLEAN_PREFIX = os.getenv("CLEAN_PREFIX", "merged_with_tracts_acs_clean")

# Max bytes we allow for a GeoJSON before we bail
MAX_GEOJSON_BYTES = int(os.getenv("MAX_GEOJSON_BYTES", "950000000"))  # ~0.95 GB

# Max results for on-screen results
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATE + COUNTY INDEXES
# ---------------------------------------------------------

STATE_CODES = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct", "delaware": "de",
    "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id",
    "illinois": "il", "indiana": "in", "iowa": "ia", "kansas": "ks",
    "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md",
    "massachusetts": "ma", "michigan": "mi", "minnesota": "mn",
    "mississippi": "ms", "missouri": "mo", "montana": "mt", "nebraska": "ne",
    "nevada": "nv", "new hampshire": "nh", "new jersey": "nj",
    "new mexico": "nm", "new york": "ny", "north carolina": "nc",
    "north dakota": "nd", "ohio": "oh", "oklahoma": "ok", "oregon": "or",
    "pennsylvania": "pa", "rhode island": "ri", "south carolina": "sc",
    "south dakota": "sd", "tennessee": "tn", "texas": "tx", "utah": "ut",
    "vermont": "vt", "virginia": "va", "washington": "wa",
    "west virginia": "wv", "wisconsin": "wi", "wyoming": "wy",
}

# (state, county_slug) -> s3_key
ALL_DATASETS = {}

# (state, phrase) -> county_slug  (phrase is like "harris", "harris county", etc.)
COUNTY_SYNONYMS = {}

AMBIGUOUS_ABBRS = {"in", "or", "me", "hi", "ok"}  # don't treat these as state codes blindly


def _normalize_county_slug(slug: str) -> str:
    """
    "st_louis_city" -> "st louis city"
    "de_soto" -> "de soto"
    """
    return slug.replace("_", " ")


def _saint_variants(name: str):
    """
    Handle St. vs Saint:
      "st louis" -> ["st louis", "saint louis"]
      "saint louis" -> ["saint louis", "st louis"]
    """
    name = name.strip()
    out = {name}
    if name.startswith("st "):
        out.add("saint " + name[3:])
    if name.startswith("saint "):
        out.add("st " + name[6:])
    return out


def scan_available_datasets():
    """
    Scan S3 for all cleaned county GeoJSONs and build:
      - ALL_DATASETS[(state, county_slug)] = s3_key
      - COUNTY_SYNONYMS[(state, phrase)] = county_slug
    """
    global ALL_DATASETS, COUNTY_SYNONYMS

    logger.info(
        "[BOOT] Scanning S3 bucket=%s prefix=%s/ for *-clean.geojson ...",
        S3_BUCKET,
        CLEAN_PREFIX,
    )

    ALL_DATASETS = {}
    COUNTY_SYNONYMS = {}

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=CLEAN_PREFIX + "/")

    total_keys = 0
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

            county_slug = fname.replace("-clean.geojson", "")
            ALL_DATASETS[(state, county_slug)] = key
            total_keys += 1

            # Build synonyms for search
            base = _normalize_county_slug(county_slug)  # e.g. "harris"
            base_variants = _saint_variants(base)

            for bv in base_variants:
                phrases = {
                    bv,                      # "harris"
                    f"{bv} county",          # "harris county"
                    f"{bv} co",              # "harris co"
                    f"{bv} parish",          # for LA, etc.
                }
                for phrase in phrases:
                    phrase = phrase.strip()
                    if phrase:
                        COUNTY_SYNONYMS[(state, phrase)] = county_slug

    logger.info(
        "[BOOT] Indexed %d cleaned county datasets.",
        total_keys,
    )
    logger.info(
        "[BOOT] Built search index with %d (state, county) synonyms.",
        len(COUNTY_SYNONYMS),
    )


scan_available_datasets()

# ---------------------------------------------------------
# HELPERS: STATE / COUNTY RESOLUTION
# ---------------------------------------------------------


def detect_state_code(query: str):
    """
    Detect which US state is being asked for.
    Priority:
      1) Full state names ("ohio", "texas", etc.)
      2) 2-letter codes ("oh", "tx"), but NOT ambiguous codes like "in", "or".
    Returns a 2-letter code or None.
    """
    q = query.lower()

    # 1) Full names
    hits = set()
    for name, code in STATE_CODES.items():
        if re.search(r"\b" + re.escape(name) + r"\b", q):
            hits.add(code)

    if hits:
        # If multiple somehow, just pick one deterministically
        return sorted(hits)[0]

    # 2) Abbreviations (avoid ambiguous codes like "in" from the word "in")
    words = re.findall(r"\b[a-z]{2}\b", q)
    for w in words:
        if w in STATE_CODES.values() and w not in AMBIGUOUS_ABBRS:
            return w

    return None


def resolve_any_county(query: str):
    """
    Resolve ANY county + state using our synonym index:
      - detect state code
      - then look for a matching county phrase for that state
    """
    q = query.lower()
    state_code = detect_state_code(q)

    if not state_code:
        logger.info("[RESOLVE] No state match in query='%s'", query)
        return None, None, None

    # First pass: exact phrase match
    for (st, phrase), county_slug in COUNTY_SYNONYMS.items():
        if st != state_code:
            continue
        # word boundary around phrase, so we don't get substring garbage
        if re.search(r"\b" + re.escape(phrase) + r"\b", q):
            logger.info(
                "[RESOLVE] Matched state=%s, county=%s via phrase='%s'",
                state_code,
                county_slug,
                phrase,
            )
            return state_code, county_slug, ALL_DATASETS[(st, county_slug)]

    logger.info(
        "[RESOLVE] No county match for state=%s in query='%s'",
        state_code,
        query,
    )
    return None, None, None


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


def load_geojson_with_guard(key: str):
    """
    Load a GeoJSON from S3, but first check its size via head_object.
    We allow up to MAX_GEOJSON_BYTES (default ~0.95 GB).
    """
    try:
        head = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        size = head.get("ContentLength", 0)
        logger.info("[S3] head_object key=%s size=%d bytes", key, size)
        if size > MAX_GEOJSON_BYTES:
            raise RuntimeError(
                f"Dataset {key} is too large to load in-memory ({size} bytes). "
                f"Max allowed is {MAX_GEOJSON_BYTES} bytes."
            )
    except ClientError as ce:
        logger.error("[S3] head_object failed for key=%s: %s", key, ce)
        raise

    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj["Body"].read()
        return json.loads(data)
    except Exception as e:
        logger.error("[ERROR] Failed to load or parse GeoJSON %s: %s", key, e)
        raise


def feature_to_obj(f):
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
            f_zip = (
                p.get("postcode")
                or p.get("POSTCODE")
                or p.get("ZCTA5CE20")
                or ""
            )
            if str(f_zip) != str(zip_code):
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

        # home value filter
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
            logger.info("[AUTH] Login success")
            session["authed"] = True
            return redirect(url_for("index"))
        logger.info("[AUTH] Login failed")
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
    return jsonify({
        "ok": True,
        "datasets": len(ALL_DATASETS),
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

    logger.info("[SEARCH] Incoming query: '%s'", query)

    try:
        state, county_slug, key = resolve_any_county(query)
        if not key:
            return jsonify({
                "ok": False,
                "error": "No cleaned dataset found for that county/state.",
            })

        # Load dataset (with size guard)
        gj = load_geojson_with_guard(key)
        feats = gj.get("features", [])

        zip_code = detect_zip(query)
        income_min, value_min = parse_filters(query)

        feats = apply_filters(feats, income_min, value_min, zip_code)

        total = len(feats)
        feats = feats[:MAX_RESULTS]

        county_name = _normalize_county_slug(county_slug).title()

        return jsonify({
            "ok": True,
            "query": query,
            "state": state.upper(),
            "county": county_name,
            "zip": zip_code,
            "dataset_key": key,
            "total": total,
            "shown": len(feats),
            "results": [feature_to_obj(f) for f in feats],
        })

    except Exception as e:
        logger.error(
            "[ERROR] Runtime error for query='%s': %s",
            query,
            e,
        )
        return jsonify({
            "ok": False,
            "error": str(e),
        }), 500


# ---------------------------------------------------------
# EXPORT (CSV STREAM)
# ---------------------------------------------------------

@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Missing query."})

    logger.info("[EXPORT] Incoming export query: '%s'", query)

    try:
        state, county_slug, key = resolve_any_county(query)
        if not key:
            return jsonify({"ok": False, "error": "Dataset not found."})

        gj = load_geojson_with_guard(key)
        feats = gj.get("features", [])

        zip_code = detect_zip(query)
        income_min, value_min = parse_filters(query)
        feats = apply_filters(feats, income_min, value_min, zip_code)

        filename = f"pelee_export_{county_slug}_{state}.csv"

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

                writer.writerow([num, street, unit, city, st, zipc, income, value, lat, lon])

                chunk = buffer.getvalue()
                if chunk:
                    yield chunk
                    buffer.seek(0)
                    buffer.truncate(0)

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"'
        }

        return Response(generate(), mimetype="text/csv", headers=headers)

    except Exception as e:
        logger.error(
            "[ERROR] Export error for query='%s': %s",
            query,
            e,
        )
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

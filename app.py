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

# CLEANED dataset prefix (what your cleaner wrote to)
CLEAN_PREFIX = "merged_with_tracts_acs_clean"

# Hard limit for loading whole file into memory (bytes)
MAX_IN_MEMORY_BYTES = 350_000_000

# Max number of rows for on-screen display
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATE NAMES
# ---------------------------------------------------------
STATE_CODES = {
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

TWO_LETTER_CODES = set(STATE_CODES.values())

# ---------------------------------------------------------
# DATASET INDEX
# ---------------------------------------------------------
# Each entry: {
#   "state": "oh",
#   "county": "cuyahoga",
#   "key": "merged_with_tracts_acs_clean/oh/cuyahoga-clean.geojson",
#   "size": 529680486
# }
DATASETS = []  # list of dicts
DATASET_BY_STATE_COUNTY = {}  # (state, county) -> dict


def scan_cleaned_datasets():
    """
    Scan S3 once on boot to discover all cleaned county-level datasets.
    Looks for:
        merged_with_tracts_acs_clean/<state>/<county>-clean.geojson
    """
    global DATASETS, DATASET_BY_STATE_COUNTY
    DATASETS = []
    DATASET_BY_STATE_COUNTY = {}

    logger.info(
        "[BOOT] Scanning S3 bucket=%s prefix=%s/ for *-clean.geojson ...",
        S3_BUCKET,
        CLEAN_PREFIX,
    )

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{CLEAN_PREFIX}/")

    total = 0
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]  # e.g. merged_with_tracts_acs_clean/oh/cuyahoga-clean.geojson
            if not key.endswith("-clean.geojson"):
                continue

            parts = key.split("/")
            if len(parts) != 3:
                continue

            _, state, filename = parts
            state = state.lower()
            if state not in TWO_LETTER_CODES:
                continue

            county = filename.replace("-clean.geojson", "").lower()
            size = obj.get("Size", 0)

            meta = {
                "state": state,
                "county": county,
                "key": key,
                "size": size,
            }
            DATASETS.append(meta)
            DATASET_BY_STATE_COUNTY[(state, county)] = meta
            total += 1

    logger.info("[BOOT] Indexed %d cleaned county datasets.", total)


scan_cleaned_datasets()

# ---------------------------------------------------------
# HELPERS: PARSING QUERY
# ---------------------------------------------------------


def detect_zip(query: str):
    m = re.search(r"\b(\d{5})\b", query)
    return m.group(1) if m else None


def parse_filters(q: str):
    """
    Parse things like:
      "over 200k income", "homes above 500000", "value over 800k"
    Returns (income_min, value_min)
    """
    q = q.lower().replace("$", "").replace(",", "")
    toks = q.split()

    income_min = None
    value_min = None

    for i, t in enumerate(toks):
        if t in ("over", "above", "greater", ">", "atleast", "at_least") and i + 1 < len(
            toks
        ):
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

            window = " ".join(toks[max(0, i - 5) : i + 5])
            if "income" in window:
                income_min = val
                continue
            if "value" in window or "home" in window or "house" in window:
                value_min = val
                continue

            if income_min is None:
                income_min = val

    return income_min, value_min


def detect_state(query: str):
    """
    Detect state code from full state name or 2-letter code.
    We avoid matching random 'in' by requiring word boundaries.
    """
    q = f" {query.lower()} "

    # First: full names ("ohio", "texas", etc.)
    for name, code in STATE_CODES.items():
        if f" {name} " in q:
            return code

    # Second: 2-letter codes, uppercase in the original query
    # We check against the original raw query for capital letters.
    raw = f" {query} "

    for code in TWO_LETTER_CODES:
        pattern = f" {code.upper()} "
        if pattern in raw:
            return code

    return None


def normalize_county_slug(county: str) -> str:
    """
    Convert county slug like 'saint_louis_city' -> 'saint louis city'
    """
    return county.replace("_", " ")


def county_matches_query(county_slug: str, q: str) -> bool:
    """
    Simple substring match with a few synonyms for saint/st.
    q is already lowercased here.
    """
    canon = normalize_county_slug(county_slug)  # e.g. 'harris', 'fort bend'
    variants = {canon}

    # Saint / St shortcuts
    if canon.startswith("st "):
        variants.add("saint " + canon[3:])
    if canon.startswith("ste "):
        variants.add("sainte " + canon[4:])

    # Add ' X county', ' X parish', etc. as variants
    extra = set()
    for v in variants:
        extra.add(f"{v} county")
        extra.add(f"{v} parish")
        extra.add(f"{v} borough")
    variants |= extra

    for v in variants:
        if v in q:
            return True

    return False


def resolve_location(query: str):
    """
    Given a natural-language query, resolve to a specific (state, county, key, size).

    Returns:
        (state, county, key, size) or (None, None, None, None)
    """
    q = query.lower()
    state = detect_state(query)

    if not state:
        logger.info("[RESOLVE] Could not detect state from query='%s'", query)
        return None, None, None, None

    # Find candidate counties for that state
    candidates = [m for m in DATASETS if m["state"] == state]
    if not candidates:
        logger.info("[RESOLVE] No datasets at all for state=%s", state)
        return None, None, None, None

    # Try to match a county by substring
    for meta in candidates:
        county = meta["county"]
        if county_matches_query(county, q):
            logger.info(
                "[RESOLVE] Matched state=%s, county=%s, key=%s, size=%d",
                state,
                county,
                meta["key"],
                meta["size"],
            )
            return meta["state"], meta["county"], meta["key"], meta["size"]

    logger.info(
        "[RESOLVE] No county match for state=%s in query='%s' (candidates=%d)",
        state,
        query,
        len(candidates),
    )
    return None, None, None, None


# ---------------------------------------------------------
# DATA LOADING + FILTERING
# ---------------------------------------------------------


def load_geojson_full(key: str, size: int):
    """
    Load entire GeoJSON into memory for "normal" sized counties.
    We enforce a guardrail by size.
    """
    if size > MAX_IN_MEMORY_BYTES:
        raise RuntimeError(
            f"Dataset {key} is too large to load in-memory ({size} bytes). "
            f"Max allowed is {MAX_IN_MEMORY_BYTES} bytes."
        )

    logger.info("[LOAD] Fetching full GeoJSON key=%s size=%d", key, size)

    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        logger.error("[LOAD] S3 get_object failed for key=%s: %s", key, e)
        raise

    body = obj["Body"].read()
    return json.loads(body)


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
    st = p.get("region") or p.get("REGION") or p.get("STUSPS") or ""
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
            logger.info("[AUTH] Login success")
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
    return jsonify(
        {
            "ok": True,
            "datasets_indexed": len(DATASETS),
            "aws_region": AWS_REGION,
        }
    )


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    logger.info("[SEARCH] Incoming query='%s'", query)

    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    # Resolve county + state
    state, county, key, size = resolve_location(query)
    if not key:
        return jsonify(
            {
                "ok": False,
                "error": "No cleaned dataset found for that county/state.",
            }
        )

    # If dataset is too big for safe in-memory load, bail with a *clear* message
    if size > MAX_IN_MEMORY_BYTES:
        msg = (
            f"Dataset {key} is too large to query directly in the web tool "
            f"({size:,} bytes). "
            "Try narrowing your query (add a ZIP code, an 'income over X' or "
            "'value over X' filter) or use a smaller county. "
            "For full-county exports, run an offline script against S3."
        )
        logger.warning("[SEARCH] %s", msg)
        return jsonify({"ok": False, "error": msg})

    # Load full GeoJSON
    try:
        gj = load_geojson_full(key, size)
    except Exception as e:
        logger.error(
            "[ERROR] Failed to load dataset key=%s: %s", key, e, exc_info=True
        )
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Failed to load dataset {key}",
                    "details": str(e),
                }
            ),
            500,
        )

    feats = gj.get("features", [])

    # Filters
    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    feats = apply_filters(feats, income_min, value_min, zip_code)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    logger.info(
        "[SEARCH] state=%s county=%s key=%s size=%d total_matches=%d shown=%d",
        state,
        county,
        key,
        size,
        total,
        len(feats),
    )

    return jsonify(
        {
            "ok": True,
            "query": query,
            "state": state.upper(),
            "county": normalize_county_slug(county).title(),
            "city": None,
            "zip": zip_code,
            "dataset_key": key,
            "total": total,
            "shown": len(feats),
            "results": [feature_to_obj(f) for f in feats],
        }
    )


# ---------------------------------------------------------
# EXPORT
# ---------------------------------------------------------


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    logger.info("[EXPORT] Incoming query='%s'", query)

    state, county, key, size = resolve_location(query)
    if not key:
        return jsonify({"ok": False, "error": "Dataset not found for that location."})

    # Same size guard for export (we still need to read everything)
    if size > MAX_IN_MEMORY_BYTES:
        msg = (
            f"Dataset {key} is too large for direct export via the web tool "
            f"({size:,} bytes). For full-county exports, run an offline script "
            "directly against S3 (using this key) or shard the county into smaller files."
        )
        logger.warning("[EXPORT] %s", msg)
        return jsonify({"ok": False, "error": msg})

    try:
        gj = load_geojson_full(key, size)
    except Exception as e:
        logger.error(
            "[ERROR] Failed to load dataset key=%s for export: %s",
            key,
            e,
            exc_info=True,
        )
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Failed loading dataset {key}",
                    "details": str(e),
                }
            ),
            500,
        )

    feats = gj.get("features", [])

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)
    feats = apply_filters(feats, income_min, value_min, zip_code)

    filename = f"pelee_export_{county}_{state}.csv"

    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        # Header
        writer.writerow(
            [
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
            ]
        )
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
            st = p.get("region") or p.get("REGION") or p.get("STUSPS") or ""
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

            writer.writerow([num, street, unit, city, st, zipc, income, value, lat, lon])

            data_chunk = buffer.getvalue()
            if data_chunk:
                yield data_chunk
                buffer.seek(0)
                buffer.truncate(0)

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
    }

    logger.info(
        "[EXPORT] state=%s county=%s key=%s size=%d rows=%d",
        state,
        county,
        key,
        size,
        len(feats),
    )

    return Response(generate(), mimetype="text/csv", headers=headers)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

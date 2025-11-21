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
# CONFIG & LOGGING
# ---------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# CLEANED dataset prefix on S3
CLEAN_PREFIX = os.getenv("CLEAN_PREFIX", "merged_with_tracts_acs_clean")

# Hard cap on how big a single GeoJSON file we will load in memory.
# 350MB is a compromise so we don't OOM Render.
MAX_JSON_BYTES = int(os.getenv("MAX_JSON_BYTES", "350000000"))

# Max results to show in UI
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATE / COUNTY TABLES
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

STATE_FULL_BY_CODE = {code: name for name, code in STATE_CODES.items()}

# (state_code, county_slug) -> {"key": s3_key, "size": bytes}
ALL_DATASETS = {}

# "cuyahoga county", "cuyahoga", etc -> (state_code, county_slug)
COUNTY_SYNONYMS = {}

# ---------------------------------------------------------
# BOOT-TIME S3 SCAN
# ---------------------------------------------------------


def scan_available_datasets():
    """
    Scan S3 for all cleaned county GeoJSONs and build:
      - ALL_DATASETS[(state_code, county_slug)] = {"key": key, "size": size}
      - COUNTY_SYNONYMS["cuyahoga county"] = ("oh", "cuyahoga")
    """
    global ALL_DATASETS, COUNTY_SYNONYMS
    ALL_DATASETS = {}
    COUNTY_SYNONYMS = {}

    logger.info(
        "[BOOT] Scanning S3 bucket=%s prefix=%s/ for *-clean.geojson ...",
        S3_BUCKET,
        CLEAN_PREFIX,
    )

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{CLEAN_PREFIX}/")

    total_files = 0
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            parts = key.split("/")
            # Expect: merged_with_tracts_acs_clean/state/county-clean.geojson
            if len(parts) != 3:
                continue

            _, state_code, filename = parts
            if state_code not in STATE_FULL_BY_CODE:
                # Skip weird directories
                continue

            size = obj.get("Size", 0)
            county_slug = filename.replace("-clean.geojson", "")

            ALL_DATASETS[(state_code, county_slug)] = {
                "key": key,
                "size": size,
            }
            total_files += 1

    logger.info("[BOOT] Indexed %d cleaned county datasets.", total_files)

    # Build synonyms
    for (state_code, county_slug), meta in ALL_DATASETS.items():
        state_full = STATE_FULL_BY_CODE.get(state_code, state_code.upper())
        base = county_slug.replace("_", " ")

        synonyms = set()

        # bare
        synonyms.add(base)
        # "cuyahoga county"
        synonyms.add(f"{base} county")
        # "cuyahoga county ohio"
        synonyms.add(f"{base} county {state_full.lower()}")
        # "cuyahoga, ohio"
        synonyms.add(f"{base}, {state_full.lower()}")

        # add each to map
        for syn in synonyms:
            syn_norm = syn.strip().lower()
            COUNTY_SYNONYMS[syn_norm] = (state_code, county_slug)

    logger.info(
        "[BOOT] Built search index with %d (state, county) synonyms.",
        len(COUNTY_SYNONYMS),
    )


scan_available_datasets()

# ---------------------------------------------------------
# PARSING HELPERS
# ---------------------------------------------------------


def detect_zip(query: str):
    m = re.search(r"\b(\d{5})\b", query)
    return m.group(1) if m else None


def parse_filters(q: str):
    """
    Parse "over 200k income", "homes above 500000 value", etc.
    Return (income_min, value_min).
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


def detect_state_code(query: str):
    """
    Only match full state names like 'ohio', 'texas', etc.
    This avoids 'in' -> Indiana, 'or' -> Oregon, etc.
    """
    q = query.lower()
    hits = []
    for full_name, code in STATE_CODES.items():
        if full_name in q:
            hits.append(code)

    if not hits:
        return None
    # If multiple, just take the first. Real queries are usually unambiguous.
    return hits[0]


def resolve_county_and_state(query: str):
    """
    Use the free-form query to resolve to (state_code, county_slug, s3_key, size_bytes)
    using the pre-built COUNTY_SYNONYMS & ALL_DATASETS.
    """
    q = query.lower()

    # Step 1: detect state by full name only
    state_code = detect_state_code(q)
    if not state_code:
        msg = (
            "Could not detect state from your query. "
            "Please include the full state name, e.g. 'Ohio', 'Texas'."
        )
        logger.info("[RESOLVE] %s query='%s'", msg, query)
        return None, None, None, None, msg

    # Step 2: find best county synonym in that state
    best_key = None
    best_county = None
    best_state = None
    best_len = 0

    for syn, (st, county_slug) in COUNTY_SYNONYMS.items():
        if st != state_code:
            continue
        if syn in q:
            if len(syn) > best_len:
                meta = ALL_DATASETS.get((st, county_slug))
                if not meta:
                    continue
                best_state = st
                best_county = county_slug
                best_key = meta["key"]
                best_len = len(syn)

    if not best_key:
        msg = f"No cleaned dataset found for that county/state (state={state_code.upper()})."
        logger.info("[RESOLVE] %s query='%s'", msg, query)
        return None, None, None, None, msg

    size = ALL_DATASETS[(best_state, best_county)]["size"]
    logger.info(
        "[RESOLVE] Matched state=%s, county=%s, key=%s, size=%d",
        best_state,
        best_county,
        best_key,
        size,
    )
    return best_state, best_county, best_key, size, None


# ---------------------------------------------------------
# S3 / GEOJSON HELPERS
# ---------------------------------------------------------


def load_geojson_from_s3(key: str, size: int):
    """
    Simple in-memory loader with a size guard so we don't blow up memory.

    If the file is bigger than MAX_JSON_BYTES, we *do not* attempt to load it.
    Instead we raise a RuntimeError which we catch in /search and /export and
    return a friendly error.
    """
    logger.info(
        "[LOAD] Requesting GeoJSON key=%s (size=%d bytes, max=%d)",
        key,
        size,
        MAX_JSON_BYTES,
    )

    if size > MAX_JSON_BYTES:
        raise RuntimeError(
            f"Dataset {key} is too large to query directly in the web tool "
            f"({size:,} bytes). Please narrow your query (add a ZIP code, "
            f"income/value filter, or use a smaller county)."
        )

    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj["Body"].read()
        logger.info("[LOAD] Downloaded %d bytes for key=%s", len(data), key)
        return json.loads(data)
    except ClientError as e:
        logger.error("[ERROR] S3 get_object failed for key=%s: %s", key, e)
        raise
    except Exception as e:
        logger.error("[ERROR] Failed to parse JSON for key=%s: %s", key, e)
        raise


def get_zip_from_props(p: dict):
    return (
        p.get("postcode")
        or p.get("POSTCODE")
        or p.get("ZCTA5CE20")
        or ""
    )


def feature_to_obj(f: dict):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}

    coords = geom.get("coordinates") or [None, None]
    if isinstance(coords, (list, tuple)) and coords and isinstance(
        coords[0], (list, tuple)
    ):
        # Nested coords (polygon-ish) – take first vertex
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

    address = " ".join(str(x) for x in [num, street, unit] if x)

    return {
        "address": address,
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
            if str(get_zip_from_props(p)) != str(zip_code):
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
    return jsonify(
        {
            "ok": True,
            "datasets": len(ALL_DATASETS),
            "synonyms": len(COUNTY_SYNONYMS),
            "max_json_bytes": MAX_JSON_BYTES,
        }
    )


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    logger.info("[SEARCH] Incoming query='%s'", query)

    # Resolve location
    state_code, county_slug, key, size, resolve_err = resolve_county_and_state(query)
    if resolve_err:
        return jsonify({"ok": False, "error": resolve_err})

    state_full = STATE_FULL_BY_CODE.get(state_code, state_code.upper())
    county_title = county_slug.replace("_", " ").title()

    # Load dataset (with size guard)
    try:
        gj = load_geojson_from_s3(key, size)
    except RuntimeError as e:
        # File too big or similar logical error – return as user-friendly message
        logger.error("[ERROR] %s", e)
        return jsonify({"ok": False, "error": str(e)})
    except Exception as e:
        logger.error("[ERROR] Runtime error for query='%s': %s", query, e)
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Server error while loading dataset: {key}",
                    "details": str(e),
                }
            ),
            500,
        )

    feats = gj.get("features", [])
    logger.info("[SEARCH] Loaded %d features for %s, %s", len(feats), county_title, state_full)

    # Filters
    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    feats = apply_filters(feats, income_min, value_min, zip_code)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    logger.info(
        "[SEARCH] After filters: total=%d, shown=%d (zip=%s, income_min=%s, value_min=%s)",
        total,
        len(feats),
        zip_code,
        income_min,
        value_min,
    )

    return jsonify(
        {
            "ok": True,
            "query": query,
            "state": state_full,
            "county": county_title,
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

    if not query:
        return jsonify({"ok": False, "error": "Missing query."})

    logger.info("[EXPORT] Incoming query='%s'", query)

    state_code, county_slug, key, size, resolve_err = resolve_county_and_state(query)
    if resolve_err:
        return jsonify({"ok": False, "error": resolve_err})

    state_full = STATE_FULL_BY_CODE.get(state_code, state_code.upper())
    county_title = county_slug.replace("_", " ").title()

    try:
        gj = load_geojson_from_s3(key, size)
    except RuntimeError as e:
        logger.error("[ERROR] %s", e)
        return jsonify({"ok": False, "error": str(e)})
    except Exception as e:
        logger.error("[ERROR] Runtime error during export for query='%s': %s", query, e)
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Server error while loading dataset for export: {key}",
                    "details": str(e),
                }
            ),
            500,
        )

    feats = gj.get("features", [])

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)
    feats = apply_filters(feats, income_min, value_min, zip_code)

    logger.info(
        "[EXPORT] Exporting %d filtered features for %s, %s",
        len(feats),
        county_title,
        state_full,
    )

    filename = f"pelee_export_{county_slug}_{state_code}.csv"

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
            r = feature_to_obj(f)
            writer.writerow(
                [
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
                ]
            )
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
                buffer.seek(0)
                buffer.truncate(0)

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    # Local dev
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

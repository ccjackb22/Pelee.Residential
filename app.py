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
# CONFIG
# ---------------------------------------------------------
load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

CLEAN_PREFIX = os.getenv("CLEAN_PREFIX", "merged_with_tracts_acs_clean")

# Max size we will load in-memory for the web tool
MAX_BYTES_WEB = int(os.getenv("MAX_BYTES_WEB", "350000000"))  # ~350MB

MAX_RESULTS = int(os.getenv("MAX_RESULTS", "500"))

# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# FLASK + S3
# ---------------------------------------------------------
app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATE / COUNTY METADATA
# ---------------------------------------------------------

STATE_NAME_TO_CODE = {
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

STATE_CODES = set(STATE_NAME_TO_CODE.values())

# For 2-letter codes that are common English words, we avoid auto-detecting them.
AMBIGUOUS_CODES = {"in", "or", "me", "hi", "ok", "la"}

# DATASETS[(state_code, county_slug)] = {
#   "key": s3_key,
#   "size": size_bytes,
#   "synonyms": [ "harris", "harris county", "harris county texas", ... ],
# }
DATASETS = {}


def scan_cleaned_datasets():
    """
    Scan S3 for all *-clean.geojson under CLEAN_PREFIX and build
    DATASETS index + synonyms for county matching.
    """
    global DATASETS
    DATASETS = {}

    logger.info(
        "[BOOT] Scanning S3 bucket=%s prefix=%s/ for *-clean.geojson ...",
        S3_BUCKET,
        CLEAN_PREFIX,
    )

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{CLEAN_PREFIX}/")

    synonyms_count = 0
    dataset_count = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            parts = key.split("/")
            if len(parts) != 3:
                continue

            _, state, fname = parts
            state = state.lower()
            if state not in STATE_CODES:
                continue

            county_slug = fname.replace("-clean.geojson", "").lower()
            base_name = county_slug.replace("_", " ")

            # Build synonyms for matching, e.g.:
            #   "harris"
            #   "harris county"
            #   "harris co"
            #   "harris county tx"
            synonyms = set()
            synonyms.add(base_name)
            synonyms.add(f"{base_name} county")
            synonyms.add(f"{base_name} co")

            # Attach state variants
            full_state_name = None
            for name, code in STATE_NAME_TO_CODE.items():
                if code == state:
                    full_state_name = name
                    break

            if full_state_name:
                synonyms.add(f"{base_name} {full_state_name}")
                synonyms.add(f"{base_name} county {full_state_name}")

            synonyms.add(f"{base_name} {state}")
            synonyms.add(f"{base_name} county {state}")

            DATASETS[(state, county_slug)] = {
                "key": key,
                "size": obj["Size"],
                "synonyms": list(synonyms),
            }
            dataset_count += 1
            synonyms_count += len(synonyms)

    logger.info(
        "[BOOT] Indexed %d cleaned county datasets.", dataset_count
    )
    logger.info(
        "[BOOT] Built search index with %d (state,county) synonyms.",
        synonyms_count,
    )


scan_cleaned_datasets()

# ---------------------------------------------------------
# HELPERS: PARSING QUERY
# ---------------------------------------------------------


def detect_state(query: str):
    """
    Try to detect state from the query using:
      1) Full state names (e.g. 'ohio')
      2) 2-letter uppercase codes (e.g. 'OH'), avoiding ambiguous ones.
    Returns a 2-letter state code or None.
    """
    q_lower = query.lower()

    # 1) Full name
    for name, code in STATE_NAME_TO_CODE.items():
        if re.search(r"\b" + re.escape(name) + r"\b", q_lower):
            return code

    # 2) 2-letter codes in uppercase tokens
    tokens = re.findall(r"\b([A-Z]{2})\b", query)
    for t in tokens:
        code = t.lower()
        if code in STATE_CODES and code not in AMBIGUOUS_CODES:
            return code

    return None


def detect_county(query: str, state_code: str):
    """
    Given a query and a state_code (e.g. 'oh'),
    find the best matching county using our synonyms.
    Returns (county_slug, dataset_info) or (None, None).
    """
    q_lower = query.lower()
    best_match = None
    best_len = 0

    for (st, county_slug), info in DATASETS.items():
        if st != state_code:
            continue

        for syn in info["synonyms"]:
            syn = syn.lower()
            if syn in q_lower:
                if len(syn) > best_len:
                    best_len = len(syn)
                    best_match = (county_slug, info)

    if best_match:
        return best_match[0], best_match[1]
    return None, None


def detect_zip(query: str):
    m = re.search(r"\b(\d{5})\b", query)
    return m.group(1) if m else None


def parse_filters(q: str):
    """
    Parse things like:
      "income over 200k", "homes over 500000", "value above 800k"
    Returns (income_min, value_min).
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


# ---------------------------------------------------------
# HELPERS: DATA LOADING + FILTERING
# ---------------------------------------------------------


def get_zip_from_props(p: dict):
    return (
        p.get("postcode")
        or p.get("POSTCODE")
        or p.get("ZCTA5CE20")
        or ""
    )


def load_geojson_from_s3(key: str, size: int):
    """
    Load a GeoJSON from S3 into memory, enforcing MAX_BYTES_WEB.
    """
    if size > MAX_BYTES_WEB:
        raise ValueError(
            f"Dataset {key} is too large to load in-memory ({size} bytes). "
            f"Max allowed is {MAX_BYTES_WEB} bytes."
        )

    logger.info("[LOAD] Fetching key=%s size=%d", key, size)
    resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    body = resp["Body"].read()

    if len(body) != size:
        logger.info(
            "[LOAD] Size mismatch for key=%s: head=%d, body=%d",
            key,
            size,
            len(body),
        )

    if len(body) > MAX_BYTES_WEB:
        raise ValueError(
            f"Dataset {key} expanded beyond in-memory limit ({len(body)} bytes)."
        )

    gj = json.loads(body)
    return gj


def feature_to_obj(f):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}

    coords = geom.get("coordinates") or [None, None]

    if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (list, tuple)):
        # e.g. polygons; take first point
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

        if zip_code:
            feature_zip = str(get_zip_from_props(p))
            if feature_zip != str(zip_code):
                continue

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
        pw = request.form.get("password", "")
        if pw == PASSWORD:
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
    return jsonify(
        {
            "ok": True,
            "datasets_indexed": len(DATASETS),
            "env_aws_access_key": bool(os.getenv("AWS_ACCESS_KEY_ID")),
            "env_openai": bool(os.getenv("OPENAI_API_KEY")),
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

    # 1) State
    state_code = detect_state(query)
    if not state_code:
        msg = (
            "Could not detect a state from your query. "
            "Please include a full state name (e.g. 'Ohio') or a two-letter code (e.g. 'OH')."
        )
        logger.info("[RESOLVE] %s Query='%s'", msg, query)
        return jsonify({"ok": False, "error": msg})

    # 2) County
    county_slug, ds = detect_county(query, state_code)
    if not ds:
        msg = f"No cleaned dataset found for that county/state (state={state_code.upper()})."
        logger.info("[RESOLVE] %s Query='%s'", msg, query)
        return jsonify({"ok": False, "error": msg})

    key = ds["key"]
    size = ds["size"]

    logger.info(
        "[RESOLVE] Matched state=%s, county=%s, key=%s, size=%d",
        state_code,
        county_slug,
        key,
        size,
    )

    # 3) Size guard
    if size > MAX_BYTES_WEB:
        msg = (
            f"Dataset {key} is too large to query directly in the web tool "
            f"({size:,} bytes). Try narrowing your query (add a ZIP code, an "
            "'income over X' or 'value over X' filter) or use a smaller county. "
            "For full-county exports, run an offline script against S3."
        )
        logger.info("[SIZE] %s", msg)
        return jsonify({"ok": False, "error": msg})

    # 4) Load GeoJSON
    try:
        gj = load_geojson_from_s3(key, size)
    except Exception as e:
        logger.exception(
            "[ERROR] Failed loading dataset key=%s: %s", key, str(e)
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

    # 5) Filters
    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    feats = apply_filters(feats, income_min, value_min, zip_code)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    county_human = county_slug.replace("_", " ").title()

    return jsonify(
        {
            "ok": True,
            "query": query,
            "state": state_code.upper(),
            "county": county_human,
            "city": None,
            "zip": zip_code,
            "dataset_key": key,
            "total": total,
            "shown": len(feats),
            "message": None,
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

    logger.info("[EXPORT] Incoming export query='%s'", query)

    state_code = detect_state(query)
    if not state_code:
        msg = (
            "Could not detect a state from your query. "
            "Please include a full state name (e.g. 'Ohio') or a two-letter code (e.g. 'OH')."
        )
        logger.info("[EXPORT][RESOLVE] %s Query='%s'", msg, query)
        return jsonify({"ok": False, "error": msg})

    county_slug, ds = detect_county(query, state_code)
    if not ds:
        msg = f"No cleaned dataset found for that county/state (state={state_code.upper()})."
        logger.info("[EXPORT][RESOLVE] %s Query='%s'", msg, query)
        return jsonify({"ok": False, "error": msg})

    key = ds["key"]
    size = ds["size"]

    logger.info(
        "[EXPORT][RESOLVE] state=%s county=%s key=%s size=%d",
        state_code,
        county_slug,
        key,
        size,
    )

    if size > MAX_BYTES_WEB:
        msg = (
            f"Dataset {key} is too large to export directly from the web tool "
            f"({size:,} bytes). Run an offline export script against S3 for this "
            "county, or narrow your export (by ZIP, income, or value)."
        )
        logger.info("[EXPORT][SIZE] %s", msg)
        return jsonify({"ok": False, "error": msg})

    try:
        gj = load_geojson_from_s3(key, size)
    except Exception as e:
        logger.exception(
            "[EXPORT][ERROR] Failed loading dataset key=%s: %s", key, str(e)
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

    filename = f"pelee_export_{county_slug}_{state_code}.csv"

    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer)

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

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"'
    }

    return Response(generate(), mimetype="text/csv", headers=headers)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=True)

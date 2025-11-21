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

# All CLEANED datasets
CLEAN_PREFIX = os.getenv("CLEAN_PREFIX", "merged_with_tracts_acs_clean")

# Small vs big file threshold (bytes)
SMALL_FILE_MAX_BYTES = int(os.getenv("SMALL_FILE_MAX_BYTES", "350000000"))

# Max results to show on screen
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "500"))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATE / COUNTY MAPPINGS
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

# Reverse map: "oh" -> "ohio"
STATE_CODE_TO_NAME = {}
for name, code in STATE_CODES.items():
    STATE_CODE_TO_NAME[code] = name

# (state_code, county_key) → s3_key and size
DATASETS = {}        # (state, county) -> key
DATASET_SIZES = {}   # (state, county) -> size_bytes


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", s.lower()).strip()


def scan_available_datasets():
    """
    Scan S3 for all cleaned county GeoJSONs and record:
      DATASETS[(state, county)] = "merged_with_tracts_acs_clean/state/county-clean.geojson"
      DATASET_SIZES[(state, county)] = size_in_bytes
    """
    global DATASETS, DATASET_SIZES
    DATASETS = {}
    DATASET_SIZES = {}

    logger.info(
        "[BOOT] Scanning S3 bucket=%s prefix=%s/ for *-clean.geojson ...",
        S3_BUCKET,
        CLEAN_PREFIX,
    )

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{CLEAN_PREFIX}/")

    count = 0
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            parts = key.split("/")
            if len(parts) != 3:
                continue

            _, state, fname = parts
            if state not in STATE_CODE_TO_NAME:
                continue

            county = fname[:-len("-clean.geojson")]
            DATASETS[(state, county)] = key
            DATASET_SIZES[(state, county)] = obj.get("Size")
            count += 1

    logger.info("[BOOT] Indexed %d cleaned county datasets.", count)


scan_available_datasets()

# ---------------------------------------------------------
# QUERY PARSING HELPERS
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


def detect_state(query_raw: str, query_lower: str):
    """
    First try full state names ("ohio", "texas").
    If none, try 2-letter uppercase tokens ("OH", "TX") from the raw query.
    Avoid the old 'IN' / 'ME' / etc. collisions with normal English words.
    """
    # 1) Full name
    for name, code in STATE_CODES.items():
        if name in query_lower:
            return code

    # 2) Uppercase 2-letter tokens (e.g. "OH", "TX")
    for token in re.findall(r"\b([A-Z]{2})\b", query_raw):
        code = token.lower()
        if code in STATE_CODE_TO_NAME:
            return code

    return None


def detect_county(state_code: str, query_lower: str):
    """
    Given a state code and the normalized query, find a matching county key.
    Matching patterns:
      - "{county_name} county"
      - "{county_name} parish" (LA)
      - "{county_name}" (with 'county' also in query)
    """
    if not state_code:
        return None

    q = normalize_text(query_lower)
    has_word_county = "county" in q or "parish" in q

    candidates = [c for (s, c) in DATASETS.keys() if s == state_code]

    # Longer county names first so "st lawrence" beats "lawrence"
    candidates.sort(key=lambda c: len(c), reverse=True)

    for county in candidates:
        cname = county.replace("_", " ")
        cname_norm = normalize_text(cname)

        if f"{cname_norm} county" in q or f"{cname_norm} parish" in q:
            return county

        if has_word_county and cname_norm in q:
            return county

    return None


def resolve_dataset(query: str):
    """
    Resolve (state_code, county_key, s3_key, size_bytes) from the user query.
    """
    query_raw = query
    q_lower = query.lower()

    state_code = detect_state(query_raw, q_lower)
    if not state_code:
        logger.info("[RESOLVE] Failed to detect state for query=%r", query)
        return None, None, None, None

    county = detect_county(state_code, q_lower)
    if not county:
        logger.info(
            "[RESOLVE] No county match for state=%s in query=%r",
            state_code,
            query,
        )
        return state_code, None, None, None

    key = DATASETS.get((state_code, county))
    size = DATASET_SIZES.get((state_code, county))

    logger.info(
        "[RESOLVE] Matched state=%s, county=%s, key=%s, size=%s",
        state_code,
        county,
        key,
        size,
    )

    return state_code, county, key, size

# ---------------------------------------------------------
# GEOJSON LOAD / STREAMING
# ---------------------------------------------------------

def load_geojson_full(key: str):
    """
    Load entire GeoJSON into memory (only for small/medium counties).
    """
    logger.info("[S3] Full-load GeoJSON key=%s", key)
    resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    data = resp["Body"].read()
    return json.loads(data)


def stream_features_from_s3(key: str):
    """
    Streaming JSON parser for FeatureCollection GeoJSON.

    Reads the S3 object chunk-by-chunk, finds the "features" array,
    and yields each feature object as a Python dict, without loading
    the entire file into memory.

    This is intentionally simple and conservative — speed is less important
    than not blowing up memory.
    """
    logger.info("[STREAM] Streaming features for key=%s", key)

    resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    body = resp["Body"]

    buf = ""
    in_features = False
    depth = 0
    obj_start = None

    for chunk in body.iter_chunks(chunk_size=1024 * 1024):  # 1 MB chunks
        if not chunk:
            continue
        buf += chunk.decode("utf-8", errors="ignore")
        i = 0

        while True:
            if not in_features:
                idx = buf.find('"features"', i)
                if idx == -1:
                    # Keep last bit of buffer in case boundary is split
                    if len(buf) > 100:
                        buf = buf[-100:]
                    break

                idx = buf.find("[", idx)
                if idx == -1:
                    # Need more data to see '['
                    break

                in_features = True
                i = idx + 1
                obj_start = None
                depth = 0
                continue

            # We are inside features array
            n = len(buf)
            while i < n:
                if obj_start is None:
                    # Skip whitespace/commas until '{' or ']'
                    c = buf[i]
                    if c in " \r\n\t,":
                        i += 1
                        continue
                    if c == "]":
                        # End of features array
                        return
                    if c == "{":
                        obj_start = i
                        depth = 1
                        i += 1
                        continue
                    # Unexpected char, skip
                    i += 1
                    continue
                else:
                    c = buf[i]
                    if c == "{":
                        depth += 1
                    elif c == "}":
                        depth -= 1
                    i += 1

                    if depth == 0:
                        # We reached the end of an object
                        obj_text = buf[obj_start:i]
                        try:
                            feature = json.loads(obj_text)
                            yield feature
                        except Exception:
                            logger.exception("[STREAM] Failed to parse feature JSON chunk.")
                        # Trim buffer up to i (and maybe whitespace/commas following)
                        j = i
                        while j < n and buf[j] in " \r\n\t,":
                            j += 1
                        buf = buf[j:]
                        i = 0
                        n = len(buf)
                        obj_start = None
                        break  # break inner while, go back to outer 'while True'
            else:
                # Need more data
                # Keep last chunk of buffer in case JSON object is split
                if len(buf) > 2_000_000:
                    buf = buf[-2_000_000:]
                break


def iter_matching_features(key: str, size_bytes: int, zip_code, income_min, value_min, max_results=None):
    """
    Yield features that match the filters.
    Uses full-load for small files; streaming for big files.
    Stops after max_results if given.
    """
    use_full_load = size_bytes is not None and size_bytes <= SMALL_FILE_MAX_BYTES

    if use_full_load:
        logger.info(
            "[LOAD] Using full in-memory load for key=%s (size=%s bytes)",
            key,
            size_bytes,
        )
        gj = load_geojson_full(key)
        features = gj.get("features", [])
        count = 0
        for f in features:
            if feature_matches_filters(f, zip_code, income_min, value_min):
                yield f
                count += 1
                if max_results and count >= max_results:
                    return
    else:
        logger.info(
            "[LOAD] Using streaming parser for key=%s (size=%s bytes > %s)",
            key,
            size_bytes,
            SMALL_FILE_MAX_BYTES,
        )
        count = 0
        for f in stream_features_from_s3(key):
            if feature_matches_filters(f, zip_code, income_min, value_min):
                yield f
                count += 1
                if max_results and count >= max_results:
                    return


def count_matching_features(key: str, size_bytes: int, zip_code, income_min, value_min):
    """
    Count *all* matching features (for total), using the same streaming vs full-load logic.
    For small counties we already loaded them above, but to keep it simple
    we do a dedicated pass here. Speed isn't critical; correctness and
    memory safety are.
    """
    use_full_load = size_bytes is not None and size_bytes <= SMALL_FILE_MAX_BYTES

    total = 0
    if use_full_load:
        gj = load_geojson_full(key)
        for f in gj.get("features", []):
            if feature_matches_filters(f, zip_code, income_min, value_min):
                total += 1
    else:
        for f in stream_features_from_s3(key):
            if feature_matches_filters(f, zip_code, income_min, value_min):
                total += 1

    return total


def feature_matches_filters(f, zip_code, income_min, value_min):
    p = f.get("properties", {}) or {}

    # ZIP filter
    if zip_code:
        fzip = (
            p.get("postcode")
            or p.get("POSTCODE")
            or p.get("ZCTA5CE20")
            or ""
        )
        if str(fzip) != str(zip_code):
            return False

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
                return False
        except Exception:
            return False

    # Home value filter
    if value_min:
        v = (
            p.get("median_value")
            or p.get("B25077_001E")
            or p.get("DP04_0089E")
            or p.get("home_value")
        )
        try:
            if v is None or float(v) < value_min:
                return False
        except Exception:
            return False

    return True


def feature_to_obj(f):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}

    coords = geom.get("coordinates") or [None, None]
    if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (list, tuple)):
        # e.g. Polygon [[lon,lat], ...]
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
    return jsonify({
        "ok": True,
        "datasets": len(DATASETS),
        "small_file_max_bytes": SMALL_FILE_MAX_BYTES,
    })


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    logger.info("[SEARCH] Incoming query=%r", query)

    state_code, county_key, key, size_bytes = resolve_dataset(query)
    if not state_code:
        return jsonify({
            "ok": False,
            "error": "Could not detect a US state in that query. Try 'Franklin County, Ohio' or 'Harris County, TX'."
        })

    if not county_key or not key:
        state_label = state_code.upper()
        return jsonify({
            "ok": False,
            "error": f"No cleaned dataset found for that county/state (state resolved as {state_label})."
        })

    # Filters
    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    # Collect up to MAX_RESULTS
    matches = []
    try:
        for f in iter_matching_features(
            key=key,
            size_bytes=size_bytes,
            zip_code=zip_code,
            income_min=income_min,
            value_min=value_min,
            max_results=MAX_RESULTS,
        ):
            matches.append(feature_to_obj(f))
    except Exception as e:
        logger.exception("[ERROR] Runtime error while scanning features for query=%r", query)
        return jsonify({
            "ok": False,
            "error": f"Server error while scanning dataset {key}.",
            "details": str(e),
        }), 500

    # Get full count — separate pass (ok if slow)
    try:
        total = count_matching_features(
            key=key,
            size_bytes=size_bytes,
            zip_code=zip_code,
            income_min=income_min,
            value_min=value_min,
        )
    except Exception as e:
        logger.exception("[ERROR] Failed to count matches for query=%r", query)
        total = len(matches)

    state_name = STATE_CODE_TO_NAME.get(state_code, state_code)
    shown = len(matches)

    # Optional message for big counties
    message = None
    if size_bytes and size_bytes > SMALL_FILE_MAX_BYTES:
        mb = round(size_bytes / 1_000_000)
        message = f"Large county dataset (~{mb} MB). Results may take a bit longer to scan."

    return jsonify({
        "ok": True,
        "query": query,
        "state": state_code.upper(),
        "state_name": state_name.title(),
        "county": county_key.replace("_", " ").title(),
        "city": None,
        "zip": zip_code,
        "dataset_key": key,
        "total": total,
        "shown": shown,
        "message": message,
        "results": matches,
    })


# ---------------------------------------------------------
# EXPORT (STREAMING CSV)
# ---------------------------------------------------------

@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Missing query."})

    logger.info("[EXPORT] Incoming query=%r", query)

    state_code, county_key, key, size_bytes = resolve_dataset(query)
    if not state_code or not county_key or not key:
        return jsonify({
            "ok": False,
            "error": "Dataset not found for that query. Try including full county + state (e.g., 'Cuyahoga County Ohio')."
        })

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    filename = f"pelee_export_{county_key}_{state_code}.csv"

    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        # Header row
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

        # Stream rows as we iterate over matching features (no limit)
        for f in iter_matching_features(
            key=key,
            size_bytes=size_bytes,
            zip_code=zip_code,
            income_min=income_min,
            value_min=value_min,
            max_results=None,  # all matches
        ):
            p = f.get("properties", {}) or {}
            geom = f.get("geometry", {}) or {}

            coords = geom.get("coordinates") or [None, None]
            if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (list, tuple)):
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


# ---------------------------------------------------------
# MAIN (local dev)
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

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
CLEAN_PREFIX = "merged_with_tracts_acs_clean"

# For in-memory JSON load
MAX_JSON_BYTES = 350_000_000  # 350 MB

# Max results for on-screen search results
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATE + DATASET INDEX
# ---------------------------------------------------------

STATE_NAMES = {
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

# USPS codes we will consider when looking at 2-letter tokens.
# NOTE: we intentionally EXCLUDE "in" so "homes in Harris County TX"
# never gets misread as Indiana.
USPS_CODES_SAFE = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de",
    "fl", "ga", "hi", "ia", "id", "il", "ks", "ky",
    "la", "ma", "md", "me", "mi", "mn", "mo", "ms",
    "mt", "nc", "nd", "ne", "nh", "nj", "nm", "nv",
    "ny", "oh", "ok", "or", "pa", "ri", "sc", "sd",
    "tn", "tx", "ut", "va", "vt", "wa", "wi", "wv",
    "wy",
}

# (state_code, county_slug) -> {"key": s3_key, "size": bytes}
DATASETS = {}


def scan_available_datasets():
    """
    Scans S3 for all cleaned datasets and builds a lookup:
        DATASETS[("oh", "cuyahoga")] = {"key": ".../oh/cuyahoga-clean.geojson", "size": N}
    """
    global DATASETS
    DATASETS = {}

    log.info(
        "[BOOT] Scanning S3 bucket=%s prefix=%s/ for *-clean.geojson ...",
        S3_BUCKET,
        CLEAN_PREFIX,
    )

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=CLEAN_PREFIX + "/")

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
            state = state.lower()
            if state not in STATE_NAMES.values():
                continue

            county_slug = fname.replace("-clean.geojson", "")
            size = obj.get("Size", None)

            DATASETS[(state, county_slug)] = {
                "key": key,
                "size": size,
            }
            count += 1

    log.info("[BOOT] Indexed %d cleaned county datasets.", count)


scan_available_datasets()


# ---------------------------------------------------------
# HELPERS – QUERY PARSING
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


def detect_state(query: str):
    """
    Try to detect state_code ("oh", "tx") from query.
    Priority:
      1) Full state names (ohio, texas, etc.)
      2) 2-letter USPS codes as standalone tokens, but only from USPS_CODES_SAFE
    """
    q = query.lower()

    # 1) Full state names
    for name, code in STATE_NAMES.items():
        if name in q:
            return code

    # 2) Two-letter tokens (safe list, excludes "in" etc.)
    tokens = re.findall(r"\b([a-z]{2})\b", q)
    for tok in tokens:
        if tok in USPS_CODES_SAFE:
            return tok

    return None


def detect_county_for_state(state_code: str, query: str):
    """
    Given a state_code and the query, try to resolve the county_slug + S3 key.
    We match on substrings like:
        "cuyahoga", "cuyahoga county", etc.
    """
    q = query.lower()
    # normalize punctuation -> spaces
    q_clean = re.sub(r"[^a-z0-9\s]", " ", q)

    best_match = None
    best_len = 0

    for (s, county_slug), meta in DATASETS.items():
        if s != state_code:
            continue

        base = county_slug.replace("_", " ")
        synonyms = [
            base,
            base + " county",
            base + " parish",
            base + " borough",
        ]

        for name in synonyms:
            if name in q_clean:
                # choose longest match to avoid e.g. matching "st" before "st lawrence"
                if len(name) > best_len:
                    best_match = (county_slug, meta["key"], meta.get("size"))
                    best_len = len(name)

    return best_match  # (county_slug, key, size) or None


def resolve_dataset_from_query(query: str):
    """
    Resolve (state_code, county_slug, s3_key, size_bytes) from a natural language query.
    """
    state_code = detect_state(query)
    if not state_code:
        return None, None, None, None

    match = detect_county_for_state(state_code, query)
    if not match:
        return state_code, None, None, None

    county_slug, key, size = match
    return state_code, county_slug, key, size


# ---------------------------------------------------------
# HELPERS – GEOJSON LOADING / STREAMING
# ---------------------------------------------------------

def head_object_size(key: str):
    try:
        resp = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return resp.get("ContentLength")
    except ClientError as e:
        log.error("[S3] head_object failed for key=%s: %s", key, e)
        return None


def load_geojson_in_memory(key: str):
    """
    Load GeoJSON fully into memory. Only used for reasonably sized files.
    """
    log.info("[LOAD] In-memory load of key=%s", key)
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    raw = obj["Body"].read()
    return json.loads(raw)


def stream_features_from_s3(key: str, max_features: int = None):
    """
    Streaming parser for a GeoJSON FeatureCollection on S3.

    It:
      - Reads the object in chunks
      - Finds the "features" array
      - Extracts each JSON object in that array via brace depth
      - Yields one feature dict at a time
    """
    log.info("[STREAM] Streaming features from key=%s", key)
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"]

    buffer = ""
    in_features = False
    pos = 0
    yielded = 0

    for chunk in body.iter_chunks(chunk_size=65536):
        if not chunk:
            continue

        buffer += chunk.decode("utf-8", errors="ignore")

        # Step 1: find "features": [  (only once)
        if not in_features:
            idx = buffer.find('"features"')
            if idx == -1:
                # avoid unbounded growth while still keeping some tail
                if len(buffer) > 100_000:
                    buffer = buffer[-50_000:]
                continue

            bracket = buffer.find("[", idx)
            if bracket == -1:
                if len(buffer) > 100_000:
                    buffer = buffer[-50_000:]
                continue

            pos = bracket + 1
            in_features = True

        # Step 2: parse feature objects one by one
        while True:
            # skip whitespace/commas
            while pos < len(buffer) and buffer[pos] in " \n\r\t,":
                pos += 1

            if pos >= len(buffer):
                # need more data
                break

            # End of features array?
            if buffer[pos] == "]":
                return

            if buffer[pos] != "{":
                # Not enough data or unexpected char; keep tail
                if pos > 0:
                    buffer = buffer[pos:]
                    pos = 0
                break

            start = pos
            depth = 0
            i = pos
            end = None

            while i < len(buffer):
                c = buffer[i]
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
                i += 1

            if end is None:
                # We didn't get a full object yet; keep from start
                buffer = buffer[start:]
                pos = 0
                break

            json_str = buffer[start:end]
            pos = end

            try:
                feat = json.loads(json_str)
                yield feat
                yielded += 1
                if max_features is not None and yielded >= max_features:
                    return
            except Exception as e:
                log.warning("[STREAM] Failed to parse feature chunk: %s", e)
                # keep going on next feature


# ---------------------------------------------------------
# HELPERS – FEATURE SHAPING + FILTERING
# ---------------------------------------------------------

def get_zip_from_props(p: dict):
    """
    Priority for ZIP:
      1) postcode
      2) POSTCODE
      3) ZCTA5CE20
    """
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

    if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (list, tuple)):
        # something like [[lon, lat], ...]
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

    return {
        "address": " ".join(
            str(x) for x in [num, street, unit] if x
        ),
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


def feature_matches(f, income_min, value_min, zip_code):
    p = f.get("properties", {}) or {}

    # ZIP filter
    if zip_code:
        feature_zip = str(get_zip_from_props(p))
        if feature_zip != str(zip_code):
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
                return False
        except Exception:
            return False

    return True


def apply_filters_in_memory(features, income_min, value_min, zip_code):
    return [f for f in features if feature_matches(f, income_min, value_min, zip_code)]


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
        "datasets_indexed": len(DATASETS),
    })


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    log.info("[SEARCH] Incoming query='%s'", query)

    state_code, county_slug, key, size = resolve_dataset_from_query(query)

    if not state_code:
        return jsonify({
            "ok": False,
            "error": "Couldn't detect a state in that query. Try including the full state name (e.g. 'Ohio', 'Texas')."
        })

    if not county_slug or not key:
        return jsonify({
            "ok": False,
            "error": f"No cleaned dataset found for that county/state (state={state_code.upper()})."
        })

    # Ensure we have size info
    if size is None:
        size = head_object_size(key)

    log.info(
        "[RESOLVE] Matched state=%s, county=%s, key=%s, size=%s",
        state_code, county_slug, key, size,
    )

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    try:
        # Decide on strategy: in-memory vs streaming
        if size is not None and size <= MAX_JSON_BYTES:
            # In-memory path
            gj = load_geojson_in_memory(key)
            feats = gj.get("features", [])
            filtered = apply_filters_in_memory(feats, income_min, value_min, zip_code)
            total = len(filtered)
            clipped = filtered[:MAX_RESULTS]
            streaming_used = False
        else:
            # Streaming path for big counties – do NOT count total, just collect up to MAX_RESULTS
            log.info(
                "[LOAD] Using streaming search for key=%s (size=%s bytes > %s)",
                key,
                size,
                MAX_JSON_BYTES,
            )
            clipped = []
            for f in stream_features_from_s3(key, max_features=None):
                if feature_matches(f, income_min, value_min, zip_code):
                    clipped.append(f)
                    if len(clipped) >= MAX_RESULTS:
                        break
            total = None  # unknown for giant files without a full pass
            streaming_used = True

        results = [feature_to_obj(f) for f in clipped]

        # Build a friendly message for streaming cases
        if streaming_used:
            msg = (
                "Large county dataset; showing the first "
                f"{len(results)} matching addresses. Total count not computed for performance."
            )
        else:
            msg = ""

        return jsonify({
            "ok": True,
            "query": query,
            "state": state_code.upper(),
            "county": county_slug.replace("_", " ").title(),
            "zip": zip_code,
            "dataset_key": key,
            "total": total,
            "shown": len(results),
            "message": msg,
            "results": results,
        })

    except Exception as e:
        log.exception("[ERROR] Runtime error for query='%s': %s", query, e)
        return jsonify({
            "ok": False,
            "error": "Internal error while searching this dataset.",
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

    log.info("[EXPORT] Incoming query='%s'", query)

    state_code, county_slug, key, size = resolve_dataset_from_query(query)
    if not state_code or not county_slug or not key:
        return jsonify({"ok": False, "error": "Dataset not found for export."})

    # This can be heavier – we DO allow full pass, but may still be too big
    if size is None:
        size = head_object_size(key)

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    def generate_rows():
        # Header
        buffer = io.StringIO()
        writer = csv.writer(buffer)
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

        # Decide streaming vs in-memory
        if size is not None and size <= MAX_JSON_BYTES:
            # In-memory – simpler path
            gj = load_geojson_in_memory(key)
            feats = gj.get("features", [])
            feats = apply_filters_in_memory(feats, income_min, value_min, zip_code)

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
        else:
            # Streaming export for big counties – full pass, but row-by-row
            for f in stream_features_from_s3(key, max_features=None):
                if not feature_matches(f, income_min, value_min, zip_code):
                    continue
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

    filename = f"pelee_export_{county_slug}_{state_code}.csv"

    return Response(
        generate_rows(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

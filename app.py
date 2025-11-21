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

# CLEANED dataset prefix (county-level)
CLEAN_PREFIX = os.getenv("CLEAN_PREFIX", "merged_with_tracts_acs_clean")

# Hard cap on a single county file size (bytes) to avoid OOM on Render.
# Franklin is ~861 MB, Cuyahoga ~530 MB, Harris is also large.
# We keep a 350 MB default cap so the service fails gracefully instead of crashing.
MAX_GEOJSON_BYTES = int(os.getenv("MAX_GEOJSON_BYTES", "350000000"))

# Max results to show in UI
MAX_RESULTS = 500

# ---------------------------------------------------------
# STATE + COUNTY MAPPINGS
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

# 2-letter codes that are also common English words – we *do not*
# auto-detect these from plain text like "homes in Harris County".
AMBIGUOUS_STATE_ABBRS = {"in", "or", "me", "hi", "ok", "us"}

# (state, county) -> s3_key
CLEAN_DATASETS = {}

# (state, synonym) -> canonical county (the folder name under state)
# e.g. ("tx", "harris"), ("tx", "harris county"), ("ak", "anchorage borough")
COUNTY_SYNONYMS = {}

# ---------------------------------------------------------
# FLASK APP + AWS
# ---------------------------------------------------------

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# DATASET SCAN & INDEX BUILD
# ---------------------------------------------------------

def _add_county_synonyms(state: str, county: str):
    """
    Build a small set of text synonyms for a county name:
      - "harris"
      - "harris county"
      - "los angeles"
      - "los angeles county"
      - "anchorage borough"
      - etc.
    We keep it simple, but robust enough for "X County ST" style queries.
    """
    # county folder name like "harris", "los_angeles", "st_lawrence"
    base = county.lower()
    name_spaced = base.replace("_", " ")

    syns = set()

    # bare names
    syns.add(base)           # "harris"
    syns.add(name_spaced)    # "los angeles"

    # common suffix variants
    # If the name doesn't already end with county/parish/borough/census area,
    # add them as possible matches, e.g., "harris county".
    if not any(
        name_spaced.endswith(suffix)
        for suffix in (" county", " parish", " borough", " census area")
    ):
        syns.add(f"{name_spaced} county")
        syns.add(f"{name_spaced} parish")
        syns.add(f"{name_spaced} borough")

    # Very light "saint" vs "st" handling (not perfect, but helps)
    if name_spaced.startswith("st "):
        syns.add(name_spaced.replace("st ", "saint ", 1))
    if name_spaced.startswith("saint "):
        syns.add(name_spaced.replace("saint ", "st ", 1))

    for s in syns:
        COUNTY_SYNONYMS[(state, s)] = county


def scan_available_datasets():
    """
    Scan S3:
      merged_with_tracts_acs_clean/{state}/{county}-clean.geojson

    Populate:
      CLEAN_DATASETS[(state, county)] = key
      COUNTY_SYNONYMS[(state, "harris county")] = "harris"
    """
    global CLEAN_DATASETS, COUNTY_SYNONYMS
    CLEAN_DATASETS = {}
    COUNTY_SYNONYMS = {}

    prefix = CLEAN_PREFIX.rstrip("/") + "/"
    log.info("[BOOT] Scanning S3 bucket=%s prefix=%s for *-clean.geojson ...", S3_BUCKET, prefix)

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    count_files = 0
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            parts = key.split("/")
            if len(parts) != 3:
                # e.g. weird subfolders – skip
                continue

            _, state, fname = parts
            state = state.lower()
            if state not in STATE_CODES.values():
                continue

            county = fname.replace("-clean.geojson", "").lower()
            CLEAN_DATASETS[(state, county)] = key
            _add_county_synonyms(state, county)
            count_files += 1

    log.info("[BOOT] Indexed %d cleaned county datasets.", count_files)
    log.info("[BOOT] Built search index with %d (state, county) synonyms.", len(COUNTY_SYNONYMS))


# Build the index at import/boot time
try:
    scan_available_datasets()
except Exception as e:
    log.error("[BOOT] Failed to scan datasets: %s", e)


# ---------------------------------------------------------
# SMALL PARSERS
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
      - "houses over 300k"
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


def _detect_state_code(q: str):
    """
    Try to deduce the state from the query text using:
      1) Full state names ("texas", "ohio", etc.)
      2) Patterns like "county tx", "tx county", ", tx"
      3) Last token as a 2-letter code (excluding ambiguous words like "in"/"or").
    """
    q = q.lower()

    # 1) Full state names
    for name, code in STATE_CODES.items():
        if name in q:
            return code

    # 2) "county tx" / "tx county"
    m = re.search(r"county\s+([a-z]{2})\b", q)
    if m:
        abbr = m.group(1)
        if abbr in STATE_CODES.values():
            return abbr

    m = re.search(r"\b([a-z]{2})\s+county\b", q)
    if m:
        abbr = m.group(1)
        if abbr in STATE_CODES.values():
            return abbr

    # 3) ", tx"
    m = re.search(r",\s*([a-z]{2})\b", q)
    if m:
        abbr = m.group(1)
        if abbr in STATE_CODES.values():
            return abbr

    # 4) Last token fallback (avoid ambiguous ones)
    tokens = re.findall(r"[a-z]+", q)
    if tokens:
        last = tokens[-1]
        if len(last) == 2 and last in STATE_CODES.values() and last not in AMBIGUOUS_STATE_ABBRS:
            return last

    return None


def resolve_any_county(query: str):
    """
    Resolve ANY county + state from the cleaned S3 datasets using the synonyms index.

    Returns (state_code, county_name, s3_key) or (None, None, None) if no match.
    """
    q = (query or "").lower()

    state_code = _detect_state_code(q)
    log.info("[RESOLVE] Query='%s' -> state_code=%s", query, state_code)

    if not state_code:
        return None, None, None

    # Find the best matching county synonym in this state by longest substring match
    best_match = None
    best_len = -1

    for (st, syn), county in COUNTY_SYNONYMS.items():
        if st != state_code:
            continue
        if syn in q:
            if len(syn) > best_len:
                best_len = len(syn)
                best_match = (county, syn)

    if not best_match:
        # As a fallback, look for "X county" pattern and try last word(s) as county
        m = re.search(r"([a-z\s]+?)\s+county", q)
        if m:
            base = m.group(1).strip()
            base = re.sub(r"\s+", " ", base)
            # Try full base and last word only
            candidates = [base]
            parts = base.split()
            if len(parts) > 1:
                candidates.append(parts[-1])

            for cand in candidates:
                cand_l = cand.lower()
                for (st, syn), county in COUNTY_SYNONYMS.items():
                    if st != state_code:
                        continue
                    if cand_l == syn or f"{cand_l} county" == syn:
                        best_match = (county, syn)
                        break
                if best_match:
                    break

    if not best_match:
        log.info(
            "[RESOLVE] No county match for state=%s in query='%s'",
            state_code,
            query,
        )
        return None, None, None

    county, syn_used = best_match
    key = CLEAN_DATASETS.get((state_code, county))

    log.info(
        "[RESOLVE] Matched state=%s, county=%s (synonym='%s'), key=%s",
        state_code,
        county,
        syn_used,
        key,
    )

    if not key:
        return None, None, None

    return state_code, county, key


def _load_geojson_guarded(key: str):
    """
    Head the object first, enforce a max size, then load the GeoJSON.
    """
    try:
        head = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        size = head.get("ContentLength", 0)
        log.info("[S3] head_object key=%s size=%d bytes", key, size)

        if size > MAX_GEOJSON_BYTES:
            raise RuntimeError(
                f"Dataset {key} is too large to load in-memory ({size} bytes). "
                f"Max allowed is {MAX_GEOJSON_BYTES} bytes."
            )

        body = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(body)

    except ClientError as e:
        log.error("[S3] ClientError for key=%s: %s", key, e)
        raise
    except Exception as e:
        log.error("[ERROR] Failed to load or parse GeoJSON %s: %s", key, e)
        raise


def feature_to_obj(f):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}

    coords = geom.get("coordinates") or [None, None]
    # Handle both point and Multi coordinates
    if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (list, tuple)):
        # Take first coordinate in a nested array
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
    st = (p.get("region") or p.get("REGION") or p.get("STUSPS") or "").upper()
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

    address_str = " ".join([str(x) for x in [num, street, unit] if x])

    return {
        "address": address_str,
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
            feature_zip = str(
                p.get("postcode")
                or p.get("POSTCODE")
                or p.get("ZCTA5CE20")
                or ""
            )
            if feature_zip != str(zip_code):
                continue

        # Income filter
        if income_min is not None:
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
        if value_min is not None:
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
        log.info("[AUTH] Login failure")
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
        "datasets": len(CLEAN_DATASETS),
        "synonyms": len(COUNTY_SYNONYMS),
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

    log.info("[SEARCH] Incoming query: %r", query)

    try:
        state, county, key = resolve_any_county(query)
        if not key:
            return jsonify({
                "ok": False,
                "error": "No cleaned dataset found for that location. "
                         "Try including both county and state, e.g. "
                         "'homes in Harris County Texas' or "
                         "'addresses in Cuyahoga County OH'."
            })

        # Load & filter
        gj = _load_geojson_guarded(key)
        feats = gj.get("features", [])

        zip_code = detect_zip(query)
        income_min, value_min = parse_filters(query)

        feats = apply_filters(feats, income_min, value_min, zip_code)

        total = len(feats)
        feats = feats[:MAX_RESULTS]

        results = [feature_to_obj(f) for f in feats]

        return jsonify({
            "ok": True,
            "query": query,
            "state": state.upper(),
            "county": county.replace("_", " ").title(),
            "zip": zip_code,
            "total": total,
            "shown": len(results),
            "dataset_key": key,
            "results": results,
            "message": None,
        })

    except Exception as e:
        log.error("[ERROR] Runtime error for query=%r: %s", query, e)
        return jsonify({
            "ok": False,
            "error": str(e),
        }), 500


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

    log.info("[EXPORT] Incoming query: %r", query)

    try:
        state, county, key = resolve_any_county(query)
        if not key:
            return jsonify({
                "ok": False,
                "error": "Dataset not found for that location.",
            })

        gj = _load_geojson_guarded(key)
        feats = gj.get("features", [])

        zip_code = detect_zip(query)
        income_min, value_min = parse_filters(query)
        feats = apply_filters(feats, income_min, value_min, zip_code)

        filename = f"pelee_export_{county}_{state}.csv"

        def generate():
            buffer = io.StringIO()
            writer = csv.writer(buffer)

            # Header
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
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        log.error("[ERROR] Export error for query=%r: %s", query, e)
        return jsonify({
            "ok": False,
            "error": str(e),
        }), 500


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    # Local dev
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

import os
import json
import logging
import time
import re
import csv
import io
import random
from datetime import datetime

import boto3
from botocore.config import Config
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    jsonify,
    Response,
)
from dotenv import load_dotenv
import httpx

# -------------------------------------------------------------------
# ENV + LOGGING SETUP
# -------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = (
    os.getenv("AWS_DEFAULT_REGION")
    or os.getenv("AWS_REGION")
    or "us-east-2"
)

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip()

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Charlotte69")
SECRET_KEY = os.getenv("SECRET_KEY") or os.urandom(24).hex()

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

app = Flask(__name__)
app.secret_key = SECRET_KEY

boto_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
s3 = boto_session.client("s3", config=Config(max_pool_connections=10))

# (state, normalized_county) -> {"key": ..., "size": ..., "raw_name": ...}
COUNTY_INDEX: dict[str, dict[str, dict]] = {}

# -------------------------------------------------------------------
# STATE + COUNTY NORMALIZATION
# -------------------------------------------------------------------

STATE_ALIASES = {
    # 2-letter
    "al": "al", "ak": "ak", "az": "az", "ar": "ar", "ca": "ca",
    "co": "co", "ct": "ct", "de": "de", "fl": "fl", "ga": "ga",
    "hi": "hi", "id": "id", "il": "il", "in": "in", "ia": "ia",
    "ks": "ks", "ky": "ky", "la": "la", "me": "me", "md": "md",
    "ma": "ma", "mi": "mi", "mn": "mn", "ms": "ms", "mo": "mo",
    "mt": "mt", "ne": "ne", "nv": "nv", "nh": "nh", "nj": "nj",
    "nm": "nm", "ny": "ny", "nc": "nc", "nd": "nd", "oh": "oh",
    "ok": "ok", "or": "or", "pa": "pa", "ri": "ri", "sc": "sc",
    "sd": "sd", "tn": "tn", "tx": "tx", "ut": "ut", "vt": "vt",
    "va": "va", "wa": "wa", "wv": "wv", "wi": "wi", "wy": "wy",
    "dc": "dc", "pr": "pr",

    # full names
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
    "district of columbia": "dc",
    "puerto rico": "pr",
}


def normalize_state_name(text: str | None) -> str | None:
    if not text:
        return None
    t = text.strip().lower()
    if t in STATE_ALIASES:
        return STATE_ALIASES[t]
    t = t.strip(",. ")
    return STATE_ALIASES.get(t)


def normalize_county_name(name: str | None) -> str | None:
    if not name:
        return None
    n = name.strip().lower()
    n = n.replace("_", " ")
    n = re.sub(r"[-]+", " ", n)

    suffixes = [
        " county",
        " parish",
        " borough",
        " census area",
        " municipality",
        " city",
    ]
    for suf in suffixes:
        if n.endswith(suf):
            n = n[: -len(suf)]
            break
    return n.strip() or None


# -------------------------------------------------------------------
# BOOT: BUILD COUNTY INDEX FROM S3 CSVs
# -------------------------------------------------------------------

def build_county_index() -> None:
    """Scan S3 once on import and map (state, county_norm) -> key,size."""
    global COUNTY_INDEX
    logging.info(
        "[BOOT] Scanning S3 bucket=%s prefix='%s' for *.csv ...",
        S3_BUCKET,
        S3_PREFIX or "/",
    )
    total = 0
    paginator = s3.get_paginator("list_objects_v2")

    prefix = S3_PREFIX
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue

            rel = key[len(prefix):] if prefix else key
            parts = rel.split("/")
            if len(parts) != 2:
                continue

            state = parts[0].lower().strip()
            filename = parts[1]
            base = filename[:-4]

            county_norm = normalize_county_name(base)
            if not county_norm:
                continue

            COUNTY_INDEX.setdefault(state, {})
            COUNTY_INDEX[state][county_norm] = {
                "key": key,
                "size": obj["Size"],
                "raw_name": base,
            }
            total += 1

    logging.info("[BOOT] Indexed %d county CSV datasets.", total)


build_county_index()

# -------------------------------------------------------------------
# QUERY PARSING: LOCATION + FILTERS
# -------------------------------------------------------------------

def parse_query_location(query: str) -> tuple[str | None, str | None]:
    """
    Extract (state_code, county_name) from text like:
    - 'homes in wakulla county florida'
    - 'residential addresses in Cuyahoga County Ohio'
    """
    q = query.lower()
    tokens = q.split()

    county_idx = None
    for i, tok in enumerate(tokens):
        if tok in ("county", "parish", "borough"):
            county_idx = i
            break

    if county_idx is None:
        return None, None

    last_in = None
    for i in range(county_idx - 1, -1, -1):
        if tokens[i] == "in":
            last_in = i
            break
    if last_in is None:
        last_in = -1

    county_tokens = tokens[last_in + 1: county_idx]
    county_phrase = " ".join(county_tokens).strip()
    if not county_phrase:
        return None, None

    county_norm = normalize_county_name(county_phrase)
    if not county_norm:
        return None, None

    state_code = None
    for t in tokens[county_idx + 1:]:
        candidate = t.strip(",. ")
        st = normalize_state_name(candidate)
        if st:
            state_code = st
            break

    return state_code, county_norm


def parse_numeric_filters(query: str) -> tuple[int | None, int | None]:
    """
    Extract min income and min home value from query.
    e.g. 'income >100k', 'value >500000', etc.
    """
    q = query.lower()
    min_income = None
    min_value = None

    income_match = re.search(r"income\s*[>≥]\s*(\d+)([km]?)", q)
    if income_match:
        val_str = income_match.group(1)
        mult = income_match.group(2)
        val = int(val_str)
        if mult == "k":
            val *= 1000
        elif mult == "m":
            val *= 1000000
        min_income = val

    value_match = re.search(r"value\s*[>≥]\s*(\d+)([km]?)", q)
    if value_match:
        val_str = value_match.group(1)
        mult = value_match.group(2)
        val = int(val_str)
        if mult == "k":
            val *= 1000
        elif mult == "m":
            val *= 1000000
        min_value = val

    return min_income, min_value


# -------------------------------------------------------------------
# S3 CSV LOADING
# -------------------------------------------------------------------

def load_county_rows(state_code: str, county_norm: str):
    """
    Returns (row_iterator, metadata_dict) or (None, None).
    Each row is a dict with properties from the CSV.
    """
    if state_code not in COUNTY_INDEX:
        logging.warning("[LOAD] State %s not found in index.", state_code)
        return None, None

    if county_norm not in COUNTY_INDEX[state_code]:
        logging.warning(
            "[LOAD] County %s not found in state %s.",
            county_norm,
            state_code,
        )
        return None, None

    info = COUNTY_INDEX[state_code][county_norm]
    s3_key = info["key"]

    logging.info(
        "[LOAD] Loading S3 key=%s for state=%s county=%s",
        s3_key,
        state_code,
        county_norm,
    )

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        body = obj["Body"].read()
        text = body.decode("utf-8", errors="replace")
    except Exception as e:
        logging.error("[LOAD] S3 fetch error for key=%s: %s", s3_key, e)
        return None, None

    lines = text.splitlines()
    if len(lines) < 2:
        logging.warning("[LOAD] CSV has <2 lines: %s", s3_key)
        return None, None

    reader = csv.DictReader(lines)

    def row_gen():
        for row in reader:
            yield row

    meta = {
        "state": state_code,
        "county": county_norm,
        "key": s3_key,
        "size": info["size"],
    }

    return row_gen(), meta


# -------------------------------------------------------------------
# COMMERCIAL SEARCH (GOOGLE PLACES)
# -------------------------------------------------------------------

def run_commercial_search(query: str) -> tuple[bool, str, list]:
    """
    Returns (ok, error_msg, results_list).
    """
    if not GOOGLE_MAPS_API_KEY:
        return False, "Google Maps API key not configured.", []

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {
        "query": query,
        "key": GOOGLE_MAPS_API_KEY,
    }

    try:
        resp = httpx.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.error("[COMMERCIAL] Google Places API error: %s", e)
        return False, f"Error calling Google Places: {e}", []

    if data.get("status") != "OK":
        msg = f"Google Places status: {data.get('status')}"
        logging.warning("[COMMERCIAL] %s", msg)
        return False, msg, []

    places = data.get("results", [])
    results = []

    for place in places[:20]:
        name = place.get("name", "N/A")
        address = place.get("formatted_address", "N/A")
        phone = "N/A"
        website = "N/A"
        hours = "N/A"

        place_id = place.get("place_id")
        if place_id:
            details_url = "https://maps.googleapis.com/maps/api/place/details/json"
            details_params = {
                "place_id": place_id,
                "fields": "formatted_phone_number,website,opening_hours",
                "key": GOOGLE_MAPS_API_KEY,
            }
            try:
                details_resp = httpx.get(
                    details_url, params=details_params, timeout=10
                )
                details_resp.raise_for_status()
                details_data = details_resp.json()
                result = details_data.get("result", {})
                phone = result.get("formatted_phone_number", "N/A")
                website = result.get("website", "N/A")
                opening_hours = result.get("opening_hours", {})
                weekday_text = opening_hours.get("weekday_text", [])
                if weekday_text:
                    hours = "; ".join(weekday_text[:2])
            except Exception as e:
                logging.warning(
                    "[COMMERCIAL] Failed to fetch details for place_id=%s: %s",
                    place_id,
                    e,
                )

        results.append({
            "Name": name,
            "Address": address,
            "Phone": phone,
            "Website": website,
            "Hours": hours,
        })

    return True, "", results


# -------------------------------------------------------------------
# FILTERING HELPERS
# -------------------------------------------------------------------

def get_income(props: dict) -> int | None:
    val = props.get("income")
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except:
        return None


def get_home_value(props: dict) -> int | None:
    val = props.get("home_value")
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except:
        return None


def is_residential(props: dict) -> bool:
    """
    Heuristic: if 'use_code' contains 'residential' or similar.
    """
    use_code = (props.get("use_code") or "").lower()
    if "residential" in use_code or "single" in use_code:
        return True
    return False


def extract_basic_fields(props: dict) -> dict:
    """
    Build a friendly dict for display.
    """
    number = props.get("house_number") or props.get("number") or ""
    street = props.get("street_name") or props.get("street") or ""
    city = props.get("city") or ""
    state = props.get("state") or ""
    zip_code = props.get("zip") or ""

    address_parts = []
    if number:
        address_parts.append(str(number))
    if street:
        address_parts.append(str(street))
    address_str = " ".join(address_parts) if address_parts else ""

    return {
        "address": address_str,
        "number": number,
        "street": street,
        "city": city,
        "state": state,
        "zip": zip_code,
    }


def apply_filters_iter(
    row_iter,
    min_income: int | None,
    min_value: int | None,
    filter_residential: bool,
    max_results: int,
    max_scan: int,
):
    """
    Filter rows from iterator and return (preview_results, match_count, scanned_count).
    """
    preview_results = []
    matches = 0
    scanned = 0

    for props in row_iter:
        scanned += 1
        if scanned > max_scan:
            break

        if filter_residential and not is_residential(props):
            continue

        inc = get_income(props)
        val = get_home_value(props)

        if min_income is not None and (inc is None or inc < min_income):
            continue
        if min_value is not None and (val is None or val < min_value):
            continue

        matches += 1

        if len(preview_results) < max_results:
            basic = extract_basic_fields(props)
            preview_results.append({
                "address": basic.get("address"),
                "number": basic.get("number"),
                "street": basic.get("street"),
                "city": basic.get("city"),
                "state": basic.get("state"),
                "zip": basic.get("zip"),
                "income": inc,
                "home_value": val,
            })

    return preview_results, matches, scanned


# -------------------------------------------------------------------
# ROUTES: LOGIN/LOGOUT
# -------------------------------------------------------------------

def require_login():
    return session.get("authenticated") is True


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if password == ACCESS_PASSWORD:
            session["authenticated"] = True
            session["username"] = username or "user"
            logging.info("[LOGIN] User authenticated: %s", username)
            return redirect(url_for("index"))
        else:
            logging.warning("[LOGIN] Failed login attempt for user=%s", username)
            return render_template("login.html", error="Invalid credentials.")

    return render_template("login.html", error=None)


@app.route("/logout")
def logout():
    session.clear()
    logging.info("[LOGOUT] User logged out.")
    return redirect(url_for("login"))


# -------------------------------------------------------------------
# QUICK SAMPLE ROUTE (NEW FEATURE)
# -------------------------------------------------------------------

@app.route("/quick-sample", methods=["POST"])
def quick_sample():
    """
    Allows users to get 15 random addresses from a county WITHOUT logging in.
    Parse county/state from sample_query, load CSV, and return random sample.
    """
    sample_query = (request.form.get("sample_query") or "").strip()
    
    if not sample_query:
        return render_template(
            "login.html",
            error="Please enter a county and state for the quick sample."
        )

    # Parse location
    state_code, county_norm = parse_query_location(sample_query)
    
    if not state_code or not county_norm:
        return render_template(
            "login.html",
            error=f"Could not identify county/state from '{sample_query}'. Try: 'Cuyahoga County Ohio'"
        )

    # Load county data
    row_iter, meta = load_county_rows(state_code, county_norm)
    
    if row_iter is None or meta is None:
        friendly_county = f"{county_norm.title()} County, {state_code.upper()}"
        return render_template(
            "login.html",
            error=f"No data found for {friendly_county}. Please verify the county name and state."
        )

    # Collect all rows (limited to prevent memory issues)
    all_rows = []
    for i, props in enumerate(row_iter):
        if i >= 10000:  # Limit collection to first 10k rows
            break
        all_rows.append(props)

    if len(all_rows) == 0:
        return render_template(
            "login.html",
            error="County dataset is empty."
        )

    # Random sample of 15 (or fewer if dataset is small)
    sample_size = min(15, len(all_rows))
    sampled_rows = random.sample(all_rows, sample_size)

    # Format results
    results = []
    for props in sampled_rows:
        basic = extract_basic_fields(props)
        inc = get_income(props)
        val = get_home_value(props)
        
        results.append({
            "address": basic.get("address"),
            "number": basic.get("number"),
            "street": basic.get("street"),
            "city": basic.get("city"),
            "state": basic.get("state"),
            "zip": basic.get("zip"),
            "income": inc,
            "home_value": val,
        })

    location_label = f"{county_norm.title()} County, {state_code.upper()}"

    logging.info(
        "[QUICK_SAMPLE] Returned %d random samples from %s",
        len(results),
        location_label
    )

    # Render a special sample results page (we'll use index.html with special flag)
    return render_template(
        "sample_results.html",
        location=location_label,
        results=results,
        total=len(all_rows),
        shown=len(results),
        query=sample_query,
    )


# -------------------------------------------------------------------
# ROUTES: MAIN APP
# -------------------------------------------------------------------

@app.route("/")
def index():
    if not require_login():
        return redirect(url_for("login"))

    mode = request.args.get("mode", "residential")

    return render_template(
        "index.html",
        mode=mode,
        query=None,
        location=None,
        results=[],
        total=None,
        shown=0,
        error=None,
        download_available=False,
        commercial_results=[],
        commercial_error=None,
    )


@app.route("/search", methods=["POST"])
def search():
    if not require_login():
        return redirect(url_for("login"))

    query = (request.form.get("query") or "").strip()
    mode = (request.form.get("mode") or "residential").strip()

    if not query:
        return redirect(url_for("index", mode=mode))

    logging.info("[SEARCH] mode=%s query='%s'", mode, query)

    # ---------------------------------------------
    # COMMERCIAL SEARCH BRANCH (NO S3)
    # ---------------------------------------------
    if mode == "commercial":
        ok, err_msg, results = run_commercial_search(query)
        if not ok:
            return render_template(
                "index.html",
                mode=mode,
                query=query,
                location=None,
                results=[],
                total=None,
                shown=0,
                error=None,
                download_available=False,
                commercial_results=[],
                commercial_error=err_msg,
            )

        return render_template(
            "index.html",
            mode=mode,
            query=query,
            location=None,
            results=[],
            total=None,
            shown=len(results),
            error=None,
            download_available=False,
            commercial_results=results,
            commercial_error=None,
        )

    # ---------------------------------------------
    # RESIDENTIAL SEARCH BRANCH (S3 CSV)
    # ---------------------------------------------
    state_code, county_norm = parse_query_location(query)
    if not state_code or not county_norm:
        logging.info("[RESOLVE] Failed to parse state/county from query.")
        return render_template(
            "index.html",
            mode=mode,
            query=query,
            location=None,
            results=[],
            total=None,
            shown=0,
            error=(
                "Could not identify a county and state. "
                "Try format: 'addresses in Cuyahoga County Ohio'"
            ),
            download_available=False,
            commercial_results=[],
            commercial_error=None,
        )

    row_iter, meta = load_county_rows(state_code, county_norm)
    if row_iter is None or meta is None:
        friendly_county = f"{county_norm.title()} County, {state_code.upper()}"
        return render_template(
            "index.html",
            mode=mode,
            query=query,
            location=None,
            results=[],
            total=None,
            shown=0,
            error=f"No dataset found for {friendly_county}.",
            download_available=False,
            commercial_results=[],
            commercial_error=None,
        )

    min_income, min_value = parse_numeric_filters(query)
    q_lower = query.lower()
    filter_residential = (
        ("residential" in q_lower)
        or ("home" in q_lower)
        or ("homes" in q_lower)
        or ("house" in q_lower)
    )

    start = time.time()
    preview_results, matches, scanned = apply_filters_iter(
        row_iter,
        min_income=min_income,
        min_value=min_value,
        filter_residential=filter_residential,
        max_results=20,
        max_scan=50000,
    )
    elapsed = time.time() - start

    total = matches
    shown = len(preview_results)

    logging.info(
        "[RESULTS] mode=residential query='%s' state=%s county=%s total=%d shown=%d scanned=%d time=%.2fs",
        query,
        state_code,
        county_norm,
        total,
        shown,
        scanned,
        elapsed,
    )

    location_label = f"{county_norm.title()} County, {state_code.upper()}"

    if total == 0:
        error_msg = (
            "No addresses matched your search criteria. "
            "Try removing filters or verifying the county and state."
        )
    else:
        error_msg = None

    return render_template(
        "index.html",
        mode=mode,
        query=query,
        location=location_label,
        results=preview_results,
        total=total,
        shown=shown,
        error=error_msg,
        download_available=(shown > 0),
        commercial_results=[],
        commercial_error=None,
    )


@app.route("/download", methods=["POST"])
def download_csv():
    """
    Stream a full CSV of all matching rows for the given query.
    Only for residential mode.
    """
    if not require_login():
        return redirect(url_for("login"))

    query = (request.form.get("query") or "").strip()
    if not query:
        return redirect(url_for("index"))

    state_code, county_norm = parse_query_location(query)
    if not state_code or not county_norm:
        logging.info("[DOWNLOAD] Failed to parse state/county from query='%s'", query)
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            location=None,
            results=[],
            total=None,
            shown=0,
            error=(
                "Could not identify county/state from query for CSV export. "
                "Include both county name and state."
            ),
            download_available=False,
            commercial_results=[],
            commercial_error=None,
        )

    row_iter, meta = load_county_rows(state_code, county_norm)
    if row_iter is None or meta is None:
        friendly_county = f"{county_norm.title()} County, {state_code.upper()}"
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            location=None,
            results=[],
            total=None,
            shown=0,
            error=f"Could not find dataset for {friendly_county} for CSV export.",
            download_available=False,
            commercial_results=[],
            commercial_error=None,
        )

    min_income, min_value = parse_numeric_filters(query)
    q_lower = query.lower()
    filter_residential = (
        ("residential" in q_lower)
        or ("home" in q_lower)
        or ("homes" in q_lower)
        or ("house" in q_lower)
    )

    friendly_cols = ["address", "city", "state", "zip", "income", "home_value"]
    prop_keys: set[str] = set()

    rows_cache = []
    for props in row_iter:
        rows_cache.append(props)
        for k in props.keys():
            if k not in friendly_cols:
                prop_keys.add(k)

    ordered_prop_keys = sorted(prop_keys)
    fieldnames = friendly_cols + ordered_prop_keys

    def generate_csv_rows():
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)

        writer.writeheader()
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for props in rows_cache:
            if filter_residential and not is_residential(props):
                continue

            inc = get_income(props)
            val = get_home_value(props)

            if min_income is not None and (inc is None or inc < min_income):
                continue
            if min_value is not None and (val is None or val < min_value):
                continue

            basic = extract_basic_fields(props)

            row = {
                "address": (basic.get("address") or "") if basic else "",
                "city": (basic.get("city") or "") if basic else "",
                "state": (basic.get("state") or "") if basic else "",
                "zip": (basic.get("zip") or "") if basic else "",
                "income": "" if inc is None else f"{inc:.0f}",
                "home_value": "" if val is None else f"{val:.0f}",
            }

            for k in ordered_prop_keys:
                v = props.get(k)
                row[k] = "" if v is None else str(v)

            writer.writerow(row)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_county = re.sub(r"[^a-z0-9]+", "_", county_norm.lower())
    filename = f"{safe_county}_{state_code.lower()}_{ts}.csv"

    logging.info(
        "[DOWNLOAD] Streaming CSV for state=%s county=%s filename=%s",
        state_code,
        county_norm,
        filename,
    )

    return Response(
        generate_csv_rows(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

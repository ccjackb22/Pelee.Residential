import os
import json
import logging
import time
import re
import csv
import io
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
# CSVs are directly under state folders, e.g. "oh/cuyahoga.csv"
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

def build_county_index():
    """Scan S3 once on boot and map (state, county_norm) -> key,size."""
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
            key = obj["Key"]  # e.g. "oh/cuyahoga.csv"
            if not key.endswith(".csv"):
                continue

            rel = key[len(prefix):] if prefix else key
            parts = rel.split("/")
            if len(parts) != 2:
                # skip statewide.csv or odd shapes for now
                continue

            state = parts[0].lower().strip()
            filename = parts[1]  # "cuyahoga.csv"
            base = filename[:-4]  # drop ".csv"

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


def parse_numeric_filters(query: str) -> tuple[float | None, float | None]:
    """
    Extract (min_income, min_value) from phrases like:
    - 'over 200k income'
    - 'homes over 500k value'
    """
    q = query.lower()

    def parse_amount(match: re.Match) -> float | None:
        num_str, suffix = match.groups()
        try:
            val = float(num_str)
        except ValueError:
            return None
        if suffix == "k":
            val *= 1_000
        elif suffix == "m":
            val *= 1_000_000
        return val

    min_income = None
    min_value = None

    m_over = re.search(r"over\s+(\d+(?:\.\d+)?)(k|m)?", q)
    if m_over:
        amount = parse_amount(m_over)
        if amount is not None:
            if "income" in q:
                min_income = amount
            elif "value" in q or "home" in q:
                min_value = amount

    return min_income, min_value


# -------------------------------------------------------------------
# FEATURE HELPERS (RESIDENTIAL)
# -------------------------------------------------------------------

def get_income(props: dict) -> float | None:
    candidate_keys = [
        "income",
        "median_income",
        "acs_income",
        "HHINC",
        "MEDHHINC",
        "DP03_0062E",
    ]
    for k in candidate_keys:
        if k in props:
            v = props.get(k)
            if v in (None, "", "-666666666"):
                continue
            try:
                return float(v)
            except (ValueError, TypeError):
                continue
    return None


def get_home_value(props: dict) -> float | None:
    candidate_keys = [
        "home_value",
        "median_home_value",
        "acs_value",
        "med_home_value",
        "DP04_0089E",
    ]
    for k in candidate_keys:
        if k in props:
            v = props.get(k)
            if v in (None, "", "-666666666"):
                continue
            try:
                return float(v)
            except (ValueError, TypeError):
                continue
    return None


def extract_basic_fields(props: dict) -> dict:
    """
    Extract core address fields from CSV-like rows (Cuyahoga etc).
    """

    def first(*keys):
        for k in keys:
            if not k:
                continue
            if k in props:
                v = props.get(k)
                if v not in (None, ""):
                    return v
        return None

    # number
    house = first(
        "house_number",
        "HOUSE_NUM",
        "HOUSE",
        "HOUSENUM",
        "NUMBER",
        "number",
    )

    street = first("street", "STREET", "addr:street", "ROAD", "RD_NAME")
    city = first("city", "CITY")
    state = first("state", "STATE", "ST", "STUSPS", "STATE_NAME", "region", "REGION")
    postal = first("zip", "ZIP", "postal_code", "POSTCODE", "ZIPCODE", "ZIP_CODE")

    address = first("full_address", "address", "ADDR_FULL")
    if not address:
        parts = [str(house).strip() if house else None, street]
        address = " ".join([p for p in parts if p])

    return {
        "address": address,
        "number": house,
        "street": street,
        "city": city,
        "state": state,
        "zip": postal,
    }


RESIDENTIAL_VALUE_HINTS = [
    "res",
    "resid",
    "single fam",
    "single-family",
    "sfr",
    "sfh",
    "duplex",
    "triplex",
    "quadplex",
    "townhome",
    "town house",
    "condo",
    "apartment",
    "apt",
    "mobile home",
    "mh",
]

NON_RESIDENTIAL_VALUE_HINTS = [
    "farm",
    "agric",
    "agri",
    "industrial",
    "warehouse",
    "office",
    "retail",
    "church",
    "school",
    "gov",
    "government",
    "hospital",
    "hotel",
    "motel",
    "vacant",
    "land only",
]


def is_residential(props: dict) -> bool:
    """
    Heuristic classification based on textual hints.
    """
    for key, value in props.items():
        if value is None:
            continue
        if isinstance(value, (int, float)):
            continue
        text = str(value).lower()
        if len(text) > 200:
            continue
        if any(h in text for h in NON_RESIDENTIAL_VALUE_HINTS):
            if not any(h in text for h in RESIDENTIAL_VALUE_HINTS):
                return False

    for key, value in props.items():
        if value is None:
            continue
        if isinstance(value, (int, float)):
            continue
        text = str(value).lower()
        if len(text) > 200:
            continue
        if any(h in text for h in RESIDENTIAL_VALUE_HINTS):
            return True

    return True


def apply_filters_iter(
    row_iter,
    min_income: float | None = None,
    min_value: float | None = None,
    filter_residential: bool = True,
    max_results: int = 20,
    max_scan: int = 50000,
) -> tuple[list[dict], int, int]:
    """
    Scan CSV rows with early break.
    row_iter: iterable of dicts (each CSV row).
    Returns (results, total_matches, scanned_count).
    """
    results: list[dict] = []
    matches = 0
    scanned = 0

    for props in row_iter:
        scanned += 1

        if filter_residential and not is_residential(props):
            if scanned >= max_scan and len(results) >= max_results:
                break
            continue

        inc = get_income(props)
        val = get_home_value(props)

        if min_income is not None:
            if inc is None or inc < min_income:
                if scanned >= max_scan and len(results) >= max_results:
                    break
                continue

        if min_value is not None:
            if val is None or val < min_value:
                if scanned >= max_scan and len(results) >= max_results:
                    break
                continue

        matches += 1
        if len(results) < max_results:
            basic = extract_basic_fields(props)
            basic["income"] = inc
            basic["home_value"] = val
            results.append(basic)

        if scanned >= max_scan and len(results) >= max_results:
            break

    return results, matches, scanned


# -------------------------------------------------------------------
# S3 LOADING (CSV)
# -------------------------------------------------------------------

def load_county_rows(state_code: str, county_norm: str):
    """
    Resolve (state, county) to a CSV key and stream rows as dicts.
    """
    state_code = state_code.lower()
    county_norm = county_norm.lower()

    state_map = COUNTY_INDEX.get(state_code)
    if not state_map:
        return None, None

    meta = state_map.get(county_norm)
    if not meta:
        for cand_norm, cand_meta in state_map.items():
            if cand_norm == county_norm:
                meta = cand_meta
                break
            if county_norm in cand_norm or cand_norm in county_norm:
                meta = cand_meta
                break

    if not meta:
        return None, None

    key = meta["key"]
    size = meta["size"]

    logging.info(
        "[RESOLVE] Matched state=%s, county=%s, key=%s, size=%d",
        state_code,
        county_norm,
        key,
        size,
    )

    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"]
    # Stream CSV rows
    text_stream = io.TextIOWrapper(body, encoding="utf-8")
    reader = csv.DictReader(text_stream)
    return reader, meta


# -------------------------------------------------------------------
# COMMERCIAL SEARCH (GOOGLE PLACES)
# -------------------------------------------------------------------

def run_commercial_search(query: str) -> tuple[bool, str | None, list[dict]]:
    """
    Commercial search: uses Google Places Text Search + Details.
    Does NOT touch S3 or county CSVs.
    Output schema:
      {
        "Name": ...,
        "Address": ...,
        "Phone": ...,
        "Website": ...,
        "Hours": ...
      }
    """
    if not GOOGLE_MAPS_API_KEY:
        return False, "GOOGLE_MAPS_API_KEY is not configured.", []

    try:
        params = {"query": query, "key": GOOGLE_MAPS_API_KEY}
        resp = httpx.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params=params,
            timeout=15.0,
        )
        if resp.status_code != 200:
            return False, f"Places API HTTP {resp.status_code}", []

        data = resp.json()
        status = data.get("status")
        if status not in ("OK", "ZERO_RESULTS"):
            return False, f"Places API status: {status}", []

        base_results = data.get("results", [])
        if not base_results:
            return False, "No matching businesses found for that search.", []

        out: list[dict] = []

        for item in base_results[:20]:
            name = item.get("name", "")
            address = item.get("formatted_address", "")
            place_id = item.get("place_id")
            phone = "N/A"
            website = "N/A"
            hours_str = ""

            if place_id:
                try:
                    d_resp = httpx.get(
                        "https://maps.googleapis.com/maps/api/place/details/json",
                        params={
                            "place_id": place_id,
                            "fields": "international_phone_number,website,opening_hours",
                            "key": GOOGLE_MAPS_API_KEY,
                        },
                        timeout=15.0,
                    )
                    if d_resp.status_code == 200:
                        d_data = d_resp.json().get("result", {})
                        phone = d_data.get("international_phone_number") or "N/A"
                        website = d_data.get("website") or "N/A"
                        weekday = d_data.get("opening_hours", {}).get("weekday_text")
                        if weekday:
                            hours_str = "; ".join(weekday)
                except Exception as e:
                    logging.warning("Places details error: %s", e)

            out.append(
                {
                    "Name": name,
                    "Address": address,
                    "Phone": phone,
                    "Website": website,
                    "Hours": hours_str,
                }
            )

        return True, None, out

    except Exception as e:
        logging.exception("Commercial search failed.")
        return False, f"Commercial search error: {e}", []


# -------------------------------------------------------------------
# AUTH HELPERS
# -------------------------------------------------------------------

def require_login():
    return bool(session.get("authenticated"))


# -------------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "time": int(time.time()),
            "env_aws_id": bool(AWS_ACCESS_KEY_ID),
            "env_google_maps": bool(GOOGLE_MAPS_API_KEY),
        }
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == ACCESS_PASSWORD:
            session["authenticated"] = True
            logging.info("[AUTH] Login success")
            return redirect(url_for("index"))
        logging.info("[AUTH] Login failed")
        return render_template(
            "login.html",
            error="Incorrect password. Please try again.",
        )

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/", methods=["GET"])
def index():
    if not require_login():
        return redirect(url_for("login"))

    mode = request.args.get("mode", "residential")
    if mode not in ("residential", "commercial"):
        mode = "residential"

    return render_template(
        "index.html",
        mode=mode,
        query="",
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

    mode = request.form.get("mode", "residential")
    if mode not in ("residential", "commercial"):
        mode = "residential"

    query = (request.form.get("query") or "").strip()
    logging.info("[SEARCH] mode=%s query='%s'", mode, query)

    if not query:
        if mode == "commercial":
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
                commercial_error="Please enter a search query.",
            )
        else:
            return render_template(
                "index.html",
                mode=mode,
                query=query,
                location=None,
                results=[],
                total=None,
                shown=0,
                error="Please enter a search query with a county and state.",
                download_available=False,
                commercial_results=[],
                commercial_error=None,
            )

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
            shown=0,
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
                "Couldn't resolve a (county, state) from that query. "
                "Include BOTH the exact county name, the word 'County', and the full state name. "
                "Example: 'residential addresses in Wakulla County Florida'."
            ),
            download_available=False,
            commercial_results=[],
            commercial_error=None,
        )

    row_iter, meta = load_county_rows(state_code, county_norm)
    if row_iter is None or meta is None:
        return render_template(
            "index.html",
            mode=mode,
            query=query,
            location=None,
            results=[],
            total=None,
            shown=0,
            error=f"Couldn't find a dataset for {county_norm.title()} County, {state_code.upper()}.",
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
            "No matching addresses found for that filter. "
            "Try removing any income/value filters or double-checking the county + state."
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
        download_available=shown > 0,
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
                "Couldn't resolve a (county, state) from that query for CSV export. "
                "Include BOTH the exact county name, the word 'County', and the full state name."
            ),
            download_available=False,
            commercial_results=[],
            commercial_error=None,
        )

    row_iter, meta = load_county_rows(state_code, county_norm)
    if row_iter is None or meta is None:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            location=None,
            results=[],
            total=None,
            shown=0,
            error=f"Couldn't find a dataset for {county_norm.title()} County, {state_code.upper()} for CSV export.",
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

    # first pass to collect property keys
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

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

# NEW DATASET LAYOUT (what you just uploaded):
#
#   s3://residential-data-jack/<state>/<county>.csv
#
# Example:
#   s3://residential-data-jack/oh/cuyahoga.csv
#   s3://residential-data-jack/fl/wakulla.csv
#
# Each CSV row (from addresses_enriched_acs) looks like:
#   LON,LAT,NUMBER,STREET,UNIT,CITY,DISTRICT,REGION,POSTCODE,ID,HASH,
#   GEOID,median_income,median_home_value,median_rent,population,median_age
#
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip()  # IMPORTANT: leave this empty in Render

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Charlotte69")
SECRET_KEY = os.getenv("SECRET_KEY") or os.urandom(24).hex()

app = Flask(__name__)
app.secret_key = SECRET_KEY

boto_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
s3 = boto_session.client("s3", config=Config(max_pool_connections=10))

# (state, normalized_county) -> {key, size, raw_name}
COUNTY_INDEX: dict[str, dict[str, dict]] = {}

# -------------------------------------------------------------------
# STATE + COUNTY NORMALIZATION
# -------------------------------------------------------------------

STATE_ALIASES = {
    # 2-letter to 2-letter
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
    """
    Turn things like:
      'harris', 'Harris County', 'harris_county', 'HARRIS-COUNTY'
    into: 'harris'
    """
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

    return n.strip()


# -------------------------------------------------------------------
# BOOT: BUILD COUNTY INDEX FROM S3 (CSV FILES)
# -------------------------------------------------------------------

def build_county_index():
    """
    Scan S3 once on boot and map:
      (state, normalized_county) -> {key, size, raw_name}

    This reads CSV files that look like:
      <prefix><state>/<county>.csv
    e.g.:
      "oh/adams.csv"
      "tx/harris.csv"
    """
    global COUNTY_INDEX
    logging.info(
        "[BOOT] Scanning S3 bucket=%s prefix='%s' for *.csv ...",
        S3_BUCKET,
        S3_PREFIX,
    )
    total = 0
    paginator = s3.get_paginator("list_objects_v2")

    prefix = S3_PREFIX
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue

            size = obj["Size"]

            # strip prefix; if prefix is "" this is just key
            rel = key[len(prefix):] if prefix else key
            parts = rel.split("/")
            if len(parts) != 2:
                # unexpected layout — skip
                continue

            state = parts[0].lower().strip()
            filename = parts[1]  # e.g. "cuyahoga.csv"

            if not filename.lower().endswith(".csv"):
                continue

            base = filename[:-4]  # drop ".csv"
            norm = normalize_county_name(base) or base.lower()

            COUNTY_INDEX.setdefault(state, {})
            COUNTY_INDEX[state][norm] = {
                "key": key,
                "size": size,
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
      'homes in wakulla county florida'
      'residential addresses in Cuyahoga County Ohio'

    NOTE: Option A — we REQUIRE the word 'county' / 'parish' / 'borough'.
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

    # last "in" before 'county'
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
      'over 200k income'
      'homes over 500k value'
      'earning 200k+'
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

    # pattern: "over 200k", "over 500000"
    m_over = re.search(r"over\s+(\d+(?:\.\d+)?)(k|m)?", q)
    if m_over:
        amount = parse_amount(m_over)
        if amount is not None:
            if "income" in q or "earning" in q or "earners" in q:
                min_income = amount
            elif "value" in q or "home" in q or "homes" in q:
                min_value = amount

    # "200k+ income"
    m_plus = re.search(r"(\d+(?:\.\d+)?)(k|m)?\s*\+", q)
    if m_plus:
        amount = parse_amount(m_plus)
        if amount is not None:
            if "income" in q or "earning" in q or "earners" in q:
                min_income = amount
            elif "value" in q or "home" in q or "homes" in q:
                min_value = amount

    return min_income, min_value


# -------------------------------------------------------------------
# VALUE HELPERS (operate on CSV ROWS)
# -------------------------------------------------------------------

def get_income(row: dict) -> float | None:
    """
    Try multiple possible ACS income keys.
    With your pipeline we expect 'median_income', but also support
    other common names.
    """
    candidate_keys = [
        "income",
        "median_income",
        "acs_income",
        "HHINC",
        "MEDHHINC",
        "MEDHHINC_2023",
        "DP03_0062E",
    ]
    for k in candidate_keys:
        if k in row:
            v = row.get(k)
            if v in (None, "", "-666666666"):
                continue
            try:
                return float(v)
            except (ValueError, TypeError):
                continue
    return None


def get_home_value(row: dict) -> float | None:
    """
    Try multiple possible home value keys.
    With your pipeline we expect 'median_home_value'.
    """
    candidate_keys = [
        "home_value",
        "median_home_value",
        "acs_value",
        "med_home_value",
        "MEDVAL",
        "DP04_0089E",
    ]
    for k in candidate_keys:
        if k in row:
            v = row.get(k)
            if v in (None, "", "-666666666"):
                continue
            try:
                return float(v)
            except (ValueError, TypeError):
                continue
    return None


def extract_basic_fields(row: dict) -> dict:
    """
    Extract core address fields from a CSV row.
    The addresses_enriched_acs files use:
      NUMBER, STREET, CITY, REGION, POSTCODE
    """

    def first(*keys):
        for k in keys:
            if not k:
                continue
            if k in row:
                v = row.get(k)
                if v not in (None, ""):
                    return v
        return None

    house = first(
        "NUMBER",
        "number",
        "house_number",
        "HOUSE_NUM",
        "addr:housenumber",
        "HOUSE",
        "HOUSENUM",
    )
    street = first("STREET", "street", "addr:street", "ROAD", "RD_NAME")
    city = first("CITY", "city", "City")
    # REGION is your 2-letter state code in the addresses CSVs
    state = first("REGION", "state", "STATE", "ST", "STUSPS", "STATE_NAME")
    postal = first("POSTCODE", "postal_code", "zip", "ZIP", "ZIPCODE", "ZIP_CODE")

    address = first("full_address", "address", "ADDR_FULL")
    if not address:
        parts = []
        if house:
            parts.append(str(house).strip())
        if street:
            parts.append(str(street).strip())
        address = " ".join(parts) if parts else None

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


def is_residential(row: dict) -> bool:
    """
    Heuristic: look at short string values in the row and try to decide
    if it's clearly non-res. If ambiguous, default to residential.
    """
    for _, value in row.items():
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

    for _, value in row.items():
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
    rows: list[dict],
    min_income: float | None = None,
    min_value: float | None = None,
    filter_residential: bool = True,
    max_results: int = 20,
    max_scan: int = 50_000,
) -> tuple[list[dict], int, int]:
    """
    Scan rows with early break to avoid timeouts on huge counties.
    Returns (results_for_preview, total_matches, scanned_count).
    """
    results: list[dict] = []
    matches = 0
    scanned = 0

    for row in rows:
        scanned += 1

        if filter_residential and not is_residential(row):
            if scanned >= max_scan and len(results) >= max_results:
                break
            continue

        inc = get_income(row)
        val = get_home_value(row)

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
            basic = extract_basic_fields(row)
            basic["income"] = inc
            basic["home_value"] = val
            results.append(basic)

        if scanned >= max_scan and len(results) >= max_results:
            break

    return results, matches, scanned


# -------------------------------------------------------------------
# S3 LOADING (ROWS INSTEAD OF GEOJSON FEATURES)
# -------------------------------------------------------------------

def load_county_rows(state_code: str, county_norm: str) -> tuple[list[dict] | None, dict | None]:
    """
    Find the S3 key for (state, county) from COUNTY_INDEX and
    load its CSV rows into memory for preview.
    """
    state_code = state_code.lower()
    county_norm = county_norm.lower()

    state_map = COUNTY_INDEX.get(state_code)
    if not state_map:
        logging.info("[RESOLVE] No state entry found in index for '%s'", state_code)
        return None, None

    meta = state_map.get(county_norm)

    # fuzzy fallback for minor spelling differences
    if not meta:
        for cand_norm, cand_meta in state_map.items():
            if cand_norm == county_norm:
                meta = cand_meta
                break
            if county_norm in cand_norm or cand_norm in county_norm:
                meta = cand_meta
                break

    if not meta:
        logging.info(
            "[RESOLVE] No county entry found for state=%s county_norm='%s'",
            state_code,
            county_norm,
        )
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
    body = obj["Body"].read().decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(body))
    rows = list(reader)
    return rows, meta


# -------------------------------------------------------------------
# AUTH HELPERS
# -------------------------------------------------------------------

def require_login():
    if not session.get("authenticated"):
        return False
    return True


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
            "env_openai": bool(os.getenv("OPENAI_API_KEY")),
            "bucket": S3_BUCKET,
            "prefix": S3_PREFIX,
            "indexed_states": len(COUNTY_INDEX),
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

    return render_template(
        "index.html",
        query="",
        location=None,
        dataset_key=None,
        results=[],
        total=None,
        shown=0,
        error=None,
        download_available=False,
    )


@app.route("/search", methods=["POST"])
def search():
    if not require_login():
        return redirect(url_for("login"))

    query = (request.form.get("query") or "").strip()
    logging.info("[SEARCH] Incoming query='%s'", query)

    if not query:
        return render_template(
            "index.html",
            query=query,
            location=None,
            dataset_key=None,
            results=[],
            total=None,
            shown=0,
            error="Please enter a search query with a county and state.",
            download_available=False,
        )

    # 1) Parse location (county + state)
    state_code, county_norm = parse_query_location(query)
    if not state_code or not county_norm:
        logging.info("[RESOLVE] Failed to parse state/county from query.")
        return render_template(
            "index.html",
            query=query,
            location=None,
            dataset_key=None,
            results=[],
            total=None,
            shown=0,
            error=(
                "Couldn't resolve a (county, state) from that query. "
                "Include BOTH the exact county name, the word 'County', and the full state name. "
                "Example: 'residential addresses in Wakulla County Florida'."
            ),
            download_available=False,
        )

    # 2) Load rows from S3
    rows, meta = load_county_rows(state_code, county_norm)
    if rows is None or meta is None:
        return render_template(
            "index.html",
            query=query,
            location=None,
            dataset_key=None,
            results=[],
            total=None,
            shown=0,
            error=(
                f"Couldn't find a dataset for {county_norm.title()} County, "
                f"{state_code.upper()} in S3."
            ),
            download_available=False,
        )

    # 3) Parse filters (income/value)
    min_income, min_value = parse_numeric_filters(query)
    q_lower = query.lower()
    filter_residential = (
        ("residential" in q_lower)
        or ("home" in q_lower)
        or ("homes" in q_lower)
        or ("house" in q_lower)
    )

    # 4) Apply filters for preview only
    start = time.time()
    preview_results, matches, scanned = apply_filters_iter(
        rows,
        min_income=min_income,
        min_value=min_value,
        filter_residential=filter_residential,
        max_results=20,
        max_scan=50_000,
    )
    elapsed = time.time() - start

    total = matches
    shown = len(preview_results)

    logging.info(
        "[RESULTS] query='%s' state=%s county=%s total=%d shown=%d scanned=%d time=%.2fs",
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
        query=query,
        location=location_label,
        dataset_key=meta["key"],
        results=preview_results,
        total=total,
        shown=shown,
        error=error_msg,
        download_available=shown > 0,
    )


@app.route("/download", methods=["POST"])
def download_csv():
    """
    Stream a full CSV of all matching rows for the given query.
    Includes:
      - address, city, state, zip, income, home_value
      - all original columns from the county CSV
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
            query=query,
            location=None,
            dataset_key=None,
            results=[],
            total=None,
            shown=0,
            error=(
                "Couldn't resolve a (county, state) from that query for CSV export. "
                "Include BOTH the exact county name, the word 'County', and the full state name."
            ),
            download_available=False,
        )

    # Resolve S3 key via index
    state_map = COUNTY_INDEX.get(state_code.lower())
    meta = None
    if state_map:
        meta = state_map.get(county_norm.lower())
        if not meta:
            for cand_norm, cand_meta in state_map.items():
                if cand_norm == county_norm.lower() or \
                   county_norm.lower() in cand_norm or \
                   cand_norm in county_norm.lower():
                    meta = cand_meta
                    break

    if not meta:
        return render_template(
            "index.html",
            query=query,
            location=None,
            dataset_key=None,
            results=[],
            total=None,
            shown=0,
            error=(
                f"Couldn't find a dataset for {county_norm.title()} County, "
                f"{state_code.upper()} for CSV export."
            ),
            download_available=False,
        )

    key = meta["key"]

    # Same filter semantics as preview, but we stream the whole file
    min_income, min_value = parse_numeric_filters(query)
    q_lower = query.lower()
    filter_residential = (
        ("residential" in q_lower)
        or ("home" in q_lower)
        or ("homes" in q_lower)
        or ("house" in q_lower)
    )

    def generate_csv_rows():
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        text_stream = io.TextIOWrapper(obj["Body"], encoding="utf-8")
        reader = csv.DictReader(text_stream)

        base_keys = reader.fieldnames or []
        # Friendly columns first
        friendly = ["address", "city", "state", "zip", "income", "home_value"]
        fieldnames = friendly + [k for k in base_keys if k not in friendly]

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)

        # header
        writer.writeheader()
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        # rows
        for row in reader:
            if filter_residential and not is_residential(row):
                continue

            inc = get_income(row)
            val = get_home_value(row)

            if min_income is not None:
                if inc is None or inc < min_income:
                    continue

            if min_value is not None:
                if val is None or val < min_value:
                    continue

            basic = extract_basic_fields(row)

            out = {
                "address": (basic.get("address") or "") if basic else "",
                "city": (basic.get("city") or "") if basic else "",
                "state": (basic.get("state") or "") if basic else "",
                "zip": (basic.get("zip") or "") if basic else "",
                "income": "" if inc is None else f"{inc:.0f}",
                "home_value": "" if val is None else f"{val:.0f}",
            }

            # add all raw CSV columns
            for k in base_keys:
                v = row.get(k)
                out[k] = "" if v is None else str(v)

            writer.writerow(out)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_county = re.sub(r"[^a-z0-9]+", "_", county_norm.lower())
    filename = f"{safe_county}_{state_code.lower()}_{ts}.csv"

    logging.info(
        "[DOWNLOAD] Streaming CSV for state=%s county=%s key=%s filename=%s",
        state_code,
        county_norm,
        key,
        filename,
    )

    return Response(
        generate_csv_rows(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )


# -------------------------------------------------------------------
# ENTRYPOINT
# -------------------------------------------------------------------

if __name__ == "__main__":
    # For local testing only; Render uses gunicorn app:app
    app.run(host="0.0.0.0", port=5000, debug=True)

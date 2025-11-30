import os
import json
import logging
import time
import re
import csv
import io
from datetime import datetime

import requests
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

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip()  # leave empty on Render

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Charlotte69")
SECRET_KEY = os.getenv("SECRET_KEY") or os.urandom(24).hex()

# Google Maps / Places
GOOGLE_MAPS_API_KEY = (
    os.getenv("GOOGLE_MAPS_API_KEY")
    or os.getenv("GOOGLE_API_KEY")
)

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

def build_county_index() -> None:
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

    We REQUIRE the word 'county' / 'parish' / 'borough'.
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

    min_income: float | None = None
    min_value: float | None = None

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

def require_login() -> bool:
    return bool(session.get("authenticated"))


# -------------------------------------------------------------------
# GOOGLE PLACES COMMERCIAL SEARCH
# -------------------------------------------------------------------

def google_places_search(query: str, max_results: int = 10) -> tuple[bool, str | None, list[dict]]:
    if not GOOGLE_MAPS_API_KEY:
        return False, "Google Maps API key not configured", []

    try:
        params = {
            "query": query,
            "key": GOOGLE_MAPS_API_KEY,
        }
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params=params,
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()

        status = data.get("status")
        if status not in ("OK", "ZERO_RESULTS"):
            msg = data.get("error_message") or status
            return False, f"Google Places error: {msg}", []

        results = data.get("results", [])[:max_results]
        out: list[dict] = []

        for place in results:
            place_id = place.get("place_id")
            name = place.get("name")
            address = place.get("formatted_address")

            phone = "N/A"
            website = "N/A"
            hours_str = ""

            if place_id:
                try:
                    details_params = {
                        "place_id": place_id,
                        "fields": "formatted_phone_number,website,opening_hours",
                        "key": GOOGLE_MAPS_API_KEY,
                    }
                    dr = requests.get(
                        "https://maps.googleapis.com/maps/api/place/details/json",
                        params=details_params,
                        timeout=10,
                    )
                    dr.raise_for_status()
                    ddata = dr.json()
                    if ddata.get("status") == "OK":
                        det = ddata.get("result", {})
                        phone = det.get("formatted_phone_number") or "N/A"
                        website = det.get("website") or "N/A"
                        opening = det.get("opening_hours", {})
                        weekday = opening.get("weekday_text")
                        if isinstance(weekday, list):
                            hours_str = "; ".join(weekday)
                except Exception as e:
                    logging.warning("Places details error for %s: %s", place_id, e)

            out.append(
                {
                    "Name": name or "",
                    "Address": address or "",
                    "Phone": phone,
                    "Website": website,
                    "Hours": hours_str,
                }
            )

        return True, None, out

    except Exception as e:
        logging.exception("Google Places search failed")
        return False, str(e), []


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
            "bucket": S3_BUCKET,
            "prefix": S3_PREFIX,
            "indexed_states": len(COUNTY_INDEX),
            "google_configured": bool(GOOGLE_MAPS_API_KEY),
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
    return render_template("index.html")


# -----------------------------
# RESIDENTIAL SEARCH (JSON API)
# -----------------------------
@app.route("/search", methods=["POST"])
def search():
    if not require_login():
        return jsonify({"ok": False, "error": "Not authenticated"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    logging.info("[SEARCH] Residential query='%s'", query)

    if not query:
        return jsonify({"ok": False, "error": "Please enter a search query."}), 400

    state_code, county_norm = parse_query_location(query)
    if not state_code or not county_norm:
        logging.info("[RESOLVE] Failed to parse state/county from query.")
        return jsonify(
            {
                "ok": False,
                "error": (
                    "Couldn't resolve a (county, state) from that query. "
                    "Include BOTH the exact county name, the word 'County', and the full state name. "
                    "Example: 'residential addresses in Wakulla County Florida'."
                ),
            }
        ), 400

    rows, meta = load_county_rows(state_code, county_norm)
    if rows is None or meta is None:
        return jsonify(
            {
                "ok": False,
                "error": (
                    f"Couldn't find a dataset for {county_norm.title()} County, "
                    f"{state_code.upper()} in S3."
                ),
            }
        ), 404

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
        rows,
        min_income=min_income,
        min_value=min_value,
        filter_residential=filter_residential,
        max_results=20,
        max_scan=50_000,
    )
    elapsed = time.time() - start

    logging.info(
        "[RESULTS] res query='%s' state=%s county=%s total=%d shown=%d scanned=%d time=%.2fs",
        query,
        state_code,
        county_norm,
        matches,
        len(preview_results),
        scanned,
        elapsed,
    )

    return jsonify(
        {
            "ok": True,
            "location": f"{county_norm.title()} County, {state_code.upper()}",
            "total": matches,
            "shown": len(preview_results),
            "results": preview_results,
        }
    )


# -----------------------------
# COMMERCIAL SEARCH (JSON API)
# -----------------------------
@app.route("/commercial_search", methods=["POST"])
def commercial_search():
    if not require_login():
        return jsonify({"ok": False, "error": "Not authenticated"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    logging.info("[SEARCH] Commercial query='%s'", query)

    if not query:
        return jsonify({"ok": False, "error": "Please enter a search query."}), 400

    ok, error_msg, results = google_places_search(query, max_results=10)
    if not ok:
        return jsonify({"ok": False, "error": error_msg}), 500

    return jsonify(
        {
            "ok": True,
            "total_count": len(results),
            "preview_count": len(results),
            "results": results,
        }
    )


# -----------------------------
# RESIDENTIAL CSV DOWNLOAD
# -----------------------------
@app.route("/download", methods=["POST"])
def download_csv():
    if not require_login():
        return redirect(url_for("login"))

    query = (request.form.get("query") or "").strip()
    if not query:
        return redirect(url_for("index"))

    state_code, county_norm = parse_query_location(query)
    if not state_code or not county_norm:
        logging.info("[DOWNLOAD] Failed to parse state/county from query='%s'", query)
        return redirect(url_for("index"))

    state_map = COUNTY_INDEX.get(state_code.lower())
    meta = None
    if state_map:
        meta = state_map.get(county_norm.lower())
        if not meta:
            for cand_norm, cand_meta in state_map.items():
                if (
                    cand_norm == county_norm.lower()
                    or county_norm.lower() in cand_norm
                    or cand_norm in county_norm.lower()
                ):
                    meta = cand_meta
                    break

    if not meta:
        return redirect(url_for("index"))

    key = meta["key"]

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
    # For local testing; Render uses gunicorn app:app
    app.run(host="0.0.0.0", port=5000, debug=True)

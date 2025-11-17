import os
import json
import io
import csv
import zipfile
import re
from datetime import datetime

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    session,
    send_file,
)
from dotenv import load_dotenv
import boto3

# -----------------------------------------------------------
# ENV & CLIENTS
# -----------------------------------------------------------
load_dotenv()

app = Flask(__name__)

# Use the same names you set in Render
app.secret_key = os.getenv("SECRET_KEY", "dev_secret")
PASSWORD = os.getenv("ACCESS_PASSWORD", "CaLuna")

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION,
)

# -----------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------

# State names + abbreviations
STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}
STATE_ABBRS = {abbr.lower(): abbr for abbr in STATE_NAMES.values()}

DISPLAY_LIMIT = 500
EXPORT_LIMIT = 5000

# Cache of { "OH": { "cuyahoga": "merged_with_tracts/oh/cuyahoga-with-values-income.geojson", ... } }
STATE_KEYS_CACHE = {}

# -----------------------------------------------------------
# HELPERS: COUNTY / KEY RESOLUTION
# -----------------------------------------------------------

def _normalize_county_name(name):
    """
    Normalize a county name or slug to a simple token:
    'Cuyahoga County' -> 'cuyahoga'
    'king-with-values-income' -> 'king'
    'city_of_sheridan-with-values-income' -> 'sheridan'
    """
    if not name:
        return None

    s = str(name).lower().strip()

    # strip common suffixes used in filenames
    for suffix in [
        "-with-values-income.geojson",
        "-with-values-income",
        ".geojson",
    ]:
        if s.endswith(suffix):
            s = s[: -len(suffix)]

    # replace separators with spaces
    s = s.replace("_", " ").replace("-", " ")

    # drop words that are not helpful for matching
    drop_words = {
        "county", "parish", "borough", "census", "area",
        "city", "of", "municipality",
    }
    tokens = [t for t in s.split() if t not in drop_words]

    if not tokens:
        return None

    norm = " ".join(tokens)

    # final clean-up: keep only letters/numbers/spaces
    norm = re.sub(r"[^a-z0-9 ]+", "", norm)
    norm = re.sub(r"\s+", " ", norm).strip()

    return norm or None


def _load_state_index(state_abbr):
    """
    Build a mapping for a state:
        { normalized_county_name: S3 key }
    using list_objects_v2 on the 'merged_with_tracts/<state>/' prefix.
    """
    state_abbr = (state_abbr or "").upper()
    if state_abbr in STATE_KEYS_CACHE:
        return STATE_KEYS_CACHE[state_abbr]

    prefix = f"merged_with_tracts/{state_abbr.lower()}/"
    print(f"[DEBUG] Listing S3 for state index: prefix={prefix}")
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    mapping = {}
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if not key:
                continue
            if not key.endswith(".geojson"):
                continue

            # filename part
            filename = key.split("/")[-1]
            # Normalize filename to county key
            norm = _normalize_county_name(filename)
            if not norm:
                continue

            # Only keep one key per normalized name; first one wins
            if norm not in mapping:
                mapping[norm] = key

    STATE_KEYS_CACHE[state_abbr] = mapping
    print(f"[DEBUG] State index for {state_abbr}: {len(mapping)} counties")
    return mapping


def choose_dataset_for_location(state_abbr, county_name, city=None):
    """
    Decide which GeoJSON to use, based on:
      - state_abbr (required)
      - county_name (optional, but recommended)
      - city (optional; for OH we default to Cuyahoga if county is missing)
    """
    if not state_abbr:
        return None

    state_abbr = state_abbr.upper()
    state_index = _load_state_index(state_abbr)

    if not state_index:
        print(f"[DEBUG] No state index for {state_abbr}")
        return None

    # If county explicitly provided, try to match
    if county_name:
        norm_query = _normalize_county_name(county_name)
        print(f"[DEBUG] choose_dataset_for_location: county_name='{county_name}' → norm='{norm_query}'")

        if norm_query and norm_query in state_index:
            return state_index[norm_query]

        # fallback: fuzzy contains match
        if norm_query:
            candidates = []
            for norm_key, key in state_index.items():
                if norm_query in norm_key or norm_key in norm_query:
                    candidates.append((norm_key, key))
            if candidates:
                # pick the shortest norm_key as best match
                best = sorted(candidates, key=lambda x: len(x[0]))[0]
                print(f"[DEBUG] Fuzzy county match for '{norm_query}' → '{best[0]}'")
                return best[1]

        # no match
        return None

    # No county given:
    # For now, special-case OH to make your Strongsville/Berea/Westlake demos work:
    if state_abbr == "OH":
        # try Cuyahoga by common norm
        for candidate in ["cuyahoga", "cuyahoga county"]:
            norm = _normalize_county_name(candidate)
            if norm and norm in state_index:
                print("[DEBUG] Defaulting to Cuyahoga County, OH (no county specified).")
                return state_index[norm]

    # For other states, county is required
    return None

# -----------------------------------------------------------
# S3 HELPER
# -----------------------------------------------------------

def load_geojson_from_s3(key):
    """Download a single GeoJSON file from S3 and parse it."""
    try:
        print(f"[DEBUG] Fetching S3 object: bucket={S3_BUCKET}, key={key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read()
        print(f"[DEBUG] Downloaded {len(body)} bytes from {key}")
        data = json.loads(body.decode("utf-8"))
        return data
    except Exception as e:
        print(f"[ERROR] S3 read failed for {key}: {e}")
        return None

# -----------------------------------------------------------
# QUERY PARSING
# -----------------------------------------------------------

def parse_basic_query(q):
    """
    Parse basic location intent:
      - state_abbr: from full name ("ohio") or abbreviation ("oh")
      - county_name: '<name> county' if present
      - city: first word after "in"/"near"/"at"/"around"
    """
    q = (q or "").strip()
    q_lower = q.lower()

    # Detect state from full name
    state_abbr = None
    for name, abbr in STATE_NAMES.items():
        if name in q_lower:
            state_abbr = abbr
            break

    # Detect state from 2-letter abbreviation
    tokens = re.split(r"[,\s]+", q_lower)
    for tok in tokens:
        if tok in STATE_ABBRS:
            state_abbr = STATE_ABBRS[tok]
            break

    # County: look for "<word> county"
    county_name = None
    for i, w in enumerate(tokens):
        if w == "county" and i > 0:
            county_name = tokens[i - 1]
            break

    # City: first word after "in/near/at/around" that is not 'county'
    city = None
    for i, w in enumerate(tokens):
        if w in ("in", "near", "at", "around"):
            if i + 1 < len(tokens):
                nxt = tokens[i + 1]
                if nxt not in ("county", "city", "town") and nxt not in STATE_NAMES and nxt not in STATE_ABBRS:
                    city = nxt
                    break

    print(f"[DEBUG] parse_basic_query → state={state_abbr}, county={county_name}, city={city}")
    return state_abbr, county_name, city


def _parse_number_token(tok):
    """
    Convert things like "300k", "1,200,000", "$500k" → float
    """
    if not tok:
        return None
    cleaned = tok.replace("$", "").replace(",", "").strip()
    multiplier = 1.0
    if cleaned.lower().endswith("k"):
        multiplier = 1_000.0
        cleaned = cleaned[:-1]
    elif cleaned.lower().endswith("m"):
        multiplier = 1_000_000.0
        cleaned = cleaned[:-1]

    try:
        return float(cleaned) * multiplier
    except ValueError:
        return None


def parse_numeric_filters(q):
    """
    Parse simple numeric filters for income / home value.

    Examples:
      "homes in westlake ohio with values over 500k"
      "addresses in strongsville ohio with incomes over 300000"
    """
    q = (q or "").strip()
    q_lower = q.lower()

    min_income = None
    min_home_value = None

    has_income = "income" in q_lower or "incomes" in q_lower
    has_value = "value" in q_lower or "values" in q_lower or "price" in q_lower or "prices" in q_lower

    # Grab the first numeric-like token
    num_matches = re.findall(r"(\$?\d[\d,\.]*\s*[kKmM]?)", q_lower)
    first_num = num_matches[0] if num_matches else None
    numeric_val = _parse_number_token(first_num) if first_num else None

    if numeric_val is not None:
        if has_income and not has_value:
            min_income = numeric_val
        elif has_value and not has_income:
            min_home_value = numeric_val
        elif has_income and has_value:
            # If both mentioned, we’ll apply the number to income first.
            min_income = numeric_val

    print(f"[DEBUG] parse_numeric_filters → min_home_value={min_home_value}, min_income={min_income}")
    return min_home_value, min_income

# -----------------------------------------------------------
# FILTERING
# -----------------------------------------------------------

def _to_float(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        s = str(val).replace(",", "").replace("$", "").strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _get_numeric_from_props(props, keys):
    for k in keys:
        if k in props and props[k] not in (None, ""):
            v = _to_float(props[k])
            if v is not None:
                return v
    return None


def filter_features(
    features,
    city=None,
    min_home_value=None,
    min_income=None,
    limit=DISPLAY_LIMIT,
    for_export=False,
):
    """
    Apply filters:
      - optional city match
      - optional min_home_value
      - optional min_income

    If for_export=False:
        return simplified rows for display.
    If for_export=True:
        return full features (properties + geometry) for CSV export.
    """
    results = []
    city_norm = city.lower() if city else None

    for f in features:
        props = f.get("properties", {}) or {}

        # City filter
        if city_norm:
            prop_city = str(props.get("city", "")).lower()
            if prop_city != city_norm:
                continue

        # Income filter
        if min_income is not None:
            income_val = _get_numeric_from_props(
                props,
                ["income", "median_income", "household_income", "acs_income"],
            )
            if income_val is None or income_val < min_income:
                continue

        # Home value filter
        if min_home_value is not None:
            value_val = _get_numeric_from_props(
                props,
                ["home_value", "median_home_value", "value", "acs_home_value"],
            )
            if value_val is None or value_val < min_home_value:
                continue

        if for_export:
            results.append({
                "properties": props,
                "geometry": f.get("geometry", {}),
            })
        else:
            results.append({
                "number": props.get("number", ""),
                "street": props.get("street", ""),
                "unit": props.get("unit", ""),
                "city": props.get("city", ""),
                "postcode": props.get("postcode", ""),
                "region": props.get("region", ""),
            })

        if len(results) >= limit:
            break

    print(f"[DEBUG] filter_features → returned {len(results)} rows (for_export={for_export})")
    return results

# -----------------------------------------------------------
# AUTH
# -----------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "").strip()
        if pw == PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        return render_template("login.html", error="Incorrect password.")
    return render_template("login.html")

# -----------------------------------------------------------
# HOME
# -----------------------------------------------------------

@app.route("/")
def home():
    if not session.get("logged_in"):
        return redirect("/login")
    return render_template("index.html")

# -----------------------------------------------------------
# CLEAR
# -----------------------------------------------------------

@app.route("/clear")
def clear():
    if not session.get("logged_in"):
        return redirect("/login")
    session.pop("last_query_meta", None)
    return redirect("/")

# -----------------------------------------------------------
# SEARCH
# -----------------------------------------------------------

@app.route("/search", methods=["POST"])
def search():
    if not session.get("logged_in"):
        return redirect("/login")

    query = request.form.get("query", "") or ""
    print(f"[DEBUG] /search query='{query}'")

    state_abbr, county_name, city = parse_basic_query(query)
    min_home_value, min_income = parse_numeric_filters(query)

    if not state_abbr:
        return render_template(
            "index.html",
            error="Couldn't detect a state. Try 'addresses in strongsville ohio' or 'addresses in king county washington'."
        )

    dataset_key = choose_dataset_for_location(state_abbr, county_name, city=city)

    if not dataset_key:
        # For states other than OH, county is required
        if state_abbr.upper() == "OH":
            msg = "Couldn't pick a county in Ohio. Try including the county name (e.g. 'in cuyahoga county ohio')."
        else:
            msg = f"For {state_abbr}, please include the county name (e.g. 'in king county {state_abbr.lower()}')."
        return render_template("index.html", error=msg)

    data = load_geojson_from_s3(dataset_key)
    if not data or "features" not in data:
        return render_template(
            "index.html",
            error=f"Failed to load dataset for {state_abbr} (key={dataset_key})."
        )

    features = data["features"]
    results = filter_features(
        features,
        city=city,
        min_home_value=min_home_value,
        min_income=min_income,
        limit=DISPLAY_LIMIT,
        for_export=False,
    )

    if not results:
        return render_template(
            "index.html",
            error="No matching addresses found. Try another city, or adjust your income/value filters."
        )

    # Store query meta so /export can re-run against same dataset
    session["last_query_meta"] = {
        "raw_query": query,
        "state_abbr": state_abbr,
        "county_name": county_name,
        "city": city,
        "min_home_value": min_home_value,
        "min_income": min_income,
        "dataset_key": dataset_key,
    }

    return render_template("index.html", results=results)

# -----------------------------------------------------------
# EXPORT → ZIPPED CSV
# -----------------------------------------------------------

@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    meta = session.get("last_query_meta")
    if not meta:
        return render_template(
            "index.html",
            error="No results to export. Run a search first."
        )

    dataset_key = meta.get("dataset_key")
    state_abbr = meta.get("state_abbr")
    county_name = meta.get("county_name")
    city = meta.get("city")
    min_home_value = meta.get("min_home_value")
    min_income = meta.get("min_income")

    data = load_geojson_from_s3(dataset_key)
    if not data or "features" not in data:
        return render_template(
            "index.html",
            error="Could not reload data for export. Please try your search again."
        )

    export_features = filter_features(
        data["features"],
        city=city,
        min_home_value=min_home_value,
        min_income=min_income,
        limit=EXPORT_LIMIT,
        for_export=True,
    )

    if not export_features:
        return render_template(
            "index.html",
            error="No results to export. Try adjusting your search."
        )

    # Collect all property keys across results
    all_prop_keys = set()
    for item in export_features:
        props = item.get("properties", {}) or {}
        all_prop_keys.update(props.keys())

    # Base display columns
    base_prop_to_col = {
        "number": "Address Number",
        "street": "Street",
        "unit": "Unit",
        "city": "City",
        "region": "State",
        "postcode": "ZIP",
    }

    # Commercial-friendly aliases
    alias_definitions = [
        ("County", ["county_name", "NAME", "NAMELSADCO"]),
        ("Census Tract", ["GEOID", "GEOIDFQ", "TRACTCE"]),
        ("Land Area (sq m)", ["ALAND"]),
        ("Water Area (sq m)", ["AWATER"]),
        ("Household Income (USD)", ["income", "median_income", "household_income", "acs_income"]),
        ("Home Value (USD)", ["home_value", "median_home_value", "value", "acs_home_value"]),
    ]

    consumed_keys = set(base_prop_to_col.keys())
    for _, key_list in alias_definitions:
        consumed_keys.update(key_list)

    extra_prop_keys = sorted(all_prop_keys - consumed_keys)

    fieldnames = list(base_prop_to_col.values())
    alias_names = [alias_name for alias_name, _ in alias_definitions]
    fieldnames.extend(alias_names)
    fieldnames.extend(["Latitude", "Longitude"])
    fieldnames.extend(extra_prop_keys)

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()

    for item in export_features:
        props = item.get("properties", {}) or {}
        geom = item.get("geometry", {}) or {}
        row = {}

        for raw_key, col_name in base_prop_to_col.items():
            row[col_name] = props.get(raw_key, "")

        for alias_name, key_candidates in alias_definitions:
            value = ""
            for k in key_candidates:
                if k in props and props[k] not in (None, ""):
                    value = props[k]
                    break
            row[alias_name] = value

        lat = ""
        lon = ""
        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            lon, lat = coords[0], coords[1]
        row["Latitude"] = lat
        row["Longitude"] = lon

        for k in extra_prop_keys:
            row[k] = props.get(k, "")

        writer.writerow(row)

    csv_bytes = csv_buffer.getvalue().encode("utf-8")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        city_part = (city or "results").replace(" ", "_").lower()
        state_part = (state_abbr or "").lower()
        county_part = (county_name or "").replace(" ", "_").lower()
        if county_part:
            csv_name = f"{city_part}_{county_part}_{state_part}_results.csv"
        else:
            csv_name = f"{city_part}_{state_part}_results.csv"
        zf.writestr(csv_name, csv_bytes)

    zip_buffer.seek(0)
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    download_name = f"pelee_addresses_{timestamp}.zip"

    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name=download_name,
    )

# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

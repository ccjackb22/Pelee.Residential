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
app.secret_key = os.getenv("SECRET_KEY", "dev_secret")

# Password: uses env if set, otherwise "CaLuna"
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

# Ohio baseline dataset (still valid, just not the only one anymore)
OH_CUYAHOGA_DATASET = "merged_with_tracts/oh/cuyahoga-with-values-income.geojson"

# Special-case mapping for Ohio city-only queries → Cuyahoga file
OH_CITY_TO_DATASET = {
    "strongsville": OH_CUYAHOGA_DATASET,
    "westlake": OH_CUYAHOGA_DATASET,
    "lakewood": OH_CUYAHOGA_DATASET,
    "berea": OH_CUYAHOGA_DATASET,
    "rocky": OH_CUYAHOGA_DATASET,
    "rocky_river": OH_CUYAHOGA_DATASET,
    "parma": OH_CUYAHOGA_DATASET,
    "cleveland": OH_CUYAHOGA_DATASET,
}

DISPLAY_LIMIT = 500
EXPORT_LIMIT = 5000


# -----------------------------------------------------------
# S3 HELPERS
# -----------------------------------------------------------
def load_geojson_from_s3(key: str) -> dict | None:
    try:
        print(f"[DEBUG] Fetching S3 object: {key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read()
        print(f"[DEBUG] Bytes read: {len(body)}")
        return json.loads(body.decode("utf-8"))
    except Exception as e:
        print(f"[ERROR] S3 read failed: {e}")
        return None


def slugify_county_name(county: str) -> str:
    """
    Turn 'Santa Clara' -> 'santa_clara'
    Turn 'st. louis' -> 'st_louis'
    """
    if not county:
        return ""
    c = county.lower().strip()
    # remove trailing "county" if still present
    c = re.sub(r"\bcounty\b", "", c).strip()
    # replace non-alphanumeric with underscores
    c = re.sub(r"[^a-z0-9]+", "_", c)
    c = re.sub(r"_+", "_", c).strip("_")
    return c


# -----------------------------------------------------------
# COUNTY PARSER (THIS WAS THE BROKEN PIECE)
# -----------------------------------------------------------
def extract_county_name(q_lower: str) -> str | None:
    """
    Bulletproof: walk backwards from the word 'county' and collect
    the actual county name tokens, ignoring junk like 'addresses', 'homes', etc.

    Example:
      'addresses in alameda county california' → 'alameda'
      'residential properties in palm beach county fl' → 'palm beach'
      'homes in santa clara county california' → 'santa clara'
    """
    tokens = q_lower.split()
    if "county" not in tokens:
        return None

    idx = tokens.index("county")

    STOPWORDS = {
        "in", "the", "of", "and", "for", "to", "all", "any",
        "addresses", "address", "homes", "home", "houses",
        "parcels", "properties", "residential", "commercial",
        "data", "with", "over", "above",
    }

    county_tokens = []
    i = idx - 1
    while i >= 0:
        w = tokens[i]
        if w in STOPWORDS:
            break
        # accept alphabetic or alphabetic+dot tokens (st., etc.)
        if not re.match(r"^[a-z\.]+$", w):
            break
        county_tokens.append(w)
        i -= 1

    if not county_tokens:
        return None

    county_tokens.reverse()
    return " ".join(county_tokens)


def resolve_dataset_key(state_abbr: str | None, county: str | None, city: str | None) -> str | None:
    """
    Decide which S3 key to hit based on:
      - Ohio city shortcuts
      - Explicit county + state
    Pattern: merged_with_tracts/{state}/{county}-with-values-income.geojson
    """
    if not state_abbr:
        return None

    state_abbr = state_abbr.upper()
    state_prefix = state_abbr.lower()

    # 1) Ohio city-only shortcuts (Strongsville, etc.)
    if state_abbr == "OH" and city:
        c = city.lower()
        if c in OH_CITY_TO_DATASET:
            key = OH_CITY_TO_DATASET[c]
            print(f"[DEBUG] resolve_dataset_key → OH city shortcut key={key}")
            return key

    # 2) Explicit COUNTY + state: merged_with_tracts/{state}/{county}-with-values-income.geojson
    if county:
        county_slug = slugify_county_name(county)
        if county_slug:
            key = f"merged_with_tracts/{state_prefix}/{county_slug}-with-values-income.geojson"
            print(f"[DEBUG] resolve_dataset_key → county-based key={key}")
            return key

    # 3) Fallback: none
    print("[DEBUG] resolve_dataset_key → no key resolved")
    return None


# -----------------------------------------------------------
# QUERY PARSING
# -----------------------------------------------------------
def parse_basic_query(q: str):
    """
    Detect:
      - state_abbr: e.g. 'CA'
      - county: e.g. 'santa clara'
      - city: first token after 'in/near/around/at'
    """
    q = (q or "").strip()
    q_lower = q.lower()
    tokens = re.split(r"[,\s]+", q_lower)

    state_abbr = None
    city = None

    # --- FULL-NAME STATE DETECTION ---
    for name, abbr in STATE_NAMES.items():
        if re.search(r"\b" + re.escape(name) + r"\b", q_lower):
            state_abbr = abbr
            break

    # --- ABBREVIATION DETECTION (CA, FL, OH, etc.) ---
    if state_abbr is None:
        skip_words = {
            "in", "me", "my", "give", "get", "show", "find", "for", "to",
            "of", "the", "a", "an", "on", "at", "near", "around",
            "homes", "home", "houses", "addresses", "address",
            "residential", "commercial", "properties", "property",
        }
        for tok in tokens:
            if tok in skip_words:
                continue
            if len(tok) == 2 and tok in STATE_ABBRS:
                state_abbr = STATE_ABBRS[tok]
                break

    # --- COUNTY via backwards scanner ---
    county = extract_county_name(q_lower)

    # --- CITY DETECTION (word after in/near/around/at) ---
    for i, w in enumerate(tokens):
        if w in ("in", "near", "around", "at"):
            if i + 1 < len(tokens):
                nxt = tokens[i + 1]
                if nxt in ("the",):
                    continue
                city = nxt
                break

    print(f"[DEBUG] parse_basic_query → state={state_abbr}, county={county}, city={city}")
    return state_abbr, county, city


def _parse_number_token(tok: str) -> float | None:
    if not tok:
        return None
    cleaned = tok.replace("$", "").replace(",", "").strip()
    multiplier = 1
    if cleaned.lower().endswith("k"):
        multiplier = 1000
        cleaned = cleaned[:-1]
    elif cleaned.lower().endswith("m"):
        multiplier = 1_000_000
        cleaned = cleaned[:-1]
    try:
        return float(cleaned) * multiplier
    except:
        return None


def parse_numeric_filters(q: str):
    q_lower = q.lower()

    has_income = "income" in q_lower or "incomes" in q_lower
    has_value = "value" in q_lower or "values" in q_lower or "price" in q_lower

    nums = re.findall(r"(\$?\d[\d,\.]*\s*[kKmM]?)", q_lower)
    first = nums[0] if nums else None
    numeric_val = _parse_number_token(first)

    min_income = None
    min_home_value = None

    if numeric_val is not None:
        if has_income and not has_value:
            min_income = numeric_val
        elif has_value and not has_income:
            min_home_value = numeric_val
        elif has_income and has_value:
            # if both mentioned, lean toward income
            min_income = numeric_val

    print(f"[DEBUG] parse_numeric_filters → value={min_home_value}, income={min_income}")
    return min_home_value, min_income


# -----------------------------------------------------------
# FILTERING
# -----------------------------------------------------------
def _to_float(val):
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").replace("$", ""))
    except:
        return None


def _get_numeric_from_props(props: dict, keys: list[str]):
    for k in keys:
        if k in props and props[k] not in ("", None):
            v = _to_float(props[k])
            if v is not None:
                return v
    return None


def filter_features(features, city=None, min_home_value=None, min_income=None,
                    limit=DISPLAY_LIMIT, for_export=False):

    results = []
    city_norm = city.lower() if city else None

    for f in features:
        props = f.get("properties", {}) or {}

        # City filter
        if city_norm:
            if str(props.get("city", "")).lower() != city_norm:
                continue

        # Income filter
        if min_income is not None:
            inc = _get_numeric_from_props(
                props,
                ["income", "median_income", "household_income", "acs_income"]
            )
            if inc is None or inc < min_income:
                continue

        # Home value filter
        if min_home_value is not None:
            val = _get_numeric_from_props(
                props,
                ["home_value", "median_home_value", "value", "acs_home_value"]
            )
            if val is None or val < min_home_value:
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

    print(f"[DEBUG] filter_features → {len(results)} rows")
    return results


# -----------------------------------------------------------
# AUTH
# -----------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "")
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

    query = request.form.get("query", "")
    print(f"[DEBUG] /search query='{query}'")

    state_abbr, county, city = parse_basic_query(query)
    min_home_value, min_income = parse_numeric_filters(query)

    if not state_abbr:
        return render_template(
            "index.html",
            error="Couldn't detect a state. Try 'addresses in santa clara county california'."
        )

    dataset_key = resolve_dataset_key(state_abbr, county, city)
    if not dataset_key:
        return render_template(
            "index.html",
            error="Couldn't detect a target county/dataset. For now, include a county like 'in santa clara county california', or use an Ohio city like Strongsville."
        )

    data = load_geojson_from_s3(dataset_key)
    if not data or "features" not in data:
        return render_template(
            "index.html",
            error=f"Failed to load dataset for that area (key: {dataset_key})."
        )

    results = filter_features(
        data["features"],
        city=city,
        min_home_value=min_home_value,
        min_income=min_income,
        limit=DISPLAY_LIMIT,
        for_export=False
    )

    if not results:
        return render_template(
            "index.html",
            error="No matching addresses found."
        )

    session["last_query_meta"] = {
        "query": query,
        "state_abbr": state_abbr,
        "county": county,
        "city": city,
        "min_home_value": min_home_value,
        "min_income": min_income,
        "dataset_key": dataset_key,
    }

    return render_template("index.html", results=results)


# -----------------------------------------------------------
# EXPORT
# -----------------------------------------------------------
@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    meta = session.get("last_query_meta")
    if not meta:
        return render_template(
            "index.html",
            error="No results to export."
        )

    data = load_geojson_from_s3(meta["dataset_key"])
    if not data:
        return render_template(
            "index.html",
            error="Failed to reload dataset."
        )

    export_features = filter_features(
        data["features"],
        city=meta["city"],
        min_home_value=meta["min_home_value"],
        min_income=meta["min_income"],
        limit=EXPORT_LIMIT,
        for_export=True
    )

    if not export_features:
        return render_template(
            "index.html",
            error="No results to export."
        )

    # Collect all property keys
    all_prop_keys = set()
    for item in export_features:
        all_prop_keys.update(item["properties"].keys())

    # Base column mapping
    base_map = {
        "number": "Address Number",
        "street": "Street",
        "unit": "Unit",
        "city": "City",
        "region": "State",
        "postcode": "ZIP",
    }

    alias_defs = [
        ("County", ["county_name", "NAME", "NAMELSADCO"]),
        ("Census Tract", ["GEOID", "GEOIDFQ", "TRACTCE"]),
        ("Land Area (sq m)", ["ALAND"]),
        ("Water Area (sq m)", ["AWATER"]),
        ("Household Income (USD)", ["income", "median_income", "household_income", "acs_income"]),
        ("Home Value (USD)", ["home_value", "median_home_value", "value", "acs_home_value"]),
    ]

    consumed = set(base_map.keys())
    for _, arr in alias_defs:
        consumed.update(arr)

    extra = sorted(all_prop_keys - consumed)

    fieldnames = list(base_map.values())
    fieldnames.extend([name for name, _ in alias_defs])
    fieldnames.extend(["Latitude", "Longitude"])
    fieldnames.extend(extra)

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()

    for item in export_features:
        props = item["properties"]
        geom = item.get("geometry", {})
        row = {}

        for raw, col in base_map.items():
            row[col] = props.get(raw, "")

        for col, keys in alias_defs:
            val = ""
            for k in keys:
                if k in props and props[k] not in ("", None):
                    val = props[k]
                    break
            row[col] = val

        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            lon, lat = coords[0], coords[1]
        else:
            lat = lon = ""
        row["Latitude"] = lat
        row["Longitude"] = lon

        for k in extra:
            row[k] = props.get(k, "")

        writer.writerow(row)

    csv_bytes = buf.getvalue().encode("utf-8")
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("results.csv", csv_bytes)

    zip_buf.seek(0)
    return send_file(
        zip_buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name="pelee_results.zip"
    )


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

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

# For now we wire this demo specifically to Cuyahoga County, OH
OH_CUYAHOGA_DATASET = "merged_with_tracts/oh/cuyahoga-with-values-income.geojson"

# Cap results to keep Render happy
DISPLAY_LIMIT = 500
EXPORT_LIMIT = 5000


# -----------------------------------------------------------
# S3 HELPER
# -----------------------------------------------------------
def load_geojson_from_s3(key: str) -> dict | None:
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
def parse_basic_query(q: str):
    """
    Parse basic location intent:
      - state_abbr: from full name ("ohio") or abbreviation ("oh")
      - city: first word after "in"/"near"/"at" (rough, but works well)
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

    # City: naive grab after "in / near / at"
    city = None
    for i, w in enumerate(tokens):
        if w in ("in", "near", "at", "around"):
            if i + 1 < len(tokens):
                city = tokens[i + 1]
                break

    print(f"[DEBUG] parse_basic_query → state={state_abbr}, city={city}")
    return state_abbr, city


def _parse_number_token(tok: str) -> float | None:
    """
    Convert things like "300k", "1,200,000", "$500k" → float
    """
    if not tok:
        return None
    # Strip $ and commas
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


def parse_numeric_filters(q: str):
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


def _get_numeric_from_props(props: dict, keys: list[str]) -> float | None:
    for k in keys:
        if k in props and props[k] not in (None, ""):
            v = _to_float(props[k])
            if v is not None:
                return v
    return None


def filter_features(
    features,
    city: str | None = None,
    min_home_value: float | None = None,
    min_income: float | None = None,
    limit: int = DISPLAY_LIMIT,
    for_export: bool = False,
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

        # Income filter: look for a few possible keys
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
            # Simplified row for on-screen table
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
    # Just re-render a clean page
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

    state_abbr, city = parse_basic_query(query)
    min_home_value, min_income = parse_numeric_filters(query)

    if not state_abbr:
        return render_template(
            "index.html",
            error="Couldn't detect a state. Try 'addresses in strongsville ohio'."
        )

    # For now, this demo is wired specifically to Cuyahoga County, OH.
    if state_abbr != "OH":
        return render_template(
            "index.html",
            error="Right now this demo is wired to Cuyahoga County, Ohio only. Other states are coming soon."
        )

    dataset_key = OH_CUYAHOGA_DATASET

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
            error="No matching addresses found. Try 'addresses in strongsville ohio' or 'addresses in westlake ohio'."
        )

    # Store just enough meta to re-run the query for export later
    session["last_query_meta"] = {
        "raw_query": query,
        "state_abbr": state_abbr,
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
        # No prior search
        return render_template(
            "index.html",
            error="No results to export. Run a search first."
        )

    dataset_key = meta.get("dataset_key") or OH_CUYAHOGA_DATASET
    state_abbr = meta.get("state_abbr")
    city = meta.get("city")
    min_home_value = meta.get("min_home_value")
    min_income = meta.get("min_income")

    data = load_geojson_from_s3(dataset_key)
    if not data or "features" not in data:
        return render_template(
            "index.html",
            error="Could not reload data for export. Please try your search again."
        )

    # Re-run filter specifically for export (could be same limit or slightly higher)
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

    # ---- Build CSV ----
    # 1) Collect all property keys used in this result set
    all_prop_keys = set()
    for item in export_features:
        props = item.get("properties", {}) or {}
        all_prop_keys.update(props.keys())

    # Base columns with friendly names
    base_prop_to_col = {
        "number": "Address Number",
        "street": "Street",
        "unit": "Unit",
        "city": "City",
        "region": "State",
        "postcode": "ZIP",
    }

    # Commercial-friendly aliases for important numeric fields
    alias_definitions = [
        ("County", ["county_name", "NAME", "NAMELSADCO"]),
        ("Census Tract", ["GEOID", "GEOIDFQ", "TRACTCE"]),
        ("Land Area (sq m)", ["ALAND"]),
        ("Water Area (sq m)", ["AWATER"]),
        ("Household Income (USD)", ["income", "median_income", "household_income", "acs_income"]),
        ("Home Value (USD)", ["home_value", "median_home_value", "value", "acs_home_value"]),
    ]

    # Which raw keys are already "consumed" by base + alias, so we don't duplicate them
    consumed_keys = set(base_prop_to_col.keys())
    for _, key_list in alias_definitions:
        consumed_keys.update(key_list)

    # Remaining raw property fields to expose (technical but complete)
    extra_prop_keys = sorted(all_prop_keys - consumed_keys)

    # Build CSV header
    fieldnames = list(base_prop_to_col.values())  # address columns
    alias_names = [alias_name for alias_name, _ in alias_definitions]
    fieldnames.extend(alias_names)
    fieldnames.extend(["Latitude", "Longitude"])
    fieldnames.extend(extra_prop_keys)  # all remaining raw fields

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()

    for item in export_features:
        props = item.get("properties", {}) or {}
        geom = item.get("geometry", {}) or {}
        row = {}

        # Base columns
        for raw_key, col_name in base_prop_to_col.items():
            row[col_name] = props.get(raw_key, "")

        # Alias columns
        for alias_name, key_candidates in alias_definitions:
            value = ""
            for k in key_candidates:
                if k in props and props[k] not in (None, ""):
                    value = props[k]
                    break
            row[alias_name] = value

        # Coordinates (assuming Point geometry)
        lat = ""
        lon = ""
        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            lon = coords[0]
            lat = coords[1]
        row["Latitude"] = lat
        row["Longitude"] = lon

        # Extra raw properties
        for k in extra_prop_keys:
            row[k] = props.get(k, "")

        writer.writerow(row)

    # ---- Zip it ----
    csv_bytes = csv_buffer.getvalue().encode("utf-8")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Simple file names
        city_part = (city or "results").replace(" ", "_").lower()
        csv_name = f"{city_part}_ohio_results.csv"
        zf.writestr(csv_name, csv_bytes)

    zip_buffer.seek(0)
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    zip_name = f"pelee_addresses_{city_part}_{timestamp}.zip"

    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name=zip_name,
    )


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
if __name__ == "__main__":
    # Local dev only; Render runs gunicorn.
    app.run(host="0.0.0.0", port=5000, debug=True)

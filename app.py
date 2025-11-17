import os
import json
import io
import csv
import zipfile
from typing import Optional, Tuple, Dict, Any, List

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
from openai import OpenAI

# -----------------------------------------------------------
# ENV & CLIENTS
# -----------------------------------------------------------
load_dotenv()

app = Flask(__name__)

# Flask session secret + password (matching Render env vars)
app.secret_key = os.getenv("SECRET_KEY", "dev_secret")
PASSWORD = os.getenv("ACCESS_PASSWORD", "CaLuna")

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION,
)

# OpenAI client (for smarter parsing later if we want to expand)
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# -----------------------------------------------------------
# STATE NAME MAP (50 states)
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


# -----------------------------------------------------------
# UTILITIES
# -----------------------------------------------------------
def slugify(text: str) -> str:
    """
    Convert 'Cuyahoga County' -> 'cuyahoga' etc,
    to match your S3 naming like: cuyahoga-with-values-income.geojson
    """
    if not text:
        return ""
    t = text.strip().lower()
    # strip the word "county" or "parish" if present
    for suffix in (" county", " parish", " borough", " city"):
        if t.endswith(suffix):
            t = t[: -len(suffix)]
    # spaces, commas, etc. → underscores
    cleaned = []
    for ch in t:
        if ch.isalnum():
            cleaned.append(ch)
        elif ch in (" ", "-", "/", ",", "'"):
            cleaned.append("_")
        # else drop weird chars
    slug = "".join(cleaned)
    # collapse multiple underscores
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug.strip("_")


def parse_basic_query(raw: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Very basic parser as a fallback:
    - detects spelled-out state name like "ohio"
    - city = word after 'in' / 'near' / 'at'
    """
    q = (raw or "").strip()
    q_lower = q.lower()

    # State detection
    state_abbr = None
    for name, abbr in STATE_NAMES.items():
        if name in q_lower:
            state_abbr = abbr
            break

    # City detection
    words = q_lower.replace(",", "").split()
    city = None
    for i, w in enumerate(words):
        if w in ("in", "near", "at"):
            if i + 1 < len(words):
                city = words[i + 1]
                break

    print(f"[DEBUG] parse_basic_query → state={state_abbr}, city={city}")
    return state_abbr, city


def parse_numeric_filters(raw: str) -> Dict[str, Optional[int]]:
    """
    Best-effort numeric extraction for things like:
    - "over 500k"
    - "above 250,000"
    - "income over 150k"
    - "values over 800k"

    Returns:
      {
        "min_home_value": int | None,
        "min_income": int | None
      }
    """
    q = (raw or "").lower()
    numbers: List[int] = []

    # crude scan for integers in the string
    current = []
    for ch in q:
        if ch.isdigit():
            current.append(ch)
        else:
            if current:
                num = int("".join(current))
                numbers.append(num)
                current = []
    if current:
        num = int("".join(current))
        numbers.append(num)

    if not numbers:
        return {"min_home_value": None, "min_income": None}

    # If they use "k" notation or "m" near numbers, scale accordingly
    def scale(val: int, after_index: int) -> int:
        if after_index < len(q):
            window = q[after_index : after_index + 6]  # look ahead
            if "k" in window:
                return val * 1000
            if "m" in window:
                return val * 1_000_000
        return val

    # We'll use:
    # - first number for home value if context mentions "value", "home", "price"
    # - second number for income if context mentions "income", "household"
    min_home_value = None
    min_income = None

    if numbers:
        # find where first number appears in text
        first_str = str(numbers[0])
        idx = q.find(first_str)
        scaled_first = scale(numbers[0], idx + len(first_str))

        if any(k in q for k in ["value", "home", "price", "worth"]):
            min_home_value = scaled_first
        elif any(k in q for k in ["income", "household"]):
            min_income = scaled_first
        else:
            # default: treat first as value
            min_home_value = scaled_first

    if len(numbers) > 1:
        second_str = str(numbers[1])
        idx2 = q.find(second_str)
        scaled_second = scale(numbers[1], idx2 + len(second_str))
        # If we didn't already assign income, use the second as income
        if min_income is None:
            min_income = scaled_second

    print(f"[DEBUG] parse_numeric_filters → min_home_value={min_home_value}, min_income={min_income}")
    return {"min_home_value": min_home_value, "min_income": min_income}


def normalize_state_abbr(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    val = val.strip()
    if len(val) == 2:
        return val.upper()
    v_lower = val.lower()
    return STATE_NAMES.get(v_lower)


# -----------------------------------------------------------
# DATASET SELECTION
# -----------------------------------------------------------
def choose_dataset_for_state_county(
    state_abbr: Optional[str],
    county_name: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Build the S3 key for a single county dataset, like:
      merged_with_tracts/oh/cuyahoga-with-values-income.geojson

    If direct guess fails, we try listing objects in that state's prefix
    and pick the first one that contains the county slug in its key.
    """
    if not state_abbr:
        return None, "Couldn't detect a state. Try 'addresses in strongsville ohio'."

    state_abbr = state_abbr.upper()
    state_code = state_abbr.lower()

    if not county_name:
        return None, (
            f"Couldn't detect a county for {state_abbr}. "
            "Try including the county name, e.g. 'in harrison county ohio'."
        )

    county_slug = slugify(county_name)
    if not county_slug:
        return None, (
            f"Couldn't interpret the county name '{county_name}'. "
            "Try phrasing it like 'in cuyahoga county ohio'."
        )

    # 1) Direct guess
    guess_key = f"merged_with_tracts/{state_code}/{county_slug}-with-values-income.geojson"
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=guess_key)
        print(f"[DEBUG] choose_dataset → using direct key: {guess_key}")
        return guess_key, None
    except Exception:
        print(f"[DEBUG] Direct key not found: {guess_key}")

    # 2) Fallback: scan that state's prefix and find any key containing the slug
    prefix = f"merged_with_tracts/{state_code}/"
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if county_slug in key:
                    print(f"[DEBUG] choose_dataset → fallback key match: {key}")
                    return key, None
    except Exception as e:
        print(f"[ERROR] listing S3 objects for prefix={prefix}: {e}")
        return None, f"Error listing datasets for state {state_abbr}."

    return None, (
        f"No matching dataset found for {county_name} County, {state_abbr}. "
        "Check that this county is loaded in S3."
    )


def load_geojson_from_s3(key: str) -> Optional[Dict[str, Any]]:
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
# FILTERING
# -----------------------------------------------------------
def passes_numeric_filters(
    props: Dict[str, Any],
    min_home_value: Optional[int],
    min_income: Optional[int],
) -> bool:
    """
    Try to apply numeric filters best-effort, using the fields
    that are likely to contain value/income if present.
    If a field is missing, we don't filter on it.
    """
    # Home value-like fields
    if min_home_value is not None:
        home_val = None
        for key in [
            "home_value",
            "median_home_value",
            "MEDIAN_HOME_VALUE",
            "VALUE",
            "VAL_MED",
        ]:
            if key in props and isinstance(props[key], (int, float)):
                home_val = props[key]
                break
        if home_val is not None and home_val < min_home_value:
            return False

    # Income-like fields
    if min_income is not None:
        income_val = None
        for key in [
            "income",
            "household_income",
            "median_income",
            "MEDIAN_INCOME",
            "median_household_income",
        ]:
            if key in props and isinstance(props[key], (int, float)):
                income_val = props[key]
                break
        if income_val is not None and income_val < min_income:
            return False

    return True


def filter_features_for_display(
    features: List[Dict[str, Any]],
    city: Optional[str] = None,
    min_home_value: Optional[int] = None,
    min_income: Optional[int] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """
    Filter for on-screen results:
    - filter by city (if provided)
    - apply numeric filters best-effort (if fields exist)
    - cap at 'limit' to keep the response snappy
    """
    results = []
    city_norm = city.lower() if city else None

    for f in features:
        props = f.get("properties", {})

        # City filter
        if city_norm:
            prop_city = str(props.get("city", "")).lower()
            if prop_city != city_norm:
                continue

        # Numeric filters
        if not passes_numeric_filters(props, min_home_value, min_income):
            continue

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

    print(f"[DEBUG] filter_features_for_display → returned {len(results)} rows")
    return results


def filter_features_for_export(
    features: List[Dict[str, Any]],
    city: Optional[str] = None,
    min_home_value: Optional[int] = None,
    min_income: Optional[int] = None,
    max_export: int = 50000,
) -> List[Dict[str, Any]]:
    """
    Build a list of full property dicts for CSV export.
    Includes ALL available columns in the file.
    """
    rows: List[Dict[str, Any]] = []
    city_norm = city.lower() if city else None

    for f in features:
        props = f.get("properties", {})
        # City
        if city_norm:
            prop_city = str(props.get("city", "")).lower()
            if prop_city != city_norm:
                continue

        if not passes_numeric_filters(props, min_home_value, min_income):
            continue

        # Make a copy so we can ensure 'unit' exists and not modify original
        row = dict(props)
        if "unit" not in row:
            row["unit"] = props.get("unit", "")

        # Optionally, you could add a friendly full_address column:
        number = str(row.get("number", "") or "").strip()
        street = str(row.get("street", "") or "").strip()
        unit = str(row.get("unit", "") or "").strip()
        city_val = str(row.get("city", "") or "").strip()
        region_val = str(row.get("region", "") or "").strip()
        postcode_val = str(row.get("postcode", "") or "").strip()

        addr_parts = [part for part in [number, street] if part]
        addr = " ".join(addr_parts)
        if unit:
            addr += f", Unit {unit}"
        locality = ", ".join(
            [p for p in [city_val, region_val] if p]
        )
        if postcode_val:
            if locality:
                locality += f" {postcode_val}"
            else:
                locality = postcode_val

        row["full_address"] = ", ".join(
            [p for p in [addr, locality] if p]
        )

        rows.append(row)
        if len(rows) >= max_export:
            break

    print(f"[DEBUG] filter_features_for_export → {len(rows)} rows for CSV")
    return rows


# -----------------------------------------------------------
# AUTH
# -----------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "").strip()
        if pw == PASSWORD:
            session["logged_in"] = True
            # clear any prior search when logging in
            session.pop("last_search", None)
            return redirect("/")
        return render_template("index.html", error="Incorrect password.")
    return render_template("login.html")


# -----------------------------------------------------------
# HOME
# -----------------------------------------------------------
@app.route("/")
def home():
    if not session.get("logged_in"):
        return redirect("/login")
    return render_template("index.html")


@app.route("/clear")
def clear():
    """Clear last search + results / errors."""
    if not session.get("logged_in"):
        return redirect("/login")
    session.pop("last_search", None)
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

    # Basic parse for state & city
    state_abbr_basic, city_basic = parse_basic_query(query)
    numeric_filters = parse_numeric_filters(query)
    min_home_value = numeric_filters["min_home_value"]
    min_income = numeric_filters["min_income"]

    # For now, we'll rely on the user including the county in free text
    # (e.g. "in cuyahoga county ohio"). We try to extract "X county" if present.
    q_lower = query.lower()
    county_name = None
    if " county" in q_lower:
        # take the word immediately before "county"
        words = q_lower.replace(",", "").split()
        for i, w in enumerate(words):
            if w == "county" and i > 0:
                county_name = " ".join(words[i - 1: i + 1])  # "cuyahoga county"
                break

    # If they explicitly say e.g. "cuyahoga" without 'county', we won't catch that yet,
    # but we can grow this later. For now, keep it simple & robust.

    state_abbr = state_abbr_basic
    city = city_basic

    if not state_abbr:
        return render_template(
            "index.html",
            error="Couldn't detect a state. Try 'addresses in strongsville ohio'.",
        )

    dataset_key, dataset_error = choose_dataset_for_state_county(state_abbr, county_name)
    if not dataset_key:
        return render_template("index.html", error=dataset_error)

    data = load_geojson_from_s3(dataset_key)
    if not data or "features" not in data:
        return render_template(
            "index.html",
            error=f"Failed to load dataset for {state_abbr} (key={dataset_key}).",
        )

    features = data["features"]
    results = filter_features_for_display(
        features,
        city=city,
        min_home_value=min_home_value,
        min_income=min_income,
        limit=500,
    )

    if not results:
        return render_template(
            "index.html",
            error="No matching addresses found. Try another city or adjust your filters.",
        )

    # Store the last search *parameters* so export can re-run the same filter
    session["last_search"] = {
        "query": query,
        "state_abbr": state_abbr,
        "county_name": county_name,
        "city": city,
        "dataset_key": dataset_key,
        "min_home_value": min_home_value,
        "min_income": min_income,
    }

    return render_template("index.html", results=results)


# -----------------------------------------------------------
# EXPORT
# -----------------------------------------------------------
@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    last = session.get("last_search")
    if not last:
        # No prior search to export from
        return render_template(
            "index.html",
            error="No results to export. Run a search first.",
        )

    dataset_key = last.get("dataset_key")
    if not dataset_key:
        return render_template(
            "index.html",
            error="Export error: missing dataset information for your last search.",
        )

    data = load_geojson_from_s3(dataset_key)
    if not data or "features" not in data:
        return render_template(
            "index.html",
            error="Export error: couldn't reload the data file from storage.",
        )

    features = data["features"]
    city = last.get("city")
    min_home_value = last.get("min_home_value")
    min_income = last.get("min_income")

    rows = filter_features_for_export(
        features,
        city=city,
        min_home_value=min_home_value,
        min_income=min_income,
        max_export=50000,
    )
    if not rows:
        return render_template(
            "index.html",
            error="No matching addresses to export for your last search.",
        )

    # Build union of keys across all rows
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())

    # Prioritize commercial-feeling core columns
    priority = [
        "full_address",
        "number",
        "street",
        "unit",
        "city",
        "region",
        "postcode",
    ]
    ordered_keys = [k for k in priority if k in all_keys] + sorted(all_keys - set(priority))

    # Create CSV in memory, then zip it
    buf = io.BytesIO()

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=ordered_keys)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in ordered_keys})
        zf.writestr("addresses.csv", csv_buffer.getvalue())

    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name="addresses_export.zip",
    )


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
if __name__ == "__main__":
    # Local dev only; Render runs gunicorn.
    app.run(host="0.0.0.0", port=5000, debug=True)

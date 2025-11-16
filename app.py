import os
import json
import csv
import io
import difflib

import boto3
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    session,
    jsonify,
    send_file,
)
from botocore.config import Config
from openai import OpenAI

# ----------------------------
# CONFIG
# ----------------------------
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY env var is required")

client = OpenAI(api_key=OPENAI_API_KEY)

s3 = boto3.client(
    "s3",
    region_name="us-east-2",
    config=Config(signature_version="s3v4"),
)

BUCKET = "residential-data-jack"
PASSWORD = "CaLuna"  # simple gate

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-key")

loading_state = {"active": False}


# ----------------------------
# LOGIN SYSTEM
# ----------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        else:
            return render_template("login.html", error="Incorrect password")

    return render_template("login.html")


@app.before_request
def require_login():
    # Let login, static files, and health checks through
    allowed = {"login", "static", "health"}
    if request.endpoint in allowed or request.endpoint is None:
        return

    if not session.get("logged_in"):
        return redirect("/login")


@app.route("/health")
def health():
    return jsonify({"ok": True})


# ----------------------------
# GPT HELPERS
# ----------------------------
def parse_query(prompt: str) -> dict:
    """
    Use GPT to convert user natural-language query into structured filters.
    Expected keys (all optional): city, state, county, street,
    min_income, max_income, min_value, max_value.
    """
    system_msg = (
        "You convert user real-estate targeting prompts into strict JSON filters.\n"
        "Respond with JSON ONLY, no explanation.\n"
        "Allowed keys: city, state, county, street, min_income, max_income, "
        "min_value, max_value.\n"
        "- state must be a 2-letter code (e.g., OH, FL, AZ).\n"
        "- If a city is mentioned, include city and state.\n"
        "- If only a county is mentioned, include county and state.\n"
        "- Income/value ranges go to min_/max_ fields.\n"
        "Example:\n"
        'User: "residential addresses in Strongsville OH with incomes over 300k"\n'
        'You: {"city":"Strongsville","state":"OH","min_income":300000}\n'
    )

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": prompt},
        ],
    )

    raw = resp.choices[0].message.content
    try:
        data = json.loads(raw)
    except Exception:
        return {}

    # Coerce numeric fields
    for key in ["min_income", "max_income", "min_value", "max_value"]:
        if key in data and data[key] is not None:
            try:
                data[key] = int(data[key])
            except Exception:
                data.pop(key, None)

    # Strip whitespace on strings
    for key in ["city", "state", "county", "street"]:
        if key in data and isinstance(data[key], str):
            data[key] = data[key].strip()

    return data


def lookup_county(city: str, state: str) -> str | None:
    """
    Ask GPT for county name given city + state.
    Returns plain-text county name or None.
    """
    prompt = f"Return ONLY the county name (no suffix like 'County') for: {city}, {state}"

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": "Reply with county name ONLY, no extra text."},
            {"role": "user", "content": prompt},
        ],
    )
    text = resp.choices[0].message.content.strip()
    # Strip the word "County" if GPT includes it
    text = text.replace("County", "").strip()
    return text or None


# ----------------------------
# S3 / DATA HELPERS
# ----------------------------
def load_county_file(state: str, county_name: str) -> dict | None:
    """
    Given state (2-letter) and county name, try multiple key patterns.
    Returns loaded GeoJSON dict or None if nothing found.
    """
    if not state or not county_name:
        return None

    state = state.lower()
    county_snake = county_name.lower().replace(" ", "_")

    candidates = [
        # Enriched address-level data per state
        f"{state}/{county_snake}-with-values-income.geojson",
        f"{state}/{county_snake}.geojson",
        # Merged-with-tracts patterns (addresses then parcels)
        f"merged_with_tracts/{state}/{county_snake}-addresses-county-with-tracts.geojson",
        f"merged_with_tracts/{state}/{county_snake}-parcels-county-with-tracts.geojson",
        # Fallback plain merged county
        f"merged_with_tracts/{county_snake}-with-values-income.geojson",
        f"merged_with_tracts/{county_snake}-addresses-county-with-tracts.geojson",
    ]

    for key in candidates:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(obj["Body"].read())
            # Keep track of which key we used (for debugging/export if needed)
            data["_pelee_source_key"] = key
            return data
        except Exception:
            continue

    return None


def normalize_city_name(name: str) -> str:
    """
    Normalize city strings so 'Berea', 'Berea City', 'BEREA', etc. line up.
    """
    if not name:
        return ""
    s = name.upper().strip()

    # Strip common suffixes
    suffixes = [
        " CITY",
        " VILLAGE",
        " TOWNSHIP",
        " TWP",
        " BOROUGH",
        " BORO",
    ]
    for suf in suffixes:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()

    # Remove dots, double spaces, etc.
    s = s.replace(".", " ")
    s = " ".join(s.split())
    return s


def resolve_city_against_dataset(requested_city: str, features: list[dict]) -> str | None:
    """
    Given a requested city name and a dataset, return the *canonical* city
    string actually used in the data (so equality matches will work).
    """
    if not requested_city:
        return None

    target_norm = normalize_city_name(requested_city)

    unique_cities = {}
    for feat in features:
        props = feat.get("properties", {})
        c = props.get("city") or props.get("CITY")
        if not c:
            continue
        if c not in unique_cities:
            unique_cities[c] = normalize_city_name(c)

    if not unique_cities:
        return None

    # 1) Exact normalized match
    for raw, norm in unique_cities.items():
        if norm == target_norm:
            return raw

    # 2) Fuzzy match on norm
    norm_values = list(set(unique_cities.values()))
    best = difflib.get_close_matches(target_norm, norm_values, n=1, cutoff=0.7)
    if best:
        best_norm = best[0]
        for raw, norm in unique_cities.items():
            if norm == best_norm:
                return raw

    # 3) Substring fallback
    for raw, norm in unique_cities.items():
        if target_norm in norm or norm in target_norm:
            return raw

    return None


def feature_matches(feature: dict, filters: dict) -> bool:
    """
    Apply filter dict to a single GeoJSON feature.
    Expects filters to potentially contain:
    - city_resolved
    - street
    - min_income, max_income
    - min_value, max_value
    """
    props = feature.get("properties", {})

    # City
    city_resolved = filters.get("city_resolved")
    if city_resolved:
        feat_city = props.get("city") or props.get("CITY") or ""
        if feat_city != city_resolved:
            return False

    # Street partial match
    street = filters.get("street")
    if street:
        if street.lower() not in (props.get("street") or "").lower():
            return False

    # Income
    income = props.get("DP03_0062E")
    try:
        income_val = int(income) if income is not None else None
    except Exception:
        income_val = None

    if filters.get("min_income") is not None and income_val is not None:
        if income_val < filters["min_income"]:
            return False

    if filters.get("max_income") is not None and income_val is not None:
        if income_val > filters["max_income"]:
            return False

    # Home value
    home_val = props.get("DP04_0089E")
    try:
        home_val_num = int(home_val) if home_val is not None else None
    except Exception:
        home_val_num = None

    if filters.get("min_value") is not None and home_val_num is not None:
        if home_val_num < filters["min_value"]:
            return False

    if filters.get("max_value") is not None and home_val_num is not None:
        if home_val_num > filters["max_value"]:
            return False

    return True


def format_feature_for_chat(feature: dict) -> dict:
    """
    Return a slim address row for the chat UI.
    """
    props = feature.get("properties", {})
    geom = feature.get("geometry", {})
    coords = geom.get("coordinates") or [None, None]
    lon, lat = (coords + [None, None])[:2]

    number = props.get("number") or ""
    street = props.get("street") or ""
    unit = props.get("unit") or ""
    addr = " ".join([number, street, unit]).strip()

    return {
        "address": addr,
        "city": props.get("city") or "",
        "state": props.get("region") or props.get("STUSPS") or "",
        "zip": props.get("postcode") or "",
        "geoid": props.get("GEOID") or "",
        "income": props.get("DP03_0062E"),
        "home_value": props.get("DP04_0089E"),
        "lat": lat,
        "lon": lon,
    }


# ----------------------------
# ROUTES
# ----------------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/loading")
def loading():
    return jsonify(loading_state)


@app.route("/chat", methods=["POST"])
def chat():
    """
    JSON endpoint used by the Pelée Data Chat front-end.
    Input: { "query": "<user text>" }
    Output: { ok, error?, count, results[] }
    """
    global loading_state
    loading_state["active"] = True

    try:
        data = request.get_json(force=True, silent=True) or {}
        query = (data.get("query") or "").strip()
        if not query:
            loading_state["active"] = False
            return jsonify({"ok": False, "error": "Empty query"}), 400

        # 1) Parse user query into filters
        filters = parse_query(query)
        if not filters:
            loading_state["active"] = False
            return jsonify(
                {
                    "ok": False,
                    "error": "I couldn’t understand that. Try including city + state or county + state.",
                }
            )

        state = filters.get("state")
        city = filters.get("city")
        county = filters.get("county")

        if not state:
            loading_state["active"] = False
            return jsonify(
                {
                    "ok": False,
                    "error": "Please include a state (e.g., OH, FL, WA).",
                }
            )

        # 2) If city + state but no county, ask GPT for county
        if city and not county:
            county = lookup_county(city, state)
            if not county:
                loading_state["active"] = False
                return jsonify(
                    {
                        "ok": False,
                        "error": f"Couldn’t determine county for {city}, {state}.",
                    }
                )
            filters["county"] = county

        if not county:
            loading_state["active"] = False
            return jsonify(
                {
                    "ok": False,
                    "error": "Please mention a city+state or county+state.",
                }
            )

        # 3) Load dataset from S3
        dataset = load_county_file(state, county)
        if not dataset:
            loading_state["active"] = False
            return jsonify(
                {
                    "ok": False,
                    "error": f"Dataset not found for {county} County, {state}.",
                }
            )

        features = dataset.get("features", [])

        # 4) Resolve city name against actual dataset values
        resolved_city = None
        if city:
            resolved_city = resolve_city_against_dataset(city, features)
            if resolved_city:
                filters["city_resolved"] = resolved_city

        # 5) Filter features
        results = []
        total_found = 0
        MAX_RESULTS = 500  # hard cap

        for feat in features:
            if feature_matches(feat, filters):
                total_found += 1
                if len(results) < MAX_RESULTS:
                    results.append(format_feature_for_chat(feat))

        # Save last search in session for Export
        session["last_search"] = {
            "state": state,
            "county": county,
            "filters": filters,
            "resolved_city": resolved_city,
        }

        loading_state["active"] = False

        return jsonify(
            {
                "ok": True,
                "state": state,
                "county": county,
                "resolved_city": resolved_city,
                "count": total_found,
                "results": results,
            }
        )
    except Exception as e:
        loading_state["active"] = False
        return jsonify({"ok": False, "error": f"Server error: {e}"}), 500


@app.route("/export", methods=["GET"])
def export():
    """
    Re-run the last search stored in session and stream a CSV.
    """
    info = session.get("last_search")
    if not info:
        return "No search to export.", 400

    state = info.get("state")
    county = info.get("county")
    filters = info.get("filters") or {}
    resolved_city = info.get("resolved_city")
    if resolved_city:
        filters["city_resolved"] = resolved_city

    dataset = load_county_file(state, county)
    if not dataset:
        return "Dataset not found for last search.", 404

    features = dataset.get("features", [])

    # Build CSV in-memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "number",
            "street",
            "unit",
            "city",
            "state",
            "zip",
            "GEOID",
            "income_DP03_0062E",
            "home_value_DP04_0089E",
            "lat",
            "lon",
        ]
    )

    rows_written = 0
    MAX_EXPORT = 50000

    for feat in features:
        if not feature_matches(feat, filters):
            continue

        props = feat.get("properties", {})
        geom = feat.get("geometry", {})
        coords = geom.get("coordinates") or [None, None]
        lon, lat = (coords + [None, None])[:2]

        writer.writerow(
            [
                props.get("number") or "",
                props.get("street") or "",
                props.get("unit") or "",
                props.get("city") or "",
                props.get("region") or props.get("STUSPS") or "",
                props.get("postcode") or "",
                props.get("GEOID") or "",
                props.get("DP03_0062E") or "",
                props.get("DP04_0089E") or "",
                lat,
                lon,
            ]
        )

        rows_written += 1
        if rows_written >= MAX_EXPORT:
            break

    output.seek(0)
    csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))

    filename = f"pelee_export_{state}_{county}.csv".replace(" ", "_")
    return send_file(
        csv_bytes,
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    # Local dev
    app.run(host="0.0.0.0", port=5000, debug=True)

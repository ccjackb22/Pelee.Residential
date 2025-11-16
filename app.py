import os
import json
import boto3
from flask import Flask, render_template, request, redirect, session, jsonify
from botocore.config import Config
from openai import OpenAI

# ----------------------------
# CONFIG
# ----------------------------
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

s3 = boto3.client(
    "s3",
    region_name="us-east-2",
    config=Config(signature_version="s3v4")
)

BUCKET = "residential-data-jack"
PASSWORD = "CaLuna"

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-password")

loading_state = {"active": False}


# ----------------------------
# STATE / COUNTY NORMALIZATION
# ----------------------------

STATE_NAME_TO_ABBR = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "district of columbia": "dc",
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


def normalize_state(state_raw):
    """Turn 'Ohio' or 'OH' into 'oh' (for S3 paths)."""
    if not state_raw:
        return None
    s = state_raw.strip()
    if not s:
        return None

    # If it's already a 2-letter code
    if len(s) == 2:
        return s.lower()

    # Try full-name map
    s_lower = s.lower()
    if s_lower in STATE_NAME_TO_ABBR:
        return STATE_NAME_TO_ABBR[s_lower]

    # Fallback – just lowercase
    return s_lower


def normalize_county(county_raw):
    """Turn 'Cuyahoga County' → 'cuyahoga', 'Pinellas County' → 'pinellas'."""
    if not county_raw:
        return None
    c = county_raw.strip().lower()

    # Strip common suffixes
    suffixes = [
        " county",
        " parish",
        " borough",
        " census area",
        " city and borough",
        " municipality",
    ]
    for suf in suffixes:
        if c.endswith(suf):
            c = c[: -len(suf)]
            break

    # Strip extra spaces and replace spaces with underscores for S3
    c = "_".join(c.split())
    return c


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
    # Don't block login/static
    if request.endpoint in ("login", "static"):
        return
    # Some internal endpoints might be None
    if request.endpoint is None:
        return
    if not session.get("logged_in"):
        return redirect("/login")


# ----------------------------
# GPT QUERY PARSER
# ----------------------------

def parse_query(prompt: str) -> dict:
    """
    Ask GPT to convert natural language into a JSON filter:
    {city, state, county, min_income, max_income, min_value, max_value, street}
    """
    system_msg = (
        "Convert user real-estate queries into structured filters.\n"
        "Return JSON ONLY, no prose.\n"
        "Allowed keys: city, state, county, min_income, max_income, "
        "min_value, max_value, street.\n"
        "If a city is mentioned, include both city and state.\n"
        "If only a county is mentioned, include county and state.\n"
        "Use two-letter state abbreviations where possible.\n"
        "Example output: "
        '{"city": "Strongsville", "state": "OH", "min_income": 300000}\n'
        "Never wrap JSON in backticks or code fences."
    )

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": prompt},
        ],
    )

    content = resp.choices[0].message.content.strip()
    print("RAW GPT PARSE:", content)

    # Try direct JSON
    try:
        return json.loads(content)
    except Exception:
        pass

    # Try to extract the JSON object between { ... }
    try:
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            snippet = content[start : end + 1]
            return json.loads(snippet)
    except Exception:
        pass

    return {}


# ----------------------------
# S3 LOADER
# ----------------------------

def load_county_file(state_raw, county_raw):
    """
    Use normalized state+county to build the S3 key and load the GeoJSON.
    We assume keys like:
      merged_with_tracts/oh/cuyahoga-with-values-income.geojson
    """
    state = normalize_state(state_raw)
    county_slug = normalize_county(county_raw)

    print("RESOLVED STATE:", state_raw, "->", state)
    print("RESOLVED COUNTY:", county_raw, "->", county_slug)

    if not state or not county_slug:
        return None

    candidates = [
        f"merged_with_tracts/{state}/{county_slug}-with-values-income.geojson",
        f"merged_with_tracts/{state}/{county_slug}.geojson",
        # Extra fallback in case some counties are not nested by state:
        f"merged_with_tracts/{county_slug}-with-values-income.geojson",
        f"merged_with_tracts/{county_slug}.geojson",
    ]

    for key in candidates:
        try:
            print("TRYING S3 KEY:", key)
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            data = json.loads(body)
            print("LOADED S3 KEY:", key, "FEATURES:", len(data.get("features", [])))
            return data
        except Exception as e:
            print("S3 MISS FOR KEY:", key, "ERROR:", e)

    return None


# ----------------------------
# FILTER LOGIC
# ----------------------------

def matches(feature: dict, f: dict) -> bool:
    prop = feature.get("properties", {})

    # Street substring match
    if f.get("street"):
        if f["street"].lower() not in prop.get("street", "").lower():
            return False

    # City exact (case-insensitive)
    if f.get("city"):
        if f["city"].lower() != prop.get("city", "").lower():
            return False

    # Income filter
    if f.get("min_income") and prop.get("DP03_0062E") is not None:
        try:
            if float(prop["DP03_0062E"]) < float(f["min_income"]):
                return False
        except Exception:
            pass

    # Home value filter
    if f.get("min_value") and prop.get("DP04_0089E") is not None:
        try:
            if float(prop["DP04_0089E"]) < float(f["min_value"]):
                return False
        except Exception:
            pass

    return True


# ----------------------------
# ROUTES
# ----------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/loading")
def loading():
    return jsonify(loading_state)


@app.route("/export")
def export():
    # Placeholder – just to avoid 404 when you click "Export"
    return "Export not implemented yet.", 501


@app.route("/search", methods=["POST"])
def search():
    global loading_state
    loading_state["active"] = True

    query = request.form.get("query", "").strip()
    print("USER QUERY:", query)

    if not query:
        loading_state["active"] = False
        return render_template(
            "index.html",
            results=[],
            error="Please enter a search query."
        )

    parsed = parse_query(query)
    print("PARSED FILTERS:", parsed)

    # If GPT gave city+state but no county, ask GPT for county
    if parsed.get("city") and parsed.get("state") and not parsed.get("county"):
        prompt = f"Return ONLY the county name (no word 'County') for: {parsed['city']}, {parsed['state']}"
        county_resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Reply with county name only, no extra words."},
                {"role": "user", "content": prompt},
            ],
        )
        county_name_raw = county_resp.choices[0].message.content.strip()
        print("GPT COUNTY RAW:", county_name_raw)
        parsed["county"] = county_name_raw

    state_dbg = parsed.get("state")
    county_dbg = parsed.get("county")

    if not state_dbg or not county_dbg:
        loading_state["active"] = False
        return render_template(
            "index.html",
            results=[],
            error=f"Could not determine state/county from query (got: state={state_dbg}, county={county_dbg})."
        )

    dataset = load_county_file(state_dbg, county_dbg)

    if not dataset:
        loading_state["active"] = False
        return render_template(
            "index.html",
            results=[],
            error=f"Dataset not found for {county_dbg}, {state_dbg}."
        )

    # Filter features
    results = []
    for feature in dataset.get("features", []):
        if matches(feature, parsed):
            results.append(feature["properties"])
            if len(results) >= 500:
                break

    loading_state["active"] = False
    print("RESULT COUNT:", len(results))

    return render_template("index.html", results=results)


# ----------------------------
# RUN
# ----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

import os
import json
import boto3
import re
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
    allowed = ["login", "static"]
    if request.endpoint not in allowed and not session.get("logged_in"):
        return redirect("/login")


# ----------------------------
# GPT QUERY PARSER
# ----------------------------
def parse_query(prompt):
    system_msg = (
        "Convert user real-estate queries into structured filters.\n"
        "Return JSON ONLY.\n"
        "Allowed keys: city, state, county, min_income, max_income, "
        "min_value, max_value, street.\n"
        "If city is mentioned, output city and state.\n"
        "If only county is mentioned, output county and state.\n"
        "NEVER return commentary — JSON only."
    )

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": prompt}
        ]
    )

    try:
        return json.loads(resp.choices[0].message.content)
    except:
        return {}


# ----------------------------
# COUNTY NORMALIZER
# ----------------------------
def normalize_county(county):
    if not county:
        return None

    county = county.lower().strip()

    # Remove suffixes GPT often returns
    suffixes = [
        " county",
        " parish",
        " borough",
        " census area",
        " municipality"
    ]
    for suf in suffixes:
        if county.endswith(suf):
            county = county.replace(suf, "")

    # Remove punctuation and double spaces
    county = re.sub(r"[^a-z0-9\s]", "", county)
    county = re.sub(r"\s+", " ", county)

    return county.strip().replace(" ", "_")


# ----------------------------
# S3 LOADER (VERY ROBUST)
# ----------------------------
def load_county_file(state, county_name):
    if not state or not county_name:
        return None

    state = state.lower()
    base = normalize_county(county_name)

    # Try all combinations
    possible_names = [
        base,
        base + "_county",
        base + "-county",
        base + "_parish",
        base + "_borough",
    ]

    file_variants = []
    for name in possible_names:
        file_variants.append(f"{state}/{name}-with-values-income.geojson")
        file_variants.append(f"{state}/{name}.geojson")

    # Try each until one succeeds
    for key in file_variants:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="merged_with_tracts/" + key)
            return json.loads(obj["Body"].read())
        except:
            continue

    return None


# ----------------------------
# FILTER LOGIC
# ----------------------------
def matches(record, f):
    prop = record.get("properties", {})

    # Street match
    if f.get("street"):
        if f["street"].lower() not in prop.get("street", "").lower():
            return False

    # City match
    if f.get("city"):
        if f["city"].lower() != prop.get("city", "").lower():
            return False

    # Income
    if f.get("min_income") and prop.get("DP03_0062E"):
        if prop["DP03_0062E"] < f["min_income"]:
            return False

    # Home value
    if f.get("min_value") and prop.get("DP04_0089E"):
        if prop["DP04_0089E"] < f["min_value"]:
            return False

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


@app.route("/search", methods=["POST"])
def search():
    global loading_state
    loading_state["active"] = True

    query = request.form.get("query", "")
    parsed = parse_query(query)

    # Get county automatically when user gives city+state
    if "city" in parsed and "state" in parsed:
        lookup = f"City: {parsed['city']}, State: {parsed['state']}\nReturn only the county name."
        county_resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Return ONLY the county name."},
                {"role": "user", "content": lookup}
            ]
        )
        parsed["county"] = county_resp.choices[0].message.content.strip()

    dataset = load_county_file(parsed.get("state"), parsed.get("county"))

    if not dataset:
        loading_state["active"] = False
        err = f"Dataset not found for {parsed.get('county')}, {parsed.get('state')}."
        return render_template("index.html", results=[], error=err)

    # Filtering
    results = []
    for feature in dataset.get("features", []):
        if matches(feature, parsed):
            results.append(feature["properties"])
            if len(results) >= 500:
                break

    loading_state["active"] = False
    return render_template("index.html", results=results)


# ----------------------------
# RUN
# ----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

import os
import json
import re
import boto3
from flask import Flask, render_template, request, redirect, session, jsonify
from botocore.config import Config
from openai import OpenAI

# ------------------------------------
# CONFIG
# ------------------------------------
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

s3 = boto3.client(
    "s3",
    region_name="us-east-2",
    config=Config(signature_version="s3v4")
)

BUCKET = "residential-data-jack"
PASSWORD = "CaLuna"

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "devsecret")

loading_state = {"active": False}


# ------------------------------------
# LOGIN SYSTEM
# ------------------------------------
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
    allowed_routes = ["login", "static"]
    if request.endpoint not in allowed_routes and not session.get("logged_in"):
        return redirect("/login")


# ------------------------------------
# GPT QUERY PARSER
# ------------------------------------
def parse_query(prompt):
    system_msg = (
        "Convert real estate queries into structured JSON filters.\n"
        "Allowed keys: city, state, county, min_income, max_income, "
        "min_value, max_value, street.\n"
        "Return JSON ONLY. No extra text."
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


# ------------------------------------
# COUNTY NORMALIZATION
# ------------------------------------
def normalize_county_name(name):
    if not name:
        return None

    # remove suffixes
    name = re.sub(r"\b(county|parish|borough)\b", "", name, flags=re.IGNORECASE)

    # remove punctuation
    name = re.sub(r"[^a-zA-Z0-9 ]+", "", name)

    # convert to snake_case
    name = "_".join(name.lower().strip().split())

    return name


# ------------------------------------
# S3 LOADER
# ------------------------------------
def load_county_file(state, raw_county):
    if not state or not raw_county:
        return None

    county = normalize_county_name(raw_county)

    key_prefix = f"{state.lower()}/{county}"

    file_options = [
        f"{key_prefix}-with-values-income.geojson",
        f"{key_prefix}.geojson",
    ]

    for k in file_options:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            return json.loads(obj["Body"].read())
        except:
            continue

    return None


# ------------------------------------
# FILTER LOGIC
# ------------------------------------
def matches(record, f):
    prop = record.get("properties", {})

    # Street
    if f.get("street"):
        if f["street"].lower() not in prop.get("street", "").lower():
            return False

    # City
    if f.get("city"):
        if f["city"].lower() != prop.get("city", "").lower():
            return False

    # Income (DP03_0062E)
    if f.get("min_income") and prop.get("DP03_0062E"):
        if prop["DP03_0062E"] < f["min_income"]:
            return False

    # Home value (DP04_0089E)
    if f.get("min_value") and prop.get("DP04_0089E"):
        if prop["DP04_0089E"] < f["min_value"]:
            return False

    return True


# ------------------------------------
# ROUTES
# ------------------------------------
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

    # If GPT gives city + state → look up county
    if "city" in parsed and "state" in parsed:
        prompt = f"Return ONLY the county name for: {parsed['city']} {parsed['state']}"
        county_resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Return county name ONLY."},
                {"role": "user", "content": prompt}
            ]
        )
        parsed["county"] = county_resp.choices[0].message.content.strip()

    dataset = load_county_file(parsed.get("state"), parsed.get("county"))

    if not dataset:
        loading_state["active"] = False
        return render_template("index.html", error=f"Dataset not found for {parsed.get('county')}, {parsed.get('state')}.")

    # Apply filters
    results = []
    for feature in dataset.get("features", []):
        if matches(feature, parsed):
            results.append(feature["properties"])
        if len(results) >= 500:
            break

    loading_state["active"] = False
    return render_template("index.html", results=results)


# ------------------------------------
# RUN
# ------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

import os
import json
import boto3
from flask import Flask, render_template, request, redirect, session, jsonify
from botocore.config import Config
from openai import OpenAI
import re

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
app.secret_key = os.environ.get("FLASK_SECRET", "dev-key")

loading_state = {"active": False}

# ----------------------------
# LOGIN
# ----------------------------
@app.before_request
def require_login():
    allowed_routes = ["login", "static"]
    if request.endpoint not in allowed_routes and not session.get("logged_in"):
        return redirect("/login")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        else:
            return render_template("login.html", error="Incorrect password")
    return render_template("login.html")


# ----------------------------
# NORMALIZATION UTIL
# ----------------------------
def normalize_county(name):
    if not name:
        return ""

    name = name.lower().strip()

    # remove trailing words
    name = re.sub(r"\b(county|parish|borough|city|census area)\b", "", name).strip()

    # replace spaces
    name = name.replace(" ", "_")

    return name


# ----------------------------
# GPT: QUERY PARSER
# ----------------------------
def parse_query(prompt):
    system_msg = (
        "Convert the user's natural language real estate request into JSON.\n"
        "Fields allowed: city, state, county, min_income, max_income, min_value, max_value, street.\n"
        "Always include state when city mentioned.\n"
        "Return JSON only."
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
# LOAD COUNTY FILE FROM S3
# ----------------------------
def load_county_file(state, county_name):
    if not state or not county_name:
        return None

    state = state.lower()
    base = normalize_county(county_name)

    # always search both:
    # cuyahoga
    # cuyahoga_county
    variants = list(set([
        base,
        base + "_county",
        base.replace("_county", ""),
        base.replace("_parish", ""),
        base.replace("_borough", ""),
    ]))

    attempts = []

    for v in variants:
        attempts.append(f"merged_with_tracts/{state}/{v}-with-values-income.geojson")
        attempts.append(f"merged_with_tracts/{state}/{v}.geojson")

    # Try S3 keys until one works
    for key in attempts:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            return json.loads(obj["Body"].read())
        except:
            continue

    return None


# ----------------------------
# FILTER
# ----------------------------
def matches(feature, f):
    prop = feature.get("properties", {})

    # street
    if f.get("street"):
        if f["street"].lower() not in prop.get("street", "").lower():
            return False

    # city
    if f.get("city"):
        if f["city"].lower() != prop.get("city", "").lower():
            return False

    # min income
    if f.get("min_income") and prop.get("DP03_0062E") is not None:
        if prop["DP03_0062E"] < f["min_income"]:
            return False

    # min value
    if f.get("min_value") and prop.get("DP04_0089E") is not None:
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

    # GPT must always return county
    if parsed.get("city") and parsed.get("state"):
        prompt = f"Return ONLY the county name for: {parsed['city']}, {parsed['state']}"
        county_resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Return county name only. No punctuation."},
                {"role": "user", "content": prompt}
            ]
        )
        parsed["county"] = county_resp.choices[0].message.content.strip()

    # load dataset
    dataset = load_county_file(parsed.get("state"), parsed.get("county"))

    if not dataset:
        loading_state["active"] = False
        return render_template(
            "index.html",
            results=None,
            error=f"Dataset not found for {parsed.get('county')}, {parsed.get('state')}."
        )

    # filter
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

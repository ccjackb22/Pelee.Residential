import os
import json
import boto3
from flask import Flask, render_template, request, jsonify, redirect, url_for, session
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

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "supersecret")

PASSWORD = "CaLuna"  # <<< YOUR PASSWORD


# Loading flag for frontend
loading_state = {"active": False}


# ----------------------------
# LOGIN REQUIRED DECORATOR
# ----------------------------
def login_required(func):
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return func(*args, **kwargs)
    wrapper.__name__ = func.__name__
    return wrapper


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
        "If only county mentioned, output county and state.\n"
        "Never return commentary — JSON only."
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
# S3 HELPER
# ----------------------------
def load_county_file(state, county_name):
    if not state or not county_name:
        return None

    key_prefix = f"{state.lower()}/{county_name.lower().replace(' ', '_')}"
    possible_keys = [
        f"{key_prefix}-with-values-income.geojson",
        f"{key_prefix}.geojson"
    ]

    for k in possible_keys:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            data = json.loads(obj["Body"].read())
            return data
        except:
            continue

    return None


# ----------------------------
# FILTER LOGIC
# ----------------------------
def matches(record, f):
    try:
        prop = record["properties"]
    except:
        return False

    # Street match
    if f.get("street"):
        if f["street"].lower() not in prop.get("street", "").lower():
            return False

    # City
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
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password")
        if pw == PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        return render_template("login.html", error="Incorrect password")
    return render_template("login.html")


@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/loading")
@login_required
def loading():
    return jsonify(loading_state)


@app.route("/search", methods=["POST"])
@login_required
def search():
    global loading_state
    loading_state["active"] = True

    query = request.form.get("query", "")
    parsed = parse_query(query)

    # Determine correct county via GPT if needed
    if "city" in parsed and "state" in parsed:
        county_lookup_prompt = (
            "Return ONLY the county name for this city.\n"
            f"City: {parsed['city']} State: {parsed['state']}"
        )
        county_resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Return county ONLY as plain text."},
                {"role": "user", "content": county_lookup_prompt}
            ]
        )
        parsed["county"] = county_resp.choices[0].message.content.strip()

    dataset = load_county_file(parsed.get("state"), parsed.get("county"))

    if not dataset:
        loading_state["active"] = False
        return render_template("index.html", results=[], error="Dataset not found")

    # Filter results
    results = []
    for feature in dataset.get("features", []):
        if matches(feature, parsed):
            results.append(feature["properties"])

        if len(results) >= 500:
            break

    loading_state["active"] = False
    return render_template("index.html", results=results)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

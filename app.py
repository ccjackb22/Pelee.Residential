import os
import json
import boto3
from flask import Flask, render_template, request, redirect, session, jsonify
from botocore.config import Config
from openai import OpenAI

# -------------------------------------------
# CONFIG
# -------------------------------------------
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

s3 = boto3.client(
    "s3",
    region_name="us-east-2",
    config=Config(signature_version="s3v4")
)

BUCKET = "residential-data-jack"
PASSWORD = "CaLuna"

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret")

loading_state = {"active": False}


# -------------------------------------------
# LOGIN SYSTEM
# -------------------------------------------
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


# -------------------------------------------
# GPT QUERY PARSER
# -------------------------------------------
def parse_query(prompt):
    system_msg = (
        "Convert user real-estate queries into structured filters.\n"
        "Allowed keys: city, state, county, min_income, max_income, "
        "min_value, max_value, street.\n"
        "Return ONLY valid JSON. No explanation."
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


# -------------------------------------------
# SMART S3 COUNTY FILE FINDER (NEW + FIXED)
# -------------------------------------------
def load_county_file(state, county):
    if not state or not county:
        return None

    state = state.lower()
    county = county.lower().replace(" ", "_")

    prefix = f"merged_with_tracts/{state}/"

    # List everything in that state
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    if "Contents" not in resp:
        return None

    candidates = []
    for obj in resp["Contents"]:
        key = obj["Key"].lower()

        # we only accept files with the county name
        if county in key and key.endswith(".geojson"):
            candidates.append(obj["Key"])

    if not candidates:
        return None

    # Prioritize best dataset
    priority = [
        "with-values-income",
        "addresses-county-with-tracts",
        "parcels-county-with-tracts",
        county
    ]

    def rank(k):
        k_low = k.lower()
        for i, p in enumerate(priority):
            if p in k_low:
                return i
        return 999

    candidates.sort(key=rank)

    best_file = candidates[0]

    obj = s3.get_object(Bucket=BUCKET, Key=best_file)
    return json.loads(obj["Body"].read())


# -------------------------------------------
# FILTER LOGIC
# -------------------------------------------
def matches(record, f):
    prop = record.get("properties", {})

    # Street match
    if f.get("street"):
        if f["street"].lower() not in prop.get("street", "").lower():
            return False

    # City
    if f.get("city"):
        if prop.get("city", "").lower() != f["city"].lower():
            return False

    # Income
    if f.get("min_income") and prop.get("DP03_0062E") is not None:
        if prop["DP03_0062E"] < f["min_income"]:
            return False

    # Home value
    if f.get("min_value") and prop.get("DP04_0089E") is not None:
        if prop["DP04_0089E"] < f["min_value"]:
            return False

    return True


# -------------------------------------------
# ROUTES
# -------------------------------------------
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

    q = request.form.get("query", "")
    parsed = parse_query(q)

    # If GPT gives city + state, we must look up county
    if "city" in parsed and "state" in parsed:
        lookup_prompt = f"Return ONLY the county name for {parsed['city']}, {parsed['state']}."
        county_resp = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "Return only the county name."},
                {"role": "user", "content": lookup_prompt}
            ]
        )
        parsed["county"] = county_resp.choices[0].message.content.strip()

    dataset = load_county_file(parsed.get("state"), parsed.get("county"))

    if not dataset:
        loading_state["active"] = False
        return render_template(
            "index.html",
            results=[],
            error=f"Dataset not found for {parsed.get('county')}, {parsed.get('state')}."
        )

    results = []
    for feature in dataset.get("features", []):
        if matches(feature, parsed):
            results.append(feature["properties"])

        if len(results) >= 500:
            break

    loading_state["active"] = False

    return render_template("index.html", results=results)


# -------------------------------------------
# RUN
# -------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

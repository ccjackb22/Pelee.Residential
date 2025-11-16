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
# LOGIN
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
        "Allowed keys: city, state, street, min_income, max_income, min_value, max_value.\n"
        "Never include county.\n"
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
# LOAD ALL COUNTY FILES FOR A STATE
# ----------------------------
def list_state_county_files(state):
    """
    Example return:
    [
        "merged_with_tracts/oh/cuyahoga.geojson",
        "merged_with_tracts/oh/cuyahoga-with-values-income.geojson",
        ...
    ]
    """
    prefix = f"merged_with_tracts/{state.lower()}/"
    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    if "Contents" not in objects:
        return []

    return [obj["Key"] for obj in objects["Contents"] if obj["Key"].ends_with(".geojson")]


def load_geojson(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except:
        return None


# ----------------------------
# MATCHING LOGIC
# ----------------------------
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

    # Income
    if f.get("min_income") and prop.get("DP03_0062E"):
        if prop["DP03_0062E"] < f["min_income"]:
            return False

    # Value
    if f.get("min_value") and prop.get("DP04_0089E"):
        if prop["DP04_0089E"] < f["min_value"]:
            return False

    return True


# ----------------------------
# MAIN SEARCH ROUTE
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
    f = parse_query(query)

    if "state" not in f:
        loading_state["active"] = False
        return render_template("index.html", results=[], error="State not recognized from query.")

    state = f["state"].lower()

    # List all counties for that state
    files = list_state_county_files(state)

    if not files:
        loading_state["active"] = False
        return render_template("index.html", results=[], error=f"No datasets found for state {state.upper()}.")

    all_results = []

    # Iterate through ALL county files for that state
    for key in files:
        data = load_geojson(key)
        if not data:
            continue

        for feature in data.get("features", []):
            if matches(feature, f):
                all_results.append(feature["properties"])

            if len(all_results) >= 500:
                break

        if len(all_results) >= 500:
            break

    loading_state["active"] = False

    if not all_results:
        return render_template("index.html", results=[], error="No matching addresses found.")

    return render_template("index.html", results=all_results)


# ----------------------------
# RUN
# ----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

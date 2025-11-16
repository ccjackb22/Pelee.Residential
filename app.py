import os
import json
import boto3
from flask import Flask, render_template, request, redirect, session, jsonify
from botocore.config import Config
from openai import OpenAI

# -------------------
# CONFIG
# -------------------
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


# -------------------
# LOGIN
# -------------------
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
    allowed = ("login", "static")
    if request.endpoint not in allowed and not session.get("logged_in"):
        return redirect("/login")


# -------------------
# GPT PARSER
# -------------------
def parse_query(q):
    system_msg = (
        "Convert the user's natural-language address query into JSON.\n"
        "Allowed keys: city, state, county, street, min_income, max_income, min_value, max_value.\n"
        "Return ONLY valid JSON. No commentary."
    )

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": q}
        ]
    )

    try:
        return json.loads(resp.choices[0].message.content)
    except:
        return {}


# -------------------
# CLEAN COUNTY NAME
# -------------------
def normalize_county_name(name):
    if not name:
        return None

    name = name.lower().replace(" county", "").replace(" parish", "")
    name = name.replace("-", " ").strip()
    name = name.replace("  ", " ")
    name = name.replace(" ", "_")
    return name


# -------------------
# S3 LOADER
# -------------------
def load_county_file(state, county):
    if not state or not county:
        return None

    state = state.lower()
    county = normalize_county_name(county)

    keys = [
        f"merged_with_tracts/{state}/{county}-with-values-income.geojson",
        f"merged_with_tracts/{state}/{county}.geojson",
    ]

    for k in keys:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            return json.loads(obj["Body"].read())
        except:
            pass

    return None


# -------------------
# FILTERING LOGIC
# -------------------
def matches(rec, f):
    p = rec.get("properties", {})

    # street contains
    if f.get("street"):
        if f["street"].lower() not in p.get("street", "").lower():
            return False

    # city exact
    if f.get("city"):
        if f["city"].lower() != p.get("city", "").lower():
            return False

    # income
    if f.get("min_income"):
        if p.get("DP03_0062E") and p["DP03_0062E"] < f["min_income"]:
            return False

    # home value
    if f.get("min_value"):
        if p.get("DP04_0089E") and p["DP04_0089E"] < f["min_value"]:
            return False

    return True


# -------------------
# ROUTES
# -------------------
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search", methods=["POST"])
def search():
    global loading_state
    loading_state["active"] = True

    query_text = request.form.get("query", "")
    parsed = parse_query(query_text)

    # Resolve county via GPT if user gave city+state
    if parsed.get("city") and parsed.get("state") and not parsed.get("county"):
        lookup_prompt = f"Return ONLY the county name for: {parsed['city']}, {parsed['state']}"
        r = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[
                {"role": "system", "content": "County name only."},
                {"role": "user", "content": lookup_prompt}
            ]
        )
        parsed["county"] = r.choices[0].message.content.strip()

    dataset = load_county_file(parsed.get("state"), parsed.get("county"))

    if not dataset:
        loading_state["active"] = False
        return render_template(
            "index.html",
            results=[],
            error=f"Dataset not found for {parsed.get('county')}, {parsed.get('state')}."
        )

    results = []
    for f in dataset.get("features", []):
        if matches(f, parsed):
            results.append(f["properties"])
        if len(results) >= 500:
            break

    loading_state["active"] = False
    return render_template("index.html", results=results)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

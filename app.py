import os
import json
import re
import boto3
from flask import Flask, render_template, request, send_file, redirect
from io import BytesIO
from openai import OpenAI

app = Flask(__name__)

# -----------------------------
# CONFIG
# -----------------------------
BUCKET = "residential-data-jack"
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

s3 = boto3.client("s3")

# -----------------------------
# HELPERS
# -----------------------------
def normalize(text):
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9 ]+", "", text)
    text = text.replace(" city", "")
    return text.strip()


# -----------------------------
# BUILD CITY → COUNTY LOOKUP
# -----------------------------
city_to_county = {}

def build_city_lookup():
    global city_to_county

    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="merged_with_tracts/")
    if "Contents" not in resp:
        print("⚠ No files found in bucket.")
        return

    for obj in resp["Contents"]:
        key = obj["Key"]
        if not key.endswith("-with-values-income.geojson"):
            continue

        # Path looks like: merged_with_tracts/oh/medina-with-values-income.geojson
        parts = key.split("/")
        if len(parts) < 3:
            continue
        _, state, filename = parts
        county = filename.replace("-with-values-income.geojson", "")

        # Load file
        file_obj = s3.get_object(Bucket=BUCKET, Key=key)
        geo = json.loads(file_obj["Body"].read().decode("utf-8"))

        # Extract cities
        for feat in geo.get("features", []):
            city_raw = feat["properties"].get("city")
            city_norm = normalize(city_raw)
            if city_norm:
                city_to_county[f"{city_norm}|{state}"] = county

    print(f"✅ City lookup built with {len(city_to_county)} city entries.")


print("🔧 Building city→county lookup...")
build_city_lookup()


# -----------------------------
# GPT QUERY PARSER
# -----------------------------
def parse_query(nl_query):
    prompt = f"""
You extract search filters from user queries.

User query: "{nl_query}"

Return a JSON object ONLY with these keys if present:
- city
- state
- county
- street
- min_value
- max_value
- min_income
- max_income
- zipcode

If something is not present, return null for it.
Return ONLY valid JSON.
"""

    resp = client.responses.create(
        model="gpt-4.1-mini",
        input=prompt,
        max_output_tokens=200
    )

    text = resp.output_text.strip()
    try:
        data = json.loads(text)
        return data
    except:
        return {}


# -----------------------------
# FILTER LOGIC
# -----------------------------
def feature_matches(feat, filters):
    props = feat["properties"]

    # Street
    if filters.get("street"):
        if filters["street"] not in normalize(props.get("street", "")):
            return False

    # ZIP
    if filters.get("zipcode"):
        if str(filters["zipcode"]) != str(props.get("postcode")):
            return False

    # Income
    if filters.get("min_income"):
        val = props.get("ACS_HH_INCOME")
        if val is None or val < filters["min_income"]:
            return False

    if filters.get("max_income"):
        val = props.get("ACS_HH_INCOME")
        if val is None or val > filters["max_income"]:
            return False

    # Home value
    if filters.get("min_value"):
        val = props.get("ACS_HOME_VALUE")
        if val is None or val < filters["min_value"]:
            return False

    if filters.get("max_value"):
        val = props.get("ACS_HOME_VALUE")
        if val is None or val > filters["max_value"]:
            return False

    return True


# -----------------------------
# MAIN SEARCH ROUTE
# -----------------------------
@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


@app.route("/search", methods=["POST"])
def search():
    query = request.form.get("query", "")
    if not query:
        return render_template("index.html", results=[], error="Query required")

    parsed = parse_query(query)

    # Normalize city/state
    city_clean = normalize(parsed.get("city"))
    state_clean = normalize(parsed.get("state"))

    # Determine county
    county = None

    # If county provided by GPT, trust it
    if parsed.get("county"):
        county = normalize(parsed["county"])

    # else infer: city + state → county
    elif city_clean and state_clean:
        key = f"{city_clean}|{state_clean}"
        county = city_to_county.get(key)

    if not county:
        return render_template("index.html", results=[], error="County not found for query.")

    # Load the correct file
    key = f"merged_with_tracts/{state_clean}/{county}-with-values-income.geojson"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except:
        return render_template("index.html", results=[], error=f"Dataset not found: {key}")

    geo = json.loads(obj["Body"].read().decode("utf-8"))

    # Apply filters
    filters = {
        "street": normalize(parsed.get("street")),
        "zipcode": parsed.get("zipcode"),
        "min_value": parsed.get("min_value"),
        "max_value": parsed.get("max_value"),
        "min_income": parsed.get("min_income"),
        "max_income": parsed.get("max_income"),
    }

    results = []
    for feat in geo.get("features", []):
        if feature_matches(feat, filters):
            results.append(feat["properties"])

    return render_template("index.html", results=results)


# -----------------------------
# EXPORT ENDPOINT
# -----------------------------
@app.route("/export")
def export_data():
    return "Export coming soon"


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

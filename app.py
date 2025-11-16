import os
import json
from flask import Flask, render_template, request, redirect, session, send_file
from dotenv import load_dotenv
import boto3
import io

load_dotenv()

# ------------------------------------------------------
# FLASK + SESSION
# ------------------------------------------------------
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev_secret")

PASSWORD = os.getenv("ACCESS_PASSWORD", "CaLuna")

BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

# ------------------------------------------------------
# S3 CLIENT
# ------------------------------------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=REGION
)

# ------------------------------------------------------
# LOGIN PAGE
# ------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "").strip()
        if pw == PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        return render_template("login.html", error="Incorrect password.")
    return render_template("login.html")

# ------------------------------------------------------
# HOME PAGE
# ------------------------------------------------------
@app.route("/")
def home():
    if not session.get("logged_in"):
        return redirect("/login")
    return render_template("index.html")

# ------------------------------------------------------
# S3: LOAD ONE GEOJSON
# ------------------------------------------------------
def load_geojson_from_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        txt = obj["Body"].read().decode("utf-8")
        return json.loads(txt)
    except Exception as e:
        print("S3 READ ERROR:", e)
        return None

# ------------------------------------------------------
# FIND COUNTY FILES FOR A GIVEN STATE
# ------------------------------------------------------
def list_state_files(state_abbrev):
    prefix = f"merged_with_tracts/{state_abbrev.lower()}/"
    try:
        objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    except Exception as e:
        print("S3 LIST ERROR:", e)
        return []

    if "Contents" not in objects:
        return []

    return [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".geojson")]

# ------------------------------------------------------
# PARSE USER QUERY
# Extract: state abbrev, city name (simple heuristic)
# ------------------------------------------------------
def parse_query(query):
    q = query.lower().strip()

    states = {
        "alabama": "AL","alaska": "AK","arizona": "AZ","arkansas": "AR","california": "CA",
        "colorado": "CO","connecticut": "CT","delaware": "DE","florida": "FL","georgia": "GA",
        "hawaii": "HI","idaho": "ID","illinois": "IL","indiana": "IN","iowa": "IA","kansas": "KS",
        "kentucky": "KY","louisiana": "LA","maine": "ME","maryland": "MD","massachusetts": "MA",
        "michigan": "MI","minnesota": "MN","mississippi": "MS","missouri": "MO","montana": "MT",
        "nebraska": "NE","nevada": "NV","new hampshire": "NH","new jersey": "NJ","new mexico": "NM",
        "new york": "NY","north carolina": "NC","north dakota": "ND","ohio": "OH","oklahoma": "OK",
        "oregon": "OR","pennsylvania": "PA","rhode island": "RI","south carolina": "SC",
        "south dakota": "SD","tennessee": "TN","texas": "TX","utah": "UT","vermont": "VT",
        "virginia": "VA","washington": "WA","west virginia": "WV","wisconsin": "WI","wyoming": "WY"
    }

    # Detect state
    found_state = None
    for long_name, abbr in states.items():
        if long_name.lower() in q:
            found_state = abbr
            break
        if f" {abbr.lower()} " in q:
            found_state = abbr
            break

    # Detect city after words like "in" or "near"
    words = q.replace(",", "").split()
    city = None
    for i, w in enumerate(words):
        if w in ["in", "near", "at"]:
            if i + 1 < len(words):
                city = words[i + 1]
                break

    return found_state, city

# ------------------------------------------------------
# CITY-AWARE FEATURE FILTERING
# Robust detection of ANY city-field naming
# ------------------------------------------------------
def filter_features(features, city_name):
    if not city_name:
        return []  # we require a city for now

    city_name = city_name.lower()

    # All possible names that counties use
    city_keys = [
        "city","City","CITY",
        "town","Town","TOWN",
        "place","Place","PLACE",
        "municipality","Municipality","MUNICIPALITY",
        "locality","Locality","LOCALITY",
        "community","Community","COMMUNITY"
    ]

    results = []

    for f in features:
        p = f.get("properties", {})
        if not isinstance(p, dict):
            continue

        found_city_value = None
        for ck in city_keys:
            if ck in p and isinstance(p[ck], str):
                found_city_value = p[ck].lower()
                break

        # If we found a city value, match it
        if found_city_value and found_city_value == city_name:
            results.append({
                "number": p.get("number", ""),
                "street": p.get("street", ""),
                "unit": p.get("unit", ""),
                "city": p.get("city", "") or p.get("City", "") or "",
                "postcode": p.get("postcode", ""),
                "region": p.get("region", "")
            })

    return results

# ------------------------------------------------------
# SEARCH ENDPOINT
# ------------------------------------------------------
@app.route("/search", methods=["POST"])
def search():
    if not session.get("logged_in"):
        return redirect("/login")

    query = request.form.get("query", "")
    state, city = parse_query(query)

    if not state:
        return render_template("index.html", error="Couldn't detect a state.")

    if not city:
        return render_template("index.html", error="Couldn't detect a city.")

    # Load only ONE county at a time until we find matches
    files = list_state_files(state)

    if not files:
        return render_template("index.html", error=f"No datasets found for {state}.")

    all_results = []

    for key in files:
        data = load_geojson_from_s3(key)
        if not data or "features" not in data:
            continue

        matched = filter_features(data["features"], city)
        if matched:
            all_results.extend(matched)
            break   # stop after first county hit

    if not all_results:
        return render_template("index.html", error="No matching addresses found.")

    return render_template("index.html", results=all_results)

# ------------------------------------------------------
# EXPORT
# ------------------------------------------------------
@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    # Dummy export for now
    buf = io.BytesIO()
    buf.write(json.dumps({"export": "ok"}, indent=2).encode("utf-8"))
    buf.seek(0)
    return send_file(buf, mimetype="application/json",
                     as_attachment=True, download_name="results.json")

# ------------------------------------------------------
# MAIN
# ------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

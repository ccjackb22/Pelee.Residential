import os
import json
from flask import Flask, render_template, request, redirect, session, send_file
from dotenv import load_dotenv
import boto3
import io

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev_secret")

PASSWORD = os.getenv("ACCESS_PASSWORD", "CaLuna")
BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# ------------------ S3 CLIENT ------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-2"),
)


# =========================================================
# LOGIN
# =========================================================
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "").strip()
        if pw == PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        return render_template("login.html", error="Incorrect password.")
    return render_template("login.html")


# =========================================================
# HOME PAGE
# =========================================================
@app.route("/")
def home():
    if not session.get("logged_in"):
        return redirect("/login")
    return render_template("index.html")


# =========================================================
# S3 HELPERS
# =========================================================
def load_geojson_from_s3(key):
    """Load a single county file from S3"""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        txt = obj["Body"].read().decode("utf-8")
        return json.loads(txt)
    except Exception as e:
        print("S3 READ ERROR:", e)
        return None


def list_state_county_files(state):
    """
    Load ONLY the county file that matches the city county name.
    Format on S3:
        merged_with_tracts/oh/cuyahoga-with-values-income.geojson
    We search inside merged_with_tracts/{state}/
    """
    prefix = f"merged_with_tracts/{state.lower()}/"

    try:
        objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    except Exception as e:
        print("LIST ERROR:", e)
        return []

    if "Contents" not in objects:
        return []

    return [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".geojson")]


# =========================================================
# QUERY PARSER  (STATE + CITY + VALUE DETECTOR)
# =========================================================
def parse_query(q):
    q = q.lower().strip()
    words = q.replace(",", "").split()

    # Full names
    full_states = {
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

    # Abbreviations (for OH, AZ, FL...)
    abbr_states = {v.lower(): v for v in full_states.values()}

    found_state = None
    city = None
    filters = {}

    # 1. Find abbreviation
    for w in words:
        if w in abbr_states:
            found_state = abbr_states[w]
            break

    # 2. Find full name only if no abbreviation found
    if not found_state:
        for name, abbr in full_states.items():
            if name in q:
                found_state = abbr
                break

    # 3. Detect city after "in"
    if "in" in words:
        i = words.index("in")
        if i + 1 < len(words):
            city = words[i + 1]

    # 4. Detect city before state (“berea oh”)
    if not city and found_state:
        for i in range(len(words) - 1):
            if words[i + 1] == found_state.lower():
                city = words[i]
                break

    # 5. Detect value filters “over 300k”
    if "over" in q:
        digits = "".join(filter(str.isdigit, q))
        if digits:
            filters["min_value"] = int(digits)

    return found_state, city, filters


# =========================================================
# FEATURE FILTERING
# =========================================================
def filter_features(features, city=None, min_value=None):
    good = []

    for f in features:
        p = f.get("properties", {})

        if city and p.get("city", "").lower() != city.lower():
            continue

        if min_value:
            # First look for income
            income = p.get("income")

            # Then look for home value
            home_value = p.get("home_value")

            # Both missing → skip
            if income is None and home_value is None:
                continue

            # If income exists and is below threshold → skip
            if income is not None and income < min_value:
                continue

            # If home value exists and is below threshold → skip
            if home_value is not None and home_value < min_value:
                continue

        good.append({
            "number": p.get("number", ""),
            "street": p.get("street", ""),
            "unit": p.get("unit", ""),
            "city": p.get("city", ""),
            "postcode": p.get("postcode", ""),
            "region": p.get("region", "")
        })

    return good


# =========================================================
# SEARCH ENDPOINT — LOADS 1 COUNTY FILE ONLY
# =========================================================
@app.route("/search", methods=["POST"])
def search():
    if not session.get("logged_in"):
        return redirect("/login")

    q = request.form.get("query", "")
    state, city, filters = parse_query(q)

    if not state:
        return render_template("index.html", error="Couldn't detect a state.")

    # Load state folder
    files = list_state_county_files(state)

    if not files:
        return render_template("index.html", error=f"No data for {state}.")

    # We must pick only ONE file to avoid memory overload
    # If city found, pick county containing that city name
    chosen_key = None
    city_lower = city.lower() if city else None

    if city_lower:
        for key in files:
            if city_lower in key.lower():
                chosen_key = key
                break

    # If no match, fallback to largest county (e.g., Cuyahoga for OH)
    if not chosen_key:
        chosen_key = files[0]

    data = load_geojson_from_s3(chosen_key)

    if not data or "features" not in data:
        return render_template("index.html", error="Failed to load dataset.")

    results = filter_features(
        data["features"],
        city=city,
        min_value=filters.get("min_value")
    )

    if len(results) == 0:
        return render_template("index.html", error="No matching results found.")

    return render_template("index.html", results=results)


# =========================================================
# EXPORT DUMMY ENDPOINT
# =========================================================
@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    buf = io.BytesIO()
    buf.write(json.dumps({"status": "ok"}).encode("utf-8"))
    buf.seek(0)
    return send_file(buf, mimetype="application/json", as_attachment=True, download_name="export.json")


# =========================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

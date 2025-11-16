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

BUCKET = "residential-data-jack"

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-2"
)


# -----------------------------------------------------------
# LOGIN
# -----------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "").strip()
        if pw == PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        return render_template("login.html", error="Incorrect password.")
    return render_template("login.html")


# -----------------------------------------------------------
# HOME
# -----------------------------------------------------------
@app.route("/")
def home():
    if not session.get("logged_in"):
        return redirect("/login")
    return render_template("index.html")


# -----------------------------------------------------------
# READ GEOJSON FROM S3
# -----------------------------------------------------------
def load_geojson_from_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        txt = obj["Body"].read().decode("utf-8")
        return json.loads(txt)
    except Exception as e:
        print("S3 READ ERROR:", e)
        return None


# -----------------------------------------------------------
# LIST ALL GEOJSON FILES FOR A STATE
# -----------------------------------------------------------
def list_state_county_files(state):
    """Returns all .geojson county files inside merged_with_tracts/{state}/"""
    prefix = f"merged_with_tracts/{state.lower()}/"
    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    if "Contents" not in objects:
        return []

    return [
        obj["Key"]
        for obj in objects["Contents"]
        if obj["Key"].endswith(".geojson")
    ]


# -----------------------------------------------------------
# PARSE QUERY → STATE, CITY, LIMITER
# -----------------------------------------------------------
def parse_query(q):
    q = q.lower().strip()

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

    found_state = None
    for name, abbr in states.items():
        if name.lower() in q:
            found_state = abbr
            break

    # numeric filter
    numeric_words = {}
    if "over" in q:
        try:
            val = int("".join(filter(str.isdigit, q)))
            numeric_words["min_value"] = val
        except:
            pass

    # attempt to detect simple city word after "in"
    words = q.replace(",", "").split()
    city = None
    for i, w in enumerate(words):
        if w in ["in", "near", "at"]:
            if i + 1 < len(words):
                city = words[i + 1]
                break

    return found_state, city, numeric_words


# -----------------------------------------------------------
# FILTER GEOJSON FEATURES  (FIXED)
# -----------------------------------------------------------
def filter_features(features, city=None, min_value=None):
    results = []
    for f in features:
        p = f.get("properties", {})

        if city and p.get("city", "").lower() != city.lower():
            continue

        # SAFE min_value filtering
        if min_value:
            # DP04_0089E = home value
            # DP03_0062E = household income
            val = p.get("DP04_0089E") or p.get("DP03_0062E")
            if isinstance(val, (int, float)) and val < min_value:
                continue

        results.append({
            "number": p.get("number", ""),
            "street": p.get("street", ""),
            "unit": p.get("unit", ""),
            "city": p.get("city", ""),
            "postcode": p.get("postcode", ""),
            "region": p.get("region", "")
        })

    return results


# -----------------------------------------------------------
# SEARCH ENDPOINT
# -----------------------------------------------------------
@app.route("/search", methods=["POST"])
def search():
    if not session.get("logged_in"):
        return redirect("/login")

    q = request.form.get("query", "")
    state, city, filters = parse_query(q)

    if not state:
        return render_template("index.html", error="Could not find a state in your query.")

    files = list_state_county_files(state)

    if not files:
        return render_template("index.html", error=f"No datasets found for {state}.")

    all_results = []

    for key in files:
        data = load_geojson_from_s3(key)
        if not data or "features" not in data:
            continue

        res = filter_features(
            data["features"],
            city=city,
            min_value=filters.get("min_value")
        )
        all_results.extend(res)

    if len(all_results) == 0:
        return render_template("index.html", error="No matching addresses found.")

    return render_template("index.html", results=all_results)


# -----------------------------------------------------------
# EXPORT
# -----------------------------------------------------------
@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    dummy = [{"status": "export works"}]

    buf = io.BytesIO()
    buf.write(json.dumps(dummy, indent=2).encode("utf-8"))
    buf.seek(0)
    return send_file(buf, mimetype="application/json", as_attachment=True, download_name="export.json")


# -----------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

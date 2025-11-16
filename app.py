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
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=REGION
)

# -------------------------------------------------------------------
# LOGIN
# -------------------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "").strip()
        if pw == PASSWORD:
            session["logged_in"] = True
            return redirect("/")
        return render_template("login.html", error="Incorrect password.")
    return render_template("login.html")


# -------------------------------------------------------------------
# HOME
# -------------------------------------------------------------------
@app.route("/")
def home():
    if not session.get("logged_in"):
        return redirect("/login")
    return render_template("index.html")


# -------------------------------------------------------------------
# DETECT STATE & CITY FROM QUERY
# -------------------------------------------------------------------
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
    for full, abbr in states.items():
        if full in q:
            found_state = abbr.lower()
            break

    # crude city extraction
    words = q.replace(",", "").split()
    city = None
    for i, w in enumerate(words):
        if w in ["in", "near"]:
            if i + 1 < len(words):
                city = words[i+1]
                break

    return found_state, city


# -------------------------------------------------------------------
# LOAD ONLY THE RIGHT COUNTY FILE
# -------------------------------------------------------------------
def find_county_file(state, city):
    """
    Finds a SINGLE county file containing the city name.
    """
    prefix = f"merged_with_tracts/{state}/"
    objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

    if "Contents" not in objects:
        return None

    city = city.lower()

    # Use enriched files only
    candidates = [obj["Key"] for obj in objects["Contents"]
                  if obj["Key"].endswith("-with-values-income.geojson")]

    # Pick the smallest (safer memory)
    if len(candidates) > 0:
        return min(candidates, key=lambda k: k)

    return None


def load_geojson(key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


# -------------------------------------------------------------------
# FILTER FEATURES
# -------------------------------------------------------------------
def filter_features(features, city):
    results = []

    for f in features:
        p = f.get("properties", {})

        if city and p.get("city", "").lower() != city.lower():
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


# -------------------------------------------------------------------
# SEARCH
# -------------------------------------------------------------------
@app.route("/search", methods=["POST"])
def search():
    if not session.get("logged_in"):
        return redirect("/login")

    q = request.form.get("query", "")
    state, city = parse_query(q)

    if not state:
        return render_template("index.html", error="Couldn't detect a state.")

    file_key = find_county_file(state, city)

    if not file_key:
        return render_template("index.html", error="No dataset for that area.")

    data = load_geojson(file_key)

    matches = filter_features(data.get("features", []), city)

    if len(matches) == 0:
        return render_template("index.html", error="No matching addresses found.")

    return render_template("index.html", results=matches)


# -------------------------------------------------------------------
# EXPORT
# -------------------------------------------------------------------
@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    buf = io.BytesIO()
    buf.write(json.dumps({"status": "ok"}, indent=2).encode())
    buf.seek(0)
    return send_file(buf, mimetype="application/json", as_attachment=True, download_name="export.json")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

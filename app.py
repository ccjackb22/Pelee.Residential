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
# LIST GEOJSON FILES FOR A STATE
# -----------------------------------------------------------
def list_state_files(state):
    prefix = f"merged_with_tracts/{state.lower()}/"
    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    if "Contents" not in objects:
        return []

    return [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".geojson")]


# -----------------------------------------------------------
# LIGHTWEIGHT CITY CHECK (only first 200 lines)
# -----------------------------------------------------------
def county_might_contain_city(key, city):
    """Reads only the first ~200 lines of the GeoJSON and checks if the city string appears."""
    try:
        stream = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
        first_part = stream.read(30000).decode("utf-8", errors="ignore")
        return city.lower() in first_part.lower()
    except Exception as e:
        print("STREAM CHECK ERROR:", e)
        return False


# -----------------------------------------------------------
# LOAD FULL GEOJSON (only when needed)
# -----------------------------------------------------------
def load_full_geojson(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        txt = obj["Body"].read().decode("utf-8")
        return json.loads(txt)
    except Exception as e:
        print("FULL LOAD ERROR:", e)
        return None


# -----------------------------------------------------------
# PARSE QUERY → STATE, CITY, LIMITS
# -----------------------------------------------------------
def parse_query(q):
    q = q.lower().strip()

    states = {
        "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA",
        "colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA",
        "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS",
        "kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD","massachusetts":"MA",
        "michigan":"MI","minnesota":"MN","mississippi":"MS","missouri":"MO","montana":"MT",
        "nebraska":"NE","nevada":"NV","new hampshire":"NH","new jersey":"NJ","new mexico":"NM",
        "new york":"NY","north carolina":"NC","north dakota":"ND","ohio":"OH","oklahoma":"OK",
        "oregon":"OR","pennsylvania":"PA","rhode island":"RI","south carolina":"SC",
        "south dakota":"SD","tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT",
        "virginia":"VA","washington":"WA","west virginia":"WV","wisconsin":"WI","wyoming":"WY"
    }

    state = None
    for name, abbr in states.items():
        if name in q:
            state = abbr
            break

    # Pull city name
    city = None
    words = q.replace(",", "").split()
    for i, w in enumerate(words):
        if w in ["in", "near", "at"] and i + 1 < len(words):
            city = words[i + 1]
            break

    # Simple numeric extraction (income/home value)
    filters = {}
    if "over" in q:
        try:
            filters["min_value"] = int("".join([c for c in q if c.isdigit()]))
        except:
            pass

    return state, city, filters


# -----------------------------------------------------------
# FILTER ACTUAL FEATURES
# -----------------------------------------------------------
def filter_features(features, city=None, min_value=None):
    out = []
    city = city.lower() if city else None

    for f in features:
        p = f.get("properties", {})

        if city and p.get("city", "").lower() != city:
            continue

        if min_value:
            val = p.get("income") or p.get("home_value") or None
            if val is None or val < min_value:
                continue

        out.append({
            "number": p.get("number", ""),
            "street": p.get("street", ""),
            "unit": p.get("unit", ""),
            "city": p.get("city", ""),
            "postcode": p.get("postcode", ""),
            "region": p.get("region", "")
        })

    return out


# -----------------------------------------------------------
# SEARCH (ONE-COUNTY LOADER)
# -----------------------------------------------------------
@app.route("/search", methods=["POST"])
def search():
    if not session.get("logged_in"):
        return redirect("/login")

    query = request.form.get("query", "")
    state, city, filters = parse_query(query)

    if not state:
        return render_template("index.html", error="Couldn't detect a state.")

    files = list_state_files(state)
    if not files:
        return render_template("index.html", error=f"No datasets for {state}.")

    # Try counties one by one
    for key in files:
        if city and not county_might_contain_city(key, city):
            continue

        data = load_full_geojson(key)
        if not data or "features" not in data:
            continue

        results = filter_features(
            data["features"],
            city=city,
            min_value=filters.get("min_value")
        )

        if results:
            return render_template("index.html", results=results)

    # No matches in any county
    return render_template("index.html", error="No matching addresses found.")


# -----------------------------------------------------------
# EXPORT
# -----------------------------------------------------------
@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    dummy = [{"status": "export OK"}]
    buf = io.BytesIO()
    buf.write(json.dumps(dummy, indent=2).encode("utf-8"))
    buf.seek(0)
    return send_file(buf, mimetype="application/json", as_attachment=True, download_name="export.json")


# -----------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

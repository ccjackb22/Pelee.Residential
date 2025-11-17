import os
import json
import io
import csv
import zipfile

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    session,
    send_file,
)
from dotenv import load_dotenv
import boto3

# -----------------------------------------------------------
# ENV & CLIENTS
# -----------------------------------------------------------
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "dev_secret")
PASSWORD = os.getenv("ACCESS_PASSWORD", "CaLuna")

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION,
)


# -----------------------------------------------------------
# DATASET SELECTION (one county → safe memory)
# -----------------------------------------------------------
def choose_dataset_for_state(state_abbr: str) -> str | None:
    state_abbr = (state_abbr or "").upper()

    if state_abbr == "OH":
        return "merged_with_tracts/oh/cuyahoga-with-values-income.geojson"

    if state_abbr == "AK":
        return "merged_with_tracts/ak/haines-with-values-income.geojson"

    return None


# -----------------------------------------------------------
# S3 LOADER
# -----------------------------------------------------------
def load_geojson_from_s3(key: str) -> dict | None:
    try:
        print(f"[DEBUG] Fetching S3 object: {key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj["Body"].read().decode("utf-8")
        return json.loads(data)
    except Exception as e:
        print(f"[ERROR] Failed S3 load: {e}")
        return None


# -----------------------------------------------------------
# PARSE QUERY
# -----------------------------------------------------------
STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN",
    "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA",
    "washington": "WA", "west virginia": "WV", "wisconsin": "WI",
    "wyoming": "WY",
}

def parse_query(q: str):
    q = (q or "").lower().strip()

    state = None
    for name, abbr in STATE_NAMES.items():
        if name in q:
            state = abbr
            break

    city = None
    words = q.replace(",", "").split()
    for i, w in enumerate(words):
        if w in ("in", "near", "at"):
            if i + 1 < len(words):
                city = words[i + 1]
                break

    print(f"[DEBUG] parse_query → state={state}, city={city}")
    return state, city


# -----------------------------------------------------------
# FILTERING
# -----------------------------------------------------------
def filter_features(features, city: str | None = None, limit: int = 500):
    results = []
    city_norm = city.lower() if city else None

    for f in features:
        p = f.get("properties", {})

        if city_norm:
            if str(p.get("city", "")).lower() != city_norm:
                continue

        # include all useful commercial fields
        item = {
            "number": p.get("number", ""),
            "street": p.get("street", ""),
            "unit": p.get("unit", ""),
            "city": p.get("city", ""),
            "region": p.get("region", ""),
            "postcode": p.get("postcode", ""),
            "income": p.get("income", ""),
            "home_value": p.get("home_value", ""),
            "tract": p.get("GEOID", ""),
            "county": p.get("COUNTYFP", ""),
            "lat": "",
            "lon": "",
        }

        # extract coordinates if present
        coords = f.get("geometry", {}).get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) == 2:
            item["lon"], item["lat"] = coords[0], coords[1]

        results.append(item)

        if len(results) >= limit:
            break

    print(f"[DEBUG] filter_features → {len(results)} results")
    return results


# -----------------------------------------------------------
# LOGIN
# -----------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
            session["logged_in"] = True
            session["last_results"] = None
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
    return render_template("index.html", results=session.get("last_results"))


# -----------------------------------------------------------
# CLEAR CHAT
# -----------------------------------------------------------
@app.route("/clear")
def clear():
    session["last_results"] = None
    return redirect("/")


# -----------------------------------------------------------
# SEARCH
# -----------------------------------------------------------
@app.route("/search", methods=["POST"])
def search():
    if not session.get("logged_in"):
        return redirect("/login")

    query = request.form.get("query", "")
    state, city = parse_query(query)

    if not state:
        return render_template("index.html", error="Couldn't detect a state.")

    dataset = choose_dataset_for_state(state)
    if not dataset:
        return render_template("index.html", error=f"No dataset connected for {state} yet.")

    data = load_geojson_from_s3(dataset)
    if not data or "features" not in data:
        return render_template("index.html", error="Dataset failed to load.")

    results = filter_features(data["features"], city=city, limit=500)

    if not results:
        session["last_results"] = None
        return render_template("index.html", error="No matching addresses found.")

    session["last_results"] = results
    return render_template("index.html", results=results)


# -----------------------------------------------------------
# EXPORT
# -----------------------------------------------------------
@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    results = session.get("last_results")
    if not results:
        return render_template("index.html", error="No results to export.")

    # Create CSV in memory
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=results[0].keys())
    writer.writeheader()
    writer.writerows(results)

    # Now zip it in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("results.csv", csv_buffer.getvalue())

    zip_buffer.seek(0)

    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name="addresses.zip"
    )


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

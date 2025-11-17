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
# SIMPLE DATASET SELECTION
# -----------------------------------------------------------
def choose_dataset_for_state(state_abbr: str) -> str | None:
    state_abbr = (state_abbr or "").upper()

    if state_abbr == "OH":
        return "merged_with_tracts/oh/cuyahoga-with-values-income.geojson"

    if state_abbr == "AK":
        return "merged_with_tracts/ak/haines-with-values-income.geojson"

    return None


# -----------------------------------------------------------
# S3 READER
# -----------------------------------------------------------
def load_geojson_from_s3(key: str) -> dict | None:
    try:
        print(f"[DEBUG] Fetching S3 object: bucket={S3_BUCKET}, key={key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read()
        print(f"[DEBUG] Downloaded {len(body)} bytes")
        return json.loads(body.decode("utf-8"))
    except Exception as e:
        print(f"[ERROR] S3 read failed for {key}: {e}")
        return None


# -----------------------------------------------------------
# QUERY PARSING
# -----------------------------------------------------------
STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}


def parse_query(q: str):
    q = (q or "").strip()
    q_lower = q.lower()

    # Find spelled-out state
    state_abbr = None
    for name, abbr in STATE_NAMES.items():
        if name in q_lower:
            state_abbr = abbr
            break

    # City = first word after "in", "near", "at"
    city = None
    words = q_lower.replace(",", "").split()
    for i, w in enumerate(words):
        if w in ("in", "near", "at"):
            if i + 1 < len(words):
                city = words[i + 1]
                break

    print(f"[DEBUG] parse_query → state={state_abbr}, city={city}")
    return state_abbr, city


# -----------------------------------------------------------
# FILTERING (city only for now)
# -----------------------------------------------------------
def filter_features(features, city: str | None = None, limit: int = 500):
    results = []
    city_norm = city.lower() if city else None

    for f in features:
        props = f.get("properties", {})

        # City filter only (for now)
        if city_norm:
            prop_city = str(props.get("city", "")).lower()
            if prop_city != city_norm:
                continue

        results.append({
            "number": props.get("number", ""),
            "street": props.get("street", ""),
            "unit": props.get("unit", ""),
            "city": props.get("city", ""),
            "postcode": props.get("postcode", ""),
            "region": props.get("region", ""),
        })

        if len(results) >= limit:
            break

    print(f"[DEBUG] filter_features → returned {len(results)}")
    return results


# -----------------------------------------------------------
# AUTH
# -----------------------------------------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "").strip()
        if pw == PASSWORD:
            session.permanent = True
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
# SEARCH
# -----------------------------------------------------------
@app.route("/search", methods=["POST"])
def search():
    if not session.get("logged_in"):
        return redirect("/login")

    query = request.form.get("query", "")
    print(f"[DEBUG] /search query='{query}'")

    state_abbr, city = parse_query(query)

    if not state_abbr:
        return render_template(
            "index.html",
            error="Couldn't detect a state. Try 'addresses in strongsville ohio'."
        )

    key = choose_dataset_for_state(state_abbr)
    if not key:
        return render_template(
            "index.html",
            error=f"No dataset wired for {state_abbr}."
        )

    data = load_geojson_from_s3(key)
    if not data or "features" not in data:
        return render_template(
            "index.html",
            error=f"Failed to load dataset for {state_abbr}."
        )

    results = filter_features(data["features"], city=city)

    if not results:
        return render_template(
            "index.html",
            error="No matching addresses found."
        )

    # SAVE TO SESSION FOR EXPORT
    session["last_results"] = results

    return render_template("index.html", results=results)


# -----------------------------------------------------------
# EXPORT FULL RESULTS → CSV → ZIP
# -----------------------------------------------------------
@app.route("/export")
def export():
    if not session.get("logged_in"):
        return redirect("/login")

    results = session.get("last_results")
    if not results:
        return render_template("index.html", error="No results to export.")

    # Build CSV
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=[
        "number", "street", "unit", "city", "postcode", "region"
    ])
    writer.writeheader()
    writer.writerows(results)

    # ZIP it
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("addresses.csv", csv_buffer.getvalue())

    zip_buffer.seek(0)

    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name="addresses.zip",
    )


# -----------------------------------------------------------
# MAIN
# -----------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

import os
import json
import boto3
from flask import Flask, render_template, request, redirect, url_for, session

app = Flask(__name__)
app.secret_key = "supersecret"

# ---------------------
# AUTH
# ---------------------
PASSWORD = "CaLuna"   # YOUR PASSWORD — DO NOT CHANGE THIS

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "").strip()
        if pw == PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Wrong password.")
    return render_template("login.html")


# ---------------------
# S3 CLIENT
# ---------------------
S3_BUCKET = "residential-data-jack"

session_boto = boto3.session.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "us-east-2"),
)

s3 = session_boto.client("s3")


# ---------------------
# S3 DEBUGGING BLOCK
# ---------------------
# This tests if Render can SEE ANYTHING in S3.
@app.route("/s3test")
def s3test():
    try:
        listing = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix="merged_with_tracts/tx/"
        )
        return {
            "status": "OK",
            "found": listing.get("KeyCount", 0),
            "keys": [obj.get("Key") for obj in listing.get("Contents", [])]
        }
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}


# ---------------------
# BASIC ROUTES
# ---------------------
@app.route("/")
def index():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_template("index.html")


# ---------------------
# PARSER
# ---------------------
def normalize_county_name(raw: str) -> str:
    raw = raw.lower().strip()
    raw = raw.replace(" county", "")
    raw = raw.replace(" parish", "")
    raw = raw.replace(" borough", "")
    raw = raw.replace(" census area", "")
    raw = raw.replace(" city", "")
    raw = " ".join(raw.split())
    raw = raw.replace(" ", "_")
    return raw


def parse_query(text: str):
    text = text.lower()

    # state lookup
    state = None
    for st in STATE_MAP:
        if st.lower() in text:
            state = STATE_MAP[st]
            break

    # find county
    county = None
    if " county" in text:
        before = text.split(" county")[0].split()[-2:]
        county = " ".join(before)
    elif " parish" in text:
        before = text.split(" parish")[0].split()[-2:]
        county = " ".join(before)
    elif " borough" in text:
        before = text.split(" borough")[0].split()[-2:]
        county = " ".join(before)

    if county:
        county = normalize_county_name(county)

    # income or value
    income = None
    value = None
    nums = [int(n.replace(",", "")) for n in text.split() if n.replace(",", "").isdigit()]
    if nums:
        if "income" in text or "incomes" in text:
            income = nums[0]
        elif "value" in text or "values" in text:
            value = nums[0]

    return state, county, income, value


# ---------------------
# STATE MAP
# ---------------------
STATE_MAP = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
}


# ---------------------
# SEARCH ROUTE
# ---------------------
@app.route("/search", methods=["POST"])
def search():
    if not session.get("logged_in"):
        return redirect(url_for("login"))

    user_query = request.form.get("query", "").strip()
    if not user_query:
        return render_template("index.html", error="Empty query")

    state, county, income, value = parse_query(user_query)

    if not state or not county:
        return render_template("index.html", error="Could not understand query")

    key = f"merged_with_tracts/{state}/{county}-with-values-income.geojson"

    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        return render_template(
            "index.html",
            results=data.get("features", []),
            query=user_query,
            count=len(data.get("features", [])),
            key=key,
        )
    except Exception as e:
        return render_template("index.html", error=f"Failed to load: {key} — {e}")



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

import os
import json
import boto3
from flask import Flask, render_template, request, redirect, url_for, session

app = Flask(__name__)
app.secret_key = "supersecret"

# ---------------------
# PASSWORD LOGIN
# ---------------------
PASSWORD = "CaLuna"


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "").strip()
        if pw == PASSWORD:
            session["logged_in"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Wrong password.")
    return render_template("login.html")


@app.route("/")
def index():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_template("index.html")


# ---------------------
# AWS S3 CLIENT
# ---------------------
S3_BUCKET = "residential-data-jack"

session_boto = boto3.session.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION", "us-east-2"),
)

s3 = session_boto.client("s3")


# ---------------------
# DEBUG: S3 TEST ENDPOINT
# ---------------------
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
# COUNTY NORMALIZATION
# ---------------------
def normalize_county_name(raw: str) -> str:
    """
    Normalize county into S3 filename format:
    "palm beach county" -> "palm_beach"
    "los angeles" -> "los_angeles"
    """
    raw = raw.lower().strip()

    suffixes = [" county", " parish", " borough", " census area", " city"]
    for s in suffixes:
        if raw.endswith(s):
            raw = raw[: -len(s)]

    raw = raw.strip()
    raw = " ".join(raw.split())  # collapse multiple spaces
    raw = raw.replace(" ", "_")  # convert to S3 naming style
    return raw


def extract_county_from_text(text: str):
    """
    Extracts correct county name for 3,000+ U.S. counties.
    Handles multi-word counties and "in", "the", "of" clutter.
    """
    markers = [" county", " parish", " borough", " census area", " city"]

    for marker in markers:
        if marker in text:
            before = text.split(marker)[0].strip()
            words = before.split()

            # remove junk filler words
            junk = {"in", "the", "of", "at", "for", "near"}
            words = [w for w in words if w not in junk]

            if not words:
                return None

            # Try 3-word, then 2-word, then 1-word counties
            for n in [3, 2, 1]:
                if len(words) >= n:
                    candidate = " ".join(words[-n:])
                    return normalize_county_name(candidate)

    return None


# ---------------------
# QUERY PARSER
# ---------------------
def parse_query(text: str):
    t = text.lower()

    # ---- STATE ----
    state = None
    for st in STATE_MAP:
        if st.lower() in t:
            state = STATE_MAP[st]
            break

    # ---- COUNTY ----
    county = extract_county_from_text(t)

    # ---- NUMERIC FILTERS ----
    income = None
    value = None

    nums = [n for n in t.replace(",", "").split() if n.isdigit()]
    if nums:
        num = int(nums[0])
        if "income" in t or "incomes" in t:
            income = num
        elif "value" in t or "values" in t:
            value = num

    return state, county, income, value


# ---------------------
# STATE MAP (ALL 50)
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
        return render_template(
            "index.html",
            error=f"Failed to load: {key} — {e}",
            query=user_query,
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

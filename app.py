import os
import json
import logging
import time
import re
import csv
import io
from datetime import datetime

import boto3
from botocore.config import Config
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    jsonify,
    Response,
)
from dotenv import load_dotenv
import httpx

# -------------------------------------------------------------------
# ENV + LOGGING SETUP
# -------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = (
    os.getenv("AWS_DEFAULT_REGION")
    or os.getenv("AWS_REGION")
    or "us-east-2"
)

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip()

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Charlotte69")
SECRET_KEY = os.getenv("SECRET_KEY") or os.urandom(24).hex()

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

app = Flask(__name__)
app.secret_key = SECRET_KEY

boto_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
s3 = boto_session.client("s3", config=Config(max_pool_connections=10))

COUNTY_INDEX = {}

# -------------------------------------------------------------------
# STATE + COUNTY NORMALIZATION
# -------------------------------------------------------------------

STATE_ALIASES = { ... same as before ... }

def normalize_state_name(text):
    if not text:
        return None
    t = text.strip().lower()
    return STATE_ALIASES.get(t)

def normalize_county_name(name):
    if not name:
        return None
    n = name.lower().strip()
    n = n.replace("_", " ")
    n = re.sub(r"-+", " ", n)
    suffixes = [" county", " parish", " borough", " census area", " municipality", " city"]
    for s in suffixes:
        if n.endswith(s):
            n = n[: -len(s)]
            break
    return n.strip() or None


# -------------------------------------------------------------------
# BOOT: BUILD COUNTY INDEX
# -------------------------------------------------------------------

def build_county_index():
    global COUNTY_INDEX
    logging.info("[BOOT] Indexing county CSVs...")

    total = 0
    paginator = s3.get_paginator("list_objects_v2")

    prefix = S3_PREFIX
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue

            rel = key[len(prefix):] if prefix else key
            parts = rel.split("/")
            if len(parts) != 2:
                continue

            state = parts[0].lower()
            county_norm = normalize_county_name(parts[1][:-4])
            if not county_norm:
                continue

            COUNTY_INDEX.setdefault(state, {})
            COUNTY_INDEX[state][county_norm] = {
                "key": key,
                "size": obj["Size"],
            }
            total += 1

    logging.info("[BOOT] Indexed %d datasets", total)

build_county_index()

# -------------------------------------------------------------------
# PARSING HELPERS
# -------------------------------------------------------------------

def parse_query_location(query):
    q = query.lower().split()
    county_idx = None
    for i, tok in enumerate(q):
        if tok in ("county", "parish", "borough"):
            county_idx = i
            break
    if county_idx is None:
        return None, None

    # find the token after "in"
    pin = None
    for i in range(county_idx - 1, -1, -1):
        if q[i] == "in":
            pin = i
            break
    if pin is None:
        return None, None

    county_phrase = " ".join(q[pin + 1:county_idx]).strip()
    county_norm = normalize_county_name(county_phrase)
    if not county_norm:
        return None, None

    # find state after county
    state_code = None
    for t in q[county_idx + 1:]:
        st = normalize_state_name(t.strip(",. "))
        if st:
            state_code = st
            break

    return state_code, county_norm


def parse_numeric_filters(query):
    q = query.lower()
    min_income = None
    min_value = None

    m = re.search(r"over\s+(\d+(?:\.\d+)?)(k|m)?", q)
    if m:
        num, suffix = m.groups()
        num = float(num)
        if suffix == "k":
            num *= 1000
        elif suffix == "m":
            num *= 1_000_000

        if "income" in q:
            min_income = num
        elif "value" in q or "home" in q:
            min_value = num

    return min_income, min_value

# -------------------------------------------------------------------
# CSV HELPERS
# -------------------------------------------------------------------

def load_county_rows(state, county_norm):
    state = state.lower()
    county_norm = county_norm.lower()

    state_map = COUNTY_INDEX.get(state)
    if not state_map:
        return None, None

    meta = state_map.get(county_norm)
    if not meta:
        for k, v in state_map.items():
            if county_norm in k or k in county_norm:
                meta = v
                break

    if not meta:
        return None, None

    key = meta["key"]
    logging.info("[RESOLVE] S3 key=%s", key)

    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    text_stream = io.TextIOWrapper(obj["Body"], encoding="utf-8")
    reader = csv.DictReader(text_stream)
    return reader, meta


# -------------------------------------------------------------------
# GOOGLE PLACES (COMMERCIAL)
# -------------------------------------------------------------------

def run_commercial_search(query):
    if not GOOGLE_MAPS_API_KEY:
        return False, "GOOGLE_MAPS_API_KEY missing", []

    try:
        resp = httpx.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": query, "key": GOOGLE_MAPS_API_KEY},
            timeout=15,
        )
        data = resp.json()
        if data.get("status") not in ("OK", "ZERO_RESULTS"):
            return False, f"Google status {data.get('status')}", []

        base = data.get("results", [])
        if not base:
            return False, "No matching businesses found.", []

        output = []
        for item in base[:20]:
            place_id = item.get("place_id")
            details = {"Phone": "N/A", "Website": "N/A", "Hours": ""}

            if place_id:
                try:
                    d = httpx.get(
                        "https://maps.googleapis.com/maps/api/place/details/json",
                        params={
                            "place_id": place_id,
                            "fields": "international_phone_number,website,opening_hours",
                            "key": GOOGLE_MAPS_API_KEY,
                        },
                        timeout=15,
                    ).json().get("result", {})

                    details["Phone"] = d.get("international_phone_number") or "N/A"
                    details["Website"] = d.get("website") or "N/A"
                    weekday = d.get("opening_hours", {}).get("weekday_text")
                    if weekday:
                        details["Hours"] = "; ".join(weekday)
                except:
                    pass

            output.append({
                "Name": item.get("name", ""),
                "Address": item.get("formatted_address", ""),
                "Phone": details["Phone"],
                "Website": details["Website"],
                "Hours": details["Hours"],
            })

        return True, None, output

    except Exception as e:
        logging.exception("Places API error")
        return False, str(e), []


# -------------------------------------------------------------------
# AUTH
# -------------------------------------------------------------------

def require_login():
    return bool(session.get("authenticated"))

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == ACCESS_PASSWORD:
            session["authenticated"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Incorrect password")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# -------------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------------

@app.route("/", methods=["GET"])
def index():
    if not require_login():
        return redirect(url_for("login"))

    mode = request.args.get("mode", "residential")
    if mode not in ("residential", "commercial"):
        mode = "residential"

    return render_template(
        "index.html",
        mode=mode,
        query="",
        location=None,
        results=[],
        total=None,
        shown=0,
        error=None,
        download_available=False,
        commercial_results=[],
        commercial_error=None,
    )

@app.route("/search", methods=["POST"])
def search():
    if not require_login():
        return redirect(url_for("login"))

    mode = request.form.get("mode", "residential")
    if mode not in ("residential", "commercial"):
        mode = "residential"

    query = (request.form.get("query") or "").strip()

    # ---------------------------------------------------------
    # COMMERCIAL MODE — DOES NOT TOUCH RESIDENTIAL LOGIC
    # ---------------------------------------------------------
    if mode == "commercial":
        ok, err, results = run_commercial_search(query)
        return render_template(
            "index.html",
            mode="commercial",
            query=query,
            location=None,
            results=[],
            total=None,
            shown=0,
            error=None,
            download_available=False,
            commercial_results=results if ok else [],
            commercial_error=err,
        )

    # ---------------------------------------------------------
    # RESIDENTIAL MODE (S3 CSV)
    # ---------------------------------------------------------
    state_code, county_norm = parse_query_location(query)
    if not state_code or not county_norm:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            location=None,
            results=[],
            total=None,
            shown=0,
            error=(
                "Couldn't resolve a (county, state). Use: "
                "'residential addresses in Wakulla County Florida'"
            ),
            download_available=False,
            commercial_results=[],
            commercial_error=None,
        )

    row_iter, meta = load_county_rows(state_code, county_norm)
    if not row_iter:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            location=None,
            results=[],
            total=None,
            shown=0,
            error=f"No dataset found for {county_norm.title()} County, {state_code.upper()}",
            download_available=False,
        )

    min_income, min_value = parse_numeric_filters(query)

    shown, matches = 0, 0
    preview = []

    for props in row_iter:
        matches += 1

        inc = props.get("median_income")
        val = props.get("median_home_value")

        try:
            inc = float(inc) if inc not in (None, "", "-666666666") else None
        except:
            inc = None

        try:
            val = float(val) if val not in (None, "", "-666666666") else None
        except:
            val = None

        if min_income and (not inc or inc < min_income):
            continue
        if min_value and (not val or val < min_value):
            continue

        basic = {
            "address": props.get("address"),
            "number": props.get("NUMBER") or props.get("number"),
            "street": props.get("STREET") or props.get("street"),
            "city": props.get("CITY") or props.get("city"),
            "state": props.get("REGION") or props.get("state"),
            "zip": props.get("POSTCODE") or props.get("zip"),
            "income": inc,
            "home_value": val,
        }

        if len(preview) < 20:
            preview.append(basic)
            shown += 1

    return render_template(
        "index.html",
        mode="residential",
        query=query,
        location=f"{county_norm.title()} County, {state_code.upper()}",
        results=preview,
        total=matches,
        shown=shown,
        error=None if matches else "No matching addresses.",
        download_available=(shown > 0),
        commercial_results=[],
        commercial_error=None,
    )


# -------------------------------------------------------------------
# CSV DOWNLOAD
# -------------------------------------------------------------------

@app.route("/download", methods=["POST"])
def download_csv():
    if not require_login():
        return redirect(url_for("login"))

    query = request.form.get("query", "")
    state_code, county_norm = parse_query_location(query)
    if not state_code or not county_norm:
        return redirect(url_for("index"))

    row_iter, meta = load_county_rows(state_code, county_norm)
    if not row_iter:
        return redirect(url_for("index"))

    def generate():
        buffer = io.StringIO()
        writer = None

        for props in row_iter:
            if writer is None:
                writer = csv.DictWriter(buffer, fieldnames=list(props.keys()))
                writer.writeheader()
                yield buffer.getvalue()
                buffer.seek(0)
                buffer.truncate(0)

            writer.writerow(props)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    filename = f"{county_norm}_{state_code}.csv"

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

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
# ENV + LOGGING
# -------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip()

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Charlotte69")
SECRET_KEY = os.getenv("SECRET_KEY") or os.urandom(24).hex()

app = Flask(__name__)
app.secret_key = SECRET_KEY

boto_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
s3 = boto_session.client("s3", config=Config(max_pool_connections=10))

# (state -> county_norm -> metadata)
COUNTY_INDEX = {}


# -------------------------------------------------------------------
# STATE + COUNTY NORMALIZATION
# -------------------------------------------------------------------

STATE_ALIASES = {
    # abbreviations
    "al": "al","ak":"ak","az":"az","ar":"ar","ca":"ca","co":"co","ct":"ct","de":"de",
    "fl":"fl","ga":"ga","hi":"hi","id":"id","il":"il","in":"in","ia":"ia","ks":"ks",
    "ky":"ky","la":"la","me":"me","md":"md","ma":"ma","mi":"mi","mn":"mn","ms":"ms",
    "mo":"mo","mt":"mt","ne":"ne","nv":"nv","nh":"nh","nj":"nj","nm":"nm","ny":"ny",
    "nc":"nc","nd":"nd","oh":"oh","ok":"ok","or":"or","pa":"pa","ri":"ri","sc":"sc",
    "sd":"sd","tn":"tn","tx":"tx","ut":"ut","vt":"vt","va":"va","wa":"wa","wv":"wv",
    "wi":"wi","wy":"wy","dc":"dc","pr":"pr",

    # full names
    "alabama":"al","alaska":"ak","arizona":"az","arkansas":"ar","california":"ca",
    "colorado":"co","connecticut":"ct","delaware":"de","florida":"fl","georgia":"ga",
    "hawaii":"hi","idaho":"id","illinois":"il","indiana":"in","iowa":"ia","kansas":"ks",
    "kentucky":"ky","louisiana":"la","maine":"me","maryland":"md","massachusetts":"ma",
    "michigan":"mi","minnesota":"mn","mississippi":"ms","missouri":"mo","montana":"mt",
    "nebraska":"ne","nevada":"nv","new hampshire":"nh","new jersey":"nj","new mexico":"nm",
    "new york":"ny","north carolina":"nc","north dakota":"nd","ohio":"oh",
    "oklahoma":"ok","oregon":"or","pennsylvania":"pa","rhode island":"ri",
    "south carolina":"sc","south dakota":"sd","tennessee":"tn","texas":"tx","utah":"ut",
    "vermont":"vt","virginia":"va","washington":"wa","west virginia":"wv","wisconsin":"wi",
    "wyoming":"wy","district of columbia":"dc","puerto rico":"pr",
}


def normalize_state_name(t: str | None):
    if not t:
        return None
    t = t.strip().lower()
    return STATE_ALIASES.get(t.strip(",. "))


def normalize_county_name(name: str | None):
    if not name:
        return None
    n = name.lower().strip()
    n = n.replace("_", " ")
    n = re.sub(r"-+", " ", n)
    for suf in [" county"," parish"," borough"," census area"," municipality"," city"]:
        if n.endswith(suf):
            n = n[: -len(suf)]
            break
    return n.strip()


# -------------------------------------------------------------------
# BOOT: BUILD INDEX (RUN ON IMPORT)
# -------------------------------------------------------------------

def build_county_index():
    """Scan S3 for CSV ≤ county-level."""
    global COUNTY_INDEX
    COUNTY_INDEX = {}

    prefix = S3_PREFIX
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    logging.info("[BOOT] Indexing county CSVs...")

    paginator = s3.get_paginator("list_objects_v2")
    total = 0

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".csv"):
                continue

            rel = key[len(prefix):] if prefix else key
            parts = rel.split("/")

            # Expect state/filename.csv
            if len(parts) != 2:
                continue

            st = parts[0].lower()
            base = parts[1][:-4]  # remove .csv

            county_norm = normalize_county_name(base)
            if not county_norm:
                continue

            COUNTY_INDEX.setdefault(st, {})
            COUNTY_INDEX[st][county_norm] = {
                "key": key,
                "size": obj["Size"],
                "raw_name": base,
            }
            total += 1

    logging.info("[BOOT] Indexed %d datasets", total)


# build index once on startup
build_county_index()


# -------------------------------------------------------------------
# QUERY PARSING
# -------------------------------------------------------------------

def parse_query_location(query: str):
    q = query.lower().split()
    county_idx = None

    for i, tok in enumerate(q):
        if tok in ("county","parish","borough"):
            county_idx = i
            break
    if county_idx is None:
        return None, None

    # find last "in" before county
    last_in = None
    for i in range(county_idx - 1, -1, -1):
        if q[i] == "in":
            last_in = i
            break
    if last_in is None:
        return None, None

    county_phrase = " ".join(q[last_in+1:county_idx]).strip()
    county_norm = normalize_county_name(county_phrase)
    if not county_norm:
        return None, None

    # find state after county
    for t in q[county_idx+1:]:
        st = normalize_state_name(t)
        if st:
            return st, county_norm

    return None, None


def parse_numeric_filters(query: str):
    q = query.lower()
    min_income = None
    min_value = None

    m = re.search(r"over\s+(\d+(?:\.\d+)?)(k|m)?", q)
    if m:
        num, suf = m.groups()
        val = float(num)
        if suf == "k":
            val *= 1000
        elif suf == "m":
            val *= 1_000_000

        if "income" in q:
            min_income = val
        elif "value" in q or "home" in q:
            min_value = val

    return min_income, min_value


# -------------------------------------------------------------------
# RESIDENTIAL HELPERS
# -------------------------------------------------------------------

def get_income(props):
    for k in ["income","median_income","acs_income","HHINC","MEDHHINC","DP03_0062E"]:
        if k in props and props[k] not in (None,"","-666666666"):
            try:
                return float(props[k])
            except:
                pass
    return None


def get_home_value(props):
    for k in ["home_value","median_home_value","acs_value","med_home_value","DP04_0089E"]:
        if k in props and props[k] not in (None,"","-666666666"):
            try:
                return float(props[k])
            except:
                pass
    return None


def extract_basic_fields(props):
    def first(*keys):
        for k in keys:
            if k in props and props[k]:
                return props[k]
        return None

    house = first("house_number","NUMBER","HOUSE","HOUSENUM","number")
    street = first("street","STREET","addr:street","ROAD","RD_NAME")
    city = first("city","CITY")
    state = first("state","STATE","ST")
    zipc = first("zip","ZIP","postal_code","POSTCODE")

    full = first("full_address","address","ADDR_FULL")
    if not full:
        full = " ".join(x for x in [house, street] if x)

    return {
        "address": full,
        "number": house,
        "street": street,
        "city": city,
        "state": state,
        "zip": zipc,
    }


# -------------------------------------------------------------------
# LOADING RESIDENTIAL DATA
# -------------------------------------------------------------------

def load_county_rows(state_code, county_norm):
    st = state_code.lower()
    county_norm = county_norm.lower()

    if st not in COUNTY_INDEX:
        return None, None

    # exact match
    meta = COUNTY_INDEX[st].get(county_norm)

    # fuzzy
    if not meta:
        for key, val in COUNTY_INDEX[st].items():
            if county_norm in key or key in county_norm:
                meta = val
                break

    if not meta:
        return None, None

    key = meta["key"]
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    body = obj["Body"]
    text_stream = io.TextIOWrapper(body, encoding="utf-8")
    reader = csv.DictReader(text_stream)
    return reader, meta


def apply_filters_iter(row_iter, min_income, min_value, filter_residential):
    results = []
    matches = 0
    scanned = 0

    for props in row_iter:
        scanned += 1

        inc = get_income(props)
        val = get_home_value(props)

        if min_income and (inc is None or inc < min_income):
            continue
        if min_value and (val is None or val < min_value):
            continue

        matches += 1
        if len(results) < 20:
            basic = extract_basic_fields(props)
            basic["income"] = inc
            basic["home_value"] = val
            results.append(basic)

        if scanned >= 50000 and len(results) >= 20:
            break

    return results, matches, scanned


# -------------------------------------------------------------------
# COMMERCIAL SEARCH (Google Places)
# -------------------------------------------------------------------

def run_commercial_search(query: str):
    if not GOOGLE_MAPS_API_KEY:
        return False, "Google Maps API key missing.", []

    try:
        resp = httpx.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query":query, "key":GOOGLE_MAPS_API_KEY},
            timeout=15,
        )
        data = resp.json()
        status = data.get("status")
        if status not in ("OK","ZERO_RESULTS"):
            return False, f"Places API status: {status}", []

        out = []
        for item in data.get("results", [])[:20]:
            name = item.get("name","")
            addr = item.get("formatted_address","")
            pid = item.get("place_id")
            phone = "N/A"
            website = "N/A"
            hours = ""

            if pid:
                det = httpx.get(
                    "https://maps.googleapis.com/maps/api/place/details/json",
                    params={
                        "place_id":pid,
                        "fields":"international_phone_number,website,opening_hours",
                        "key":GOOGLE_MAPS_API_KEY
                    },
                    timeout=15,
                ).json().get("result",{})

                phone = det.get("international_phone_number") or "N/A"
                website = det.get("website") or "N/A"
                weekday = det.get("opening_hours",{}).get("weekday_text")
                if weekday:
                    hours = "; ".join(weekday)

            out.append({
                "Name":name,
                "Address":addr,
                "Phone":phone,
                "Website":website,
                "Hours":hours,
            })

        return True, None, out

    except Exception as e:
        logging.exception("Commercial search error")
        return False, str(e), []


# -------------------------------------------------------------------
# AUTH HELPERS
# -------------------------------------------------------------------

def require_login():
    return bool(session.get("authenticated"))


# -------------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"ok":True})


@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        if request.form.get("password","") == ACCESS_PASSWORD:
            session["authenticated"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Incorrect password.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def index():
    if not require_login():
        return redirect(url_for("login"))

    mode = request.args.get("mode","residential")
    if mode not in ("residential","commercial"):
        mode = "residential"

    return render_template(
        "index.html",
        mode=mode,
        query="",
        results=[],
        commercial_results=[],
        commercial_error=None,
        location=None,
        total=None,
        shown=0,
        error=None,
        download_available=False,
    )


@app.route("/search", methods=["POST"])
def search():
    if not require_login():
        return redirect(url_for("login"))

    mode = request.form.get("mode","residential")
    query = (request.form.get("query") or "").strip()

    logging.info("[SEARCH] mode=%s query='%s'", mode, query)

    if mode == "commercial":
        ok, err, results = run_commercial_search(query)
        return render_template(
            "index.html",
            mode="commercial",
            query=query,
            commercial_results=results,
            commercial_error=err,
            results=[],
            error=None,
            location=None,
            total=None,
            shown=0,
            download_available=False,
        )

    # Residential
    if not query:
        return render_template(
            "index.html",
            mode="residential",
            query="",
            results=[],
            error="Please enter a search query with a county and state.",
            commercial_results=[],
            commercial_error=None,
            location=None,
            total=None,
            shown=0,
            download_available=False,
        )

    st, county = parse_query_location(query)
    if not st or not county:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            results=[],
            error=(
                "Couldn't resolve a (county, state). Use: "
                "'residential addresses in Cuyahoga County Ohio'."
            ),
            commercial_results=[],
            commercial_error=None,
            location=None,
            total=None,
            shown=0,
            download_available=False,
        )

    row_iter, meta = load_county_rows(st, county)
    if not row_iter:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            results=[],
            error=f"No dataset found for {county.title()} County, {st.upper()}",
            commercial_results=[],
            commercial_error=None,
            location=None,
            total=None,
            shown=0,
            download_available=False,
        )

    min_income, min_value = parse_numeric_filters(query)

    preview, matches, scanned = apply_filters_iter(
        row_iter,
        min_income=min_income,
        min_value=min_value,
        filter_residential=True
    )

    return render_template(
        "index.html",
        mode="residential",
        query=query,
        results=preview,
        total=matches,
        shown=len(preview),
        location=f"{county.title()} County, {st.upper()}",
        error=None if matches else "No matching results.",
        commercial_results=[],
        commercial_error=None,
        download_available=bool(preview),
    )


@app.route("/download", methods=["POST"])
def download_csv():
    if not require_login():
        return redirect(url_for("login"))

    query = (request.form.get("query") or "").strip()
    st, county = parse_query_location(query)

    row_iter, meta = load_county_rows(st, county)
    if not row_iter:
        return "Dataset missing", 400

    min_income, min_value = parse_numeric_filters(query)

    # cache rows
    cache = list(row_iter)

    fieldnames = set()
    for r in cache:
        fieldnames.update(r.keys())

    fieldnames = list(fieldnames)

    def generate():
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for props in cache:
            inc = get_income(props)
            val = get_home_value(props)

            if min_income and (inc is None or inc < min_income):
                continue
            if min_value and (val is None or val < min_value):
                continue

            writer.writerow(props)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    fname = f"{county}_{st}_{int(time.time())}.csv"

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition":f'attachment; filename="{fname}"'}
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

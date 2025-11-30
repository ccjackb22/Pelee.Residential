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
import requests  # For commercial lookups

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
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs_clean/")

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Charlotte69")
SECRET_KEY = os.getenv("SECRET_KEY") or os.urandom(24).hex()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

app = Flask(__name__)
app.secret_key = SECRET_KEY

boto_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
s3 = boto_session.client("s3", config=Config(max_pool_connections=10))

COUNTY_INDEX: dict[str, dict[str, dict]] = {}

# -------------------------------------------------------------------
# STATE + COUNTY NORMALIZATION
# (UNCHANGED FROM YOUR WORKING VERSION)
# -------------------------------------------------------------------

STATE_ALIASES = {
    "al": "al","ak": "ak","az": "az","ar": "ar","ca": "ca",
    "co": "co","ct": "ct","de": "de","fl": "fl","ga": "ga",
    "hi": "hi","id": "id","il": "il","in": "in","ia": "ia",
    "ks": "ks","ky": "ky","la": "la","me": "me","md": "md",
    "ma": "ma","mi": "mi","mn": "mn","ms": "ms","mo": "mo",
    "mt": "mt","ne": "ne","nv": "nv","nh": "nh","nj": "nj",
    "nm": "nm","ny": "ny","nc": "nc","nd": "nd","oh": "oh",
    "ok": "ok","or": "or","pa": "pa","ri": "ri","sc": "sc",
    "sd": "sd","tn": "tn","tx": "tx","ut": "ut","vt": "vt",
    "va": "va","wa": "wa","wv": "wv","wi": "wi","wy": "wy",
    "dc": "dc","pr": "pr",
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca","colorado": "co","connecticut": "ct","delaware": "de",
    "florida": "fl","georgia": "ga","hawaii": "hi","idaho": "id",
    "illinois": "il","indiana": "in","iowa": "ia","kansas": "ks",
    "kentucky": "ky","louisiana": "la","maine": "me","maryland": "md",
    "massachusetts": "ma","michigan": "mi","minnesota": "mn","mississippi": "ms",
    "missouri": "mo","montana": "mt","nebraska": "ne","nevada": "nv",
    "new hampshire": "nh","new jersey": "nj","new mexico": "nm","new york": "ny",
    "north carolina": "nc","north dakota": "nd","ohio": "oh","oklahoma": "ok",
    "oregon": "or","pennsylvania": "pa","rhode island": "ri","south carolina": "sc",
    "south dakota": "sd","tennessee": "tn","texas": "tx","utah": "ut",
    "vermont": "vt","virginia": "va","washington": "wa","west virginia": "wv",
    "wisconsin": "wi","wyoming": "wy","district of columbia": "dc",
    "puerto rico": "pr",
}

def normalize_state_name(text):
    if not text:
        return None
    t = text.strip().lower()
    t = t.strip(",. ")
    return STATE_ALIASES.get(t)

def normalize_county_name(name):
    if not name:
        return None
    n = name.strip().lower()
    n = n.replace("_", " ")
    n = re.sub(r"[-]+", " ", n)
    suffixes = [
        " county"," parish"," borough"," census area"," municipality"," city",
    ]
    for suf in suffixes:
        if n.endswith(suf):
            n = n[: -len(suf)]
            break
    return n.strip()

# -------------------------------------------------------------------
# BUILD COUNTY INDEX (UNCHANGED)
# -------------------------------------------------------------------

def build_county_index():
    global COUNTY_INDEX
    logging.info("[BOOT] scanning S3…")

    paginator = s3.get_paginator("list_objects_v2")
    total = 0

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            rel = key[len(S3_PREFIX):]
            parts = rel.split("/")
            if len(parts) != 2:
                continue

            state = parts[0].lower()
            filename = parts[1]
            base = filename[: -len("-clean.geojson")]
            norm = normalize_county_name(base)
            if not norm:
                continue

            COUNTY_INDEX.setdefault(state, {})
            COUNTY_INDEX[state][norm] = {
                "key": key,
                "size": obj["Size"],
                "raw_name": base,
            }
            total += 1

    logging.info("[BOOT] Indexed %d datasets", total)

build_county_index()

# -------------------------------------------------------------------
# QUERY PARSING (UNCHANGED)
# -------------------------------------------------------------------

def parse_query_location(query):
    q = query.lower()
    tokens = q.split()

    county_idx = None
    for i, tok in enumerate(tokens):
        if tok in ("county", "parish", "borough"):
            county_idx = i
            break
    if county_idx is None:
        return None, None

    last_in = None
    for i in range(county_idx - 1, -1, -1):
        if tokens[i] == "in":
            last_in = i
            break
    if last_in is None:
        last_in = -1

    county_tokens = tokens[last_in + 1: county_idx]
    county_phrase = " ".join(county_tokens).strip()
    county_norm = normalize_county_name(county_phrase)
    if not county_norm:
        return None, None

    state_code = None
    for t in tokens[county_idx + 1:]:
        st = normalize_state_name(t.strip(",. "))
        if st:
            state_code = st
            break

    return state_code, county_norm

def parse_numeric_filters(query):
    q = query.lower()

    def parse_amount(m):
        num_str, suffix = m.groups()
        try:
            val = float(num_str)
        except:
            return None
        if suffix == "k":
            val *= 1000
        elif suffix == "m":
            val *= 1_000_000
        return val

    min_income = None
    min_value = None

    m_over = re.search(r"over\s+(\d+(?:\.\d+)?)(k|m)?", q)
    if m_over:
        amount = parse_amount(m_over)
        if amount:
            if "income" in q:
                min_income = amount
            elif "value" in q or "home" in q:
                min_value = amount

    return min_income, min_value

# -------------------------------------------------------------------
# FEATURE HELPERS (unchanged, full fidelity)
# -------------------------------------------------------------------

def get_income(props):
    keys = ["income","median_income","acs_income","HHINC","MEDHHINC","DP03_0062E"]
    for k in keys:
        if k in props:
            v = props.get(k)
            if v not in ("", None, "-666666666"):
                try: return float(v)
                except: pass
    return None

def get_home_value(props):
    keys = ["home_value","median_home_value","acs_value","med_home_value","DP04_0089E"]
    for k in keys:
        if k in props:
            v = props.get(k)
            if v not in ("", None, "-666666666"):
                try: return float(v)
                except: pass
    return None

def extract_basic_fields(props):
    def first(*keys):
        for k in keys:
            if k and props.get(k):
                return props.get(k)
        return None

    house = first("house_number","HOUSE_NUM","addr:housenumber","HOUSE","HOUSENUM","number")
    street = first("street","STREET","addr:street","ROAD","RD_NAME")
    city = first("city","CITY","City")
    state = first("state","STATE","ST","STUSPS","STATE_NAME","region")
    postal = first("zip","ZIP","postal_code","POSTCODE","ZIPCODE","ZIP_CODE")

    addr = first("full_address","address","ADDR_FULL")
    if not addr:
        parts = [str(house).strip() if house else None, street]
        addr = " ".join([p for p in parts if p])

    return {
        "address": addr,
        "number": house,
        "street": street,
        "city": city,
        "state": state,
        "zip": postal,
    }

def is_residential(props):
    NON = [
        "farm","agric","industrial","warehouse","office","retail","church",
        "school","gov","hospital","hotel","motel","vacant","land only",
    ]
    RES = [
        "res","single fam","sfr","sfh","duplex","triplex","quadplex",
        "townhome","condo","apartment","mobile home",
    ]

    for v in props.values():
        if not isinstance(v, str):
            continue
        t = v.lower()
        if any(x in t for x in NON):
            return False

    for v in props.values():
        if not isinstance(v, str): continue
        if any(x in v.lower() for x in RES):
            return True

    return True

def apply_filters_iter(features, min_income=None, min_value=None,
                       filter_residential=True, max_results=20,
                       max_scan=50000):

    results = []
    matches = 0
    scanned = 0

    for feat in features:
        scanned += 1
        props = feat.get("properties", {}) or {}

        if filter_residential and not is_residential(props):
            continue

        inc = get_income(props)
        val = get_home_value(props)

        if min_income is not None:
            if inc is None or inc < min_income:
                continue

        if min_value is not None:
            if val is None or val < min_value:
                continue

        matches += 1
        if len(results) < max_results:
            basic = extract_basic_fields(props)
            basic["income"] = inc
            basic["home_value"] = val
            results.append(basic)

        if scanned >= max_scan and len(results) >= max_results:
            break

    return results, matches, scanned

# -------------------------------------------------------------------
# LOAD COUNTY FEATURES (unchanged)
# -------------------------------------------------------------------

def load_county_features(state_code, county_norm):
    state_code = state_code.lower()
    county_norm = county_norm.lower()

    state_map = COUNTY_INDEX.get(state_code)
    if not state_map:
        return None, None

    meta = state_map.get(county_norm)
    if not meta:
        for cand, cand_meta in state_map.items():
            if county_norm in cand or cand in county_norm:
                meta = cand_meta
                break

    if not meta:
        return None, None

    key = meta["key"]
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = json.loads(obj["Body"].read())
    feats = data.get("features") or []
    return feats, meta

# -------------------------------------------------------------------
# AUTH HELPERS
# -------------------------------------------------------------------

def require_login():
    return bool(session.get("authenticated"))

# -------------------------------------------------------------------
# ROUTES: health, login, logout, index
# -------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"ok": True})

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password","")
        if pw == ACCESS_PASSWORD:
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
    return render_template("index.html")

# -------------------------------------------------------------------
# RESIDENTIAL SEARCH  (UNTOUCHED — EXACTLY YOUR WORKING VERSION)
# -------------------------------------------------------------------

@app.route("/search", methods=["POST"])
def search():
    if not require_login():
        return jsonify({"ok": False, "error": "Not logged in."})

    query = (request.form.get("query") or "").strip()
    logging.info("[SEARCH] '%s'", query)

    if not query:
        return jsonify({"ok": False, "error": "Enter a query."})

    state_code, county_norm = parse_query_location(query)
    if not state_code or not county_norm:
        return jsonify({"ok": False, "error": "Could not parse county & state."})

    feats, meta = load_county_features(state_code, county_norm)
    if feats is None:
        return jsonify({"ok": False, "error": "County dataset not found."})

    min_income, min_value = parse_numeric_filters(query)
    filter_res = any(w in query.lower() for w in ["res","home","house"])

    preview, matches, scanned = apply_filters_iter(
        feats, min_income=min_income, min_value=min_value,
        filter_residential=filter_res
    )

    return jsonify({
        "ok": True,
        "results": preview,
        "total": matches,
        "location": f"{county_norm.title()} County, {state_code.upper()}",
    })

# -------------------------------------------------------------------
# COMMERCIAL SEARCH (NEW)
# -------------------------------------------------------------------

@app.route("/commercial_search", methods=["POST"])
def commercial_search():
    if not require_login():
        return jsonify({"ok": False, "error": "Not logged in."})

    query = (request.json or {}).get("query","").strip()
    if not query:
        return jsonify({"ok": False, "error": "Enter a query."})

    if not GOOGLE_API_KEY:
        return jsonify({"ok": False, "error": "Google API key missing."})

    try:
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        params = {"query": query, "key": GOOGLE_API_KEY}

        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        results = []
        for p in data.get("results", []):
            place_id = p.get("place_id")

            details = requests.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={"place_id": place_id, "key": GOOGLE_API_KEY},
                timeout=10
            ).json()

            info = details.get("result", {})
            results.append({
                "Name": info.get("name",""),
                "Address": info.get("formatted_address",""),
                "Phone": info.get("formatted_phone_number",""),
                "Website": info.get("website","N/A"),
                "Hours": ", ".join(info.get("weekday_text", [])),
            })

        return jsonify({"ok": True, "results": results})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

# -------------------------------------------------------------------
# CSV DOWNLOAD (unchanged)
# -------------------------------------------------------------------

@app.route("/download", methods=["POST"])
def download_csv():
    if not require_login():
        return redirect(url_for("login"))

    query = (request.form.get("query") or "").strip()

    state_code, county_norm = parse_query_location(query)
    feats, meta = load_county_features(state_code, county_norm)

    min_income, min_value = parse_numeric_filters(query)
    filter_res = any(w in query.lower() for w in ["res","home","house"])

    friendly_cols = ["address","city","state","zip","income","home_value"]

    prop_keys = set()
    for feat in feats:
        for k in feat.get("properties",{}).keys():
            if k not in friendly_cols:
                prop_keys.add(k)
    fieldnames = friendly_cols + sorted(prop_keys)

    def generate():
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=fieldnames)
        w.writeheader()
        yield buf.getvalue(); buf.seek(0); buf.truncate(0)

        for feat in feats:
            props = feat.get("properties",{})

            if filter_res and not is_residential(props):
                continue

            inc = get_income(props)
            val = get_home_value(props)

            if min_income and (inc is None or inc < min_income):
                continue
            if min_value and (val is None or val < min_value):
                continue

            b = extract_basic_fields(props)

            row = {
                "address": b.get("address",""),
                "city": b.get("city",""),
                "state": b.get("state",""),
                "zip": b.get("zip",""),
                "income": inc if inc is not None else "",
                "home_value": val if val is not None else "",
            }

            for k in sorted(prop_keys):
                row[k] = props.get(k,"")

            w.writerow(row)
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)

    fname = f"{county_norm}_{state_code}_{int(time.time())}.csv".replace(" ","_")
    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"}
    )

# -------------------------------------------------------------------
# ENTRYPOINT
# -------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

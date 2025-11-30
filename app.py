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
    format="%(asctime)s [%(levelname)s] %(message)s",
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION") or "us-east-2"

S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "").strip()

ACCESS_PASSWORD = os.getenv("ACCESS_PASSWORD", "Charlotte69")
SECRET_KEY = os.getenv("SECRET_KEY") or os.urandom(24).hex()

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

app = Flask(__name__)
app.secret_key = SECRET_KEY


# boto
boto_session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
s3 = boto_session.client("s3", config=Config(max_pool_connections=10))

COUNTY_INDEX = {}

# -------------------------------------------------------------------
# STATE / COUNTY NORMALIZATION
# -------------------------------------------------------------------

STATE_ALIASES = {k: k for k in [
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia",
    "ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj",
    "nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut","vt",
    "va","wa","wv","wi","wy","dc","pr"
]}

# full names
FULL = {
    "alabama":"al","alaska":"ak","arizona":"az","arkansas":"ar","california":"ca",
    "colorado":"co","connecticut":"ct","delaware":"de","florida":"fl","georgia":"ga",
    "hawaii":"hi","idaho":"id","illinois":"il","indiana":"in","iowa":"ia",
    "kansas":"ks","kentucky":"ky","louisiana":"la","maine":"me","maryland":"md",
    "massachusetts":"ma","michigan":"mi","minnesota":"mn","mississippi":"ms",
    "missouri":"mo","montana":"mt","nebraska":"ne","nevada":"nv","new hampshire":"nh",
    "new jersey":"nj","new mexico":"nm","new york":"ny","north carolina":"nc",
    "north dakota":"nd","ohio":"oh","oklahoma":"ok","oregon":"or","pennsylvania":"pa",
    "rhode island":"ri","south carolina":"sc","south dakota":"sd","tennessee":"tn",
    "texas":"tx","utah":"ut","vermont":"vt","virginia":"va","washington":"wa",
    "west virginia":"wv","wisconsin":"wi","wyoming":"wy","district of columbia":"dc",
    "puerto rico":"pr"
}

STATE_ALIASES.update(FULL)


def normalize_state(s):
    if not s:
        return None
    s = s.strip().lower().strip(",. ")
    return STATE_ALIASES.get(s)


def normalize_county(c):
    if not c:
        return None
    c = c.strip().lower()
    c = re.sub(r"[_\-]+", " ", c)
    suffixes = [
        " county", " parish", " borough", " census area",
        " municipality", " city"
    ]
    for suf in suffixes:
        if c.endswith(suf):
            c = c[:-len(suf)]
            break
    c = c.strip()
    return c or None


# -------------------------------------------------------------------
# INDEX S3 CSVs
# -------------------------------------------------------------------

def build_county_index():
    global COUNTY_INDEX
    logging.info("[BOOT] Indexing county CSVs...")
    total = 0

    prefix = S3_PREFIX
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    paginator = s3.get_paginator("list_objects_v2")
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
            fname = parts[1]       # e.g. cuyahoga.csv
            county_norm = normalize_county(fname[:-4])

            if not county_norm:
                continue

            COUNTY_INDEX.setdefault(state, {})
            COUNTY_INDEX[state][county_norm] = {
                "key": key,
                "size": obj["Size"],
                "raw": fname[:-4]
            }
            total += 1

    logging.info("[BOOT] Indexed %d datasets", total)

build_county_index()


# -------------------------------------------------------------------
# PARSE RESIDENTIAL LOCATION
# -------------------------------------------------------------------

def parse_query_location(query: str):
    q = query.lower()
    tokens = q.split()

    # find the word "county" / "parish" / etc
    idx = None
    for i, t in enumerate(tokens):
        if t in ("county", "parish", "borough"):
            idx = i
            break

    if idx is None:
        return None, None

    # find "in"
    last_in = None
    for i in range(idx - 1, -1, -1):
        if tokens[i] == "in":
            last_in = i
            break
    if last_in is None:
        last_in = -1

    county_phrase = " ".join(tokens[last_in + 1: idx])
    county_norm = normalize_county(county_phrase)
    if not county_norm:
        return None, None

    # state is after "county"
    state_code = None
    for t in tokens[idx + 1:]:
        st = normalize_state(t)
        if st:
            state_code = st
            break

    return state_code, county_norm


# -------------------------------------------------------------------
# HELPERS FOR RESIDENTIAL
# -------------------------------------------------------------------

def get_income(p):
    for k in ["income","median_income","acs_income","HHINC","MEDHHINC","DP03_0062E"]:
        if k in p:
            v = p.get(k)
            if v not in (None,"","-666666666"):
                try: return float(v)
                except: pass
    return None


def get_value(p):
    for k in ["home_value","median_home_value","acs_value","med_home_value","DP04_0089E"]:
        if k in p:
            v = p.get(k)
            if v not in (None,"","-666666666"):
                try: return float(v)
                except: pass
    return None


def extract_basic(p):
    def first(*keys):
        for k in keys:
            if k in p and p[k]:
                return p[k]
        return None

    num = first("house_number","NUMBER","number")
    street = first("street","STREET","addr:street")
    city = first("city","CITY")
    state = first("state","STATE","STUSPS")
    zipc = first("zip","ZIP","postal_code","POSTCODE")

    address = first("full_address","address")
    if not address:
        address = " ".join([str(num or ""), str(street or "")]).strip()

    return {
        "address": address,
        "number": num,
        "street": street,
        "city": city,
        "state": state,
        "zip": zipc,
    }


def is_residential(p):
    # crude, unchanged from your working version
    TEXT = ""
    for v in p.values():
        if isinstance(v, str):
            TEXT += " " + v.lower()

    NON = ["industrial","warehouse","office","retail","farm","church","school","gov",
           "hospital","hotel","motel","vacant","land only"]

    YES = ["res","home","house","single","condo","town","apt","duplex","triplex","quad"]

    if any(x in TEXT for x in NON) and not any(x in TEXT for x in YES):
        return False
    return True


# -------------------------------------------------------------------
# LOAD A COUNTY CSV STREAM
# -------------------------------------------------------------------

def load_county_rows(state, county):
    state = state.lower()
    county = county.lower()

    sm = COUNTY_INDEX.get(state)
    if not sm:
        return None, None

    meta = sm.get(county)
    if not meta:
        # partial match fallback
        for k, m in sm.items():
            if county in k or k in county:
                meta = m
                break

    if not meta:
        return None, None

    key = meta["key"]
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    reader = csv.DictReader(io.TextIOWrapper(obj["Body"], encoding="utf-8"))
    return reader, meta


# -------------------------------------------------------------------
# COMMERCIAL (GOOGLE PLACES)
# -------------------------------------------------------------------

def run_commercial_search(query: str):
    if not GOOGLE_MAPS_API_KEY:
        return False, "GOOGLE_MAPS_API_KEY not configured.", []

    try:
        # Text search
        r = httpx.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params={"query": query, "key": GOOGLE_MAPS_API_KEY},
            timeout=12.0,
        )
        data = r.json()
        status = data.get("status")

        if status not in ("OK", "ZERO_RESULTS"):
            return False, f"Places API status: {status}", []

        base = data.get("results", [])
        if not base:
            return False, "No matching businesses found.", []

        out = []

        # details
        for item in base[:20]:
            pid = item.get("place_id")
            name = item.get("name","")
            addr = item.get("formatted_address","")
            phone = "N/A"
            website = "N/A"
            hours = ""

            if pid:
                try:
                    d = httpx.get(
                        "https://maps.googleapis.com/maps/api/place/details/json",
                        params={
                            "place_id": pid,
                            "fields": "international_phone_number,website,opening_hours",
                            "key": GOOGLE_MAPS_API_KEY,
                        },
                        timeout=12.0,
                    ).json().get("result", {})

                    phone = d.get("international_phone_number") or "N/A"
                    website = d.get("website") or "N/A"
                    wk = d.get("opening_hours", {}).get("weekday_text")
                    if wk:
                        hours = "; ".join(wk)

                except Exception:
                    pass

            out.append({
                "Name": name,
                "Address": addr,
                "Phone": phone,
                "Website": website,
                "Hours": hours,
            })

        return True, None, out

    except Exception as e:
        logging.exception("Commercial search failed")
        return False, str(e), []


# -------------------------------------------------------------------
# LOGIN REQUIRED
# -------------------------------------------------------------------

def require_login():
    return bool(session.get("authenticated"))


# -------------------------------------------------------------------
# ROUTES
# -------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == ACCESS_PASSWORD:
            session["authenticated"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Incorrect password.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


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

    # DECIDE MODE FROM FORM OR URL
    mode = request.args.get("mode") or request.form.get("mode") or "residential"
    mode = mode if mode in ("residential","commercial") else "residential"

    query = (request.form.get("query") or "").strip()

    logging.info("[SEARCH] mode=%s query='%s'", mode, query)

    if not query:
        return render_template(
            "index.html",
            mode=mode,
            query=query,
            results=[],
            commercial_results=[],
            commercial_error=("Enter a query" if mode=="commercial" else None),
            error=("Enter a residential query" if mode=="residential" else None),
            location=None,
            total=None,
            shown=0,
            download_available=False,
        )

    # COMMERCIAL → NEVER TOUCH RESIDENTIAL LOGIC
    if mode == "commercial":
        ok, err, results = run_commercial_search(query)
        return render_template(
            "index.html",
            mode="commercial",
            query=query,
            results=[],
            commercial_results=results,
            commercial_error=err,
            error=None,
            location=None,
            total=None,
            shown=0,
            download_available=False,
        )

    # RESIDENTIAL BRANCH (unchanged)
    state, county = parse_query_location(query)
    if not state or not county:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            results=[],
            commercial_results=[],
            commercial_error=None,
            error=(
                "Couldn't resolve a (county, state). Example: "
                "'residential addresses in Cuyahoga County Ohio'."
            ),
            location=None,
            total=None,
            shown=0,
            download_available=False,
        )

    row_iter, meta = load_county_rows(state, county)
    if not row_iter:
        return render_template(
            "index.html",
            mode="residential",
            query=query,
            results=[],
            commercial_results=[],
            commercial_error=None,
            error=f"No dataset found for {county.title()} County, {state.upper()}",
            location=None,
            total=None,
            shown=0,
            download_available=False,
        )

    # filters
    ql = query.lower()

    min_income = None
    min_value = None

    m = re.search(r"over\s+(\d+(?:\.\d+)?)(k|m)?", ql)
    if m:
        raw, suf = m.groups()
        try:
            amt = float(raw)
            if suf == "k":
                amt *= 1000
            elif suf == "m":
                amt *= 1_000_000
        except:
            amt = None

        if amt:
            if "income" in ql:
                min_income = amt
            else:
                min_value = amt

    filter_res = any(x in ql for x in ["residential","home","house","homes"])

    results = []
    matches = 0
    scanned = 0
    for p in row_iter:
        scanned += 1
        inc = get_income(p)
        val = get_value(p)

        if filter_res and not is_residential(p):
            continue

        if min_income and (inc is None or inc < min_income):
            continue
        if min_value and (val is None or val < min_value):
            continue

        matches += 1
        if len(results) < 20:
            b = extract_basic(p)
            b["income"] = inc
            b["home_value"] = val
            results.append(b)

        if scanned > 50000 and len(results) >= 20:
            break

    location = f"{county.title()} County, {state.upper()}"
    shown = len(results)

    return render_template(
        "index.html",
        mode="residential",
        query=query,
        results=results,
        commercial_results=[],
        commercial_error=None,
        error=(None if shown > 0 else "No matching addresses found."),
        location=location,
        total=matches,
        shown=shown,
        download_available=(shown > 0),
    )


@app.route("/download", methods=["POST"])
def download_csv():
    if not require_login():
        return redirect(url_for("login"))

    query = (request.form.get("query") or "").strip()
    state, county = parse_query_location(query)
    if not state or not county:
        return redirect(url_for("index"))

    row_iter, meta = load_county_rows(state, county)
    if not row_iter:
        return redirect(url_for("index"))

    ql = query.lower()

    min_income = None
    min_value = None

    m = re.search(r"over\s+(\d+(?:\.\d+)?)(k|m)?", ql)
    if m:
        raw, suf = m.groups()
        amt = float(raw)
        if suf == "k": amt *= 1000
        if suf == "m": amt *= 1_000_000
        if "income" in ql:
            min_income = amt
        else:
            min_value = amt

    filter_res = any(x in ql for x in ["residential","home","house","homes"])

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"{county}_{state}_{ts}.csv"

    def generate():
        buf = io.StringIO()
        w = None

        for p in row_iter:
            if filter_res and not is_residential(p):
                continue

            inc = get_income(p)
            val = get_value(p)

            if min_income and (inc is None or inc < min_income):
                continue
            if min_value and (val is None or val < min_value):
                continue

            base = extract_basic(p)
            row = {
                "address": base["address"],
                "city": base["city"],
                "state": base["state"],
                "zip": base["zip"],
                "income": "" if inc is None else f"{inc:.0f}",
                "home_value": "" if val is None else f"{val:.0f}",
            }

            if w is None:
                fields = list(row.keys()) + sorted(k for k in p.keys() if k not in row)
                w = csv.DictWriter(buf, fieldnames=fields)
                w.writeheader()
                yield buf.getvalue()
                buf.seek(0); buf.truncate(0)

            for k in p:
                if k not in row:
                    row[k] = p[k]

            w.writerow(row)
            yield buf.getvalue()
            buf.seek(0); buf.truncate(0)

    return Response(
        generate(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

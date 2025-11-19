import os
import io
import json
import csv
import zipfile
import re

from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    redirect,
    url_for,
    session,
    send_file,
)
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")

MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

_STATE_KEYS_CACHE = {}

# ---------------------------------------------------------
# LOOKUP TABLES
# ---------------------------------------------------------

US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}

# Lowercase → uppercase
STATE_CODES = {code.lower(): code for code in US_STATES.values()}

# City → county helpers
CITY_TO_COUNTY = {
    "wa": {
        "vancouver": ("clark", "Vancouver"),
    },
    "oh": {
        "strongsville": ("cuyahoga", "Strongsville"),
        "berea": ("cuyahoga", "Berea"),
        "rocky river": ("cuyahoga", "Rocky River"),
        "lakewood": ("cuyahoga", "Lakewood"),
        "westlake": ("cuyahoga", "Westlake"),
        "bay village": ("cuyahoga", "Bay Village"),
        "avon": ("lorain", "Avon"),
        "avon lake": ("lorain", "Avon Lake"),
    },
    "tx": {
        # can add Houston, etc. later if you want city-level TX defaults
    },
}

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------


def norm(s):
    return " ".join((s or "").lower().strip().split())


def canonical_county(name: str) -> str:
    if not name:
        return ""
    name = name.lower().replace("_", " ").replace("-", " ")
    name = name.replace(" county", "").replace(" parish", "")
    return " ".join(name.split())


def detect_state(query: str):
    """
    Don't treat random words like 'me', 'in', 'or' as states.

    Strategy:
      1) Match full state names as whole words (e.g. 'texas').
      2) Match TWO-LETTER ALL-CAPS tokens in the ORIGINAL string
         (e.g. 'TX', 'FL', 'WA'), not lowercase words.
    """

    q_lower = query.lower()

    # 1) Full names, whole-word match
    for fullname, code in US_STATES.items():
        if re.search(r"\b" + re.escape(fullname) + r"\b", q_lower):
            return code

    # 2) Two-letter ALL CAPS codes in original text, like "TX"
    for m in re.finditer(r"\b([A-Z]{2})\b", query):
        t = m.group(1).lower()
        if t in STATE_CODES:
            return STATE_CODES[t]

    return None


def detect_zip(query: str):
    m = re.search(r"\b(\d{5})\b", query)
    return m.group(1) if m else None


def parse_filters(q: str):
    q = q.lower().replace("$", "").replace(",", "")
    toks = q.split()

    income_min = None
    value_min = None

    for i, t in enumerate(toks):
        if t in ("over", "above") and i + 1 < len(toks):
            raw = toks[i + 1]
            mult = 1
            if raw.endswith("k"):
                mult = 1000
                raw = raw[:-1]
            if raw.endswith("m"):
                mult = 1_000_000
                raw = raw[:-1]

            try:
                val = float(raw) * mult
            except:
                continue

            window = " ".join(toks[max(0, i - 5): i + 5])
            if "income" in window or "household" in window:
                income_min = val
                continue
            if "value" in window or "home" in window or "properties" in window:
                value_min = val
                continue

            # fallback → assume income
            if income_min is None:
                income_min = val

    return income_min, value_min


def parse_location(query: str):
    q = norm(query)
    tokens = q.split()

    state = detect_state(query)
    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    county = None
    city = None

    # "___ County ___"
    if "county" in tokens:
        idx = tokens.index("county")
        if idx > 0:
            county = tokens[idx - 1]

    # City → county mapping, if we know this combo
    if state:
        st_key = state.lower()
        city_map = CITY_TO_COUNTY.get(st_key, {})
        for cname, (c_county, pretty) in city_map.items():
            if cname in q:
                county = c_county
                city = pretty
                break

    # Special: Ohio "Strongsville Ohio" / "Berea OH" → Cuyahoga
    if not county and state == "OH":
        for i, t in enumerate(tokens):
            if t in ("oh", "ohio"):
                if i > 0:
                    city = tokens[i - 1]
                county = "cuyahoga"
                break

    return state, county, city, zip_code, income_min, value_min


def list_keys(state: str):
    if not state:
        return []

    state = state.lower()

    if state in _STATE_KEYS_CACHE:
        return _STATE_KEYS_CACHE[state]

    prefix = f"{S3_PREFIX}/{state}/"
    keys = []
    token = None

    while True:
        kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        try:
            resp = s3_client.list_objects_v2(**kwargs)
        except ClientError as e:
            app.logger.error(f"S3 list_objects_v2 failed for state={state}: {e}")
            break

        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".geojson"):
                keys.append(k)

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    _STATE_KEYS_CACHE[state] = keys
    return keys


def _strip_known_suffixes(base: str) -> str:
    """
    Take a filename without extension and remove common county/address suffixes
    so 'harris-with-values-income' or 'harris-parcels-county-with-tracts'
    both reduce to 'harris'.
    """
    suffixes = [
        "-with-values-income",
        "-parcels-county-with-tracts",
        "-addresses-county-with-tracts",
        "-buildings-county-with-tracts",
        "-parcels-county",
        "-addresses-county",
        "-buildings-county",
        "-parcels",
        "-addresses",
        "-buildings",
        "-county",
    ]
    name = base
    for suf in suffixes:
        if name.endswith(suf):
            name = name[: -len(suf)]
    return name


def pick_dataset(state: str, county: str):
    if not state or not county:
        return None

    state = state.lower()
    county = canonical_county(county)  # e.g. "harris"

    keys = list_keys(state)
    if not keys:
        app.logger.warning(f"No keys found in S3 for state={state}")
        return None

    exact_candidates = []
    fuzzy_candidates = []

    for k in keys:
        base = os.path.basename(k).replace(".geojson", "")
        core = _strip_known_suffixes(base)
        core_clean = canonical_county(core)

        # strict match on cleaned base
        if core_clean == county:
            exact_candidates.append(k)
        # fallback: filename contains the county name somewhere
        elif county in base.lower():
            fuzzy_candidates.append(k)

    # Prefer exact match (with enriched versions first)
    def prefer_enriched(cands):
        for c in cands:
            if "with-values-income" in c:
                return c
        return cands[0] if cands else None

    choice = prefer_enriched(exact_candidates)
    if choice:
        app.logger.info(f"pick_dataset: exact match for county={county}, state={state} -> {choice}")
        return choice

    choice = prefer_enriched(fuzzy_candidates)
    if choice:
        app.logger.info(f"pick_dataset: fuzzy match for county={county}, state={state} -> {choice}")
        return choice

    app.logger.warning(f"pick_dataset: no match for county={county}, state={state}")
    return None


def load_geojson(key: str):
    try:
        data = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        return json.loads(data)
    except Exception as e:
        app.logger.error(f"S3 load error {key}: {e}")
        raise


def feature_to_obj(f):
    props = f.get("properties", {})
    geom = f.get("geometry", {}).get("coordinates", [None, None])

    return {
        "address": " ".join(
            str(x) for x in [
                props.get("number") or props.get("house_number") or "",
                props.get("street") or props.get("road") or "",
                props.get("unit") or "",
            ] if x
        ),
        "city": props.get("city") or "",
        "state": props.get("region") or props.get("STUSPS") or "",
        "zip": props.get("postcode") or "",
        "income": props.get("DP03_0062E") or props.get("median_income") or props.get("income"),
        "value": props.get("DP04_0089E") or props.get("median_value") or props.get("home_value"),
        "lat": geom[1],
        "lon": geom[0],
    }


def apply_filters(features, income_min, value_min, zip_code):
    out = []
    for f in features:
        p = f.get("properties", {})

        # ZIP filter
        if zip_code:
            if str(p.get("postcode")) != str(zip_code):
                continue

        # income filter
        if income_min:
            v = p.get("DP03_0062E") or p.get("median_income") or p.get("income")
            try:
                if v is None or float(v) < income_min:
                    continue
            except:
                continue

        # value filter
        if value_min:
            v = p.get("DP04_0089E") or p.get("median_value") or p.get("home_value")
            try:
                if v is None or float(v) < value_min:
                    continue
            except:
                continue

        out.append(f)
    return out


# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
            session["authed"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Invalid password.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def index():
    if not session.get("authed"):
        return redirect(url_for("login"))
    return render_template("index.html")


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    state, county, city, zip_code, income_min, value_min = parse_location(query)

    app.logger.info(
        f"[search] q={query!r} → state={state}, county={county}, city={city}, zip={zip_code}, "
        f"income_min={income_min}, value_min={value_min}"
    )

    if not state:
        return jsonify({"ok": False, "error": "No state detected."})
    if not county:
        return jsonify({"ok": False, "error": "No county detected."})

    key = pick_dataset(state, county)
    if not key:
        return jsonify({"ok": False, "error": f"No dataset found for {county} County, {state}."})

    try:
        gj = load_geojson(key)
    except Exception:
        return jsonify({"ok": False, "error": "Failed loading dataset."}), 500

    feats = gj.get("features", [])
    feats = apply_filters(feats, income_min, value_min, zip_code)

    total = len(feats)
    feats = feats[:MAX_RESULTS]
    results = [feature_to_obj(f) for f in feats]

    return jsonify({
        "ok": True,
        "query": query,
        "state": state,
        "county": county,
        "city": city,
        "zip": zip_code,
        "dataset_key": key,
        "total": total,
        "shown": len(results),
        "results": results,
    })


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

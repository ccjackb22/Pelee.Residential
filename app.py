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

# NEW: OpenAI client
try:
    from openai import OpenAI
    _openai_client = OpenAI()  # will read OPENAI_API_KEY from env
except Exception:
    _openai_client = None

# ----------------- CONFIG -----------------

load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-2"))
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# per-county enriched files like: merged_with_tracts_acs/fl/wakulla-with-values-income.geojson
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")

# how many address rows to show in UI
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# in-memory cache of S3 keys by state
_STATE_KEYS_CACHE = {}

# ----------------- LOOKUP TABLES -----------------

US_STATES = {
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

# allow detection of 2-letter codes like "fl", "oh"
STATE_CODES = {v: v for v in US_STATES.values()}

# minimal ZIP → (STATE, COUNTY) mapping for overrides if you want
ZIP_TO_STATE_COUNTY = {
    # "44116": ("OH", "cuyahoga"),
    # add overrides here if you have them / need to force a specific county
}

# ----------------- BASIC UTILITIES -----------------


def normalize_text(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def canonicalize_county_name(name: str) -> str:
    """
    normalize "Wakulla", "Wakulla County" etc → "wakulla"
    """
    if not name:
        return ""
    s = name.lower().replace("_", " ").replace("-", " ")
    for word in [" county", " parish"]:
        if word in s:
            s = s.replace(word, "")
    return " ".join(s.split())


def detect_state_heuristic(q: str):
    """
    Fallback if LLM doesn't give a state.
    """
    q = (q or "").lower()
    tokens = q.split()

    # look for 2-letter codes first
    for t in tokens:
        t_clean = t.strip(",.").lower()
        if t_clean == "in":  # don't treat "in" as Indiana
            continue
        if t_clean in STATE_CODES:
            return t_clean.upper()

    # look for full names like "florida", "washington"
    for name, code in US_STATES.items():
        if name in q:
            return code.upper()

    return None


def extract_zip_heuristic(query: str):
    """
    Return the first 5-digit ZIP code found, or None (fallback).
    """
    m = re.search(r"\b(\d{5})\b", query or "")
    return m.group(1) if m else None


def parse_numeric_filters_heuristic(query: str):
    """
    Legacy simple 'over X' / 'above X' parsing as backup if LLM fails.
    """
    q = (query or "").lower()
    income_min = None
    value_min = None

    tokens = q.replace("$", "").replace(",", "").split()
    for i, t in enumerate(tokens):
        if t in {"over", "above"} and i + 1 < len(tokens):
            raw = tokens[i + 1].strip(".,)")
            mult = 1
            if raw.endswith("k"):
                mult = 1000
                raw = raw[:-1]
            elif raw.endswith("m"):
                mult = 1_000_000
                raw = raw[:-1]

            try:
                num = float(raw) * mult
            except ValueError:
                continue

            window = " ".join(tokens[max(0, i - 3): i + 6])
            if "income" in window or "household" in window:
                income_min = num
            elif "value" in window or "home" in window or "homes" in window or "properties" in window:
                value_min = num
            else:
                if income_min is None:
                    income_min = num

    return income_min, value_min


# ----------------- LLM-ASSISTED PARSING -----------------


def llm_parse_query(raw_query: str):
    """
    Use GPT to interpret the query:
    - state (2-letter)
    - county (without "County")
    - city (plain city name)
    - zip (5-digit string)
    - min_income, min_value (integer dollars)
    Returns dict or None if anything fails.
    """
    if not _openai_client:
        return None

    # If no API key set, bail out
    if not os.getenv("OPENAI_API_KEY"):
        return None

    prompt = f"""
You are a strict US address query parser.

User query:
{raw_query}

Extract the following fields about where the user wants addresses from:

- "state": 2-letter US state code, or null if unknown.
- "county": county name WITHOUT the word "County" or "Parish" (e.g. "Wakulla", "Cuyahoga"), or null.
- "city": city/town name only (e.g. "Berea", "Vancouver"), or null.
- "zip": 5-digit ZIP code as a string if clearly specified (e.g. "44116"), or null.
- "min_income": minimum household income in dollars (integer) if clearly requested (e.g. "over 200k" → 200000), else null.
- "min_value": minimum home/property value in dollars (integer) if clearly requested, else null.

Rules:
- Prefer county + state if both are specified.
- If a city and state are given and you know what county that city is in, fill in "county" as that county name.
- If a ZIP and city/state conflict, trust the ZIP for the zip field, but still set the county to whatever county you believe that area is in.
- If the user only says "homes over 200k", that refers to min_value.
- If they say "incomes over 200k" or "household income above 150k", that refers to min_income.
- If ambiguous "over 200k" appears alone, treat it as min_income.

Respond ONLY with a JSON object, no extra text.
Example response:
{{"state":"OH","county":"cuyahoga","city":"Berea","zip":"44116","min_income":150000,"min_value":200000}}
"""

    try:
        resp = _openai_client.chat.completions.create(
            model="gpt-4.1-mini",
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "You are a precise JSON-only geo parser for US address queries.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
        )

        content = resp.choices[0].message.content
        if isinstance(content, list):
            # safety: handle multi-part content
            content = "".join(part.get("text", "") for part in content if isinstance(part, dict))
        data = json.loads(content)
        return data
    except Exception as e:
        app.logger.error(f"[llm_parse_query] LLM parsing failed: {e}")
        return None


def parse_location_and_filters(query: str):
    """
    Combined LLM + heuristic parser.

    Returns:
      state, county, city, zip_code, income_min, value_min
    """
    raw = query or ""
    q_norm = normalize_text(raw)

    # legacy numeric fallback
    legacy_income, legacy_value = parse_numeric_filters_heuristic(raw)
    legacy_zip = extract_zip_heuristic(raw)
    legacy_state = detect_state_heuristic(raw)

    llm_data = llm_parse_query(raw)

    state = None
    county = None
    city = None
    zip_code = None
    income_min = None
    value_min = None

    if llm_data:
        state = (llm_data.get("state") or "").upper() or None
        county = llm_data.get("county") or None
        city = llm_data.get("city") or None
        # zip always string or null
        zip_code = llm_data.get("zip") or None

        try:
            mi = llm_data.get("min_income", None)
            if mi is not None:
                income_min = int(mi)
        except Exception:
            income_min = None

        try:
            mv = llm_data.get("min_value", None)
            if mv is not None:
                value_min = int(mv)
        except Exception:
            value_min = None

    # Fallbacks if LLM missed something
    if not state:
        state = legacy_state

    if not zip_code and legacy_zip:
        zip_code = legacy_zip

    # if ZIP is mapped explicitly to state/county overrides
    if zip_code and zip_code in ZIP_TO_STATE_COUNTY:
        st, ct = ZIP_TO_STATE_COUNTY[zip_code]
        state = st or state
        county = ct or county

    # numeric fallbacks
    if income_min is None and legacy_income is not None:
        income_min = legacy_income
    if value_min is None and legacy_value is not None:
        value_min = legacy_value

    return state, county, city, zip_code, income_min, value_min


# ----------------- S3 + FILTERING -----------------


def list_state_keys(state: str):
    """
    Return all GeoJSON keys under S3_PREFIX/<state>/, cached in memory.
    """
    state = (state or "").lower()
    if not state:
        return []

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
            app.logger.error(f"[S3] list_objects_v2 failed for state={state}: {e}")
            break

        for obj in resp.get("Contents", []):
            key = obj.get("Key")
            if key and key.endswith(".geojson"):
                keys.append(key)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    _STATE_KEYS_CACHE[state] = keys
    app.logger.info(f"[S3] Cached {len(keys)} keys for state={state}")
    return keys


def resolve_dataset_key(state: str, county: str):
    """
    Given state code (e.g., 'FL') and county name (e.g., 'Wakulla'),
    pick the best matching S3 key.

    Preference:
      1) exact county + '-with-values-income'
      2) exact county + '.geojson'
      3) fuzzy with '-with-values-income'
      4) fuzzy raw '.geojson'
    """
    if not state or not county:
        return None

    state = state.lower()
    county_clean = canonicalize_county_name(county)

    keys = list_state_keys(state)
    if not keys:
        return None

    enriched_exact = []
    raw_exact = []
    enriched_fuzzy = []
    raw_fuzzy = []

    for key in keys:
        fname = key.split("/")[-1]
        if not fname.endswith(".geojson"):
            continue
        base = fname[:-len(".geojson")]
        enriched = "with-values-income" in base
        base_no_suffix = base.replace("-with-values-income", "")
        base_canon = canonicalize_county_name(base_no_suffix)

        if base_canon == county_clean:
            (enriched_exact if enriched else raw_exact).append(key)
        elif base_canon.startswith(county_clean) or county_clean.startswith(base_canon):
            (enriched_fuzzy if enriched else raw_fuzzy).append(key)

    for bucket in (enriched_exact, raw_exact, enriched_fuzzy, raw_fuzzy):
        if bucket:
            return sorted(bucket, key=len)[0]

    return None


def load_geojson_from_s3(key: str):
    """
    Simple S3 load: read the whole object and json.loads it.
    """
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        body = resp["Body"].read()
        return json.loads(body)
    except Exception as e:
        app.logger.error(f"[S3] load failed for {key}: {e}")
        raise


def filter_features(features, income_min=None, value_min=None, city=None, zip_code=None):
    """
    Filter by income, value, optional city, and optional ZIP (postcode).

    Rules:
    - If zip_code is present, we always filter by postcode == zip_code (string compare).
    - If city is present AND zip_code is None, we filter by city as well.
    """
    if (income_min is None) and (value_min is None) and not zip_code and not city:
        return features

    income_keys = ["DP03_0062E", "median_income", "income", "household_income"]
    value_keys = ["DP04_0089E", "median_value", "home_value", "value"]

    city_norm = None
    if city:
        # Normalize: "Berea" == "berea" == "BEREA CITY"
        c = str(city).strip().lower()
        city_norm = c.replace(" city", "")

    def read_val(props, keys):
        for k in keys:
            if k in props and props[k] not in ("", None):
                try:
                    return float(props[k])
                except Exception:
                    pass
        return None

    out = []
    for f in features:
        p = f.get("properties", {})
        keep = True

        # ZIP filter
        if zip_code is not None:
            pc = str(p.get("postcode") or "").strip()
            if pc != str(zip_code):
                keep = False

        # City filter (only if we DON'T already have a zip; zip is more specific)
        if keep and city_norm is not None and zip_code is None:
            city_val = str(p.get("city") or "").strip().lower()
            city_val = city_val.replace(" city", "")
            if not city_val or city_val != city_norm:
                keep = False

        if keep and income_min is not None:
            v = read_val(p, income_keys)
            if v is None or v < income_min:
                keep = False

        if keep and value_min is not None:
            v = read_val(p, value_keys)
            if v is None or v < value_min:
                keep = False

        if keep:
            out.append(f)

    return out


def feature_to_address_obj(feat):
    """
    Convert a GeoJSON feature into the object your front-end expects.
    """
    props = feat.get("properties", {})
    geom = feat.get("geometry", {}) or {}
    coords = geom.get("coordinates") or [None, None]

    number = props.get("number") or props.get("house_number") or ""
    street = props.get("street") or props.get("road") or ""
    unit = props.get("unit") or ""

    city = props.get("city") or ""
    region = props.get("region") or props.get("STUSPS") or ""
    postcode = props.get("postcode") or ""

    income = (
        props.get("DP03_0062E")
        or props.get("median_income")
        or props.get("income")
    )
    value = (
        props.get("DP04_0089E")
        or props.get("median_value")
        or props.get("home_value")
        or props.get("value")
    )

    return {
        "address": " ".join(x for x in [str(number), street, unit] if x),
        "city": city,
        "state": region,
        "zip": postcode,
        "income": income,
        "value": value,
        "lat": coords[1],
        "lon": coords[0],
    }


# ----------------- ROUTES -----------------


@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == PASSWORD:
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

    try:
        if request.is_json:
            data = request.get_json(silent=True) or {}
            query = (data.get("query") or "").strip()
        else:
            query = (request.form.get("query") or "").strip()

        if not query:
            return jsonify({"ok": False, "error": "Please enter a query."})

        (
            state,
            county,
            city,
            zip_code,
            income_min,
            value_min,
        ) = parse_location_and_filters(query)

        app.logger.info(
            f"[search] query={query!r} → state={state}, county={county}, city={city}, zip={zip_code}, "
            f"income_min={income_min}, value_min={value_min}"
        )

        if not state:
            return jsonify({"ok": False, "error": "No state detected."})
        if not county:
            return jsonify({"ok": False, "error": "No county detected."})

        key = resolve_dataset_key(state, county)
        app.logger.info(f"[search] resolved dataset_key={key}")

        if not key:
            return jsonify(
                {
                    "ok": False,
                    "error": f"No dataset found for {county} County, {state}.",
                }
            )

        try:
            gj = load_geojson_from_s3(key)
        except Exception as e:
            return jsonify(
                {
                    "ok": False,
                    "error": f"Failed loading {key}: {str(e)}",
                }
            )

        feats = gj.get("features", [])
        feats = filter_features(
            feats,
            income_min=income_min,
            value_min=value_min,
            city=city,
            zip_code=zip_code,
        )

        total = len(feats)
        feats = feats[:MAX_RESULTS]

        addresses = [feature_to_address_obj(f) for f in feats]

        return jsonify(
            {
                "ok": True,
                "query": query,
                "state": state,
                "county": county,
                "city": city,
                "zip": zip_code,
                "dataset_key": key,
                "total": total,
                "shown": len(addresses),
                "results": addresses,
            }
        )
    except Exception as e:
        app.logger.exception(f"[search] unexpected error: {e}")
        return jsonify({"ok": False, "error": "Server error (search failed)."}), 500


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    if request.is_json:
        data = request.get_json(silent=True) or {}
        query = (data.get("query") or "").strip()
    else:
        query = (request.form.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Missing query for export."}), 400

    state, county, city, zip_code, income_min, value_min = parse_location_and_filters(query)

    if not state or not county:
        return jsonify({"ok": False, "error": "Need a state + county to export."}), 400

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify(
            {
                "ok": False,
                "error": f"No dataset found for export for {county} County, {state}.",
            }
        ), 400

    try:
        gj = load_geojson_from_s3(key)
    except Exception as e:
        return jsonify(
            {"ok": False, "error": f"Failed to load dataset for export: {str(e)}"}
        ), 500

    feats = gj.get("features", [])
    feats = filter_features(
        feats,
        income_min=income_min,
        value_min=value_min,
        city=city,
        zip_code=zip_code,
    )

    rows = []
    fieldnames = set()

    for f in feats:
        props = f.get("properties", {}).copy()
        geom = f.get("geometry", {}) or {}
        coords = geom.get("coordinates") or [None, None]
        props["lon"] = coords[0]
        props["lat"] = coords[1]
        rows.append(props)
        fieldnames.update(props.keys())

    fieldnames = sorted(fieldnames)

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as zf:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
        zf.writestr("addresses.csv", buf.getvalue())

    mem.seek(0)
    fname = f"pelee_export_{state}_{canonicalize_county_name(county).replace(' ', '_')}.zip"

    return send_file(
        mem,
        mimetype="application/zip",
        as_attachment=True,
        download_name=fname,
    )


# ----------------- MAIN -----------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)

import os
import io
import json
import csv
import re
import logging

from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    redirect,
    url_for,
    session,
    Response,
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

# Clean, ACS-enriched nationwide dataset
CLEAN_PREFIX = "merged_with_tracts_acs_clean"
MAX_RESULTS = 500  # on-screen

# Toggle: if True, only return "residential" records (Option B)
FILTER_RESIDENTIAL = True

# ---------------------------------------------------------
# FLASK + LOGGING
# ---------------------------------------------------------
app = Flask(__name__)
app.secret_key = SECRET_KEY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = app.logger  # use Flask logger wrapper

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATE + COUNTY INDEX
# ---------------------------------------------------------

STATE_CODES = {
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

# (state_code, county_name) -> {"key": s3_key, "size": bytes}
ALL_DATASETS = {}
# state_code -> set(counties)
STATE_TO_COUNTIES = {}


def scan_available_datasets():
    """
    Scan S3 for *-clean.geojson under merged_with_tracts_acs_clean/
    and build ALL_DATASETS[(state, county)].
    """
    global ALL_DATASETS, STATE_TO_COUNTIES
    ALL_DATASETS = {}
    STATE_TO_COUNTIES = {}

    prefix = CLEAN_PREFIX + "/"
    logger.info(
        "[BOOT] Scanning S3 bucket=%s prefix=%s for *-clean.geojson ...",
        S3_BUCKET,
        prefix,
    )

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    count = 0
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            parts = key.split("/")
            if len(parts) != 3:
                # Expect merged_with_tracts_acs_clean/<state>/<county>-clean.geojson
                continue

            _, state_dir, filename = parts
            state_code = state_dir.lower()
            county_name_raw = filename.replace("-clean.geojson", "").lower()

            # Normalize some weird names: strip trailing "-county" if present
            county_name = county_name_raw
            if county_name.endswith("-county"):
                county_name = county_name[:-len("-county")]
            if county_name.endswith("_county"):
                county_name = county_name[:-len("_county")]

            ALL_DATASETS[(state_code, county_name)] = {
                "key": key,
                "size": obj.get("Size", 0),
                "raw_name": county_name_raw,
            }
            STATE_TO_COUNTIES.setdefault(state_code, set()).add(county_name)
            count += 1

    logger.info(
        "[BOOT] Indexed %d cleaned county datasets.",
        count,
    )


scan_available_datasets()

# ---------------------------------------------------------
# TEXT PARSING HELPERS
# ---------------------------------------------------------


def detect_zip(query: str):
    m = re.search(r"\b(\d{5})\b", query)
    return m.group(1) if m else None


def parse_filters(q: str):
    """
    Parse phrases like:
      - "over 200k income"
      - "homes above 500000"
      - "value over 800k"
    Returns (income_min, value_min)
    """
    q = q.lower().replace("$", "").replace(",", "")
    toks = q.split()

    income_min = None
    value_min = None

    for i, t in enumerate(toks):
        if t in ("over", "above", "greater", ">", "atleast", "at_least") and i + 1 < len(
            toks
        ):
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
            except Exception:
                continue

            window = " ".join(toks[max(0, i - 5) : i + 5])
            if "income" in window:
                income_min = val
                continue
            if "value" in window or "home" in window or "house" in window:
                value_min = val
                continue

            if income_min is None:
                income_min = val

    return income_min, value_min


AMBIG_ABBRS = {"in", "or", "me", "hi", "ok", "nd", "id", "oh"}


def detect_state(query: str):
    """
    Try to detect US state from query.
    Priority:
      1) Full state name (e.g., 'texas', 'ohio')
      2) Two-letter uppercase abbreviation (e.g., 'TX', 'OH'),
         but skipping ones that are English words (IN, OR, ME, HI, OK, ND, ID, OH)
    Returns lowercase 2-letter code or None.
    """
    q_lower = query.lower()

    # 1) Full names
    for name, code in STATE_CODES.items():
        if name in q_lower:
            return code

    # 2) Uppercase code in original query
    orig = query
    for code in set(STATE_CODES.values()):
        if code in AMBIG_ABBRS:
            continue
        pattern = r"\b" + code.upper() + r"\b"
        if re.search(pattern, orig):
            return code

    return None


# Common city -> county shortcuts for your core areas
CITY_TO_COUNTY = {
    ("tx", "houston"): "harris",
    ("tx", "corpus christi"): "nueces",
    ("oh", "cleveland"): "cuyahoga",
    ("oh", "columbus"): "franklin",
    # a few extras for your usual queries
    ("oh", "strongsville"): "cuyahoga",
    ("oh", "berea"): "cuyahoga",
}


def _pick_first_state(county: str):
    """
    If state is unknown, but we matched a county name,
    pick the first (state, county) combo from ALL_DATASETS.
    This is a fallback, mostly for debugging.
    """
    for (s, c), meta in ALL_DATASETS.items():
        if c == county:
            return s
    return None


def detect_county(query: str, state_code: str | None):
    """
    Detect county name given query and (optional) state_code.
    - Prefer matches in the detected state
    - Match "X County", "X Parish", "X Borough"
    - Fall back to exact county name as a standalone word
    - Finally, use city synonyms (Houston -> Harris, etc.)
    Returns (state_code, county_name) or (None, None).
    """
    q_lower = query.lower()

    # If we have a state, restrict search to that state first
    if state_code and state_code in STATE_TO_COUNTIES:
        candidates = list(STATE_TO_COUNTIES[state_code])
    else:
        # No state? consider all counties (we'll still try to choose best)
        candidates = list({c for (_, c) in ALL_DATASETS.keys()})

    # 1) "X county" / "X parish" / "X borough"
    for county in candidates:
        name = county.replace("_", " ").replace("-", " ")
        if f"{name} county" in q_lower or f"{name} parish" in q_lower or f"{name} borough" in q_lower:
            if state_code:
                return state_code, county
            guessed_state = _pick_first_state(county)
            if guessed_state:
                return guessed_state, county

    # 2) standalone county name
    for county in candidates:
        name = county.replace("_", " ").replace("-", " ")
        if re.search(r"\b" + re.escape(name) + r"\b", q_lower):
            if state_code:
                return state_code, county
            guessed_state = _pick_first_state(county)
            if guessed_state:
                return guessed_state, county

    # 3) city synonyms (e.g., "houston" -> Harris County, TX)
    for (st, city), county in CITY_TO_COUNTY.items():
        if city in q_lower and (not state_code or state_code == st):
            return st, county

    return None, None


def get_zip_from_props(p: dict):
    return (
        p.get("postcode")
        or p.get("POSTCODE")
        or p.get("ZCTA5CE20")
        or ""
    )


def feature_to_obj(f):
    p = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}

    coords = geom.get("coordinates") or [None, None]
    if isinstance(coords, (list, tuple)) and coords and isinstance(
        coords[0], (list, tuple)
    ):
        # nested
        try:
            lon, lat = coords[0][0], coords[0][1]
        except Exception:
            lon, lat = None, None
    else:
        try:
            lon, lat = coords[0], coords[1]
        except Exception:
            lon, lat = None, None

    num = p.get("number") or p.get("NUMBER") or p.get("house_number") or ""
    street = p.get("street") or p.get("STREET") or p.get("road") or ""
    unit = p.get("unit") or p.get("UNIT") or ""

    city = p.get("city") or p.get("CITY") or ""
    st = p.get("region") or p.get("REGION") or p.get("STUSPS") or ""
    zipc = get_zip_from_props(p)

    income = (
        p.get("median_income")
        or p.get("B19013_001E")
        or p.get("DP03_0062E")
        or p.get("income")
    )
    value = (
        p.get("median_value")
        or p.get("B25077_001E")
        or p.get("DP04_0089E")
        or p.get("home_value")
    )

    return {
        "address": " ".join(str(x) for x in [num, street, unit] if x),
        "number": num,
        "street": street,
        "unit": unit,
        "city": city,
        "state": st,
        "zip": zipc,
        "income": income,
        "value": value,
        "lat": lat,
        "lon": lon,
    }

# ---------------------------------------------------------
# RESIDENTIAL FILTER (Option B)
# ---------------------------------------------------------

RES_YES_TOKENS = {
    "residential",
    "res",
    "single family",
    "single-family",
    "sfr",
    "sfh",
    "multi family",
    "multi-family",
    "multifamily",
    "apartment",
    "apartments",
    "condo",
    "condominium",
    "townhome",
    "townhouse",
    "duplex",
    "triplex",
    "quadplex",
    "mobile home",
    "manufactured home",
    "mh",
    "dwelling",
    "house",
    "home",
}

RES_NO_TOKENS = {
    "commercial",
    "comm",
    "industrial",
    "ind",
    "warehouse",
    "office",
    "retail",
    "store",
    "shop",
    "shopping center",
    "mall",
    "restaurant",
    "hotel",
    "motel",
    "hospital",
    "school",
    "university",
    "college",
    "gas station",
    "service station",
    "auto",
    "parking",
    "park-and-ride",
    "church",
    "religious",
    "gov",
    "government",
    "utility",
    "utilities",
    "agricultural",
    "agriculture",
    "farm",
    "ranch",
    "vacant commercial",
    "vacant industrial",
}

LANDUSE_KEY_HINTS = {
    "landuse",
    "land_use",
    "land_use_desc",
    "use",
    "use_desc",
    "use_type",
    "lu",
    "lu_code",
    "lu_descr",
    "zoning",
    "zone",
    "zone_desc",
    "prop_class",
    "prop_type",
    "prop_use",
    "bldg_type",
    "building_type",
    "bldg_use",
    "bldgclass",
}


def is_residential(p: dict) -> bool:
    """
    Heuristic residential filter:
      - If we find obvious COMMERCIAL / INDUSTRIAL tokens => False
      - Else if we find obvious RESIDENTIAL tokens => True
      - Else (no strong signal) => True (don't drop whole counties by accident)
    """
    texts = []

    for k, v in p.items():
        if v is None:
            continue
        k_lower = str(k).lower()
        if k_lower in LANDUSE_KEY_HINTS or any(h in k_lower for h in LANDUSE_KEY_HINTS):
            texts.append(str(v).lower())

    if not texts:
        # No landuse-ish context -> assume residential for now
        return True

    blob = " ".join(texts)

    for token in RES_NO_TOKENS:
        if token in blob:
            return False

    for token in RES_YES_TOKENS:
        if token in blob:
            return True

    # No strong signals either way -> keep
    return True


def apply_filters(features, income_min, value_min, zip_code, filter_residential=True):
    out = []
    for f in features:
        p = f.get("properties", {}) or {}

        # Residential filter first
        if filter_residential and not is_residential(p):
            continue

        if zip_code:
            feature_zip = str(get_zip_from_props(p))
            if feature_zip != str(zip_code):
                continue

        if income_min is not None:
            v = (
                p.get("median_income")
                or p.get("B19013_001E")
                or p.get("DP03_0062E")
                or p.get("income")
            )
            try:
                if v is None or float(v) < income_min:
                    continue
            except Exception:
                continue

        if value_min is not None:
            v = (
                p.get("median_value")
                or p.get("B25077_001E")
                or p.get("DP04_0089E")
                or p.get("home_value")
            )
            try:
                if v is None or float(v) < value_min:
                    continue
            except Exception:
                continue

        out.append(f)

    return out


def load_geojson(key: str):
    """
    Simple: pull whole file from S3 and json.loads it.
    No size guard anymore (Option A).
    """
    try:
        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        data = resp["Body"].read()
        return json.loads(data)
    except ClientError as e:
        logger.error("[S3] Failed to load %s: %s", key, e)
        raise
    except Exception as e:
        logger.error("[S3] Error parsing JSON for %s: %s", key, e)
        raise


# ---------------------------------------------------------
# AUTH
# ---------------------------------------------------------


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
            logger.info("[AUTH] Login success")
            session["authed"] = True
            return redirect(url_for("index"))
        return render_template("login.html", error="Invalid password.")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------------------------------------------------
# CORE ROUTES
# ---------------------------------------------------------


@app.route("/")
def index():
    if not session.get("authed"):
        return redirect(url_for("login"))
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify(
        {
            "ok": True,
            "datasets": len(ALL_DATASETS),
            "filter_residential": FILTER_RESIDENTIAL,
        }
    )


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    logger.info("[SEARCH] Incoming query='%s'", query)

    state_code = detect_state(query)
    county_state, county_name = detect_county(query, state_code)

    if not county_name or not county_state:
        msg = (
            "Couldn't resolve a (county, state) from that query. "
            "Try including BOTH 'X County' and the full state name, e.g. "
            "'residential addresses in Harris County Texas'."
        )
        logger.info("[RESOLVE] %s Query='%s'", msg, query)
        return jsonify({"ok": False, "error": msg})

    state_code = county_state.lower()
    county_key = county_name.lower()

    ds = ALL_DATASETS.get((state_code, county_key))
    if not ds:
        msg = f"No cleaned dataset found for that county/state (state={state_code.upper()}, county={county_key})."
        logger.info("[RESOLVE] %s Query='%s'", msg, query)
        return jsonify({"ok": False, "error": msg})

    key = ds["key"]
    size = ds.get("size", 0)
    logger.info(
        "[RESOLVE] Matched state=%s, county=%s, key=%s, size=%s",
        state_code,
        county_key,
        key,
        size,
    )

    try:
        gj = load_geojson(key)
    except Exception as e:
        logger.error(
            "[ERROR] Failed to load dataset key=%s for query='%s': %s", key, query, e
        )
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Failed loading dataset {key}.",
                    "details": str(e),
                }
            ),
            500,
        )

    feats = gj.get("features", [])

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    feats = apply_filters(
        feats,
        income_min=income_min,
        value_min=value_min,
        zip_code=zip_code,
        filter_residential=FILTER_RESIDENTIAL,
    )

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    logger.info(
        "[RESULTS] query='%s' state=%s county=%s total=%d shown=%d",
        query,
        state_code,
        county_key,
        total,
        len(feats),
    )

    return jsonify(
        {
            "ok": True,
            "query": query,
            "state": state_code.upper(),
            "county": county_key.replace("_", " ").title(),
            "zip": zip_code,
            "dataset_key": key,
            "total": total,
            "shown": len(feats),
            "results": [feature_to_obj(f) for f in feats],
        }
    )


# ---------------------------------------------------------
# EXPORT
# ---------------------------------------------------------


@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()

    if not query:
        return jsonify({"ok": False, "error": "Missing query."})

    logger.info("[EXPORT] Incoming query='%s'", query)

    state_code = detect_state(query)
    county_state, county_name = detect_county(query, state_code)

    if not county_name or not county_state:
        return jsonify({"ok": False, "error": "Couldn't resolve county/state."}), 400

    state_code = county_state.lower()
    county_key = county_name.lower()

    ds = ALL_DATASETS.get((state_code, county_key))
    if not ds:
        msg = f"No cleaned dataset found for that county/state (state={state_code.upper()}, county={county_key})."
        logger.info("[EXPORT] %s Query='%s'", msg, query)
        return jsonify({"ok": False, "error": msg}), 400

    key = ds["key"]
    size = ds.get("size", 0)
    logger.info(
        "[EXPORT] Using dataset state=%s county=%s key=%s size=%s",
        state_code,
        county_key,
        key,
        size,
    )

    try:
        gj = load_geojson(key)
    except Exception as e:
        logger.error(
            "[ERROR] Failed to load dataset for export key=%s query='%s': %s",
            key,
            query,
            e,
        )
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Failed loading dataset {key}.",
                    "details": str(e),
                }
            ),
            500,
        )

    feats = gj.get("features", [])

    zip_code = detect_zip(query)
    income_min, value_min = parse_filters(query)

    feats = apply_filters(
        feats,
        income_min=income_min,
        value_min=value_min,
        zip_code=zip_code,
        filter_residential=FILTER_RESIDENTIAL,
    )

    filename = f"pelee_export_{county_key}_{state_code}.csv"

    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        writer.writerow(
            [
                "address_number",
                "street",
                "unit",
                "city",
                "state",
                "zip",
                "income",
                "value",
                "lat",
                "lon",
            ]
        )
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for f in feats:
            r = feature_to_obj(f)
            writer.writerow(
                [
                    r["number"],
                    r["street"],
                    r["unit"],
                    r["city"],
                    r["state"],
                    r["zip"],
                    r["income"],
                    r["value"],
                    r["lat"],
                    r["lon"],
                ]
            )
            chunk = buffer.getvalue()
            if chunk:
                yield chunk
                buffer.seek(0)
                buffer.truncate(0)

    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}

    return Response(generate(), mimetype="text/csv", headers=headers)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

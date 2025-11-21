import os
import io
import json
import csv
import re

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

# IMPORTANT: use the CLEANED county files
# s3://residential-data-jack/merged_with_tracts_acs_clean/{state}/{county}-clean.geojson
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs_clean")

# Max results for on-screen search results
MAX_RESULTS = 500

app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# ---------------------------------------------------------
# STATE + COUNTY INDEX
# ---------------------------------------------------------

STATE_NAME_TO_ABBR = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
    "washington dc": "DC",
}

STATE_ABBRS = set(STATE_NAME_TO_ABBR.values())


def normalize_slug(s: str) -> str:
    """Lowercase + collapse to a-z0-9 + underscores."""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def build_county_index():
    """
    Build index from S3:
      merged_with_tracts_acs_clean/{state}/{county_slug}-clean.geojson
    Returns:
      {
        "CA": [
           {"slug": "kern", "norm": "kern", "key": ".../ca/kern-clean.geojson"},
           ...
        ],
        ...
      }
    """
    index = {}
    prefix = S3_PREFIX.rstrip("/") + "/"
    continuation = None

    while True:
        kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation

        resp = s3_client.list_objects_v2(**kwargs)
        contents = resp.get("Contents", [])

        for obj in contents:
            key = obj["Key"]
            if not key.endswith("-clean.geojson"):
                continue

            # Strip the prefix and split: state_folder / filename
            rest = key[len(prefix) :]
            parts = rest.split("/")
            if len(parts) != 2:
                continue

            state_folder, filename = parts
            state_abbr = state_folder.upper()
            if state_abbr not in STATE_ABBRS:
                # ignore territories / weird folders for now
                continue

            slug = filename.replace("-clean.geojson", "")
            norm = normalize_slug(slug)

            entry = {"slug": slug, "norm": norm, "key": key}
            index.setdefault(state_abbr, []).append(entry)

        if resp.get("IsTruncated"):
            continuation = resp.get("NextContinuationToken")
        else:
            break

    return index


COUNTY_INDEX = {}
COUNTY_INDEX_ERROR = None
try:
    COUNTY_INDEX = build_county_index()
except Exception as e:
    COUNTY_INDEX_ERROR = str(e)
    COUNTY_INDEX = {}


# ---------------------------------------------------------
# HELPERS (ZIP / FILTERS / GEOID)
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
        if t in ("over", "above", "greater", ">", "atleast", "at_least") and i + 1 < len(toks):
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

            window = " ".join(toks[max(0, i - 5): i + 5])
            if "income" in window:
                income_min = val
                continue
            if "value" in window or "home" in window or "house" in window:
                value_min = val
                continue

            if income_min is None:
                income_min = val

    return income_min, value_min


def detect_state(query_original: str):
    """
    Detect US state from full name ("california") or 2-letter code ("CA").
    """
    q_lower = query_original.lower()

    # 1) full state name
    for name, abbr in STATE_NAME_TO_ABBR.items():
        if name in q_lower:
            return abbr

    # 2) uppercase 2-letter tokens (avoids "in", "or", "me" false positives)
    for token in re.findall(r"\b([A-Z]{2})\b", query_original):
        if token in STATE_ABBRS:
            return token

    return None


def resolve_dataset_from_query(query: str):
    """
    Resolve (state_abbr, county_slug, s3_key, message) from NATURAL query.

    Uses COUNTY_INDEX built from S3. We:
      - detect state
      - find a county in that state whose slug appears in the query
    """
    if COUNTY_INDEX_ERROR:
        return None, None, None, f"County index failed to build from S3: {COUNTY_INDEX_ERROR}"

    if not COUNTY_INDEX:
        return None, None, None, "No county index available. Check S3_PREFIX / AWS credentials."

    state_abbr = detect_state(query)
    if not state_abbr:
        return None, None, None, (
            "Couldn't detect a US state in that query. "
            "Try including a state, e.g. 'addresses in Kern County CA'."
        )

    if state_abbr not in COUNTY_INDEX:
        return None, None, None, f"No cleaned datasets found for state {state_abbr}."

    q_lower = query.lower()
    q_norm = "_" + normalize_slug(q_lower) + "_"

    # Special alias: Corpus Christi, TX -> Nueces County
    if state_abbr == "TX" and "corpus christi" in q_lower:
        for entry in COUNTY_INDEX[state_abbr]:
            if "nueces" in entry["slug"]:
                return state_abbr, entry["slug"], entry["key"], None

    candidates = []
    for entry in COUNTY_INDEX[state_abbr]:
        norm = entry["norm"]
        pattern = "_" + norm + "_"
        if pattern in q_norm:
            candidates.append(entry)

    if not candidates:
        # If they explicitly say "county", try to capture the word before it and fuzzy match
        # but keep it simple: nudge them to mention the county name as your files use it.
        return None, None, None, (
            f"Couldn't detect a county name in {state_abbr} from that query. "
            "Try something like 'addresses in Kern County CA' or "
            "'homes in Miami-Dade County Florida over 200k income'."
        )

    if len(candidates) == 1:
        chosen = candidates[0]
    else:
        # Prefer the longest norm (e.g. "maui_county" vs "maui")
        candidates.sort(key=lambda x: len(x["norm"]), reverse=True)
        chosen = candidates[0]

    return state_abbr, chosen["slug"], chosen["key"], None


def load_geojson(key: str):
    data = s3_client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    return json.loads(data)


def get_geoid(props: dict):
    # Be defensive: support various property names
    return (
        props.get("GEOID")
        or props.get("geoid")
        or props.get("TRACT_GEOID")
        or props.get("tract_geoid")
    )


def infer_geoid_prefix(features):
    """
    Look at features to infer a 5-digit state+county FIPS prefix from the GEOID.
    """
    for f in features:
        p = f.get("properties", {}) or {}
        gid = get_geoid(p)
        if not gid:
            continue
        s = str(gid)
        if len(s) >= 5:
            return s[:5]
    return None


def is_in_county(props: dict, geoid_prefix: str):
    if not geoid_prefix:
        return True
    gid = get_geoid(props)
    if not gid:
        return False
    return str(gid).startswith(geoid_prefix)


def get_zip_from_props(p: dict):
    """
    Priority for ZIP:
      1) lowercase 'postcode'
      2) uppercase 'POSTCODE'
      3) ZCTA5CE20 from ZCTA join
    """
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
    if isinstance(coords[0], (list, tuple)):
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
    st = (
        p.get("region")
        or p.get("REGION")
        or p.get("STUSPS")
    )
    if not st:
        st = ""

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


def apply_filters(features, income_min, value_min, zip_code, geoid_prefix):
    out = []
    for f in features:
        p = f.get("properties", {}) or {}

        # 1) enforce county via GEOID prefix, if available
        if geoid_prefix and not is_in_county(p, geoid_prefix):
            continue

        # 2) ZIP filter
        if zip_code:
            feature_zip = str(get_zip_from_props(p))
            if feature_zip != str(zip_code):
                continue

        # 3) Income filter
        if income_min:
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

        # 4) Home value filter
        if value_min:
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


# ---------------------------------------------------------
# AUTH
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
    return jsonify({
        "ok": True,
        "env_aws_access_key": bool(os.getenv("AWS_ACCESS_KEY_ID")),
        "env_openai": bool(os.getenv("OPENAI_API_KEY")),
        "county_index_built": bool(COUNTY_INDEX),
        "county_index_error": COUNTY_INDEX_ERROR,
        "bucket": S3_BUCKET,
        "prefix": S3_PREFIX,
    })


@app.route("/search", methods=["POST"])
def search():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "Enter a search query."})

    state, county_slug, dataset_key, msg = resolve_dataset_from_query(query)
    if not dataset_key:
        return jsonify({
            "ok": False,
            "error": msg or "Could not resolve a county/state dataset from that query.",
        })

    # Load dataset
    try:
        gj = load_geojson(dataset_key)
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"Failed loading dataset: {dataset_key}",
            "details": str(e),
        }), 500

    feats = gj.get("features", [])

    # Infer GEOID prefix for safety
    geoid_prefix = infer_geoid_prefix(feats)

    # Filters
    q_lower = query.lower()
    zip_code = detect_zip(q_lower)
    income_min, value_min = parse_filters(q_lower)

    feats = apply_filters(feats, income_min, value_min, zip_code, geoid_prefix)

    total = len(feats)
    feats = feats[:MAX_RESULTS]

    return jsonify({
        "ok": True,
        "query": query,
        "state": state,
        "county": county_slug,
        "city": None,
        "zip": zip_code,
        "dataset_key": dataset_key,
        "total": total,
        "shown": len(feats),
        "results": [feature_to_obj(f) for f in feats],
    })


# ---------------------------------------------------------
# EXPORT (STREAMING CSV)
# ---------------------------------------------------------

@app.route("/export", methods=["POST"])
def export():
    if not session.get("authed"):
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "Missing query."})

    state, county_slug, dataset_key, msg = resolve_dataset_from_query(query)
    if not dataset_key:
        return jsonify({
            "ok": False,
            "error": msg or "Could not resolve a county/state dataset from that query.",
        })

    try:
        gj = load_geojson(dataset_key)
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"Failed loading dataset: {dataset_key}",
            "details": str(e),
        }), 500

    feats = gj.get("features", [])
    geoid_prefix = infer_geoid_prefix(feats)

    q_lower = query.lower()
    zip_code = detect_zip(q_lower)
    income_min, value_min = parse_filters(q_lower)
    feats = apply_filters(feats, income_min, value_min, zip_code, geoid_prefix)

    filename = f"pelee_export_{county_slug}_{state}.csv"

    def generate():
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        # Header
        writer.writerow([
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
        ])
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for f in feats:
            p = f.get("properties", {}) or {}
            geom = f.get("geometry", {}) or {}
            coords = geom.get("coordinates") or [None, None]

            if isinstance(coords[0], (list, tuple)):
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
            st = (
                p.get("region")
                or p.get("REGION")
                or p.get("STUSPS")
            )
            if not st:
                st = ""

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

            writer.writerow([num, street, unit, city, st, zipc, income, value, lat, lon])

            data_chunk = buffer.getvalue()
            if data_chunk:
                yield data_chunk
                buffer.seek(0)
                buffer.truncate(0)

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"'
    }

    return Response(generate(), mimetype="text/csv", headers=headers)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    # Local dev
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

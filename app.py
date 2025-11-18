import os
import io
import json
import csv
import zipfile

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

# ----------------- CONFIG -----------------

load_dotenv()

SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
PASSWORD = os.getenv("PELEE_PASSWORD", "CaLuna")

AWS_REGION = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-2"))
S3_BUCKET = os.getenv("S3_BUCKET", "residential-data-jack")

# IMPORTANT: point to the enriched ACS data
# Example: merged_with_tracts_acs/az/maricopa-with-values-income.geojson
S3_PREFIX = os.getenv("S3_PREFIX", "merged_with_tracts_acs")

# Max number of addresses to show in UI
MAX_RESULTS = 500

# Hard safety cap for size of a single county file in bytes
# If a file is bigger than this, we refuse to stream it in the UI
MAX_GEOJSON_BYTES = int(os.getenv("MAX_GEOJSON_BYTES", "120000000"))  # ~120 MB


app = Flask(__name__)
app.secret_key = SECRET_KEY

s3_client = boto3.client("s3", region_name=AWS_REGION)

# simple in-memory cache of S3 keys per state
_STATE_KEYS_CACHE = {}


# ----------------- UTILITIES -----------------


US_STATES = {
    # full name -> code
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

STATE_CODES = {v: v for v in US_STATES.values()}


def normalize_text(s: str) -> str:
    return " ".join(s.strip().lower().split())


def canonicalize_county_name(name: str) -> str:
    """
    Make county names comparable:
    - lowercase
    - replace '_' and '-' with spaces
    - remove 'county' / 'parish'
    """
    if not name:
        return ""
    s = name.lower().replace("_", " ").replace("-", " ")
    for word in [" county", " parish"]:
        if word in s:
            s = s.replace(word, "")
    return " ".join(s.split())


def detect_state(query: str):
    """
    Detect 2-letter state codes or full names.

    FIX: ignore the preposition 'in' so we don't treat it as 'IN' (Indiana).
    """
    q = query.lower()
    tokens = q.split()

    # 1) look for 2-letter codes
    for t in tokens:
        t_clean = t.strip(",.").lower()
        # don't treat the word "in" as Indiana
        if t_clean == "in":
            continue
        if t_clean in STATE_CODES:
            return STATE_CODES[t_clean].upper()

    # 2) look for full names (including multi-word like "new york")
    for name, code in US_STATES.items():
        if name in q:
            return code.upper()

    return None


def parse_location(query: str):
    """
    Parse state + county + city from a free-form query.

    Priority:
    - if 'county' present → extract county tokens before 'county'
    - else if looks like 'city, state' or 'in city state' → treat as city
    """
    q = normalize_text(query)
    tokens = q.split()

    state = detect_state(q)
    county = None
    city = None

    # county pattern "... X Y county STATE ..."
    if "county" in tokens:
        idxs = [i for i, t in enumerate(tokens) if t == "county"]
        # just use first 'county' occurrence
        idx = idxs[0]
        j = idx - 1
        county_tokens_rev = []
        STOP = {"in", "of", "all", "any", "properties", "homes",
                "households", "residential", "addresses", "parcels"}
        while j >= 0:
            t = tokens[j]
            if t in STOP:
                break
            # don't cross state name
            if t.upper() in STATE_CODES or t in US_STATES:
                break
            county_tokens_rev.append(t)
            j -= 1
        county_tokens = list(reversed(county_tokens_rev))
        if county_tokens:
            county = " ".join(county_tokens)

    # If no county but we have Ohio city-only query → default to Cuyahoga
    if not county and state == "OH":
        # crude city extraction: last token before state / code
        if state:
            # find index of state token
            state_idx = None
            for i, t in enumerate(tokens):
                if (t.lower() in STATE_CODES) or (t in US_STATES):
                    state_idx = i
                    break
            if state_idx is not None and state_idx > 0:
                # walk backwards until stopword
                j = state_idx - 1
                city_tokens_rev = []
                STOP = {"in", "ohio", "oh"}
                while j >= 0 and tokens[j] not in STOP:
                    city_tokens_rev.append(tokens[j])
                    j -= 1
                city_tokens = list(reversed(city_tokens_rev))
                if city_tokens:
                    city = " ".join(city_tokens)
        # default county for OH city-only queries
        county = "cuyahoga"

    return state, county, city


def parse_numeric_filters(query: str):
    """
    Extract simple 'over X' / 'above X' for incomes or values.

    We'll check words around 'income' or 'value' but keep it simple.
    """
    q = query.lower()
    income_min = None
    value_min = None

    # crude: look for 'over' or 'above' followed by a number with optional k/m
    tokens = q.replace("$", "").replace(",", "").split()
    for i, t in enumerate(tokens):
        if t in {"over", "above"} and i + 1 < len(tokens):
            raw = tokens[i + 1]
            # strip trailing punctuation
            raw = raw.strip(".,)")
            mult = 1.0
            if raw.endswith("k"):
                mult = 1_000
                raw = raw[:-1]
            elif raw.endswith("m"):
                mult = 1_000_000
                raw = raw[:-1]
            try:
                num = float(raw) * mult
            except ValueError:
                continue

            # if the word 'income' appears nearby, treat as income filter
            window = " ".join(tokens[max(0, i - 3): i + 6])
            if "income" in window or "household" in window:
                income_min = num
            elif "value" in window or "home" in window or "homes" in window or "properties" in window:
                value_min = num
            else:
                # fallback: if we don't see context, treat as income
                if income_min is None:
                    income_min = num

    return income_min, value_min


def list_state_keys(state_code: str):
    """
    Cache + return all keys under S3_PREFIX/{state_code}/
    """
    state_code = state_code.lower()
    if state_code in _STATE_KEYS_CACHE:
        return _STATE_KEYS_CACHE[state_code]

    prefix = f"{S3_PREFIX}/{state_code}/"
    keys = []
    continuation = None

    while True:
        try:
            if continuation:
                resp = s3_client.list_objects_v2(
                    Bucket=S3_BUCKET,
                    Prefix=prefix,
                    ContinuationToken=continuation,
                )
            else:
                resp = s3_client.list_objects_v2(
                    Bucket=S3_BUCKET,
                    Prefix=prefix,
                )
        except ClientError as e:
            app.logger.error(f"[S3] list_objects_v2 failed for state={state_code}: {e}")
            break

        contents = resp.get("Contents", [])
        for obj in contents:
            key = obj.get("Key")
            if key and key.endswith(".geojson"):
                keys.append(key)

        if resp.get("IsTruncated"):
            continuation = resp.get("NextContinuationToken")
        else:
            break

    _STATE_KEYS_CACHE[state_code] = keys
    app.logger.info(f"[S3] Cached {len(keys)} keys for state={state_code} (count={len(keys)})")
    return keys


def resolve_dataset_key(state: str, county: str):
    """
    Given a state code (e.g. 'FL') and county name (e.g. 'Palm Beach'),
    pick the best matching S3 key.

    Preference order:
    1) exact county match + '-with-values-income'
    2) exact county match + '.geojson'
    3) fuzzy (starts/contains) with '-with-values-income'
    4) fuzzy with '.geojson'
    """
    if not state or not county:
        return None

    state_code = state.lower()
    county_clean = canonicalize_county_name(county)

    keys = list_state_keys(state_code)
    if not keys:
        app.logger.warning(f"[resolve_dataset_key] No keys for state={state_code}")
        return None

    enriched_exact = []
    raw_exact = []
    enriched_fuzzy = []
    raw_fuzzy = []

    for key in keys:
        # key like 'merged_with_tracts_acs/az/maricopa-with-values-income.geojson'
        fname = key.split("/")[-1]
        if not fname.endswith(".geojson"):
            continue
        base = fname[:-len(".geojson")]  # strip .geojson
        has_values = "with-values-income" in base
        # strip suffix
        base_no_suffix = base.replace("-with-values-income", "")
        base_canon = canonicalize_county_name(base_no_suffix)

        if not base_canon:
            continue

        if base_canon == county_clean:
            if has_values:
                enriched_exact.append(key)
            else:
                raw_exact.append(key)
        elif base_canon.startswith(county_clean) or county_clean.startswith(base_canon):
            if has_values:
                enriched_fuzzy.append(key)
            else:
                raw_fuzzy.append(key)

    # pick best bucket
    for bucket in (enriched_exact, raw_exact, enriched_fuzzy, raw_fuzzy):
        if bucket:
            chosen = sorted(bucket, key=len)[0]
            app.logger.info(
                f"[resolve_dataset_key] state={state} county={county} "
                f"→ chosen key={chosen}"
            )
            return chosen

    app.logger.warning(
        f"[resolve_dataset_key] No match for state={state} county={county_clean} "
        f"(keys={len(keys)})"
    )
    return None


def load_geojson_from_s3(key: str):
    """
    Load GeoJSON from S3 with a size guard so we don't hang forever on huge files.

    1) head_object to get size
    2) if too large → raise ValueError with a clear message
    3) else → get_object and read fully
    """
    try:
        head = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        size = head.get("ContentLength", 0)
        app.logger.info(f"[S3] head_object for key={key} size={size} bytes")

        if size and size > MAX_GEOJSON_BYTES:
            # Too large for interactive UI
            raise ValueError(
                f"Dataset too large for this UI ({size} bytes). "
                f"Try a smaller geography or a different county."
            )

        resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        body = resp["Body"].read()
        return json.loads(body)

    except ValueError:
        # propagate so caller can turn it into a clean JSON error
        raise
    except ClientError as e:
        app.logger.error(f"[S3] get_object failed for {key}: {e}")
        raise
    except Exception as e:
        app.logger.error(f"[S3] Unexpected error reading {key}: {e}")
        raise


def filter_features(features, income_min=None, value_min=None):
    """
    Filter features by optional 'median_income' / 'median_value' fields (or similar).
    We'll look for several possible property names.
    """
    if income_min is None and value_min is None:
        return features

    income_keys = ["DP03_0062E", "median_income", "income", "household_income"]
    value_keys = ["DP04_0089E", "median_value", "home_value", "value"]

    def get_first(props, keys):
        for k in keys:
            if k in props and props[k] not in (None, ""):
                try:
                    return float(props[k])
                except (TypeError, ValueError):
                    continue
        return None

    filtered = []
    for feat in features:
        props = feat.get("properties", {})
        keep = True

        if income_min is not None:
            inc = get_first(props, income_keys)
            if inc is None or inc < income_min:
                keep = False

        if keep and value_min is not None:
            val = get_first(props, value_keys)
            if val is None or val < value_min:
                keep = False

        if keep:
            filtered.append(feat)

    return filtered


def feature_to_address_line(feat):
    props = feat.get("properties", {})
    number = props.get("number") or props.get("house_number") or ""
    street = props.get("street") or props.get("road") or ""
    unit = props.get("unit") or ""
    city = props.get("city") or ""
    postcode = props.get("postcode") or ""
    region = props.get("region") or props.get("STUSPS") or ""

    parts = []
    line1 = " ".join(str(x).strip() for x in [number, street, unit] if x)
    if line1:
        parts.append(line1)
    loc = ", ".join(p for p in [city, region, postcode] if p)
    if loc:
        parts.append(loc)
    return "\n".join(parts) if parts else "(no address)"


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
        return jsonify({"error": "Unauthorized"}), 401

    query = request.form.get("query", "") or (
        request.json.get("query", "") if request.is_json else ""
    )
    query = query.strip()
    app.logger.info(f"/search query={query!r}")

    if not query:
        return jsonify({"error": "Please enter a query."})

    state, county, city = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    app.logger.info(
        f"parse_location → state={state}, county={county}, city={city}; "
        f"filters: income_min={income_min}, value_min={value_min}"
    )

    if not state:
        return jsonify({"error": "Could not detect a state in your query."})

    if not county:
        return jsonify({"error": "Could not detect a county in your query."})

    key = resolve_dataset_key(state, county)
    app.logger.info(f"resolve_dataset_key → key={key}")

    if not key:
        return jsonify(
            {
                "error": f"No dataset found for {county} County, {state}. "
            }
        )

    try:
        gj = load_geojson_from_s3(key)
    except ValueError as ve:
        # size too large, or other controlled error
        return jsonify(
            {
                "error": str(ve),
                "dataset_key": key,
            }
        ), 400
    except Exception as e:
        return jsonify(
            {
                "error": f"Failed to load: {key} — {str(e)}"
            }
        ), 500

    features = gj.get("features", [])
    app.logger.info(f"Loaded {len(features)} features from {key}")

    features = filter_features(features, income_min=income_min, value_min=value_min)
    total = len(features)
    features = features[:MAX_RESULTS]

    addresses = [feature_to_address_line(f) for f in features]

    return jsonify(
        {
            "ok": True,
            "query": query,
            "state": state,
            "county": county,
            "dataset_key": key,
            "total": total,
            "shown": len(addresses),
            "results": addresses,
        }
    )


@app.route("/export", methods=["POST"])
def export():
    """
    Export matching features as zipped CSV.
    Expects 'query' in form/json, same parser as /search.
    """
    if not session.get("authed"):
        return jsonify({"error": "Unauthorized"}), 401

    query = request.form.get("query", "") or (
        request.json.get("query", "") if request.is_json else ""
    )
    query = query.strip()
    if not query:
        return jsonify({"error": "Missing query for export."}), 400

    state, county, city = parse_location(query)
    income_min, value_min = parse_numeric_filters(query)

    if not state or not county:
        return jsonify({"error": "Need a recognisable state and county to export."}), 400

    key = resolve_dataset_key(state, county)
    if not key:
        return jsonify(
            {
                "error": f"No dataset found for export for {county} County, {state}."
            }
        ), 400

    try:
        gj = load_geojson_from_s3(key)
    except ValueError as ve:
        return jsonify({"error": str(ve), "dataset_key": key}), 400
    except Exception as e:
        return jsonify({"error": f"Failed to load dataset for export: {str(e)}"}), 500

    features = gj.get("features", [])
    features = filter_features(features, income_min=income_min, value_min=value_min)

    # flatten to CSV rows
    rows = []
    fieldnames = set()

    for feat in features:
        props = feat.get("properties", {}).copy()
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            props["lon"] = coords[0]
            props["lat"] = coords[1]
        fieldnames.update(props.keys())
        rows.append(props)

    fieldnames = sorted(fieldnames)

    # write CSV into in-memory zip
    memfile = io.BytesIO()
    with zipfile.ZipFile(memfile, "w", zipfile.ZIP_DEFLATED) as zf:
        csv_buf = io.StringIO()
        writer = csv.DictWriter(csv_buf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
        zf.writestr("addresses.csv", csv_buf.getvalue())

    memfile.seek(0)
    filename = f"pelee_export_{state}_{canonicalize_county_name(county).replace(' ', '_')}.zip"

    return send_file(
        memfile,
        mimetype="application/zip",
        as_attachment=True,
        download_name=filename,
    )


# ----------------- MAIN -----------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    # For local testing; Render will ignore this and use gunicorn
    app.run(host="0.0.0.0", port=port, debug=True)

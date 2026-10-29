"""Cached job producer and LinkedIn-to-Ceipal manual applicant pipeline."""

import asyncio
import base64
import hashlib
import html
import json
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import mysql.connector
from azure_title import clean_job_title, clean_job_location, validate_city_location
from profile_rules import is_valid_city_name, is_valid_state_name

FINAL = {
    "saved",
    "duplicate",
    "form_ready",
    "skipped_db_exists",
    "skipped_linkedin_member",
    "skipped_india_location",
    "skipped_resume_parser_failed",
    "skipped_processing_failed",
}
UNCERTAIN = {"saving", "review_required"}

PUBLIC_PROFILE_RE = re.compile(
    r"https?://(?:[a-z0-9-]+\.)*linkedin\.com/in/[^\s?#]+", re.I
)

JOB_INDEX_PRIORITY = {
    "Federal": 1,
    "SLED": 2,
    "Canada": 3,
    "Healthcare": 4,
    "IT": 5,
    "Commercial": 6,
    "Non-IT": 7,
}

PARSE_RESUME_SELECTOR = "#parse_resume"
PARSE_TIMEOUT_S = int(os.getenv("CEIPAL_PARSE_TIMEOUT_S", "60"))  # 6 attempts of 10s max wait for form confirmation before skipping to next profile
PROFILE_EXTRACTION_VERSION = 11


def is_browser_closed_error(exc):
    if exc is None:
        return False
    chain = [exc]
    if getattr(exc, "__cause__", None):
        chain.append(exc.__cause__)
    if getattr(exc, "__context__", None):
        chain.append(exc.__context__)
    for e in chain:
        exc_type = type(e).__name__
        msg = str(e).lower()
        if exc_type in ("TargetClosedError", "BrowserClosedError"):
            return True
        closed_keywords = (
            "target page, context or browser has been closed",
            "target closed",
            "browser has been closed",
            "browser closed",
            "context closed",
            "page has been closed",
            "connection closed",
            "session closed",
            "process singleton lock",
            "user data directory is already in use",
            "browser process has been closed",
            "has been closed",
            "disconnected",
            "connection reset",
            "connection refused",
            "target crashed",
            "protocol error",
            "browser has disconnected",
        )
        if any(keyword in msg for keyword in closed_keywords):
            return True
    return False


LINKEDIN_LIMIT_CHECK_JS = r"""() => {
  const text = (document.body ? document.body.innerText : '').toLowerCase();
  const limitPatterns = [
    'commercial use limit',
    'monthly limit for profile searches',
    'reached your monthly limit',
    'reached the monthly limit',
    'reached your search limit',
    'reached the search limit',
    'search limit reached',
    'search limit has been reached',
    'profile search limit',
    'profile view limit',
    'view limit reached',
    'profile limit reached',
    'limit of profile views',
    'reached the limit for profile searches',
    'reached the limit of profile views',
    'maximum number of profile searches',
    'maximum number of profile views',
    'upgrade to view more profiles',
    'upgrade your plan to view more',
    'upgrade to recruiter',
    'out of profile views',
    'out of search limit',
    "you’ve reached the limit",
    "you've reached the limit",
    "you’ve reached your limit",
    "you've reached your limit",
    "you have reached your limit",
    'temporarily restricted',
    '0 profile views remaining',
    '0 search views remaining',
    '0 searches remaining'
  ];
  for (const pattern of limitPatterns) {
    if (text.includes(pattern)) {
      return pattern;
    }
  }
  const banner = document.querySelector(
    '.search-paywall, .commercial-use-limit, [data-test-search-limit-warning], .artdeco-banner--error, [data-test-commercial-use-limit], .search-results__paywall'
  );
  if (banner && (banner.innerText || '').trim()) {
    const bannerText = (banner.innerText || '').trim();
    if (/limit|upgrade|reached|quota/i.test(bannerText)) {
      return bannerText.slice(0, 100);
    }
  }
  return null;
}"""


async def check_linkedin_limit_reached(page):
    try:
        return await page.evaluate(LINKEDIN_LIMIT_CHECK_JS)
    except Exception:
        return None


def is_applicant_snapshot(url):
    return bool(
        re.match(
            r"^/(?:applicant_profiles|applicantprofiles)/snapshot/[^/]+",
            urlsplit(url).path,
            re.I,
        )
    )


def read_store(path):
    from state_store import read_json_object
    return read_json_object(path)


def json_safe(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [json_safe(v) for v in value]
    return value


def save_state_json(path, data):
    payload = json.dumps(json_safe(data), ensure_ascii=False, indent=2)
    for attempt in range(5):
        try:
            path.write_text(payload, encoding="utf-8")
            return
        except (PermissionError, OSError):
            time.sleep(0.05 * (attempt + 1))
    path.write_text(payload, encoding="utf-8")


def db_connect():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )


def build_title_mapping(titles):
    mapping = {}
    for title in titles or []:
        mapping[title] = clean_job_title(title)
    return mapping


def normalize_job_index(value):
    text = (value or "").strip().lower()
    mapping = {
        "federal": "Federal",
        "sled": "SLED",
        "canada": "Canada",
        "healthcare": "Healthcare",
        "it": "IT",
        "commercial": "Commercial",
        "non-it": "Non-IT",
        "non it": "Non-IT",
        "nonit": "Non-IT",
    }
    return mapping.get(text, "Commercial")


def job_sort_key(job):
    job_index = normalize_job_index(job.get("business_unit") or job.get("job_index"))
    return (
        JOB_INDEX_PRIORITY.get(job_index, 999),
        (job.get("code") or ""),
    )


def linkedin_public_url_exists(url, job_code=None):
    if not url:
        return False

    table = os.getenv("DB_PROFILE_URL")
    conn = db_connect()
    try:
        cur = conn.cursor()
        if job_code:
            cur.execute(
                f"SELECT id FROM {table} WHERE url=%s AND job_code=%s LIMIT 1",
                (url, job_code),
            )
        else:
            cur.execute(
                f"SELECT id FROM {table} WHERE url=%s LIMIT 1",
                (url,),
            )
        return cur.fetchone() is not None
    finally:
        conn.close()


def parse_job_created_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    formats = (
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%y %I:%M:%S %p",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%y %H:%M",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y",
        "%m/%d/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        pass
    return text


def update_linkedin_havesting_status(
    url,
    job_code=None,
    business_unit=None,
    status=1,
    job_title=None,
    cleaned_job_title=None,
    date_posted=None,
    tag_profile=0,
):
    if not url:
        return

    table = os.getenv("DB_PROFILE_URL") or os.getenv("DB_TABLE", "linkedin_havesting_url")
    conn = db_connect()
    try:
        cur = conn.cursor()
        tag_val = 1 if tag_profile else 0

        if date_posted is not None:
            parsed_dt = parse_job_created_date(date_posted)
            if isinstance(parsed_dt, datetime):
                date_posted = parsed_dt.strftime("%Y-%m-%d %H:%M:%S")
            elif parsed_dt:
                date_posted = str(parsed_dt)
        if not date_posted:
            date_posted = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def exec_sql(query_with_tag, params_with_tag, query_fallback, params_fallback):
            try:
                cur.execute(query_with_tag, params_with_tag)
            except Exception as exc:
                err_str = str(exc).lower()
                if "tag_profile" in err_str or "unknown column" in err_str:
                    try:
                        cur.execute(f"ALTER TABLE {table} ADD COLUMN tag_profile TINYINT(1) DEFAULT 0")
                        conn.commit()
                        cur.execute(query_with_tag, params_with_tag)
                    except Exception:
                        cur.execute(query_fallback, params_fallback)
                else:
                    raise

        if job_code:
            cur.execute(
                f"SELECT id FROM {table} WHERE url=%s AND job_code=%s LIMIT 1",
                (url, job_code),
            )
            existing = cur.fetchone()

            if existing:
                exec_sql(
                    f"UPDATE {table} SET status=%s, business_unit=%s, job_title=%s, cleaned_job_title=%s, date_posted=%s, tag_profile=%s WHERE url=%s AND job_code=%s",
                    (
                        status,
                        business_unit,
                        job_title,
                        cleaned_job_title,
                        date_posted,
                        tag_val,
                        url,
                        job_code,
                    ),
                    f"UPDATE {table} SET status=%s, business_unit=%s, job_title=%s, cleaned_job_title=%s, date_posted=%s WHERE url=%s AND job_code=%s",
                    (
                        status,
                        business_unit,
                        job_title,
                        cleaned_job_title,
                        date_posted,
                        url,
                        job_code,
                    ),
                )
            else:
                exec_sql(
                    f"INSERT INTO {table} (url, job_code, business_unit, status, job_title, cleaned_job_title, date_posted, tag_profile) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        url,
                        job_code,
                        business_unit,
                        status,
                        job_title,
                        cleaned_job_title,
                        date_posted,
                        tag_val,
                    ),
                    f"INSERT INTO {table} (url, job_code, business_unit, status, job_title, cleaned_job_title, date_posted) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (
                        url,
                        job_code,
                        business_unit,
                        status,
                        job_title,
                        cleaned_job_title,
                        date_posted,
                    ),
                )
        else:
            cur.execute(f"SELECT id FROM {table} WHERE url=%s LIMIT 1", (url,))
            existing = cur.fetchone()

            if existing:
                exec_sql(
                    f"UPDATE {table} SET status=%s, job_title=%s, cleaned_job_title=%s, date_posted=%s, tag_profile=%s WHERE url=%s",
                    (status, job_title, cleaned_job_title, date_posted, tag_val, url),
                    f"UPDATE {table} SET status=%s, job_title=%s, cleaned_job_title=%s, date_posted=%s WHERE url=%s",
                    (status, job_title, cleaned_job_title, date_posted, url),
                )
            else:
                exec_sql(
                    f"INSERT INTO {table} (url, status, job_title, cleaned_job_title, date_posted, tag_profile) VALUES (%s, %s, %s, %s, %s, %s)",
                    (url, status, job_title, cleaned_job_title, date_posted, tag_val),
                    f"INSERT INTO {table} (url, status, job_title, cleaned_job_title, date_posted) VALUES (%s, %s, %s, %s, %s)",
                    (url, status, job_title, cleaned_job_title, date_posted),
                )

        conn.commit()
    finally:
        conn.close()


def update_job_processing_status(job_code, status=1):
    if not job_code:
        return

    table = os.getenv("DB_PROFILE_URL", "linkedin_havesting_url")
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {table} SET job_status=%s WHERE job_code=%s", (status, job_code))
        conn.commit()
    finally:
        conn.close()


def is_linkedin_member_profile(profile):
    name = resume_name(profile).strip().casefold()
    return name == "linkedin member"


async def fill_if_empty(page, selector, value):
    field = page.locator(selector).first
    if not await field.count() or (await field.input_value()).strip():
        return False
    await field.fill(value)
    await field.blur()
    return True


async def choose_option(page, selector, label):
    field = page.locator(selector).first
    if not await field.count():
        return False
    try:
        await field.select_option(label=label)
        return True
    except Exception:
        pass
    return bool(
        await page.evaluate(
            """([selector, label]) => {
                const field = document.querySelector(selector);
                if (!field) return false;
                const wanted = label.trim().toLowerCase();
                const option = Array.from(field.options).find(o => o.text.trim().toLowerCase() === wanted);
                if (!option) return false;
                field.value = option.value;
                field.dispatchEvent(new Event('input', {bubbles: true}));
                field.dispatchEvent(new Event('change', {bubbles: true}));
                return true;
            }""",
            [selector, label],
        )
    )


async def safe_click(locator, timeout=10000):
    await locator.wait_for(state="visible", timeout=timeout)
    await locator.click(timeout=timeout)


async def detect_country_selector(page):
    selectors = [
        "#country",
        "select[name='country']",
        "select[name='country_id']",
        "select[name='Country']",
        "select[id*='country']",
        "select[name*='country']",
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if await locator.count():
                return selector
        except Exception:
            continue
    return None


def _normalized_location_token(value):
    return re.sub(r"[^a-z]", "", (value or "").strip().lower())


def _is_canada_location(value):
    token = _normalized_location_token(value)
    return token in {"canada", "ca"} or "canada" in token


def _is_district_of_columbia(value):
    token = _normalized_location_token(value)
    return token in {"districtofcolumbia", "dc"}


def _is_canada_business_unit(row):
    return _normalized_location_token(row.get("business_unit") or "") == "canada"


def _is_canada_dc_profile_job(row):
    profile = row.get("profile") or {}
    return _is_canada_business_unit(row) and _is_district_of_columbia(
        profile.get("state")
    )


def _verified_canadian_profile_city(profile):
    city = (profile.get("city") or "").strip()
    return (
        is_valid_city_name(city)
        and profile.get("city_location_validated") is True
        and _is_canada_location(profile.get("country"))
    )


CEIPAL_STATE_SELECTORS = [
    "#state",
    "#province",
    "select[name='state']",
    "select[name='State']",
    "select[name='province']",
    "select[name='Province']",
    "input[name='state']",
    "input[name='State']",
    "input[name='province']",
    "input[name='Province']",
    "select[id*='state']",
    "select[id*='province']",
    "input[id*='state']",
    "input[id*='province']",
    "input[placeholder*='State' i]",
    "input[aria-label*='State' i]",
]
CEIPAL_CITY_SELECTORS = [
    "#city",
    "input[name='city']",
    "input[name='City']",
    "input[id*='city']",
    "input[placeholder*='City' i]",
    "input[aria-label*='City' i]",
]


async def fill_input_by_selectors(page, selectors, value):
    if not value:
        return False
    for selector in selectors:
        try:
            if await fill_if_empty(page, selector, value):
                return True
        except Exception:
            continue
    return False


def _location_parts(value):
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part and part.strip()]


def _extract_city_from_location(value):
    parts = _location_parts(value)
    return parts[0] if parts else ""


def _extract_province_from_location(value):
    parts = _location_parts(value)
    if len(parts) >= 2:
        return parts[1]
    return ""


JOB_LOCATION_COUNTRIES = {
    "canada", "united states", "united states of america", "usa", "us",
    "united kingdom", "uk", "australia", "india", "mexico",
}
JOB_LOCATION_REGIONS = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming", "district of columbia",
    "alberta", "british columbia", "manitoba", "new brunswick",
    "newfoundland and labrador", "northwest territories", "nova scotia",
    "nunavut", "ontario", "prince edward island", "quebec", "saskatchewan",
    "yukon",
}
INDIA_REGIONS = {
    "andaman and nicobar islands", "andhra pradesh", "arunachal pradesh",
    "assam", "bihar", "chandigarh", "chhattisgarh", "dadra and nagar haveli",
    "daman and diu", "delhi", "goa", "gujarat", "haryana", "himachal pradesh",
    "jammu and kashmir", "jharkhand", "karnataka", "kerala", "ladakh",
    "lakshadweep", "madhya pradesh", "maharashtra", "manipur", "meghalaya",
    "mizoram", "nagaland", "odisha", "puducherry", "punjab", "rajasthan",
    "sikkim", "tamil nadu", "telangana", "tripura", "uttar pradesh",
    "uttarakhand", "west bengal",
}


def _job_city_state(location):
    parts = _location_parts(location)
    if not parts:
        return "", ""

    if (
        len(parts) == 2
        and parts[1].casefold() in JOB_LOCATION_COUNTRIES
        and parts[0].casefold() in JOB_LOCATION_REGIONS
    ):
        return "", parts[0] if is_valid_state_name(parts[0]) else ""

    city = parts[0] if is_valid_city_name(parts[0]) else ""
    state = ""
    if len(parts) >= 3:
        state = parts[1] if is_valid_state_name(parts[1]) else ""
    elif len(parts) == 2 and parts[1].casefold() not in JOB_LOCATION_COUNTRIES:
        state = parts[1] if is_valid_state_name(parts[1]) else ""
    return city, state


def _job_location_parts(location):
    parts = _location_parts(location)
    city, state = _job_city_state(location)
    country = ""
    if len(parts) >= 3:
        country = parts[-1]
    elif len(parts) == 2 and parts[-1].casefold() in JOB_LOCATION_COUNTRIES:
        country = parts[-1]
        state = ""
    return city, state, country


def _ceipal_city(profile, job_city, canada_job=False):
    profile_city = (profile.get("city") or "").strip()
    profile_country = (profile.get("country") or "").strip()
    if (
        is_valid_city_name(profile_city)
        and (not canada_job or _is_canada_location(profile_country))
    ):
        return profile_city, "profile"
    if is_valid_city_name(job_city):
        return job_city, "job"
    return "", ""


async def set_input_by_selectors(page, selectors, value):
    for selector in selectors:
        field = page.locator(selector).first
        try:
            if not await field.count():
                continue
            tag_name = (await field.evaluate("el => el.tagName")).upper()
            if tag_name == "SELECT":
                if value and await choose_option(page, selector, value):
                    return True
                if not value:
                    try:
                        return bool(await field.evaluate(
                            """el => {
                                if (el.tagName !== 'SELECT') return false;
                                const option = Array.from(el.options).find(o => !o.value);
                                if (!option) return false;
                                el.value = option.value;
                                el.dispatchEvent(new Event('input', {bubbles: true}));
                                el.dispatchEvent(new Event('change', {bubbles: true}));
                                return true;
                            }"""
                        ))
                    except Exception:
                        continue
                continue
            await field.fill(value)
            await field.blur()
            return True
        except Exception:
            continue
    return False


async def apply_job_location_before_save(page, row):
    location = (row.get("location") or "").strip()
    job_city, job_state, job_country = _job_location_parts(location)
    profile = row.get("profile") or {}
    canada_dc_profile_job = _is_canada_dc_profile_job(row)
    canada_job = (
        _is_canada_business_unit(row)
        or _is_canada_location(job_country)
        or canada_dc_profile_job
    )
    city, city_source = _ceipal_city(profile, job_city, canada_job)
    if city_source == "profile":
        state = (profile.get("state") or "").strip()
        country = (profile.get("country") or "").strip()
    else:
        state = job_state
        country = job_country
        if canada_job:
            country = "Canada"
    if canada_dc_profile_job and _is_district_of_columbia(profile.get("state")):
        state = job_state if _is_canada_location(job_country) else ""
        print(
            "    Canada business unit: removing District of Columbia from state/province before save",
            flush=True,
        )
    city_applied = (
        await set_input_by_selectors(page, CEIPAL_CITY_SELECTORS, city)
        if city or canada_dc_profile_job else False
    )
    state_applied = (
        await set_input_by_selectors(page, CEIPAL_STATE_SELECTORS, state)
        if state or canada_dc_profile_job else False
    )
    country_applied = False
    if country:
        country_selector = await detect_country_selector(page)
        if country_selector:
            country_applied = await choose_option(page, country_selector, country)
            if not country_applied:
                print(
                    f"    Ceipal country option not found for {country!r}.",
                    flush=True,
                )
        else:
            print("    Ceipal country field was not found.", flush=True)
    city_valid = is_valid_city_name(city)
    print(
        f"    Ceipal city from {city_source or 'available locations'} verified: {city_valid} "
        f"({city!r}; {len(city)}/18 characters)",
        flush=True,
    )
    if city:
        print(f"    Ceipal city filled from {city_source}: {city}", flush=True)
    else:
        print(
            f"    Ceipal city cleared: no valid city was available "
            f"({location!r})",
            flush=True,
        )
    if location:
        if state:
            print(f"    Ceipal state set from job location: {state}", flush=True)
        elif canada_dc_profile_job:
            print(
                "    Ceipal state/province cleared for Canada business unit profile.",
                flush=True,
            )
        else:
            print("    Ceipal state cleared: no job state was present.", flush=True)
    if not city_applied:
        print("    Ceipal city field was not found or could not be updated.", flush=True)
    if state and not state_applied:
        print("    Ceipal state field was not found or could not be updated.", flush=True)
    if country and not country_applied:
        print("    Ceipal country could not be updated from the selected location.", flush=True)
    return city_source


async def validate_ceipal_city_before_save(page, row, city_source):
    if city_source != "profile":
        print("    Ceipal job city is not AI-validated.", flush=True)
        return

    city_field = None
    for selector in CEIPAL_CITY_SELECTORS:
        field = page.locator(selector).first
        if await field.count():
            city_field = field
            break

    if city_field is None:
        print("    Ceipal city validation skipped: city field was not found.", flush=True)
        return

    city = (await city_field.input_value()).strip()
    profile = row.get("profile") or {}
    profile_city = (profile.get("city") or "").strip()
    profile_state = (profile.get("state") or "").strip()
    profile_country = (profile.get("country") or "").strip()
    profile_location = ", ".join(
        part for part in (profile_city, profile_state, profile_country) if part
    )
    job_location = (row.get("location") or "").strip()

    same_as_profile_city = (
        bool(city)
        and bool(profile_city)
        and is_valid_city_name(profile_city)
        and " ".join(city.split()).casefold()
        == " ".join(profile_city.split()).casefold()
    )
    is_canada_job = (
        _is_canada_business_unit(row)
        or _is_canada_location(_job_location_parts(job_location)[2])
    )
    canada_profile_country_ok = (
        not is_canada_job or _is_canada_location(profile_country)
    )
    if (
        same_as_profile_city
        and profile_country
        and canada_profile_country_ok
        and validate_city_location(profile_location)
    ):
        print(
            f"    Ceipal profile city passed AI validation for "
            f"{profile_country}: {city}",
            flush=True,
        )
        return

    job_city, job_state, job_country = _job_location_parts(job_location)
    if (
        _is_canada_business_unit(row)
        or _is_canada_location(job_country)
    ):
        job_country = "Canada"
    await city_field.fill(job_city)
    await city_field.blur()
    if job_state:
        await set_input_by_selectors(page, CEIPAL_STATE_SELECTORS, job_state)
    if job_country:
        country_selector = await detect_country_selector(page)
        if country_selector:
            await choose_option(page, country_selector, job_country)
    print(
        f"    Ceipal profile city failed AI/country validation for "
        f"{profile_location!r}; using job location instead: {job_location!r}.",
        flush=True,
    )


async def validate_canada_dc_profile_before_save(page, row):
    if not _is_canada_dc_profile_job(row):
        return

    country_selector = await detect_country_selector(page)
    if not country_selector:
        raise RuntimeError(
            "Canada business-unit profile has a DC state, but the Ceipal country field was not found."
        )
    country = await read_selected_option_label(page, country_selector)
    if not _is_canada_location(country):
        if not await force_country_value(page, "Canada"):
            raise RuntimeError(
                f"Ceipal country must be Canada before saving this profile; "
                f"got '{country}' and could not change it."
            )
        country = await read_selected_option_label(page, country_selector)
        if not _is_canada_location(country):
            raise RuntimeError(
                f"Ceipal country must be Canada before saving this profile; "
                f"got '{country}' after correction."
            )

    state_found = False
    state_value = ""
    state_field = None
    state_selector = None
    for selector in CEIPAL_STATE_SELECTORS:
        field = page.locator(selector).first
        if not await field.count():
            continue
        state_found = True
        state_field = field
        state_selector = selector
        state_value = (
            await field.evaluate(
                """el => el.tagName === 'SELECT'
                    ? (el.options[el.selectedIndex]
                        ? (el.options[el.selectedIndex].text || '').trim()
                        : '')
                    : (el.value || '').trim()"""
            )
        )
        break

    if not state_found:
        print(
            "    Ceipal state/province field not found; no District of Columbia "
            "value can be submitted.",
            flush=True,
        )
        return

    if _is_district_of_columbia(state_value):
        if (await state_field.evaluate("el => el.tagName")).upper() == "SELECT":
            await set_input_by_selectors(page, [state_selector], "")
        else:
            await state_field.fill("")
            await state_field.blur()
        cleared_value = (
            await state_field.evaluate(
                """el => el.tagName === 'SELECT'
                    ? (el.options[el.selectedIndex]
                        ? (el.options[el.selectedIndex].text || '').trim()
                        : '')
                    : (el.value || '').trim()"""
            )
        )
        if _is_district_of_columbia(cleared_value):
            raise RuntimeError(
                "Could not remove District of Columbia from the Ceipal state field "
                "for a Canada profile."
            )
        print(
            "    Removed District of Columbia from Ceipal state before save; "
            "country is Canada.",
            flush=True,
        )


async def read_selected_option_label(page, selector):
    field = page.locator(selector).first
    if not await field.count():
        return ""
    try:
        return (
            await page.evaluate(
                """(selector) => {
                    const field = document.querySelector(selector);
                    if (!field) return '';
                    const option = field.options[field.selectedIndex];
                    return option ? (option.text || '').trim() : '';
                }""",
                selector,
            )
        ).strip()
    except Exception:
        return ""


async def force_country_value(page, wanted_country):
    country_selector = await detect_country_selector(page)
    if not country_selector:
        return False

    labels = [wanted_country]
    if wanted_country == "Canada":
        labels.extend(["CANADA", "CA"])
    elif wanted_country == "United States":
        labels.extend(["USA", "US"])

    for label in labels:
        if await choose_option(page, country_selector, label):
            await asyncio.sleep(0.5)
            selected_label = await read_selected_option_label(page, country_selector)
            if (
                _is_canada_location(selected_label)
                if wanted_country == "Canada"
                else _normalized_location_token(selected_label)
                == _normalized_location_token(wanted_country)
            ):
                return True
    return False


async def enforce_canada_before_save(page, row):
    if not _is_canada_business_unit(row):
        return False
    job_location = clean_job_location((row.get("location") or "").strip()) if row.get("location") else ""
    canada_dc_profile_job = _is_canada_dc_profile_job(row)
    if not _is_canada_location(job_location) and not canada_dc_profile_job:
        return False

    country_selector = await detect_country_selector(page)
    if not country_selector:
        raise RuntimeError("Canada job detected, but the Ceipal country field was not found.")

    selected_before = await read_selected_option_label(page, country_selector)
    if _is_district_of_columbia(selected_before):
        print("    Canada patch: detected DC-like value in Canada case; forcing country to Canada", flush=True)

    if await force_country_value(page, "Canada"):
        print("    Canada patch: forced country to Canada", flush=True)
    else:
        raise RuntimeError("Canada job detected, but Ceipal country could not be set to Canada.")

    selected_after = await read_selected_option_label(page, country_selector)
    if not _is_canada_location(selected_after):
        print(f"    Canada patch: verification failed with selected country '{selected_after}', retrying Canada", flush=True)
        if not await force_country_value(page, "Canada"):
            raise RuntimeError("Ceipal country did not remain Canada after retry.")
        selected_after = await read_selected_option_label(page, country_selector)

    if not _is_canada_location(selected_after):
        print(f"    Canada patch: selected country still not Canada ('{selected_after}')", flush=True)
        raise RuntimeError(f"Ceipal country must be Canada for a Canada job; got '{selected_after}'.")

    print(f"    Canada patch: verified country as {selected_after}", flush=True)

    profile = row.get("profile") or {}
    job_city, canada_province, _ = _job_location_parts(job_location)
    if canada_dc_profile_job:
        canada_city = (
            (profile.get("city") or "").strip()
            if _verified_canadian_profile_city(profile)
            else ""
        )
        city_source = "verified Canadian profile" if canada_city else ""
        if not _is_canada_location(job_location):
            canada_province = ""
    else:
        canada_city, city_source = _ceipal_city(profile, job_city, canada_job=True)
    city_valid = is_valid_city_name(canada_city)
    print(
        f"    Canada patch: {city_source or 'available'} city {canada_city!r} "
        f"verified {city_valid} "
        f"({len(canada_city)}/18 characters)",
        flush=True,
    )
    applied = True

    if city_valid:
        city_filled = await fill_input_by_selectors(
            page,
            [
                "#city",
                "input[name='city']",
                "input[name='City']",
                "input[id*='city']",
                "input[placeholder*='City' i]",
                "input[aria-label*='City' i]",
            ],
            canada_city,
        )
        if city_filled:
            print(f"    Canada patch: filled city from {city_source}: {canada_city}", flush=True)
        else:
            print(f"    Canada patch: city fill skipped for {canada_city}", flush=True)

    if canada_province:
        province_filled = await fill_input_by_selectors(
            page,
            [
                "#state",
                "#province",
                "select[name='state']",
                "select[name='State']",
                "select[name='province']",
                "select[name='Province']",
                "input[name='state']",
                "input[name='State']",
                "input[name='province']",
                "input[name='Province']",
                "select[id*='state']",
                "select[id*='province']",
                "input[id*='state']",
                "input[id*='province']",
                "select[placeholder*='Province' i]",
                "select[aria-label*='Province' i]",
                "input[placeholder*='Province' i]",
                "input[aria-label*='Province' i]",
            ],
            canada_province,
        )
        if not province_filled:
            for selector in [
                "#state",
                "#province",
                "select[name='state']",
                "select[name='State']",
                "select[name='province']",
                "select[name='Province']",
                "select[id*='state']",
                "select[id*='province']",
            ]:
                try:
                    if await choose_option(page, selector, canada_province):
                        province_filled = True
                        break
                except Exception:
                    continue
        if province_filled:
            print(f"    Canada patch: filled province/state {canada_province}", flush=True)
        else:
            print(f"    Canada patch: province/state fill skipped for {canada_province}", flush=True)

    return applied


async def apply_dc_location_patch(page, row):
    profile = row.get("profile") or {}
    job_location = clean_job_location((row.get("location") or "").strip()) if row.get("location") else ""
    job_location_city = ""
    if job_location:
        job_location_city = job_location.split(",", 1)[0].strip()

    state = (profile.get("state") or "").strip()
    country = (profile.get("country") or "").strip()
    business_unit = (row.get("business_unit") or row.get("job_index") or "").strip()

    if any(
        _is_canada_location(value)
        for value in [country, job_location, business_unit]
        if value
    ):
        return False

    if not _is_district_of_columbia(state):
        return False

    applied = False
    if await force_country_value(page, "United States"):
        print("    District of Columbia patch: forced country to United States", flush=True)
        applied = True

    if job_location_city:
        city_filled = await fill_input_by_selectors(
            page,
            [
                "#city",
                "input[name='city']",
                "input[name='City']",
                "input[id*='city']",
                "input[placeholder*='City' i]",
                "input[aria-label*='City' i]",
            ],
            job_location_city,
        )
        if city_filled:
            print(f"    District of Columbia patch: filled city {job_location_city}", flush=True)
            applied = True

    return applied


async def click_submit_then_open_quick_tag(page):
    submit_candidates = [
        page.locator("//li[@id='restrict_status_div']//a[contains(text(),'Submit')]").first,
        page.locator("li#restrict_status_div a.dropdown-toggle").first,
        page.locator("a.dropdown-toggle:has-text('Submit')").first,
        page.get_by_role("link", name=re.compile(r"submit", re.I)).first,
    ]

    submit_clicked = False
    for locator in submit_candidates:
        try:
            if await locator.count():
                await locator.scroll_into_view_if_needed()
                await locator.click(force=True)
                await asyncio.sleep(1)
                submit_clicked = True
                break
        except Exception:
            continue

    if not submit_clicked:
        print("    quick tag skipped; Submit button not found", flush=True)
        return False

    quick_tag_candidates = [
        page.locator("//a[contains(@class,'tag_to_job_cls')]").first,
        page.locator("a.tag_to_job_cls").first,
        page.get_by_role("link", name=re.compile(r"quick tag", re.I)).first,
    ]

    for locator in quick_tag_candidates:
        try:
            if await locator.count():
                await locator.scroll_into_view_if_needed()
                await locator.click(force=True)
                await asyncio.sleep(5)
                print("    Quick Tag opened", flush=True)
                return True
        except Exception:
            continue

    print("    Quick Tag open failed", flush=True)
    return False


async def select_job_code_in_quick_tag(page, job_code):
    if not job_code:
        print("    No job_code Ã¢â€ â€™ skipping", flush=True)
        return False

    try:
        print("    Starting FINAL FIX...", flush=True)

        dropdown = page.locator('//*[@id="JobSeekerAppliedJobViewListForm"]/div[2]/div[1]').first
        await dropdown.wait_for(state="visible", timeout=15000)
        await dropdown.scroll_into_view_if_needed()
        await dropdown.click(force=True)
        print("    REAL CLICK DONE", flush=True)

        await asyncio.sleep(2)

        search_input = page.locator('//*[@id="select2-drop"]/div/input').first
        await search_input.wait_for(state="visible", timeout=15000)
        print("    Dropdown is visible now", flush=True)

        await search_input.fill("")
        await search_input.fill(job_code)
        print(f"    Typed: {job_code}", flush=True)

        await asyncio.sleep(2)

        result = page.locator("//li[contains(@class,'select2-result-selectable')]").first
        await result.wait_for(state="visible", timeout=15000)
        await result.click()
        print("    Job selected", flush=True)
        return True

    except Exception as e:
        print(f"    Job search failed: {e}", flush=True)
        return False


async def save_quick_tag(page):
    try:
        save_tag_btn = page.locator("//button[contains(@class,'save_tag_to_job')]").first
        await save_tag_btn.wait_for(state="visible", timeout=15000)
        await save_tag_btn.click(force=True)
        print("    Tag saved", flush=True)
        await asyncio.sleep(2)
        return True
    except Exception as e:
        print(f"    Tag save failed: {e}", flush=True)
        return False


async def quick_tag_job(page, job_code):
    if not job_code:
        return False

    try:
        opened = await click_submit_then_open_quick_tag(page)
        if not opened:
            return False

        selected = await select_job_code_in_quick_tag(page, job_code)
        if not selected:
            return False

        saved = await save_quick_tag(page)
        return saved

    except Exception as exc:
        print(f"    quick tag failed for job code {job_code}: {exc}", flush=True)
        return False


CANADIAN_PROVINCES = {
    "AB": "Alberta", "BC": "British Columbia", "MB": "Manitoba",
    "NB": "New Brunswick", "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia", "NT": "Northwest Territories", "NU": "Nunavut",
    "ON": "Ontario", "PE": "Prince Edward Island", "QC": "Quebec",
    "SK": "Saskatchewan", "YT": "Yukon",
}


async def parsed_location_value(page, selectors):
    for selector in selectors:
        field = page.locator(selector).first
        if await field.count() and await field.is_visible():
            if (await field.evaluate("el => el.tagName")).upper() == "SELECT":
                if not await field.input_value():
                    return ""
                return (await read_selected_option_label(page, selector)).strip()
            return (await field.input_value()).strip()
    return ""


async def location_dropdown(page, kind):
    """Find a visible native dropdown by label, id or name (State or Province)."""
    pattern = re.compile(r"country" if kind == "country" else r"state|province", re.I)
    labeled = page.get_by_label(pattern)
    for fields in (labeled, page.locator("select")):
        for field in await fields.all():
            if not await field.is_visible():
                continue
            if (await field.evaluate("el => el.tagName")).upper() != "SELECT":
                continue
            identity = await field.evaluate("el => [el.id, el.name, el.getAttribute('aria-label'), ...(el.labels ? Array.from(el.labels).map(l => l.textContent) : [])].join(' ')")
            if pattern.search(identity):
                return field
    raise RuntimeError(f"Visible {kind} dropdown not found; PDF retained")


async def dropdown_label(field):
    if not await field.input_value():
        return ""
    return (await field.locator("option:checked").inner_text()).strip()


async def select_location_dropdown(page, kind, names):
    """Wait for dependent options, select matching text/value and verify selection."""
    wanted = {_normalized_location_token(name) for name in names if name}
    for _ in range(20):
        field = await location_dropdown(page, kind)
        options = await field.locator("option").evaluate_all(
            "els => els.map(el => ({label: el.textContent.trim(), value: el.value, disabled: el.disabled}))"
        )
        for option in options:
            if option["disabled"] or not option["value"]:
                continue
            if not any(_normalized_location_token(option[key]) in wanted for key in ("label", "value")):
                continue
            await field.select_option(value=option["value"])
            await asyncio.sleep(.5)
            field = await location_dropdown(page, kind)
            if (_normalized_location_token(await dropdown_label(field)) in wanted
                    or _normalized_location_token(await field.input_value()) in wanted):
                return await dropdown_label(field)
        await asyncio.sleep(.5)
    raise RuntimeError(f"Could not select {kind} dropdown: {names}; PDF retained")


async def validate_parsed_location_before_save(page, row):
    """Verify selected country, city geography and State/Province before Save."""
    from profile_location import _country, _state

    canada = _is_canada_business_unit(row)
    country_field = await location_dropdown(page, "country")
    country = await dropdown_label(country_field)
    sources = [row.get("resume_location") or {}, row.get("profile") or {}, row,
               row.get("job_ai_location") or {}]
    if canada:
        if not _is_canada_location(country):
            country = await select_location_dropdown(page, "country", ["Canada", "CA", "CAN"])
        if not _is_canada_location(country):
            raise RuntimeError("Canada job country did not remain Canada; PDF retained")
        country = "Canada"
    elif not _country(country):
        target_country = next((source.get("country") for source in sources if source.get("country")), "")
        record = _country(target_country)
        if not record:
            raise RuntimeError("Country missing; cannot validate city before Save")
        country = await select_location_dropdown(page, "country", [record.name, record.alpha_2, record.alpha_3])

    # Read city after country selection, since changing country can reset fields.
    parsed_city = await parsed_location_value(page, CEIPAL_CITY_SELECTORS)
    parsed_state = await dropdown_label(await location_dropdown(page, "state"))
    candidates = [(parsed_city, parsed_state)]
    candidates.extend((source.get("city", ""), source.get("state", "")) for source in sources)
    seen = set()
    for city, state in candidates:
        city, state = str(city or "").strip(), str(state or "").strip()
        if not city or not state or (city, state) in seen:
            continue
        seen.add((city, state))
        country_record = _country(country)
        state_record = _state(state, country_record.alpha_2) if country_record else None
        if not state_record:
            continue
        location = f"{city}, {state_record.name}, {country}"
        if not await asyncio.to_thread(validate_city_location, location):
            continue
        await select_location_dropdown(
            page, "state", [state_record.name, state_record.code, state_record.code.split("-", 1)[-1]]
        )
        if not await set_input_by_selectors(page, CEIPAL_CITY_SELECTORS, city):
            raise RuntimeError("Could not set validated city; PDF retained")
        actual_city = await parsed_location_value(page, CEIPAL_CITY_SELECTORS)
        actual_state = await dropdown_label(await location_dropdown(page, "state"))
        actual_country = await dropdown_label(await location_dropdown(page, "country"))
        if canada and not _is_canada_location(actual_country):
            raise RuntimeError("Canada country changed after province selection; PDF retained")
        actual_location = f"{actual_city}, {actual_state}, {actual_country}"
        # Recheck actual form values after country/province dependent changes.
        if not await asyncio.to_thread(validate_city_location, actual_location):
            raise RuntimeError(f"Final city/state/country check failed: {actual_location}; PDF retained")
        row.update(city=actual_city, state=actual_state, country=actual_country,
                   location=actual_location, city_location_validated=True)
        print(f"[Ceipal] City/country rechecked and State/Province selected: {actual_location}", flush=True)
        return
    raise RuntimeError("No city with matching state/province and selected country passed validation; PDF retained")

async def validate_parsed_location_before_save_detailed(page, row):
    """Keep a successfully parsed form and correct geography without rejecting parsing."""
    from recruiting_ai import validate_city_name

    profile = row.get("profile") or {}
    final_job = row.get("job_ai_location") or {}
    target = row.get("resume_location") or profile
    country_selector = await detect_country_selector(page)
    selected_country = await read_selected_option_label(page, country_selector) if country_selector else ""
    country = selected_country or target.get("country") or final_job.get("country") or row.get("country", "")
    if _is_canada_business_unit(row):
        country = "Canada"
        if not _is_canada_location(selected_country):
            changed = await force_country_value(page, "Canada")
            print(f"[Ceipal location] Selecting Canada: {changed}", flush=True)
        else:
            print("[Ceipal location] Country already Canada; keeping it.", flush=True)

    parsed_city = await parsed_location_value(page, CEIPAL_CITY_SELECTORS)
    candidates = list(dict.fromkeys(str(value).strip() for value in (
        parsed_city, profile.get("city"), final_job.get("city"), target.get("city")
    ) if value and str(value).strip()))
    city = ""
    city_valid = False
    for candidate in candidates:
        try:
            valid = await asyncio.to_thread(validate_city_name, candidate)
        except Exception as exc:
            print(f"[Ceipal location] City AI unavailable: {exc}; using available job/profile city.", flush=True)
            break
        print(f"[Ceipal location] Is {candidate!r} a city? {valid}", flush=True)
        if valid:
            city, city_valid = candidate, True
            break
    city = city or final_job.get("city") or target.get("city") or parsed_city or ""
    state = (final_job.get("state") if _is_canada_business_unit(row) else "") or target.get("state") or profile.get("state") or ""
    if _is_canada_business_unit(row):
        state = CANADIAN_PROVINCES.get(state.upper(), state)
    if state:
        applied = False
        for _ in range(10):
            variants = [state]
            if _is_canada_business_unit(row):
                variants += [k for k, v in CANADIAN_PROVINCES.items() if v == state]
            for variant in variants:
                if await set_input_by_selectors(page, CEIPAL_STATE_SELECTORS, variant):
                    applied = True
                    break
            if applied:
                break
            await asyncio.sleep(.5)
        print(f"[Ceipal location] Province/state {state!r} selected: {applied}; parsed form retained.", flush=True)
    if city and city != parsed_city:
        applied = await set_input_by_selectors(page, CEIPAL_CITY_SELECTORS, city)
        print(f"[Ceipal location] City {city!r} filled: {applied}", flush=True)
    elif parsed_city:
        print(f"[Ceipal location] Keeping parsed city {parsed_city!r}.", flush=True)
    row.update(city=city, state=state, country=country,
               location=", ".join(v for v in (city, state, country) if v),
               city_name_validated=city_valid)
    print("[Ceipal location] Location checks finished; continuing to Save and job tagging.", flush=True)


async def validate_parsed_location_before_save_strict(page, row):
    """AI-check final profile geography, apply it, then verify parsed form values."""
    target = dict(row.get("resume_location") or row.get("profile") or {})
    city, state, country = (str(target.get(k) or "").strip() for k in ("city", "state", "country"))
    if _is_canada_business_unit(row):
        country = "Canada"
        state = CANADIAN_PROVINCES.get(state.upper(), state)
    location = ", ".join(v for v in (city, state, country) if v)
    print(f"[Ceipal location] AI validating before fill/save: {location}", flush=True)
    if not city or not country or not await asyncio.to_thread(validate_city_location, location):
        row["status"] = "review_required"
        raise ValueError(f"Final city/state/country did not pass AI validation: {location}")
    print("[Ceipal location] AI validation=true; filling country, state and city...", flush=True)
    if not await force_country_value(page, country):
        row["status"] = "review_required"
        raise RuntimeError(f"Cannot select Ceipal country {country}")
    state_applied = False
    for _ in range(20):
        state_applied = await set_input_by_selectors(page, CEIPAL_STATE_SELECTORS, state)
        if not state_applied and country == "Canada":
            abbreviation = next((k for k, v in CANADIAN_PROVINCES.items() if v == state), state)
            state_applied = await set_input_by_selectors(page, CEIPAL_STATE_SELECTORS, abbreviation)
        if state_applied:
            break
        await asyncio.sleep(.5)
    if not state_applied or not await set_input_by_selectors(page, CEIPAL_CITY_SELECTORS, city):
        row["status"] = "review_required"
        raise RuntimeError("Could not fill final Ceipal state/city")
    actual_city = await parsed_location_value(page, CEIPAL_CITY_SELECTORS)
    actual_state = await parsed_location_value(page, CEIPAL_STATE_SELECTORS)
    selector = await detect_country_selector(page)
    actual_country = await read_selected_option_label(page, selector) if selector else ""
    from profile_location import _country, _state, _key
    wanted_country, selected_country = _country(country), _country(actual_country)
    state_ok = _key(actual_state) == _key(state)
    if wanted_country and state:
        wanted_state = _state(state, wanted_country.alpha_2)
        selected_state = _state(actual_state, wanted_country.alpha_2)
        state_ok = state_ok or bool(wanted_state and selected_state and wanted_state.code == selected_state.code)
    if (_key(actual_city) != _key(city) or not state_ok or not wanted_country
            or not selected_country or wanted_country.alpha_2 != selected_country.alpha_2):
        row["status"] = "review_required"
        raise RuntimeError("Ceipal location fields did not retain the validated values")
    row.update(city=city, state=state, country=country, location=location,
               validated_ceipal_location=location)
    print(f"[Ceipal location] Fields verified: {location}; ready to save and tag.", flush=True)


async def validate_parsed_location_before_save_legacy(page, row):
    """Validate the actual parsed form, after country/province changes settle."""
    if row.get("location_source") == "job_fallback":
        mapped = row["resume_location"]
        if mapped["country"] and not await force_country_value(page, mapped["country"]):
            raise RuntimeError("Ceipal could not select the fallback job country")
        for selectors, value in ((CEIPAL_STATE_SELECTORS, mapped["state"]),
                                 (CEIPAL_CITY_SELECTORS, mapped["city"])):
            if not await set_input_by_selectors(page, selectors, value):
                raise RuntimeError("Ceipal could not apply the fallback job location")
        row["ceipal_location_source"] = "job_fallback"
        row["applied_ceipal_location"] = mapped["location"]
        return
    if _is_canada_business_unit(row):
        selector = await detect_country_selector(page)
        if not selector:
            raise RuntimeError("Ceipal country field was not found; cannot validate Canada")
        if not _is_canada_location(await read_selected_option_label(page, selector)):
            if not await force_country_value(page, "Canada"):
                raise RuntimeError("Cannot select Canada in Ceipal")
        _, province, _ = _job_location_parts(row.get("location", ""))
        province = CANADIAN_PROVINCES.get(province.upper(), province)
        if province not in CANADIAN_PROVINCES.values():
            raise RuntimeError("Job location does not contain a supported Canadian province")
        # Country changes may repopulate the dependent province dropdown asynchronously.
        applied = False
        for _ in range(20):
            for value in (province, next(k for k, v in CANADIAN_PROVINCES.items() if v == province)):
                if await set_input_by_selectors(page, CEIPAL_STATE_SELECTORS, value):
                    applied = True
                    break
            if applied:
                break
            await asyncio.sleep(.5)
        if not applied:
            raise RuntimeError(f"Cannot select Canadian province {province}")
        selected_state = await parsed_location_value(page, CEIPAL_STATE_SELECTORS)
        selected_state = CANADIAN_PROVINCES.get(selected_state.upper(), selected_state)
        if selected_state.casefold() != province.casefold():
            raise RuntimeError(f"Canadian province did not remain selected: {selected_state}")

    city = await parsed_location_value(page, CEIPAL_CITY_SELECTORS)
    state = await parsed_location_value(page, CEIPAL_STATE_SELECTORS)
    selector = await detect_country_selector(page)
    country = await read_selected_option_label(page, selector) if selector else ""
    if _is_canada_business_unit(row) and not _is_canada_location(country):
        raise RuntimeError("Ceipal country changed after province selection")
    if not city or not country:
        raise RuntimeError("Parsed city and country are required before saving")
    location = ", ".join(part for part in (city, state, country) if part)
    if not await asyncio.to_thread(validate_city_location, location):
        raise RuntimeError(f"Parsed city does not match state/country: {location}")
    row["validated_ceipal_location"] = location


async def parsed_applicant_form_ready(page, require_values=True):
    """Recognize populated personal-details fields independently of layout wrappers."""
    return await page.evaluate("""requireValues => {
        const fields = Array.from(document.querySelectorAll('input')).filter(el =>
            el.getClientRects().length && el.type !== 'hidden');
        const label = el => {
            const associated = el.labels ? Array.from(el.labels).map(l => l.textContent).join(' ') : '';
            return [el.id, el.name, el.placeholder, el.getAttribute('aria-label'), associated]
                .join(' ').toLowerCase().replace(/[^a-z]/g, '');
        };
        const first = fields.find(el => /firstname|fname/.test(label(el)) && (!requireValues || el.value.trim()));
        const last = fields.find(el => /lastname|lname|surname/.test(label(el)) && (!requireValues || el.value.trim()));
        return Boolean(first && last && first !== last);
    }""", require_values)


async def ensure_applicant_field(page, selectors, label_pattern, fallback, field_name):
    """Keep parsed values, otherwise fill the required field before saving."""
    for selector in selectors:
        fields = page.locator(selector)
        for field in await fields.all():
            if not await field.is_visible():
                continue
            if not (await field.input_value()).strip():
                if not fallback:
                    raise ValueError(f"Missing extracted {field_name}; cannot invent a candidate name")
                await field.fill(fallback)
                await field.blur()
                print(f"[Ceipal] Filled blank {field_name}.", flush=True)
            return
    field = page.get_by_label(re.compile(label_pattern, re.I)).first
    if await field.count() and await field.is_visible():
        if not (await field.input_value()).strip():
            if not fallback:
                raise ValueError(f"Missing extracted {field_name}")
            await field.fill(fallback)
            await field.blur()
        return
    raise RuntimeError(f"Required {field_name} control not found; form retained for review")


async def create_applicant(page, row, api, persist, job_code=None):
    raw = row.get("raw") or {}
    header = "\n".join(raw.get("public_sections") or []).strip().splitlines()
    names = [raw.get("name"), header[0] if header else "",
             (row.get("profile") or {}).get("name")]
    if (is_linkedin_member_profile(row.get("profile") or {}) or any(
        " ".join(str(name or "").split()).casefold() == "linkedin member"
        for name in names
    )):
        row["status"] = "skipped_linkedin_member"
        row["tag_job_status"] = "skipped_linkedin_member"
        persist()
        print(f"LinkedIn Member profile: resume upload/save skipped for {row['url']}", flush=True)
        return

    def progress(message):
        print(f"[Ceipal | job={job_code} | profile={row['url']}] {message}", flush=True)

    path = Path(row.get("resume_path", ""))
    if not path.is_file():
        raise ValueError(
            f"Resume {path} is missing; clear this profile's 'profile' entry to rebuild it."
        )

    db_url = row.get("raw", {}).get("public_url") or row.get("public_url") or row["url"]

    progress("Checking database for an existing applicant/profile URL...")
    if linkedin_public_url_exists(db_url, job_code):
        row["status"] = "skipped_db_exists"
        row["skip_reason"] = "public_url already exists in DB"
        row["public_url"] = db_url
        persist()
        print(f"    skipped Ceipal upload; public URL already exists in DB: {db_url}", flush=True)
        return

    def cleanup_resume_file():
        try:
            if path.is_file():
                path.unlink()
                row["resume_deleted"] = True
                row["resume_path"] = ""
                persist()
                print(f"    deleted local resume after Ceipal upload failure/skip: {path}", flush=True)
        except Exception as exc:
            print(f"    failed to delete local resume {path}: {exc}", flush=True)

    parsed_successfully = False
    dialog_handler = None
    try:
        progress("Opening Ceipal Add Applicant page...")
        await page.goto(
            os.getenv("CEIPAL_ADD_APPLICANT_URL", api.existing.CEIPAL_ADD_APPLICANT_URL),
            wait_until="domcontentloaded",
            timeout=180000,
        )

        progress(f"Waiting for resume upload control; PDF={path}")
        upload = page.locator(os.getenv("CEIPAL_PARSE_RESUME_SELECTOR", PARSE_RESUME_SELECTOR)).first
        await upload.wait_for(state="attached", timeout=60000)

        # Handle native browser dialogs (confirm/alert) automatically if Ceipal triggers one
        def handle_dialog(dialog):
            progress(f"Native browser dialog detected: '{dialog.message}'. Accepting ('Yes')...")
            asyncio.create_task(dialog.accept())

        dialog_handler = handle_dialog
        page.on("dialog", dialog_handler)

        await upload.set_input_files(str(path))
        progress("Resume selected. Opening applicant fields...")

        # Specific selectors for the "Yes" / Confirm popup that appears after uploading resume
        confirm_selectors = [
            # 1. Exact "Yes" buttons and links (case-insensitive in Playwright)
            "button:has-text('Yes'):visible",
            "a:has-text('Yes'):visible",
            "input[type='button'][value*='Yes' i]:visible",
            "input[type='submit'][value*='Yes' i]:visible",

            # 2. Ceipal modal specific selectors
            "#confirmation button:has-text('Yes'):visible",
            "#confirmation a:has-text('Yes'):visible",
            "#confirmation button.confirm:visible",
            "#confirmation .btn-primary:visible",
            "#confirmation button:has-text('Confirm'):visible",

            # 3. Active/visible modal dialog buttons
            ".modal.in button:has-text('Yes'):visible",
            ".modal.show button:has-text('Yes'):visible",
            ".modal:visible button:has-text('Yes')",
            ".modal:visible a:has-text('Yes')",
            ".modal:visible button.confirm",
            ".modal:visible .btn-primary:not(.close)",
            "[role='dialog']:visible button:has-text('Yes')",
            "[role='dialog']:visible a:has-text('Yes')",
            ".ui-dialog:visible button:has-text('Yes')",
            ".ui-dialog-buttonset button:has-text('Yes')",

            # 4. SweetAlert, Bootbox, and alert libraries
            ".swal2-confirm:visible",
            ".swal2-actions button:has-text('Yes'):visible",
            ".swal-button--confirm:visible",
            "button[data-bb-handler='confirm']:visible",
            "button[data-bb-handler='success']:visible",
            ".jconfirm-buttons button:has-text('Yes'):visible",

            # 5. Generic Confirm / Proceed buttons
            "button:has-text('Confirm'):visible",
            "button:has-text('Proceed'):visible",
            "button.confirm:visible",
            f"{api.existing.CEIPAL_CONFIRM_SELECTOR}:visible",
        ]

        async def try_accept_confirmation():
            for selector in confirm_selectors:
                try:
                    loc = page.locator(selector)
                    count = await loc.count()
                    for idx in range(count):
                        item = loc.nth(idx)
                        if not await item.is_visible():
                            continue
                        text = (await item.text_content() or await item.get_attribute("value") or "").strip().lower()
                        classes = (await item.get_attribute("class") or "").lower()
                        aria_label = (await item.get_attribute("aria-label") or "").lower()

                        # Prevent clicking close, dismiss, cancel or "No" buttons
                        if text in {"no", "cancel", "close", "×", "x", "dismiss"} or "close" in classes or "cancel" in classes or "close" in aria_label:
                            continue

                        progress(f"Clicking upload confirmation ('Yes') via: {selector} (text='{text}')...")
                        await item.scroll_into_view_if_needed()
                        await item.click(force=True, timeout=3000)
                        return True
                except Exception:
                    continue
            return False

        progress("Waiting for editable applicant form; parser-result classification disabled.")

        parsed = page.locator(api.existing.CEIPAL_PARSED_FORM_SELECTOR)
        duplicate = page.locator(api.existing.CEIPAL_DUPLICATE_MODAL_SELECTOR)
        deadline = time.time() + PARSE_TIMEOUT_S
        last_parse_progress = time.time()
        parse_wait_attempts = 0
        confirmation_clicked = False

        while time.time() < deadline:
            if await try_accept_confirmation():
                if not confirmation_clicked:
                    progress("Accepted resume upload confirmation ('Yes' clicked).")
                    confirmation_clicked = True

            if await duplicate.count() and await duplicate.is_visible():
                row["status"] = "duplicate"
                row["public_url"] = db_url
                row["tag_profile"] = 0
                persist()
                cleanup_resume_file()
                if job_code:
                    update_linkedin_havesting_status(
                        db_url,
                        job_code,
                        row.get("business_unit"),
                        1,
                        job_title=row.get("job_title"),
                        cleaned_job_title=row.get("cleaned_job_title"),
                        date_posted=row.get("job_created") or row.get("date_posted") or datetime.now(),
                        tag_profile=0,
                    )
                    update_job_processing_status(job_code, 1)
                print("    Ceipal already knows this applicant.", flush=True)
                return

            if ((await parsed.count() and await parsed.first.is_visible())
                    or await page.locator(os.getenv("CEIPAL_PARSE_SAVE_SELECTOR") or api.existing.CEIPAL_SAVE_SELECTOR).first.is_visible()):
                parsed_successfully = True
                progress("Applicant form ready; proceeding without field validation.")
                break

            if time.time() - last_parse_progress >= 10:
                parse_wait_attempts += 1
                last_parse_progress = time.time()
                progress(f"Still waiting for applicant form or Save control (attempt {parse_wait_attempts}/6)...")
                if parse_wait_attempts >= 6:
                    progress("Applicant form/confirm not ready after 6 attempts of 10s (60s total). Skipping to next profile...")
                    cleanup_resume_file()
                    raise TimeoutError("Applicant form / confirm not ready after 6 attempts of 10s (60s total); skipping to next profile")

            await asyncio.sleep(1)
        else:
            cleanup_resume_file()
            raise TimeoutError("Editable applicant controls were not found after 6 attempts (60s); skipping to next profile")

        if dialog_handler:
            try:
                page.remove_listener("dialog", dialog_handler)
            except Exception:
                pass

        progress("Parsed form ready; filling dummy email/mobile only when blank.")
        if not row.get("dummy_email"):
            row["dummy_email"] = api.existing.dummy_email(
                hashlib.sha256(row["url"].encode()).hexdigest()[:16]
            )
        if not row.get("dummy_mobile"):
            # .rstrip(".") ko hata diya taaki dots end mein safe rahein
            row["dummy_mobile"] = api.existing.dummy_mobile() 
        await ensure_applicant_field(
            page, (api.existing.CEIPAL_EMAIL_SELECTOR, 'input[type="email"]'),
            r"^email\s*(address)?\s*\*?$", row["dummy_email"], "Email"
        )
        await ensure_applicant_field(
            page, (api.existing.CEIPAL_MOBILE_SELECTOR, 'input[name="mobile"]', 'input[type="tel"]'),
            r"^mobile\s*(number)?\s*\*?$", row["dummy_mobile"], "Mobile"
        )
        source = os.getenv("CEIPAL_SOURCE", api.existing.CEIPAL_DEFAULT_SOURCE)
        if await choose_option(page, api.existing.CEIPAL_SOURCE_SELECTOR, source):
            print(f"    set source to {source}", flush=True)

        progress("Checking city/country and selecting State/Province; enforcing Canada for Canada business unit...")
        await validate_parsed_location_before_save(page, row)
        progress("Location step finished. Waiting for Save button...")

        save = await api.visible(page, (
            os.getenv("CEIPAL_PARSE_SAVE_SELECTOR") or api.existing.CEIPAL_SAVE_SELECTOR + ":visible",
            "button.save_all:visible",
            'button:text-is("Save"):visible',
            'button:text-is("Save All"):visible',
            'input[type="submit"][value="Save"]:visible',
        ), timeout=60)

        row["status"] = "saving"
        persist()
        progress("Clicking Save. Waiting for applicant snapshot or duplicate confirmation...")
        await save.click()

        for _ in range(60):
            if is_applicant_snapshot(page.url):
                row["status"] = "saved"
                row["applicant_url"] = page.url
                row["public_url"] = db_url
                persist()

                progress(f"Applicant save confirmed: {page.url}; tagging pipeline...")

                tagged = False
                try:
                    if job_code in (row.get("advanced_search_job_codes") or []):
                        progress("Save confirmed; waiting 5 seconds before Quick Tag...")
                        await asyncio.sleep(5)
                        progress(f"Tagging Advanced Search applicant to job {job_code}...")
                        tagged = await quick_tag_job(page, job_code)
                        row["tag_job_status"] = "tagged" if tagged else "tag_failed_or_skipped"
                    else:
                        row["tag_job_status"] = "skipped_not_advanced_search"
                        progress("Applicant saved; job tag skipped: not from this job's Advanced Search.")
                    persist()
                    progress(f"Job tagging result: {row['tag_job_status']}")
                except Exception as exc:
                    row["tag_job_status"] = f"tag_error: {exc}"
                    persist()
                    print(f"    tag job failed but continuing: {exc}", flush=True)

                tag_profile_val = 1 if tagged else 0
                row["tag_profile"] = tag_profile_val
                persist()

                if job_code:
                    update_linkedin_havesting_status(
                        db_url,
                        job_code,
                        row.get("business_unit"),
                        1,
                        job_title=row.get("job_title"),
                        cleaned_job_title=row.get("cleaned_job_title"),
                        date_posted=row.get("job_created") or row.get("date_posted") or datetime.now(),
                        tag_profile=tag_profile_val,
                    )
                    update_job_processing_status(job_code, 1)

                cleanup_resume_file()
                progress("Applicant processing finished: saved.")
                return

            duplicate_modal = page.locator(api.existing.CEIPAL_DUPLICATE_MODAL_SELECTOR).first
            if await duplicate_modal.count() and await duplicate_modal.is_visible():
                row["status"] = "duplicate"
                row["public_url"] = db_url
                row["tag_profile"] = 0
                persist()
                cleanup_resume_file()
                if job_code:
                    update_linkedin_havesting_status(
                        db_url,
                        job_code,
                        row.get("business_unit"),
                        1,
                        job_title=row.get("job_title"),
                        cleaned_job_title=row.get("cleaned_job_title"),
                        date_posted=row.get("job_created") or row.get("date_posted") or datetime.now(),
                        tag_profile=0,
                    )
                    update_job_processing_status(job_code, 1)
                return


            await asyncio.sleep(1)

        raise RuntimeError("Ceipal save not confirmed; reconcile this applicant before retrying.")

    except Exception as exc:
        if dialog_handler:
            try:
                page.remove_listener("dialog", dialog_handler)
            except Exception:
                pass
        if parsed_successfully:
            row["status"] = "review_required" if row.get("status") in {"saving", "review_required"} else "failed"
            row["error"] = str(exc)
            persist()
            progress(f"Resume parsing succeeded; later form/save step needs attention: {exc}. Resume retained; job not marked completed.")
            raise
        cleanup_resume_file()
        row["status"] = "failed"
        row["error"] = str(exc)
        row["public_url"] = db_url
        persist()
        progress(f"Upload/parsed-form detection failed: {exc}. Continuing to next profile...")
        raise

def find_public_profile_url(text, name):
    candidates = []
    for match in PUBLIC_PROFILE_RE.finditer(text or ""):
        segments = [segment for segment in urlsplit(match.group(0)).path.split("/") if segment]
        if len(segments) != 2 or segments[0] != "in":
            continue
        url = f"https://www.linkedin.com/in/{segments[1]}"
        if url not in candidates:
            candidates.append(url)
    if not candidates:
        return ""

    words = re.findall(r"[A-Za-z]{3,}", name or "")
    tokens = {words[0].casefold(), words[-1].casefold()} if words else set()
    for url in candidates:
        slug = urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1].casefold()
        if tokens and all(token in slug for token in tokens):
            return url
    return candidates[0]


async def settled_text(content, quiet_rounds=3, max_rounds=24):
    text = ""
    previous = -1
    stable = 0
    for _ in range(max_rounds):
        text = (await content.inner_text()).strip()
        if text and len(text) == previous:
            stable += 1
            if stable >= quiet_rounds:
                break
        else:
            stable = 0
            previous = len(text)
        await asyncio.sleep(0.5)
    return text


async def expand_profile_content(page, content):
    """Click each existing profile disclosure once, then return the expanded text."""
    await page.evaluate("""async () => {
      const step = Math.max(400, Math.floor(window.innerHeight * 0.8));
      for (let y = 0; y < document.body.scrollHeight; y += step) {
        window.scrollTo(0, y);
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      window.scrollTo(0, document.body.scrollHeight);
    }""")
    await page.wait_for_timeout(750)
    await page.evaluate("() => window.scrollTo(0, 0)")
    await page.wait_for_timeout(500)

    disclosures = content.locator(
        "button[data-live-test-expandable-button], "
        "button[data-test-expand-more-lower-button], "
        "button[data-test-expand-more-upper-button], "
        "button[id*='expand-more'], "
        "button[aria-label*='show all' i], "
        "button[aria-label*='show more' i], "
        "button[aria-label*='see more' i], "
        "button:has-text('Show all'), "
        "button:has-text('Show more'), "
        "button:has-text('See more'), "
        "a[aria-label*='show all' i], "
        "a[aria-label*='show more' i], "
        "a[aria-label*='see more' i], "
        "a:has-text('Show all'), "
        "a:has-text('Show more'), "
        "a:has-text('See more')"
    )
    for disclosure in await disclosures.all():
        if await disclosure.is_visible() and await disclosure.get_attribute("aria-expanded") != "true":
            await disclosure.scroll_into_view_if_needed()
            await disclosure.click()
            await page.wait_for_timeout(150)

    return (await content.inner_text()).strip()


async def expand_certification_disclosures(page, content):
    """Keep expanding disclosures inside Certifications until that section is complete."""
    sections = content.locator('[data-section="certifications"]')
    section = sections.first

    if not await sections.count():
        headings = content.locator("h1, h2, h3, h4, [role='heading']")
        for heading in await headings.all():
            label = (await heading.inner_text()).strip().casefold()
            if label not in {"licenses & certifications", "certifications"}:
                continue
            section = heading.locator(
                "xpath=ancestor::section[1]"
            )
            if await section.count():
                break
            section = heading.locator(
                "xpath=ancestor::*[.//button[@data-live-test-expandable-button] "
                "or .//button[contains(@class, 'expandable-list__button')]][1]"
            )
            if await section.count():
                break
        else:
            return

    disclosure_selector = (
        "button[data-live-test-expandable-button], "
        "button[data-test-expand-more-lower-button], "
        "button[data-test-expand-more-upper-button], "
        "button[id*='expand-more'], "
        "button[aria-label*='show all' i], "
        "button[aria-label*='show more' i], "
        "button[aria-label*='see more' i], "
        "button:has-text('Show all'), "
        "button:has-text('Show more'), "
        "button:has-text('See more'), "
        "a[aria-label*='show all' i], "
        "a[aria-label*='show more' i], "
        "a[aria-label*='see more' i], "
        "a:has-text('Show all'), "
        "a:has-text('Show more'), "
        "a:has-text('See more')"
    )

    for _ in range(100):
        buttons = section.locator(disclosure_selector)
        next_button = None
        for button in await buttons.all():
            if (
                await button.is_visible()
                and await button.get_attribute("aria-expanded") != "true"
            ):
                next_button = button
                break
        if next_button is None:
            return
        await next_button.scroll_into_view_if_needed()
        await next_button.click()
        await page.wait_for_timeout(250)

    raise RuntimeError(
        "Certification disclosures are still available after expanding 100 controls."
    )


class IndiaProfileSkipped(Exception):
    """Stop before expanding or extracting a profile located in India."""


class ExistingProfileSkipped(Exception):
    def __init__(self, public_url):
        super().__init__("Public profile URL already exists in DB for this job")
        self.public_url = public_url


async def read_profile_text(page, url, job_code=None):
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    if any(marker in page.url for marker in ("/login", "/authwall", "/checkpoint")):
        raise RuntimeError("LinkedIn profile requires login/verification.")
    limit_reason = await check_linkedin_limit_reached(page)
    if limit_reason:
        raise RuntimeError(f"LinkedIn profile view limit reached: {limit_reason}")
    override = os.getenv("LINKEDIN_PROFILE_CONTENT_SELECTOR")
    content = page.locator(override or 'main, [role="main"]').first
    await content.wait_for(state="visible", timeout=30000)
    # Inspect only the initial header location before opening any disclosures.
    initial_text = (await content.inner_text()).strip()
    if is_currently_in_india({}, {"public_sections": [initial_text]}):
        raise IndiaProfileSkipped("Visible profile header location is India")
    header_lines = re.split(
        r"(?im)^\s*(?:About|Experience|Education|Skills|Activity|Summary)\s*$",
        initial_text, maxsplit=1,
    )[0].splitlines()[:12]
    if any(re.search(r"(?:^|,)\s*India\s*(?:$|[·|])", line, re.I) for line in header_lines):
        raise IndiaProfileSkipped("Visible profile header location is India")
    public_url = find_public_profile_url(initial_text, initial_text.splitlines()[0] if initial_text else "")
    if not public_url:
        for href in await content.locator('a[href*="linkedin.com/in/"]').evaluate_all("els => els.map(el => el.href)"):
            if PUBLIC_PROFILE_RE.fullmatch(href):
                public_url = href
                break
    if public_url and job_code:
        print(f"[Profile {url}] Checking DB before full extraction: {public_url}", flush=True)
        if await asyncio.to_thread(linkedin_public_url_exists, public_url, job_code):
            raise ExistingProfileSkipped(public_url)
    text = await expand_profile_content(page, content)
    await expand_certification_disclosures(page, content)
    text = (await content.inner_text()).strip()
    if len(text) < 50:
        raise ValueError("No usable profile content found.")
    return text


async def extract_profile(page, url, job_code=None):
    text = await read_profile_text(page, url, job_code)
    listed_skills = []
    for selector in (
        'section[data-section="skills"] li h3',
        'section[data-section="skills"] .skill__name',
        '[data-section="skills"] li h3',
        '[data-section="skills"] .skill__name',
    ):
        for item in await page.locator(selector).all():
            if await item.is_visible():
                skill = (await item.inner_text()).strip()
                if skill and skill.casefold() not in {
                    value["skill"].casefold() for value in listed_skills
                }:
                    listed_skills.append({"skill": skill, "evidence": skill})

    raw = {
        "url": url,
        "public_sections": [text],
        "skill_mentions": listed_skills,
    }
    name = text.splitlines()[0].strip()
    public_url = find_public_profile_url(text, name)
    if public_url:
        raw["public_url"] = public_url
    return raw


RESUME_CSS = """
  @page { size: A4; margin: 14mm 13mm; }
  * { box-sizing: border-box; }
  body { font-family: "Segoe UI", Calibri, Arial, sans-serif; color: #1f2328;
         font-size: 10.5pt; line-height: 1.45; margin: 0; }
  header { border-bottom: 2px solid #1f2328; padding-bottom: 8px; margin-bottom: 14px; }
  h1 { font-size: 21pt; margin: 0 0 4px; letter-spacing: 0.3px; }
  .headline { font-size: 11.5pt; color: #444d56; margin-bottom: 5px; }
  .meta { font-size: 9.5pt; color: #57606a; }
  .meta span + span::before { content: "  |  "; color: #b0b7bf; }
  h2 { font-size: 11pt; text-transform: uppercase; letter-spacing: 0.9px;
       border-bottom: 1px solid #d0d7de; padding-bottom: 3px;
       margin: 14px 0 7px; color: #1f2328;
       break-after: avoid; page-break-after: avoid; }
  .section-item-title { font-size: 10.5pt; margin: 4px 0 2px; break-after: avoid; }
  .entry { margin-bottom: 6px; break-inside: avoid; white-space: pre-line; }
  .experience-entry { break-inside: auto; page-break-inside: auto; }
  .raw-entry { white-space: pre-line; }
  .role-title { font-size: 11pt; font-weight: 700; line-height: 1.3;
                margin: 0 0 2px; break-after: avoid; page-break-after: avoid; }
  .role-company, .role-dates { color: #444d56; font-size: 10pt; }
  .role-dates { margin-bottom: 4px; }
  .sub { color: #444d56; font-size: 10pt; }
  .description { white-space: pre-line; }
  .description ul { margin: 3px 0 5px; padding-left: 18px; }
  .description li { margin: 0 0 3px; }
  .chip { display: inline-block; border: 1px solid #d0d7de; border-radius: 3px;
          padding: 1px 7px; margin: 0 5px 5px 0; font-size: 9.5pt; }
"""


def escape(value):
    return html.escape(value or "", quote=True)


def resume_name(profile):
    return " ".join(
        part.strip()
        for part in (profile.get("first_name"), profile.get("last_name"))
        if part and part.strip()
    )


def resume_filename(profile, url):
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", resume_name(profile)).strip("-") or "profile"
    source = re.sub(r"[^A-Za-z0-9._-]+", "-", urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1])
    return f"{name}-{source or 'profile'}.pdf"


def extract_location_from_raw_profile(raw):
    sections = raw.get("public_sections", []) if isinstance(raw, dict) else []
    if not isinstance(sections, list):
        return {}

    lines = [line.strip() for section in sections if isinstance(section, str)
             for line in section.splitlines() if line.strip()]
    if not lines:
        return {}

    header = []
    for line in lines:
        if re.match(r"^(about|experience|education|skills|licenses & certifications|projects|contact info)\b", line, re.I):
            break
        header.append(line)

    for line in header:
        for segment in re.split(r"\s*[Â·â€¢]\s*", line):
            location = segment.split("Contact Info")[0].strip(" ,|-")
            if not location or any(ch.isdigit() for ch in location):
                continue
            fields = [part.strip() for part in location.split(",") if part.strip()]
            if len(fields) >= 2 and len(fields) <= 3:
                city = fields[0]
                state = fields[1] if len(fields) >= 2 else ""
                country = fields[2] if len(fields) >= 3 else ""
                if city and state:
                    return {
                        "city": city,
                        "state": state,
                        "country": country,
                        "full_location": ", ".join(part for part in (city, state, country) if part),
                    }
    return {}


def is_currently_in_india(profile, raw):
    profile = profile if isinstance(profile, dict) else {}
    raw_location = extract_location_from_raw_profile(raw)
    india_tokens = [
        raw_location.get("full_location", ""),
        raw_location.get("country", ""),
        raw_location.get("state", ""),
        profile.get("location", ""),
        profile.get("country", ""),
    ]
    if any(
        _normalized_location_token(part) == "india"
        or _normalized_location_token(part).endswith("india")
        for location in india_tokens
        for part in str(location or "").split(",")
    ):
        return True

    countries = [
        raw_location.get("country", ""),
        profile.get("country", ""),
    ]
    if any(str(country or "").strip() for country in countries):
        return False

    return any(
        _normalized_location_token(region) in INDIA_REGIONS
        for region in (raw_location.get("state", ""), profile.get("state", ""))
    )


def ensure_profile_location(profile, raw):
    profile = profile if isinstance(profile, dict) else {}
    raw_location = extract_location_from_raw_profile(raw)
    changed = False

    if raw_location.get("city") and not (profile.get("city") or "").strip():
        profile["city"] = raw_location["city"]
        changed = True
    if raw_location.get("state") and not (profile.get("state") or "").strip():
        profile["state"] = raw_location["state"]
        changed = True
    if raw_location.get("country") and not (profile.get("country") or "").strip():
        profile["country"] = raw_location["country"]
        changed = True

    location_parts = [part.strip() for part in (
        profile.get("city"),
        profile.get("state"),
        profile.get("country"),
    ) if part and part.strip()]
    if location_parts:
        computed = ", ".join(location_parts)
        if (profile.get("location") or "").strip() != computed:
            profile["location"] = computed
            changed = True
    elif raw_location.get("full_location") and not (profile.get("location") or "").strip():
        profile["location"] = raw_location["full_location"]
        changed = True

    return changed

def resume_block(block):
    if isinstance(block, dict):
        parts = []

        title = clean_resume_text(
            block.get("title") or block.get("school") or block.get("name")
        )
        organization = clean_resume_text(
            block.get("issuer") or block.get("company") or block.get("degree")
        )
        duration = clean_resume_text(
            block.get("duration") or block.get("dates") or block.get("issued")
        )
        location = clean_resume_text(block.get("location"))
        description = clean_resume_text(
            block.get("description") or block.get("details") or block.get("extra")
        )

        if title:
            parts.append(f"<div><strong>{escape(title)}</strong></div>")

        sub_items = [item for item in (organization, duration, location) if item]
        if sub_items:
            parts.append(f"<div class='sub'>{escape(' | '.join(sub_items))}</div>")

        if description:
            parts.append(f"<div class='description'>{escape(description)}</div>")
        if block.get("score"):
            parts.append(
                f"<div class='sub'>{escape(clean_resume_text(block['score']))}</div>"
            )
        if block.get("credential_id"):
            credential_id = clean_resume_text(block["credential_id"])
            if credential_id:
                parts.append(f"<div class='sub'>Credential ID: {escape(credential_id)}</div>")
        if block.get("credential_url"):
            credential_url = clean_resume_text(block["credential_url"])
            if credential_url:
                parts.append(f"<div class='sub'>Credential: {escape(credential_url)}</div>")

        return f"<div class='entry'>{''.join(parts)}</div>"

    return f"<div class='entry raw-entry'>{escape(clean_resume_text(block))}</div>"


def resume_certification_block(block):
    if not isinstance(block, dict):
        return resume_block(block)

    title = clean_resume_text(block.get("name") or block.get("title") or "")
    issuer = clean_resume_text(block.get("issuer") or "")
    issued = clean_resume_text(block.get("issued") or "")
    issuer = re.sub(r"^(?:issued by|issuing organization)\s*:\s*", "", issuer, flags=re.I)
    issued = re.sub(r"^issued(?:\s+date)?\s*:?\s*", "", issued, flags=re.I)
    if re.fullmatch(
        r"(?:[A-Za-z]{3,9}\s+)?\d{4}\s*[-\u2013\u2014]\s*"
        r"(?:Present|(?:[A-Za-z]{3,9}\s+)?\d{4})",
        title,
        re.I,
    ):
        issued = issued or title
        title = ""
    credential_id = clean_resume_text(block.get("credential_id") or "")
    credential_url = clean_resume_text(block.get("credential_url") or "")
    description = clean_resume_text(
        block.get("description") or block.get("details") or block.get("extra")
    )

    parts = ["<div class='entry'>"]
    if issuer:
        parts.append(f"<div class='sub'>{escape(issuer)}</div>")
    if title:
        parts.append(f"<div><strong>{escape(title)}</strong></div>")
    if issued:
        parts.append(f"<div class='sub'>{escape(issued)}</div>")
    if description:
        parts.append(f"<div class='description'>{escape(description)}</div>")
    if credential_id:
        parts.append(f"<div class='sub'>Credential ID: {escape(credential_id)}</div>")
    if credential_url:
        parts.append(f"<div class='sub'>Credential: {escape(credential_url)}</div>")
    parts.append("</div>")
    return "".join(parts)


RESUME_UI_NOISE_RE = re.compile(
    r"\b(?:see more|see less|show more|show less|show all(?:\s+\d+)?)\b",
    re.I,
)


def clean_resume_text(value):
    if not value:
        return ""
    cleaned_lines = []
    for line in str(value).splitlines():
        if re.fullmatch(r"\s*(?:more|less)\s*", line, re.I):
            continue
        cleaned = RESUME_UI_NOISE_RE.sub("", line)
        cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" \t\u00b7-|")
        if cleaned:
            cleaned_lines.append(cleaned)
    return "\n".join(cleaned_lines)


def resume_experience_block(block):
    title = clean_resume_text(block.get("title") or block.get("role") or "")
    company = clean_resume_text(block.get("company") or "")
    duration = clean_resume_text(block.get("duration") or block.get("dates") or "")
    location = clean_resume_text(block.get("location") or "")
    description = clean_resume_text(block.get("description") or block.get("details") or "")

    parts = ["<div class='entry experience-entry'>"]
    if title:
        parts.append(f"<h4 class='role-title'>{escape(title)}</h4>")
    if company:
        parts.append(f"<div class='role-company'>{escape(company)}</div>")
    date_meta = [value for value in (duration, location) if value]
    if date_meta:
        parts.append(f"<div class='role-dates'>{escape(' | '.join(date_meta))}</div>")
    if description:
        parts.append(f"<div class='description'>{escape(description)}</div>")
    parts.append("</div>")
    return "".join(parts)


RESUME_DATE_RE = re.compile(
    r"^(?:[A-Z][a-z]{2}\s+)?\d{4}\s*[-\u2013\u2014]\s*"
    r"(?:Present|(?:[A-Z][a-z]{2}\s+)?\d{4})",
    re.I,
)
RESUME_EMPLOYMENT_TYPES = (
    "Full-time|Part-time|Self-employed|Freelance|Contract|Internship|"
    "Apprenticeship|Seasonal"
)
RESUME_COMPANY_TYPE_RE = re.compile(
    rf"^(?P<company>.+?)\s*\u00b7\s*(?:{RESUME_EMPLOYMENT_TYPES})$",
    re.I,
)
RESUME_COMPANY_DURATION_RE = re.compile(
    rf"^(?:{RESUME_EMPLOYMENT_TYPES})\s*\u00b7\s*\d+\s*(?:yr|mo)",
    re.I,
)


def _profile_section_text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(
            item if isinstance(item, str) else ""
            for item in value
        )
    return ""


def _parse_resume_experience(value):
    lines = [line.strip() for line in _profile_section_text(value).splitlines() if line.strip()]
    anchors = [
        index for index, line in enumerate(lines)
        if RESUME_DATE_RE.match(line)
    ]
    entries = []
    company_at = {}
    for index, line in enumerate(lines):
        match = RESUME_COMPANY_TYPE_RE.match(line)
        if match:
            company_at[index] = match.group("company")
        elif index > 0 and RESUME_COMPANY_DURATION_RE.match(line):
            company_at[index - 1] = lines[index - 1]

    for position, date_index in enumerate(anchors):
        company_match = (
            RESUME_COMPANY_TYPE_RE.match(lines[date_index - 1])
            if date_index > 0 else None
        )
        if company_match:
            title = lines[date_index - 2] if date_index >= 2 else ""
            company = company_match.group("company")
        else:
            title = lines[date_index - 1] if date_index > 0 else ""
            company = next(
                (
                    company_at[index]
                    for index in sorted(company_at, reverse=True)
                    if index < date_index - 1
                ),
                "",
            )

        if not title:
            continue
        next_date = anchors[position + 1] if position + 1 < len(anchors) else len(lines)
        details_start = date_index + 1
        details_end = next_date
        if next_date < len(lines):
            if RESUME_COMPANY_TYPE_RE.match(lines[next_date - 1]):
                details_end = next_date - 2
            elif next_date >= 2 and RESUME_COMPANY_DURATION_RE.match(lines[next_date - 2]):
                details_end = next_date - 3
            else:
                details_end = next_date - 1
        details = lines[details_start:details_end]
        location = ""
        description = []
        for line in details:
            if line.startswith(("\u2022", "-", "\u2013")):
                description.append(line.lstrip("\u2022-\u2013 ").strip())
            elif not location and not description:
                location = line
            elif description:
                description[-1] = f"{description[-1]} {line}"
            else:
                description.append(line)
        entries.append({
            "title": title,
            "company": company,
            "duration": lines[date_index].split(" \u00b7 ")[0].strip(),
            "location": location,
            "description": "\n".join(description),
        })
    return entries


def _parse_resume_education(value):
    lines = [line.strip() for line in _profile_section_text(value).splitlines() if line.strip()]
    anchors = [
        index for index, line in enumerate(lines)
        if RESUME_DATE_RE.match(line)
    ]
    entries = []
    start = 0
    for date_index in anchors:
        details = lines[start:date_index]
        details = [line for line in details if not line.lower().startswith(("grade:", "score:"))]
        if details:
            entry = {"school": details[0]}
            if len(details) > 1:
                entry["degree"] = details[1]
            entry["dates"] = lines[date_index].split(" \u00b7 ")[0].strip()
            for line in lines[start:date_index]:
                if line.lower().startswith("grade:"):
                    entry["score"] = line
                elif line.lower().startswith("score:"):
                    entry["score"] = line
                elif line.lower().startswith("activities and societies:"):
                    entry["extra"] = line
            entries.append(entry)
        start = date_index + 1
        while start < len(lines) and lines[start].lower().startswith(
            ("grade:", "score:", "activities and societies:")
        ):
            if entries:
                entries[-1].setdefault("extra", lines[start])
            start += 1
    return entries


def _resume_section_entries(profile, field):
    value = profile.get(field)
    if not value:
        value = (profile.get("sections") or {}).get(field, "")
    if isinstance(value, dict):
        return [value]
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple)):
        values = list(value)
    else:
        return []

    if field == "experience":
        parsed = _parse_resume_experience(values)
        if parsed:
            return parsed
    elif field == "education":
        parsed = _parse_resume_education(values)
        if parsed:
            return parsed
    return values


def render_resume_html(profile, url, public_url=""):
    """Resume for the applicant: name, location, public profile URL and skills.

    The public /in/ URL is the candidate's own profile link; the talent URL is
    only a fallback for the rare profile that does not expose one.
    """
    name = resume_name(profile) or "Candidate"
    parts = ["<header>", f"<h1>{escape(name)}</h1>"]
    if profile.get("job_title"):
        parts.append(f"<div class='headline'>{escape(profile['job_title'])}</div>")
    location = (profile.get("location") or "").strip()
    if not location:
        location = ", ".join(part.strip() for part in (
            profile.get("city"), profile.get("state"), profile.get("country")
        ) if part and part.strip())
    meta = [item for item in (location, public_url or url) if item]
    if meta:
        parts.append("<div class='meta'>"
                     + "".join(f"<span>{escape(item)}</span>" for item in meta) + "</div>")
    parts.append("</header>")

    for field in ("about", "experience", "education", "honors_and_awards", "publications",
                  "courses", "organizations", "contact_information",
                  "featured", "activity"):
        blocks = profile.get(field)
        if not blocks:
            continue
        if isinstance(blocks, str):
            blocks = [blocks]
        title = "Professional Summary" if field == "about" else field.replace('_', ' ').title()
        parts.append(f"<h2>{escape(title)}</h2>")
        if field == "experience":
            blocks = _resume_section_entries(profile, field)
            for block in blocks:
                if isinstance(block, dict):
                    parts.append(resume_experience_block(block))
                else:
                    parts.append(resume_block(block))
            continue
        if field == "education":
            blocks = _resume_section_entries(profile, field)
            for block in blocks:
                if isinstance(block, dict):
                    school = (block.get("school") or block.get("title") or "").strip()
                    if school:
                        parts.append(
                            f"<h3 class='section-item-title'>"
                            f"{escape(clean_resume_text(school))}</h3>"
                        )
                        block = dict(block)
                        block.pop("school", None)
                        if block.get("title") == school:
                            block.pop("title", None)
                    parts.append(resume_block(block))
                else:
                    parts.append(resume_block(block))
            continue
        for block in blocks:
            parts.append(resume_block(block))

    skills = []
    seen_skills = set()
    for item in profile.get("skills", []):
        value = item.get("skill", "") if isinstance(item, dict) else str(item)
        skill = clean_resume_text(value).strip()
        if skill and skill.casefold() not in seen_skills:
            skills.append(skill)
            seen_skills.add(skill.casefold())
    if skills:
        parts.append("<h2>Skills</h2>")
        parts.append("<div class='skills'>"
                     + "".join(f"<span class='chip'>{escape(skill)}</span>" for skill in skills) + "</div>")

    certifications = profile.get("certifications")
    if certifications:
        parts.append("<h2>Certifications</h2>")
        if isinstance(certifications, str):
            certifications = [certifications]
        for block in certifications:
            if isinstance(block, dict):
                parts.append(resume_certification_block(block))
            else:
                cleaned = clean_resume_text(block)
                if cleaned:
                    parts.append(resume_block(cleaned))

    return ("<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{escape(name)} - Resume</title><style>{RESUME_CSS}</style></head>"
            f"<body>{''.join(parts)}</body></html>")


async def write_resume_pdf(context, html_doc, path):
    page = await context.new_page()
    try:
        await page.set_content(html_doc, wait_until="load")
        client = await context.new_cdp_session(page)
        try:
            await client.send("Page.enable")
            result = await client.send("Page.printToPDF", {
                "printBackground": True,
                "paperWidth": 8.27, "paperHeight": 11.69,
                "marginTop": 0.55, "marginBottom": 0.55,
                "marginLeft": 0.51, "marginRight": 0.51,
            })
        finally:
            await client.detach()
    finally:
        await page.close()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(base64.b64decode(result["data"]))


def sync_jobs_from_disk(job_path, jobs):
    if not job_path.exists():
        return
    for _ in range(5):
        try:
            text = job_path.read_text(encoding="utf-8")
            if not text.strip():
                return
            disk_data = json.loads(text)
            if isinstance(disk_data, dict):
                for k, v in disk_data.items():
                    if k not in jobs:
                        jobs[k] = v
                    elif isinstance(v, dict):
                        for field, val in v.items():
                            if field not in jobs[k] or val != jobs[k].get(field):
                                jobs[k][field] = val
            break
        except (PermissionError, OSError, json.JSONDecodeError):
            time.sleep(0.05)
        except Exception:
            break


def get_pending_advanced_jobs(jobs, history, args, exclude_codes=None):
    pending = []
    failed_pending = []
    for code, cached in jobs.items():
        if args.job_url and cached.get("url") != args.job_url:
            continue
        if cached.get("analysis_status") != "completed":
            continue
        if not args.search_only:
            if cached.get("linkedin_processing") == "completed":
                continue
            if history.get(code, {}).get("status") == "completed":
                continue
        saved_job = history.get(code, {})
        if saved_job.get("advanced_pipeline_status") in ("completed", "failed"):
            continue
        if exclude_codes and code in exclude_codes:
            failed_pending.append(cached)
        else:
            pending.append(cached)
    pending.sort(key=lambda j: job_sort_key(j))
    failed_pending.sort(key=lambda j: job_sort_key(j))
    return pending if pending else failed_pending


def get_pending_normal_jobs(jobs, history, args, exclude_codes=None):
    pending = []
    failed_pending = []
    for code, cached in jobs.items():
        if args.job_url and cached.get("url") != args.job_url:
            continue
        if cached.get("analysis_status") != "completed":
            continue
        saved_job = history.get(code, {})
        if saved_job.get("advanced_pipeline_status") not in ("completed", "failed"):
            continue
        if args.search_only:
            if saved_job.get("search_status") == "completed":
                continue
        else:
            if cached.get("linkedin_processing") == "completed":
                continue
            if history.get(code, {}).get("status") == "completed":
                continue
        if exclude_codes and code in exclude_codes:
            failed_pending.append(cached)
        else:
            pending.append(cached)
    pending.sort(key=lambda j: job_sort_key(j))
    failed_pending.sort(key=lambda j: job_sort_key(j))
    return pending if pending else failed_pending


def has_unready_jobs(jobs, args):
    for code, cached in jobs.items():
        if args.job_url and cached.get("url") != args.job_url:
            continue
        if cached.get("analysis_status") == "pending":
            return True
    return False


async def produce_jobs(api, args, ceipal, jobs, history, job_path, queue, failures, queued_codes=None, ceipal_lock=None):
    collected_jobs = []
    queued_codes = queued_codes if queued_codes is not None else set()

    def queue_ready_job(code, cached):
        if code in queued_codes or cached.get("analysis_status") != "completed":
            return
        if not args.search_only and cached.get("linkedin_processing") == "completed":
            return
        cached.setdefault("code", code)
        ready_job = dict(cached)
        collected_jobs.append(ready_job)
        queued_codes.add(code)
        queue.put_nowait(ready_job)
        print(f"Job {code}: business unit={cached.get('business_unit')} | index={job_sort_key(cached)[0]}; queued for LinkedIn without AI.", flush=True)

    try:
        # Feed the consumer before discovery or any new AI work starts.
        if not args.jobs_only:
            for code, cached in sorted(list(jobs.items()), key=lambda item: job_sort_key(item[1])):
                if args.job_url and cached.get("url") != args.job_url:
                    continue
                queue_ready_job(code, cached)
            await asyncio.sleep(0)

        async def _discover_and_enrich():
            urls = [args.job_url] if args.job_url else await api.discover_jobs(ceipal, args.jobs_limit)
            metadata_by_url = getattr(api, "JOB_METADATA", {})
            urls.sort(key=lambda url: job_sort_key(
                metadata_by_url.get(url) or jobs.get(api.job_code_for(url), {})
            ))
            for url in urls:
                code = api.job_code_for(url)
                if code in queued_codes:
                    continue
                if not args.search_only and history.get(code, {}).get("status") == "completed":
                    print(f"Job {code} already completed; skipping.", flush=True)
                    continue
                metadata = getattr(api, "JOB_METADATA", {}).get(url, {})
                created_val = metadata.get("job_created") or metadata.get("date_posted") or ""
                cached = jobs.setdefault(code, {
                    "url": url, "code": code,
                    "title": metadata.get("title", ""),
                    "location": metadata.get("location", ""),
                    "business_unit": metadata.get("business_unit", ""),
                    "date_posted": json_safe(created_val),
                    "job_created": json_safe(metadata.get("job_created") or created_val),
                })
                if created_val:
                    cached["date_posted"] = json_safe(created_val)
                    if metadata.get("job_created"):
                        cached["job_created"] = json_safe(metadata.get("job_created"))
                cached["job_index"] = normalize_job_index(cached.get("business_unit"))
                cached.setdefault("linkedin_processing", "pending")
                cached.setdefault("analysis_status", "pending")
                try:
                    api.save_state(job_path, jobs)
                except Exception as e:
                    print(f"Warning: could not save job state: {e}", flush=True)
                try:
                    if cached.get("analysis_status") != "completed":
                        await enrich_job_details(
                            api, ceipal, cached, lambda: api.save_state(job_path, jobs)
                        )
                        api.save_state(job_path, jobs)
                    else:
                        print(f"Using analyzed cached job: {code}", flush=True)
                    if cached.get("analysis_status") == "completed":
                        queue_ready_job(code, cached)
                        await asyncio.sleep(0)
                except Exception as exc:
                    if is_browser_closed_error(exc):
                        cached.setdefault("analysis_status", "pending")
                        try:
                            api.save_state(job_path, jobs)
                        except Exception:
                            pass
                        print(f"Browser closed suddenly during Ceipal enrichment for Job {code}; state kept pending.", flush=True)
                        raise
                    cached["analysis_status"] = "failed"
                    cached["analysis_error"] = str(exc)
                    try:
                        api.save_state(job_path, jobs)
                    except Exception:
                        pass
                    failures.append(code)
                    print(f"Job {code}: {type(exc).__name__}: {exc}; raw data retained; continuing.", flush=True)

        if ceipal_lock is not None:
            async with ceipal_lock:
                await _discover_and_enrich()
        else:
            await _discover_and_enrich()
    finally:
        queue.put_nowait(None)
        print(f"Job collection finished: {len(collected_jobs)} analyzed jobs queued.", flush=True)

async def read_original_job_description(page):
    original = page.locator("a.cpjDS.show_all_desc").first
    await page.locator(".jpDescription").first.wait_for(state="visible", timeout=60000)
    if await original.count() and await original.is_visible():
        await original.click()

    override = os.getenv("CEIPAL_JOB_DESCRIPTION_SELECTOR", "").strip()
    if override:
        content = page.locator(override).first
        await content.wait_for(state="visible", timeout=60000)
        return await settled_text(content)

    # Original Format can populate the expanded container asynchronously.
    # Wait for that request before reading either original content or the full JD.
    await page.wait_for_function("""() => {
        const expanded = document.querySelector('.jpDescription .jd_expand_div_data');
        const full = document.querySelector('.jpDescription #descmore');
        return Boolean((expanded && expanded.textContent.trim()) ||
                       (full && full.textContent.trim()));
    }""", timeout=60000)
    more = page.locator(".jpDescription #desc_more").first
    if await more.count() and await more.is_visible():
        await more.click()

    # #descless is truncated. Read #descmore even if the site's format toggle
    # hides it; text_content retains the complete description in the supplied DOM.
    previous = None
    stable = 0
    for _ in range(24):
        text = await page.evaluate("""() => {
            const expanded = document.querySelector('.jpDescription .jd_expand_div_data');
            const full = document.querySelector('.jpDescription #descmore');
            const node = expanded && expanded.textContent.trim() ? expanded : full;
            return node ? node.textContent.trim() : '';
        }""")
        if text and text == previous:
            stable += 1
            if stable >= 3:
                return text
        else:
            stable = 0
        previous = text
        await asyncio.sleep(.5)
    raise RuntimeError("Original Format JD did not finish loading")


async def enrich_job_details(api, page, cached, persist_raw=None):
    from recruiting_ai import analyze_job

    await page.goto(cached["url"], wait_until="domcontentloaded", timeout=180000)
    title_selector = os.getenv("CEIPAL_JOB_TITLE_SELECTOR", "")
    title = ""
    if title_selector:
        field = page.locator(title_selector).first
        await field.wait_for(state="visible", timeout=60000)
        title = await field.evaluate("el => el.value === undefined ? el.innerText : el.value")
    else:
        field = page.get_by_label(re.compile(r"^Job Title\s*[:*]?$", re.I))
        if await field.count() == 1 and await field.is_visible():
            title = await field.evaluate("el => el.value === undefined ? el.innerText : el.value")
    title = " ".join((title or cached.get("title", "")).split())
    cached["raw_title"] = title
    cached["title"] = title
    print(f"Job {cached['code']} opened: {cached['url']}", flush=True)
    print(f"RAW TITLE: {title}", flush=True)
    location_selector = os.getenv("CEIPAL_JOB_LOCATION_SELECTOR", "")
    if location_selector:
        field = page.locator(location_selector).first
        await field.wait_for(state="visible", timeout=60000)
        location = await field.evaluate("el => el.value === undefined ? el.innerText : el.value")
    else:
        location = await api.extract_ceipal_location_from_detail(page)
    location = location or cached.get("raw_location") or cached.get("location", "")
    cached["raw_location"] = location
    print(f"RAW LOCATION: {location}", flush=True)
    if persist_raw:
        persist_raw()
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
    try:
        text = await read_original_job_description(page)
    except PlaywrightTimeoutError:
        # Do not disguise a closed browser or expired login as a missing JD.
        if page.is_closed() or any(part in page.url for part in ("/signin", "/autologout")):
            raise
        text = ""
        print("JD not found; analyzing title/location only, skills and experience=None.", flush=True)
    cached["raw_jd"] = text
    cached["jd"] = text
    cached["extracted_at"] = datetime.now(timezone.utc).isoformat()
    cached["analysis_status"] = "pending"
    if persist_raw:
        persist_raw()
    print(f"RAW JD:\n{text}\nEND JD", flush=True)
    print("Raw title, location and JD saved. Sending all three to AI...", flush=True)
    details = await asyncio.to_thread(
        analyze_job, title, location,
        text, cached.get("business_unit", ""),
    )
    cached.update(details)
    cached["ai_final_location"] = {
        key: details[key] for key in ("location", "city", "state", "country")
    }
    cached["jd"] = details["job_description"]
    cached["location_variants"] = api.generate_location_variants(
        details["location"], cached.get("business_unit", "")
    )
    cached["location_details"] = {
        "full_location": details["location"],
        "state_country": ", ".join(v for v in (details["state"], details["country"]) if v),
        "country": details["country"],
    }
    cached["title_mapping"] = {title: details["cleaned_title"]}
    cached["job_analysis_version"] = 4
    cached["analysis_status"] = "completed"
    cached.pop("analysis_error", None)
    print("AI RESULT: " + json.dumps(details, ensure_ascii=False), flush=True)
    print("LOCATION VARIANTS: " + json.dumps(cached["location_variants"], ensure_ascii=False), flush=True)
    return cached


def skip_failed_profile(api, row, error, persist):
    """Record a terminal skip and delete only this profile's generated PDF."""
    row["status"] = "skipped_processing_failed"
    row["error"] = str(error)
    pdf = Path(row.get("resume_path") or "").resolve()
    resume_root = (api.ROOT / "resumes").resolve()
    try:
        if pdf.is_file():
            if not pdf.is_relative_to(resume_root) or pdf.suffix.lower() != ".pdf":
                raise ValueError(f"PDF cleanup outside generated resumes folder refused: {pdf}")
            pdf.unlink()
            row["resume_deleted"] = True
        row["resume_path"] = ""
    except Exception as cleanup_error:
        row["error"] += f"; PDF cleanup: {cleanup_error}"
        print(f"[Profile {row.get('url')}] PDF cleanup failed: {cleanup_error}", flush=True)
    persist()
    print(f"[Profile {row.get('url')}] Failed/skipped: {error}; continuing to next profile.", flush=True)


async def upload_pending_resumes(api, args, page, jobs, profiles, persist):
    """Attempt all tracked pending PDFs first; failures never abort the queue."""
    for url, row in profiles.items():
        if row.get("status") in FINAL or not Path(row.get("resume_path") or "").is_file():
            continue
        codes = row.get("job_codes") or []
        if args.job_url:
            codes = [code for code in codes if jobs.get(code, {}).get("url") == args.job_url]
            if not codes:
                continue
        row.setdefault("url", url)
        try:
            if not codes:
                raise RuntimeError("Pending PDF has no job code")
            code = codes[-1]
            job = jobs.get(code, {})
            for key in ("business_unit", "job_index", "date_posted", "job_created"):
                if job.get(key) is not None:
                    row[key] = job[key]
            row["job_title"] = job.get("title") or row.get("job_title", "")
            row["cleaned_job_title"] = job.get("cleaned_title") or row.get("cleaned_job_title", "")
            # Explicit restart retry: create_applicant still checks DB and Ceipal duplicates.
            row["status"] = "pending"
            persist()
            print(f"Startup: uploading pending PDF first: {row['resume_path']}", flush=True)
            await create_applicant(page, row, api, persist, code)
            if row.get("status") not in FINAL:
                raise RuntimeError("Pending PDF upload did not finish")
            row.pop("error", None)
            persist()
        except Exception as exc:
            if is_browser_closed_error(exc):
                print(f"Browser closed suddenly during pending PDF upload for {url}; status kept pending.", flush=True)
                raise
            # A confirmed save must not become a retry/skip after a tracking error.
            if row.get("status") in {"saved", "duplicate", "skipped_db_exists"}:
                row["error"] = str(exc)
                persist()
                print(f"[Profile {url}] Applicant finished; tracking error: {exc}; continuing.", flush=True)
            else:
                skip_failed_profile(api, row, exc, persist)
            continue

async def run(api, args):
    from state_store import check_and_perform_daily_830am_est_cleanup
    check_and_perform_daily_830am_est_cleanup(api.ROOT)
    job_path = api.ROOT / "extract_job.json"
    profile_path = api.ROOT / "profile_data.json"
    history_path = api.ROOT / "job_history.json"
    restart_count = 0
    while True:
        jobs = read_store(job_path)
        profiles = read_store(profile_path)
        history = read_store(history_path)

        for code, job in jobs.items():
            job["job_index"] = normalize_job_index(job.get("business_unit") or job.get("job_index"))
            # Clean cached search data locally; completed analyses never need another AI call.
            from location_utils import clean_location
            job["location"] = clean_location(job.get("location", ""))
            job["location_variants"] = api.generate_location_variants(
                job["location"], job.get("business_unit", "")
            )
            for field in ("city", "state", "country"):
                if field in job:
                    job[field] = clean_location(job[field])
            if isinstance(job.get("location_details"), dict):
                for field, value in job["location_details"].items():
                    job["location_details"][field] = clean_location(value)
            if history.get(code, {}).get("status") == "completed":
                job["linkedin_processing"] = "completed"
            elif job.get("linkedin_processing") != "completed":
                # Interrupted work resumes using the persisted profile/location history.
                job["linkedin_processing"] = "pending"
            if job.get("analysis_status") == "processing":
                job["analysis_status"] = "pending"
        api.save_state(job_path, jobs)

        queue = asyncio.Queue()
        startup_codes = set()
        if not args.jobs_only:
            for code, job in sorted(jobs.items(), key=lambda item: job_sort_key(item[1])):
                if args.job_url and job.get("url") != args.job_url:
                    continue
                if job.get("analysis_status") != "completed":
                    continue
                if not args.search_only and job.get("linkedin_processing") == "completed":
                    continue
                job.setdefault("code", code)
                queue.put_nowait(dict(job))
                startup_codes.add(code)
                print(f"Startup: analyzed job {code} ready; LinkedIn starts after login, without AI analysis.", flush=True)

        for row in profiles.values():
            if row.get("status") == "saving":
                row["status"] = "review_required"

        def persist():
            # Extracted candidate content stays in memory for PDF creation only.
            tracking_fields = {
                "url", "public_url", "status", "job_codes", "extraction_version",
                "resume_path", "resume_deleted", "applicant_url", "tag_job_status", "tag_profile", "advanced_search_job_codes",
                "city", "state", "country", "location", "location_source",
                "location_validation", "city_location_validated",
                "business_unit", "job_index", "job_title", "cleaned_job_title", "date_posted", "job_created", "error",
            }
            tracking = {
                url: {key: value for key, value in row.items() if key in tracking_fields}
                for url, row in profiles.items()
            }
            api.save_state(profile_path, tracking)

        persist()

        try:
            async with api.async_playwright() as playwright:
                context = None
                for attempt in range(1, 6):
                    try:
                        context = await playwright.chromium.launch_persistent_context(
                            str(api.ROOT / "browser-data-recruit"),
                            headless=False,
                            accept_downloads=False,
                            viewport={"width": 1440, "height": 900},
                        )
                        break
                    except Exception as launch_exc:
                        if is_browser_closed_error(launch_exc) or "singleton" in str(launch_exc).lower() or "in use" in str(launch_exc).lower():
                            print(f"Browser launch attempt {attempt}/5 failed ({launch_exc}); retrying in {attempt * 2}s...", flush=True)
                            await asyncio.sleep(attempt * 2)
                            continue
                        raise
                if context is None:
                    raise RuntimeError("Failed to launch browser persistent context after 5 attempts")

                context.set_default_timeout(30000)
                try:
                    ceipal = context.pages[0] if context.pages else await context.new_page()
                    if args.jobs_only:
                        await api.login(ceipal, True)
                        await ceipal.wait_for_function(
                            "() => !/\\/pages\\/signin|\\/users\\/autologout/.test(location.pathname)",
                            timeout=240000,
                        )
                        failures = []
                        await produce_jobs(api, args, ceipal, jobs, history, job_path, asyncio.Queue(), failures)
                        return 1 if failures else 0

                    linkedin = await context.new_page()
                    await asyncio.gather(api.login(ceipal, True), api.login(linkedin))
                    await api.wait_for_sites_ready(ceipal, linkedin)
                    failures = []

                    if not args.search_only:
                        retry_page = await context.new_page()
                        try:
                            await upload_pending_resumes(api, args, retry_page, jobs, profiles, persist)
                        finally:
                            try:
                                await retry_page.close()
                            except Exception:
                                pass
                    profile_page = await context.new_page()
                    applicant_page = await context.new_page()

                    ceipal_lock = asyncio.Lock()
                    last_30m_check_time = time.time()

                    def check_time_and_log_30min():
                        nonlocal last_30m_check_time
                        now = time.time()
                        if now - last_30m_check_time >= 1800:
                            last_30m_check_time = now
                            try:
                                from zoneinfo import ZoneInfo
                                est_tz = ZoneInfo("America/New_York")
                            except Exception:
                                est_tz = timezone(timedelta(hours=-5))
                            now_est = datetime.now(est_tz)
                            print(f"\n🕒 [30-Minute Time Check at {now_est.strftime('%Y-%m-%d %I:%M:%S %p %Z')}]", flush=True)
                            is_today_mode = getattr(api, "is_after_daily_830am_est", lambda: False)()
                            if is_today_mode:
                                print("  Current mode: Post-8:30 AM EST -> Harvesting Today's current date data only.", flush=True)
                            else:
                                print("  Current mode: Pre-8:30 AM EST -> Active/Open jobs mode (waiting for 8:30 AM EST cutoff).", flush=True)

                    async def check_ceipal_for_new_jobs():
                        if args.jobs_only or args.job_url:
                            return False
                        async with ceipal_lock:
                            if check_and_perform_daily_830am_est_cleanup(api.ROOT, jobs, profiles, history):
                                print("⏰ [8:30 AM EST Daily Cleanup] State files reset to empty. Current date data will be gathered.", flush=True)

                            print("\n🔍 Checking Ceipal job list page for new jobs...", flush=True)
                            try:
                                urls = await api.discover_jobs(ceipal, args.jobs_limit)
                                metadata_by_url = getattr(api, "JOB_METADATA", {})
                                new_urls = []
                                for url in urls:
                                    code = api.job_code_for(url)
                                    if not args.search_only and history.get(code, {}).get("status") == "completed":
                                        continue
                                    if code in failures:
                                        continue
                                    cached = jobs.get(code)
                                    if cached is None or cached.get("analysis_status") != "completed":
                                        new_urls.append(url)

                                if not new_urls:
                                    print("🔍 [Ceipal Job List Check] No new jobs found on job list page.", flush=True)
                                    return False

                                print(f"\n=======================================================", flush=True)
                                print(f"✨ [Ceipal Job List Check] Found {len(new_urls)} new job(s) on job list page: {[api.job_code_for(u) for u in new_urls]}", flush=True)
                                print(f"🚀 Enriching and prioritizing new job(s) immediately...", flush=True)
                                print(f"=======================================================\n", flush=True)

                                new_urls.sort(key=lambda url: job_sort_key(
                                    metadata_by_url.get(url) or jobs.get(api.job_code_for(url), {})
                                ))

                                added_any = False
                                for url in new_urls:
                                    code = api.job_code_for(url)
                                    metadata = getattr(api, "JOB_METADATA", {}).get(url, {})
                                    created_val = metadata.get("job_created") or metadata.get("date_posted") or ""
                                    cached = jobs.setdefault(code, {
                                        "url": url, "code": code,
                                        "title": metadata.get("title", ""),
                                        "location": metadata.get("location", ""),
                                        "business_unit": metadata.get("business_unit", ""),
                                        "date_posted": json_safe(created_val),
                                        "job_created": json_safe(metadata.get("job_created") or created_val),
                                    })
                                    if created_val:
                                        cached["date_posted"] = json_safe(created_val)
                                        if metadata.get("job_created"):
                                            cached["job_created"] = json_safe(metadata.get("job_created"))
                                    cached["job_index"] = normalize_job_index(cached.get("business_unit"))
                                    cached.setdefault("linkedin_processing", "pending")
                                    cached.setdefault("analysis_status", "pending")
                                    api.save_state(job_path, jobs)

                                    try:
                                        if cached.get("analysis_status") != "completed":
                                            await enrich_job_details(api, ceipal, cached, lambda: api.save_state(job_path, jobs))
                                            cached["analysis_status"] = "completed"
                                            api.save_state(job_path, jobs)
                                        print(f"🎯 New job {code} enriched successfully. Ready for Priority 1 Advanced Search.", flush=True)
                                        added_any = True
                                    except Exception as exc:
                                        if is_browser_closed_error(exc):
                                            cached.setdefault("analysis_status", "pending")
                                            try:
                                                api.save_state(job_path, jobs)
                                            except Exception:
                                                pass
                                            raise
                                        cached["analysis_status"] = "failed"
                                        cached["analysis_error"] = str(exc)
                                        try:
                                            api.save_state(job_path, jobs)
                                        except Exception:
                                            pass
                                        failures.append(code)
                                        print(f"⚠️ Failed to enrich new job {code}: {exc}", flush=True)

                                return added_any
                            except Exception as exc:
                                if is_browser_closed_error(exc):
                                    raise
                                print(f"Notice: Failed to check Ceipal job list for new jobs: {exc}", flush=True)
                                return False

                    async def consume_jobs():
                        producer_finished = False

                        while True:
                            check_time_and_log_30min()
                            if check_and_perform_daily_830am_est_cleanup(api.ROOT, jobs, profiles, history):
                                print("⏰ [8:30 AM EST Daily Cleanup] Fired! Resetting state to empty. Starting fresh discovery of today's jobs...", flush=True)
                                if not args.jobs_only and not args.job_url:
                                    await check_ceipal_for_new_jobs()
                                continue

                            # 1. Drain notifications from queue
                            while not queue.empty():
                                try:
                                    item = queue.get_nowait()
                                    queue.task_done()
                                    if item is None:
                                        producer_finished = True
                                except asyncio.QueueEmpty:
                                    break

                            # 2. Sync latest jobs from extract_job.json
                            sync_jobs_from_disk(job_path, jobs)

                            # 3. PRIORITY 1: ALWAYS check if ANY job in extract_job.json has pending Advanced Search!
                            adv_jobs = get_pending_advanced_jobs(jobs, history, args, exclude_codes=failures)
                            if adv_jobs:
                                print(f"\n=======================================================", flush=True)
                                print(f"🚀 Priority 1: Running Advanced Search & Tag for {len(adv_jobs)} pending job(s)...", flush=True)
                                print(f"=======================================================\n", flush=True)

                                interrupted_by_new_job = False
                                for current_job in adv_jobs:
                                    cached_job = jobs.get(current_job["code"], current_job)
                                    if not args.search_only:
                                        cached_job["linkedin_processing"] = "processing"
                                        cached_job["linkedin_started_at"] = datetime.now(timezone.utc).isoformat()
                                        api.save_state(job_path, jobs)
                                    print(
                                        f"Starting Part 1 (Advanced Search) for Job {current_job['code']} | business unit={current_job.get('business_unit')} | priority index={job_sort_key(current_job)[0]}",
                                        flush=True,
                                    )
                                    try:
                                        await process_job(
                                            api,
                                            args,
                                            linkedin,
                                            profile_page,
                                            applicant_page,
                                            current_job,
                                            profiles,
                                            history,
                                            history_path,
                                            persist,
                                            phase=1,
                                        )
                                    except Exception as exc:
                                        if is_browser_closed_error(exc):
                                            print(f"Browser closed suddenly during Part 1 for Job {current_job['code']}; state kept pending.", flush=True)
                                            cached_job = jobs.get(current_job["code"], current_job)
                                            cached_job["linkedin_processing"] = "pending"
                                            try:
                                                api.save_state(job_path, jobs)
                                            except Exception:
                                                pass
                                            raise
                                        failures.append(current_job["code"])
                                        saved_job = history.setdefault(current_job["code"], {})
                                        saved_job["advanced_pipeline_status"] = "failed"
                                        api.save_state(history_path, history)
                                        print(
                                            f"Job {current_job['code']} Part 1 incomplete: {type(exc).__name__}: {exc}",
                                            flush=True,
                                        )

                                    # After each job process completes, check job list page for new jobs!
                                    if not args.jobs_only and not args.job_url and producer_finished:
                                        found_new = await check_ceipal_for_new_jobs()
                                        if found_new:
                                            print(f"✨ New job(s) found on Ceipal job list page! Prioritizing new job...", flush=True)
                                            interrupted_by_new_job = True
                                            break

                                if interrupted_by_new_job:
                                    continue

                                print(f"\n✅ Advanced search batch completed. Re-checking all jobs before starting/continuing Normal Search...\n", flush=True)
                                continue

                            # 4. If any job is currently being analyzed in Ceipal, wait so we can run Advanced Search first
                            if has_unready_jobs(jobs, args) and not producer_finished:
                                try:
                                    item = await asyncio.wait_for(queue.get(), timeout=2.0)
                                    queue.task_done()
                                    if item is None:
                                        producer_finished = True
                                except asyncio.TimeoutError:
                                    pass
                                continue

                            # Check Ceipal job list page before Normal Search starts: if new job found, work on it first; else start normal search
                            if not args.jobs_only and not args.job_url and producer_finished:
                                found_new = await check_ceipal_for_new_jobs()
                                if found_new:
                                    print("✨ New job(s) discovered on Ceipal job list page before Normal Search! Running Advanced Search first...", flush=True)
                                    continue

                            # 5. PRIORITY 2: Only run Normal Search when ALL jobs have completed Advanced Search and no new jobs on Ceipal!
                            norm_jobs = get_pending_normal_jobs(jobs, history, args, exclude_codes=failures)
                            if norm_jobs:
                                current_job = norm_jobs[0]
                                print(f"\n=======================================================", flush=True)
                                print(f"🚀 Priority 2: Running Normal Search for Job {current_job['code']} ({len(norm_jobs)} remaining; all advance searches done)...", flush=True)
                                print(f"=======================================================\n", flush=True)

                                try:
                                    await process_job(
                                        api,
                                        args,
                                        linkedin,
                                        profile_page,
                                        applicant_page,
                                        current_job,
                                        profiles,
                                        history,
                                        history_path,
                                        persist,
                                        phase=2,
                                    )
                                except Exception as exc:
                                    if is_browser_closed_error(exc):
                                        print(f"Browser closed suddenly during Part 2 for Job {current_job['code']}; state kept pending.", flush=True)
                                        cached_job = jobs.get(current_job["code"], current_job)
                                        cached_job["linkedin_processing"] = "pending"
                                        try:
                                            api.save_state(job_path, jobs)
                                        except Exception:
                                            pass
                                        raise
                                    failures.append(current_job["code"])
                                    print(
                                        f"Job {current_job['code']} Part 2 incomplete: {type(exc).__name__}: {exc}",
                                        flush=True,
                                    )
                                finally:
                                    if not args.search_only:
                                        complete = history.get(current_job["code"], {}).get("status") == "completed"
                                        cached_job = jobs.get(current_job["code"], current_job)
                                        cached_job["linkedin_processing"] = "completed" if complete else "pending"
                                        if complete:
                                            cached_job["linkedin_completed_at"] = datetime.now(timezone.utc).isoformat()
                                        elif history.get(current_job["code"], {}).get("limit_reached"):
                                            cached_job["linkedin_error"] = history.get(current_job["code"], {}).get("limit_reason")
                                            if current_job["code"] not in failures:
                                                failures.append(current_job["code"])
                                        api.save_state(job_path, jobs)
                                        print(
                                            f"Job {current_job['code']}: history checked; LinkedIn {cached_job['linkedin_processing']}.",
                                            flush=True,
                                        )

                                # After Normal Search job completes its process, check Ceipal job list page for new jobs!
                                if not args.jobs_only and not args.job_url and producer_finished:
                                    found_new = await check_ceipal_for_new_jobs()
                                    if found_new:
                                        print(f"✨ New job(s) found on Ceipal after completing Normal Search for {current_job['code']}! Prioritizing new job...", flush=True)
                                    else:
                                        print(f"🔍 Checked Ceipal job list: No new jobs found. Continuing normal search...", flush=True)

                                # Loop back to top to verify if any new jobs arrived needing advance search!
                                continue

                            # 6. If neither adv_jobs nor norm_jobs are pending:
                            if producer_finished:
                                print(f"\n=======================================================", flush=True)
                                print(f"✅ All jobs have completed both Advanced and Normal search pipelines.", flush=True)
                                print(f"=======================================================\n", flush=True)
                                break

                            # Producer is still discovering. Wait for next job notification.
                            try:
                                item = await asyncio.wait_for(queue.get(), timeout=2.0)
                                queue.task_done()
                                if item is None:
                                    producer_finished = True
                            except asyncio.TimeoutError:
                                pass

                    # Default full run keeps watching after a batch; scoped/test modes exit.
                    watch_jobs = not args.search_only and not args.job_url
                    round_number = 0

                    async def sleep_with_daily_cleanup_check(seconds, step=5):
                        end_time = time.time() + seconds
                        while time.time() < end_time:
                            check_time_and_log_30min()
                            if check_and_perform_daily_830am_est_cleanup(api.ROOT, jobs, profiles, history):
                                return True
                            try:
                                if not context.pages or context.pages[0].is_closed():
                                    raise RuntimeError("Target page, context or browser has been closed")
                                _ = context.pages[0].url
                            except Exception as e:
                                if is_browser_closed_error(e):
                                    raise
                            await asyncio.sleep(min(step, max(0.1, end_time - time.time())))
                        return False

                    while True:
                        round_number += 1
                        check_and_perform_daily_830am_est_cleanup(api.ROOT, jobs, profiles, history)
                        if round_number > 1:
                            queue = asyncio.Queue()
                            startup_codes = set()
                            failures.clear()
                            print("Refreshing Ceipal job list for the next discovery pass...", flush=True)
                            try:
                                # open_job_list navigates afresh to /JobPosts/index, resetting pagination.
                                async with ceipal_lock:
                                    await api.open_job_list(ceipal)
                            except Exception as exc:
                                if is_browser_closed_error(exc):
                                    print(f"\nBrowser closed during Ceipal refresh: {exc}. Progress kept pending. Relaunching...", flush=True)
                                    raise
                                print(f"Job list refresh failed: {exc}; retrying in 30 seconds.", flush=True)
                                await sleep_with_daily_cleanup_check(30)
                                continue
                        consumer = asyncio.create_task(consume_jobs())
                        producer = asyncio.create_task(
                            produce_jobs(api, args, ceipal, jobs, history, job_path, queue, failures, startup_codes, ceipal_lock=ceipal_lock)
                        )
                        try:
                            await asyncio.gather(producer, consumer)
                        except Exception as exc:
                            if is_browser_closed_error(exc):
                                print(f"\nBrowser was closed suddenly: {exc}. All progress kept pending. Relaunching...", flush=True)
                                raise
                            if not watch_jobs:
                                raise
                            print(f"Discovery pass failed: {exc}; retrying after refresh.", flush=True)
                        finally:
                            for task in (producer, consumer):
                                if not task.done():
                                    task.cancel()
                            await asyncio.gather(producer, consumer, return_exceptions=True)
                            api.save_state(job_path, jobs)
                        if not watch_jobs:
                            return 1 if failures else 0
                        incomplete = any(
                            job.get("analysis_status") == "completed"
                            and job.get("linkedin_processing") != "completed"
                            and history.get(code, {}).get("status") != "completed"
                            for code, job in jobs.items()
                        )
                        if incomplete:
                            print("Batch has pending jobs; retrying pending work in 30 seconds.", flush=True)
                            await sleep_with_daily_cleanup_check(30)
                        elif failures:
                            print("Discovery errors; checking job list again in 30 seconds. Ctrl+C to stop.", flush=True)
                            await sleep_with_daily_cleanup_check(30)
                        elif not startup_codes:
                            print("No new jobs; checking job list again in 30 minutes. Ctrl+C to stop.", flush=True)
                            await sleep_with_daily_cleanup_check(1800)
                        else:
                            print("All queued jobs completed; refreshing the job list now. Ctrl+C to stop.", flush=True)
                finally:
                    try:
                        await context.close()
                    except Exception:
                        pass
        except Exception as exc:
            if is_browser_closed_error(exc):
                restart_count += 1
                print(f"\n" + "=" * 70, flush=True)
                print(f"⚠️ [Browser Closed Detected] {type(exc).__name__}: {exc}", flush=True)
                print(f"🔄 [Browser Auto-Restart #{restart_count}] Relaunching browser in 5 seconds and resuming tasks...", flush=True)
                print(f"🔄 Current state preserved as pending; no data lost.", flush=True)
                print("=" * 70 + "\n", flush=True)
                await asyncio.sleep(5)
                continue
            raise


async def process_candidate_profiles(api, args, profile_page, applicant_page, job, urls, profiles, persist):
    """Build the next PDF while one worker uploads/saves/tags ready PDFs."""
    ready = asyncio.Queue()

    async def upload_ready():
        while True:
            row = await ready.get()
            try:
                if row is None:
                    return
                try:
                    await create_applicant(applicant_page, row, api, persist, job["code"])
                    row.pop("error", None)
                    persist()
                    print(f"[Profile {row['url']}] Applicant result: {row.get('status')}", flush=True)
                except Exception as exc:
                    if is_browser_closed_error(exc):
                        print(f"Browser closed suddenly during applicant creation for {row.get('url')}; leaving pending.", flush=True)
                        raise
                    if row.get("status") in {"saved", "duplicate", "skipped_db_exists"}:
                        row["error"] = str(exc)
                        persist()
                    else:
                        skip_failed_profile(api, row, exc, persist)
            finally:
                ready.task_done()

    worker = asyncio.create_task(upload_ready())
    try:
        await prepare_candidate_profiles(
            api, args, profile_page, applicant_page, job, urls, profiles, persist, ready
        )
        await ready.put(None)
        await worker
    except Exception as exc:
        if is_browser_closed_error(exc):
            raise
        if worker.done():
            worker_exc = worker.exception()
            if is_browser_closed_error(worker_exc):
                raise worker_exc
        raise
    finally:
        if not worker.done():
            worker.cancel()
        results = await asyncio.gather(worker, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception) and not isinstance(res, asyncio.CancelledError):
                if is_browser_closed_error(res):
                    raise res
    return all(profiles.get(url, {}).get("status") in FINAL for url in urls)


async def prepare_candidate_profiles(api, args, profile_page, applicant_page, job, urls, profiles, persist, ready):
    from recruiting_ai import build_ai_resume_profile

    code = job["code"]
    for url in urls:
        existing = profiles.get(url)
        row = dict(existing) if existing else {"url": url, "status": "pending", "job_codes": []}
        row["location"] = job.get("location", "")
        row["job_ai_location"] = dict(job.get("ai_final_location") or {
            key: job.get(key, "") for key in ("city", "state", "country", "location")
        })
        row["job_skills"] = job.get("skills", [])
        row["business_unit"] = job.get("business_unit")
        row["job_index"] = job.get("job_index")
        row["job_title"] = job.get("title")
        created_val = job.get("job_created") or job.get("date_posted")
        row["date_posted"] = json_safe(created_val)
        row["job_created"] = json_safe(created_val)
        if job.get("title"):
            mappings = dict(row.get("title_mapping") or {})
            mappings[job["title"]] = row.get("cleaned_job_title")
            row["title_mapping"] = mappings

        if code not in row["job_codes"]:
            row["job_codes"].append(code)

        terminal_status = row.get("status") in FINAL
        if terminal_status and (
            row.get("status") in {"skipped_linkedin_member", "skipped_india_location", "skipped_processing_failed"}
            or row.get("extraction_version") == PROFILE_EXTRACTION_VERSION
        ):
            profiles[url] = row
            persist()
            continue
        if row.get("status") in UNCERTAIN:
            profiles[url] = row
            skip_failed_profile(api, row, "Uncertain previous Save without a retryable PDF", persist)
            continue

        try:
            known_public_url = row.get("public_url") or (row.get("raw") or {}).get("public_url")
            if known_public_url and await asyncio.to_thread(linkedin_public_url_exists, known_public_url, code):
                raise ExistingProfileSkipped(known_public_url)
            if is_currently_in_india(row.get("profile") or {}, row.get("raw") or {}):
                raise IndiaProfileSkipped("Cached profile location is India")
            needs_refresh = row.get("extraction_version") != PROFILE_EXTRACTION_VERSION
            if not row.get("raw") or needs_refresh:
                row["raw"] = await extract_profile(profile_page, url, code)
                print(f"RAW PROFILE DATA [{url}]:\n" + json.dumps(
                    json_safe(row["raw"]), ensure_ascii=False, indent=2
                ), flush=True)
                row["extracted_at"] = datetime.now(timezone.utc).isoformat()
                row["extraction_version"] = PROFILE_EXTRACTION_VERSION
                row.pop("profile", None)
                row["resume_path"] = ""
                if row["raw"].get("public_url"):
                    row["public_url"] = row["raw"]["public_url"]

            if is_currently_in_india({}, row.get("raw") or {}):
                raise IndiaProfileSkipped("Profile location is India; AI extraction skipped")

            public_url = (row.get("raw") or {}).get("public_url") or row.get("public_url")
            if public_url and await asyncio.to_thread(linkedin_public_url_exists, public_url, code):
                raise ExistingProfileSkipped(public_url)

            raw_name = " ".join(str((row.get("raw") or {}).get("name") or "").split()).casefold()
            if raw_name == "linkedin member":
                row["status"] = "skipped_linkedin_member"
                row["tag_job_status"] = "skipped_linkedin_member"
                profiles[url] = row
                persist()
                print(f"LinkedIn Member profile: extraction/resume skipped for {url}", flush=True)
                continue

            if not row.get("profile"):
                row["profile"] = await asyncio.to_thread(build_ai_resume_profile, row["raw"])
                row["status"] = "extracted"
                row["resume_path"] = ""  # A newly built profile requires a fresh PDF.

            print(f"EXTRACTED PROFILE DATA [{url}]:\n" + json.dumps(
                json_safe(row["profile"]), ensure_ascii=False, indent=2
            ), flush=True)

            profile = row["profile"]
            if ensure_profile_location(profile, row.get("raw") or {}):
                row["profile"] = profile
                row["status"] = "extracted"
                print(
                    f"    location backfilled for {url}: {profile.get('location')!r}",
                    flush=True,
                )
            if is_linkedin_member_profile(profile):
                row["status"] = "skipped_linkedin_member"
                profiles[url] = row
                persist()
                print(
                    f"    skipped resume/applicant creation for {url}: LinkedIn Member profile",
                    flush=True,
                )
                continue

            if is_currently_in_india(profile, row.get("raw") or {}):
                row["status"] = "skipped_india_location"
                profiles[url] = row
                persist()
                print(
                    f"    skipped resume/applicant creation for {url}: "
                    f"current profile location is India ({profile.get('location')!r})",
                    flush=True,
                )
                continue

            from location_utils import clean_location
            from profile_location import validate_profile_location
            original_location = row.get("extracted_profile_location") if row.get("location_source") == "job_fallback" else None
            validation_input = original_location or profile
            print(f"[Profile {url}] Validating city/state/country before resume generation...", flush=True)
            validation = row.get("location_validation") or {}
            if validation.get("status") != "valid" or any(
                validation_input.get(key) != validation.get(key)
                for key in ("city", "state", "country")
            ):
                validation = await asyncio.to_thread(validate_profile_location, validation_input)
            row["location_validation"] = validation
            final_job = job.get("ai_final_location") or job
            source = validation if validation.get("status") == "valid" else final_job
            mapped = {
                key: clean_location(source.get(key, ""))
                for key in ("city", "state", "country")
            }
            if not mapped["country"]:
                mapped["country"] = clean_location((job.get("location_details") or {}).get("country", ""))
            mapped["location"] = ", ".join(mapped[k] for k in ("city", "state", "country") if mapped[k])
            if validation.get("status") == "valid":
                row["location_source"] = "validated_profile"
            else:
                row.setdefault("extracted_profile_location", {
                    key: profile.get(key) for key in ("location", "city", "state", "country")
                })
                row["location_source"] = "job_fallback"
                row["fallback_location_source"] = "job_ai_final"
            print(f"[Profile {url}] Geography={validation.get('status')} ({validation.get('reason')}); resume location source={row['location_source']}: {mapped['location']}", flush=True)
            row["resume_location"] = dict(mapped)
            row.update(mapped)
            if any(profile.get(key) != value for key, value in mapped.items()):
                row["resume_path"] = ""  # Rebuild a cached PDF when canonical location changes.
            profile.update(mapped)
            profile["city_location_validated"] = validation.get("status") == "valid"
            row["city_location_validated"] = profile["city_location_validated"]
            row["profile"] = profile
            profiles[url] = row
            persist()

            print(f"RESUME PROFILE DATA [{url}]:\n" + json.dumps(
                json_safe(profile), ensure_ascii=False, indent=2
            ), flush=True)

            if not Path(row.get("resume_path", "")).is_file():
                target = api.ROOT / "resumes" / resume_filename(profile, url)
                print(f"[Profile {url}] Building resume HTML and rendering PDF: {target}", flush=True)
                await write_resume_pdf(
                    profile_page.context,
                    render_resume_html(profile, url, row["raw"].get("public_url")),
                    target,
                )
                row["resume_path"] = str(target)
                print(f"    resume written to {target}", flush=True)
            else:
                print(f"[Profile {url}] Reusing existing resume PDF: {row['resume_path']}", flush=True)

            print(
                f"    profile: {profile.get('first_name')} {profile.get('last_name')}"
                f" | city={profile.get('city')!r} state={profile.get('state')!r}"
                f" | skills={len(profile.get('skills', []))}"
                f" | job_index={row.get('job_index')!r}",
                flush=True,
            )

            if terminal_status:
                profiles[url] = row
                persist()
                print(
                    f"    refreshed resume extraction for completed profile {url}; "
                    "applicant was not submitted again.",
                    flush=True,
                )
                continue

            row.pop("error", None)
            row["status"] = "resume_ready"
            profiles[url] = row
            persist()
            await ready.put(row)
            print(f"[Profile {url}] PDF queued for Ceipal; preparing next profile in parallel.", flush=True)

        except ExistingProfileSkipped as exc:
            row["status"] = "skipped_db_exists"
            row["public_url"] = exc.public_url
            row["extraction_version"] = PROFILE_EXTRACTION_VERSION
            profiles[url] = row
            persist()
            print(f"[Profile {url}] Already in DB for job {code}; skipping AI, location lookup, resume and upload.", flush=True)
            continue
        except IndiaProfileSkipped as exc:
            row["status"] = "skipped_india_location"
            row["skip_reason"] = str(exc)
            row["extraction_version"] = PROFILE_EXTRACTION_VERSION
            profiles[url] = row
            persist()
            print(f"    skipped India profile before data extraction: {url}", flush=True)
            continue
        except Exception as exc:
            if is_browser_closed_error(exc):
                print(f"Browser closed suddenly during profile preparation for {url}; status kept pending.", flush=True)
                profiles[url] = row
                persist()
                raise
            profiles[url] = row
            if row.get("status") in {"saved", "duplicate", "skipped_db_exists"}:
                row["error"] = str(exc)
                persist()
                print(f"[Profile {url}] Applicant finished; tracking error: {exc}; continuing.", flush=True)
            else:
                skip_failed_profile(api, row, exc, persist)
            continue
    return all(
        profiles.get(url, {}).get("status") in FINAL
        for url in urls
    )


async def process_job(api, args, linkedin, profile_page, applicant_page, job, profiles, history, history_path, persist, phase=None):
    from linkedin_search import (
        job_search_requirements, run_advanced_search, apply_jd_filters, submit_search, clear_search_filters, advanced_search_enabled
    )

    requirements = job_search_requirements(job)
    advanced_required = requirements["has_jd"] and advanced_search_enabled()
    code = job["code"]
    final_location = job.get("ai_final_location") or job
    final_country = final_location.get("country") or (job.get("location_details") or {}).get("country")
    if not final_country:
        raise ValueError(f"Job {code} has no AI-final country; LinkedIn processing remains pending")
    business_unit = job.get("business_unit", "")
    if args.location:
        source_location = api.clean_job_location(args.location.strip())
        location_variants = api.generate_location_variants(source_location, business_unit)
    else:
        source_location = job.get("location", "")
        location_variants = job.get("location_variants") or []
        if not source_location and location_variants:
            source_location = location_variants[0]
    location_variants = list(dict.fromkeys(
        " ".join(str(variant or "").split())
        for variant in location_variants
        if " ".join(str(variant or "").split())
    ))[:3]
    if source_location and not location_variants:
        location_variants = api.generate_location_variants(
            source_location, business_unit
        )
        location_variants = list(dict.fromkeys(
            " ".join(str(variant or "").split())
            for variant in location_variants
            if " ".join(str(variant or "").split())
        ))[:3]

    source_location = " ".join(str(source_location or "").split())
    saved_job = history.get(code, {})
    saved_job["location"] = source_location
    saved_job["location_search_variants"] = location_variants
    history[code] = saved_job
    api.save_state(history_path, history)
    if not source_location or not location_variants:
        raise ValueError(
            f"Job {code} has no location. A job location is required; "
            "LinkedIn search was not started."
        )

    # -------------------------------------------------------------
    # PART 1: Advanced Search & Profile Tagging Pipeline
    # -------------------------------------------------------------
    if phase in (None, 1):
        if not (saved_job.get("advanced_pipeline_status") == "completed" and not args.search_only):
            advanced, advanced_urls = await run_advanced_search(
                api, args, linkedin, code, location_variants, requirements, saved_job,
                lambda: api.save_state(history_path, history),
                title=job.get("cleaned_title") or job["title"],
            )
            advanced_urls = list(dict.fromkeys(advanced_urls))[:20]
            saved_job["profiles"] = advanced_urls
            history[code] = saved_job
            api.save_state(history_path, history)

            if not args.search_only and advanced_urls:
                verified_advanced_urls = set(advanced_urls) if advanced and advanced.get("search_status") == "completed" else set()
                for url in advanced_urls:
                    row = profiles.setdefault(url, {"url": url, "status": "pending", "job_codes": []})
                    sources = row.setdefault("advanced_search_job_codes", [])
                    if code in sources:
                        sources.remove(code)
                    if url in verified_advanced_urls:
                        sources.append(code)
                persist()

                print(
                    f"Job {code}: Part 1 processing {len(advanced_urls)} Advanced Search candidates (save + quick tag)...",
                    flush=True,
                )
                await process_candidate_profiles(
                    api, args, profile_page, applicant_page, job, advanced_urls, profiles, persist
                )

            if advanced and advanced.get("search_status") == "completed":
                advanced["status"] = "completed" if all(
                    profiles.get(url, {}).get("status") in FINAL for url in advanced_urls
                ) else "processing"

            saved_job["advanced_pipeline_status"] = "completed"
            history[code] = saved_job
            api.save_state(history_path, history)

        if phase == 1:
            return

    # -------------------------------------------------------------
    # PART 2: Normal Search & Profile Save Pipeline (after Part 1)
    # -------------------------------------------------------------
    advanced = saved_job.get("advanced_search") or {}
    advanced_urls = list(dict.fromkeys(advanced.get("profiles", [])))[:20]
    advanced_set = set(advanced_urls)
    location_searches = saved_job.setdefault("location_searches", [])
    legacy_urls = saved_job.get("profiles", []) if not location_searches else []
    search_title = job.get("cleaned_title") or job["title"]
    search_only_urls = list(advanced_urls)
    normal_filters = dict(title=search_title, version=2)
    all_urls = list(advanced_urls)
    for pass_number, variant in enumerate(location_variants, start=1):
        record = next(
            (item for item in location_searches if item.get("pass") == pass_number),
            None,
        )
        if record and record.get("filters") == normal_filters and record.get("location") == variant and record.get("status") == "completed" and not args.search_only:
            search_only_urls.extend(record.get("profiles", []))
            continue

        if args.search_only:
            candidates = None
        elif record and record.get("filters") == normal_filters and record.get("location") == variant and (
            record.get("status") in {"processing", "collected", "completed"}
            or record.get("search_status") == "completed"
        ):
            candidates = record.get("profiles", [])
        elif pass_number == 1 and legacy_urls and not requirements["has_jd"]:
            candidates = legacy_urls
        else:
            candidates = None

        if candidates is None:
            try:
                print(f"Job {code}: next step - running normal search pass {pass_number} for {variant!r}...", flush=True)
                await linkedin.goto(api.SEARCH_URL, wait_until="domcontentloaded")
                await clear_search_filters(linkedin)
                await api.apply_title(linkedin, search_title)
                await api.apply_location(linkedin, variant)
                await submit_search(linkedin, required=False)
                await api.wait_for_search_results(linkedin)
                candidates = await api.collect_profiles(linkedin, 30)
                if not candidates:
                    limit_fn = getattr(api, "check_linkedin_limit_reached", None)
                    limit_reason = await limit_fn(linkedin) if limit_fn else None
                    if limit_reason:
                        print(f"⚠️ Job {code}: LinkedIn profile view / search limit detected on pass {pass_number}: {limit_reason!r}", flush=True)
                        saved_job["limit_reached"] = True
                        saved_job["limit_reason"] = limit_reason
            except Exception as exc:
                _is_closed = globals().get('is_browser_closed_error', lambda e: False)
                if _is_closed(exc):
                    print(f"Job {code}: Browser closed suddenly during title search pass {pass_number}; leaving pending.", flush=True)
                    raise
                if record is None:
                    record = {"pass": pass_number, "location": variant, "profiles": []}
                    location_searches.append(record)
                record.update(filters=normal_filters, search_status="failed", status="failed", error=str(exc))
                api.save_state(history_path, history)
                print(f"Job {code}: title search pass {pass_number} failed: {exc}; continuing.", flush=True)
                continue

        print(
            f"Job {code}: location search pass {pass_number} using {variant!r} "
            f"returned {len(candidates)} profile(s).",
            flush=True,
        )
        search_only_urls.extend(candidates)
        if record is None:
            record = {"pass": pass_number, "location": variant}
            location_searches.append(record)
        record["filters"] = normal_filters
        record.pop("error", None)
        record["location"] = variant
        record["profiles"] = candidates
        record["search_status"] = "limit_reached" if saved_job.get("limit_reached") and not candidates else "completed"
        record["status"] = "pending" if saved_job.get("limit_reached") and not candidates else "collected"
        all_urls.extend(candidates)
        saved_job.update({
            "status": "pending",
            "job_url": job.get("url"),
            "job_index": job.get("job_index"),
            "job_title": job.get("title"),
            "location_search_variants": location_variants,
            "profiles": list(dict.fromkeys(all_urls)),
        })
        history[code] = saved_job
        api.save_state(history_path, history)

        print(f"Job {code}: pass {pass_number} URLs stored; continuing to next location before profile extraction.", flush=True)

    advanced_urls = list(dict.fromkeys(advanced_urls))[:20]
    advanced_set = set(advanced_urls)
    all_advanced_set = set((advanced or {}).get("profiles", [])) | advanced_set
    normal_urls = list(dict.fromkeys(url for url in all_urls if url not in all_advanced_set))
    target_limit = getattr(args, "limit", 30) or 30
    normal_needed = max(0, target_limit - len(advanced_urls))
    selected_normal = normal_urls[:normal_needed]
    selected_urls = advanced_urls + selected_normal
    selected_set = set(selected_urls)
    saved_job["profile_sources"] = {
        url: "advanced_search" if url in all_advanced_set else "normal_search"
        for url in list(dict.fromkeys(all_urls))
    }
    saved_job["profile_selection"] = {
        "advanced_search": advanced_urls,
        "normal_search": selected_normal,
        "skipped_normal_limit": normal_urls[normal_needed:],
        "skipped_advanced_limit": list(dict.fromkeys(
            url for url in (advanced or {}).get("profiles", []) if url not in advanced_set
        )),
        "limits": {"advanced_search": 20, "normal_search": normal_needed, "total": len(selected_urls)},
    }
    for record in location_searches:
        record["source"] = "normal_search"
        record["skipped_limit_profiles"] = [url for url in record.get("profiles", []) if url not in selected_set]
        record["profiles"] = [url for url in record.get("profiles", []) if url in selected_set]
    if advanced:
        advanced["source"] = "advanced_search"
        advanced["skipped_limit_profiles"] = [
            url for url in advanced.get("profiles", []) if url not in advanced_set
        ]
        advanced["profiles"] = advanced_urls

    if args.search_only:
        saved_job["profiles"] = selected_urls
        saved_job["search_status"] = "completed"
        api.save_state(history_path, history)
        print(
            f"Search-only: {len(set(search_only_urls))} profile URLs for {code} "
            f"across {len(location_variants)} location pass(es): {location_variants}",
            flush=True,
        )
        return

    unique_urls = selected_urls
    saved_job["profiles"] = unique_urls
    saved_job["search_status"] = "completed"
    api.save_state(history_path, history)
    if not unique_urls:
        limit_reason = saved_job.get("limit_reason") or (advanced or {}).get("limit_reason")
        if not limit_reason:
            try:
                limit_fn = getattr(api, "check_linkedin_limit_reached", None)
                if limit_fn:
                    limit_reason = await limit_fn(linkedin)
            except Exception:
                limit_reason = None

        if limit_reason:
            print(f"\n=======================================================", flush=True)
            print(f"⚠️ Job {code}: LinkedIn profile view / search limit reached: {limit_reason!r}!", flush=True)
            print(f"DO NOT mark completed! Job {code} remains PENDING.", flush=True)
            print(f"=======================================================\n", flush=True)
            for record in location_searches:
                record["status"] = "pending"
            if advanced:
                advanced["status"] = "pending"
            saved_job["status"] = "pending"
            saved_job["limit_reached"] = True
            saved_job["limit_reason"] = limit_reason
            saved_job["profiles"] = []
            saved_job["location_search_variants"] = location_variants
            history[code] = saved_job
            api.save_state(history_path, history)
            return

        print(f"Job {code}: 0 LinkedIn profiles found across all searches. Marking job completed with 0 candidates.", flush=True)
        for record in location_searches:
            if record.get("search_status") == "completed":
                record["status"] = "completed"
        if advanced and advanced.get("search_status") == "completed":
            advanced["status"] = "completed"
        saved_job["status"] = "completed"
        saved_job["completed_at"] = datetime.now(timezone.utc).isoformat()
        saved_job["profiles"] = []
        saved_job["location_search_variants"] = location_variants
        history[code] = saved_job
        api.save_state(history_path, history)
        update_job_processing_status(code, 1)
        return

    print(
        f"Job {code}: searches finished; collected {len(unique_urls)} profiles total "
        f"({len(advanced_urls)} from Advanced Search to tag, {len(selected_normal)} from Normal Search to save only).",
        flush=True,
    )
    pending_urls = [url for url in unique_urls if profiles.get(url, {}).get("status") not in FINAL]
    verified_advanced_urls = set(advanced_urls) if advanced and advanced.get("search_status") == "completed" else set()
    for url in pending_urls:
        row = profiles.setdefault(url, {"url": url, "status": "pending", "job_codes": []})
        sources = row.setdefault("advanced_search_job_codes", [])
        if code in sources:
            sources.remove(code)
        if url in verified_advanced_urls:
            sources.append(code)
    persist()
    if pending_urls:
        await process_candidate_profiles(
            api, args, profile_page, applicant_page, job, pending_urls, profiles, persist
        )
    for record in location_searches:
        if record.get("search_status") != "completed":
            continue
        record["status"] = "completed" if all(
            profiles.get(url, {}).get("status") in FINAL
            for url in record.get("profiles", []) if url in selected_set
        ) else "processing"
    api.save_state(history_path, history)

    if advanced and advanced.get("search_status") == "completed":
        advanced["status"] = "completed" if all(
            profiles.get(url, {}).get("status") in FINAL for url in advanced_urls
        ) else "processing"
    if advanced and advanced.get("location_searches"):
        for record in advanced["location_searches"]:
            if record.get("search_status") != "completed":
                continue
            record["status"] = "completed" if all(
                profiles.get(url, {}).get("status") in FINAL
                for url in record.get("profiles", []) if url in selected_set
            ) else "processing"

    completed_all = (
        (not advanced_required or (advanced and advanced.get("status") == "completed"))
        and
        all(
            any(item.get("pass") == number and item.get("status") == "completed"
                for item in location_searches)
            for number in range(1, len(location_variants) + 1)
        )
        and all(profiles.get(url, {}).get("status") in FINAL for url in unique_urls)
    )
    if completed_all:
        saved_job["status"] = "completed"
        saved_job["completed_at"] = datetime.now(timezone.utc).isoformat()
        update_job_processing_status(code, 1)
    else:
        saved_job["status"] = "pending"
    saved_job["profiles"] = unique_urls
    saved_job["location_search_variants"] = location_variants
    history[code] = saved_job
    api.save_state(history_path, history)

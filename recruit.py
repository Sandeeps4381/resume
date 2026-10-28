"""Cache Ceipal jobs, extract LinkedIn profiles, and manually create applicants.

Stored jobs are reused and never re-extracted; profile and save history prevent
duplicate submissions. Resumes are written to resumes/ but never parsed.
"""
import argparse
import base64
import hmac
import struct
import time
import asyncio
import hashlib
import json
import itertools
import os
import sys
from pathlib import Path
import random
import re
import urllib.request
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime, timezone, timedelta
import uuid

from playwright.async_api import async_playwright
from azure_title import clean_job_location
from location_utils import clean_location_parts
import main as existing

ROOT = Path(__file__).resolve().parent
SEARCH_URL = "https://www.linkedin.com/talent/search?start=0&uiOrigin=GLOBAL_SEARCH_HEADER"
JOB_METADATA = {}
CLEAN_CACHE = {}
CEIPAL_LOCATION_XPATHS = (
    'xpath=//*[@id="sticky"]/div[36]/div[1]/div[2]/div/div[2]/div[2]/div[3]',
    'xpath=//*[contains(@class, "jpLIcon") and .//i[contains(@class, "fa-map-marker-alt")]]',
)


async def prompt(message):
    try:
        return await asyncio.to_thread(input, message + "\n> ")
    except EOFError:
        print("No console input available; continuing without the manual step.", flush=True)
        return ""


async def visible(page, selectors, timeout=30):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        for selector in selectors:
            item = page.locator(selector).first
            if await item.is_visible():
                return item
        await asyncio.sleep(.5)
    raise RuntimeError("Control not found: " + ", ".join(selectors))


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


async def login(page, ceipal=False):
    prefix = "CEIPAL" if ceipal else "LINKEDIN"
    url = os.getenv("CEIPAL_SIGNIN_URL", existing.CEIPAL_SIGNIN_URL) if ceipal else os.getenv(
        "LINKEDIN_TALENT_URL", os.getenv("Linkedin_tanlent", "https://www.linkedin.com/talent/home/"))
    print(f"Logging in to {prefix}: {url}", flush=True)
    await page.goto(url, wait_until="domcontentloaded")
    emails = existing.CEIPAL_EMAIL_SELECTORS if ceipal else existing.EMAIL_SELECTORS
    passwords = existing.CEIPAL_PASSWORD_SELECTORS if ceipal else existing.PASSWORD_SELECTORS
    try:
        field = await visible(page, emails, timeout=8)
    except RuntimeError:
        print(f"No {prefix} login form; using the saved session.", flush=True)
        if not ceipal:
            await linkedin_2fa(page)
        return
    if not os.getenv(prefix + "_EMAIL") or not os.getenv(prefix + "_PASSWORD"):
        print(f"No {prefix} credentials in .env; complete the login manually.", flush=True)
        return
    await field.fill(os.environ[prefix + "_EMAIL"])
    await (await visible(page, passwords)).fill(os.environ[prefix + "_PASSWORD"])
    submit = existing.CEIPAL_SUBMIT_SELECTORS if ceipal else ('button[type="submit"]',)
    await (await visible(page, submit)).click()
    print(f"{prefix} credentials submitted.", flush=True)
    if not ceipal:
        await linkedin_2fa(page)


async def site_ready(page, ceipal):
    markers = ("/pages/signin", "/users/autologout") if ceipal else (
        "/login", "/checkpoint", "/authwall", "/uas/")
    return not any(marker in page.url for marker in markers)


async def wait_for_sites_ready(ceipal, linkedin, timeout=240):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if await site_ready(ceipal, True) and await site_ready(linkedin, False):
            print("Both sites are ready.", flush=True)
            return True
        print(f"Waiting for login - ceipal={ceipal.url} linkedin={linkedin.url}", flush=True)
        print("Complete any verification in the open browser windows.", flush=True)
        await asyncio.sleep(5)
    raise RuntimeError("Login was not completed on both sites in time")


def totp(secret, timestamp=None):
    secret = re.sub(r"\s+", "", secret).upper()
    key = base64.b32decode(secret + "=" * ((-len(secret)) % 8))
    counter = int(time.time() if timestamp is None else timestamp) // 30
    digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 15
    return f"{(struct.unpack('>I', digest[offset:offset + 4])[0] & 0x7fffffff) % 1000000:06d}"


async def linkedin_2fa(page):
    secret = os.getenv("Code", "").strip()
    if not secret:
        return
    try:
        field = await visible(page, ('input[autocomplete="one-time-code"]',
                                     'input[name="pin"]', '#input__phone_verification_pin'), timeout=15)
    except RuntimeError:
        return
    if not (urlsplit(page.url).hostname or "").endswith(".linkedin.com"):
        raise ValueError("Refusing to enter authentication code outside LinkedIn")
    if time.time() % 30 > 25:
        await asyncio.sleep(31 - time.time() % 30)
    await field.fill(totp(secret))
    await (await visible(page, ('button[type="submit"]', 'input[type="submit"]'))).click()
    print("LinkedIn authenticator code submitted locally.", flush=True)


def is_after_daily_830am_est():
    """Check if current time is at or after 8:30 AM EST (New York time)."""
    try:
        from zoneinfo import ZoneInfo
        est_tz = ZoneInfo("America/New_York")
    except Exception:
        est_tz = timezone(timedelta(hours=-5))
    now_est = datetime.now(est_tz)
    today_830am_est = now_est.replace(hour=8, minute=30, second=0, microsecond=0)
    return now_est >= today_830am_est


is_after_daily_8am_est = is_after_daily_830am_est


def is_job_created_today(text):
    if not text:
        return True
    text_clean = " ".join(str(text).split()).lower()
    if "today" in text_clean:
        return True
    now_dates = []
    try:
        from zoneinfo import ZoneInfo
        now_dates.append(datetime.now(ZoneInfo("America/New_York")))
    except Exception:
        now_dates.append(datetime.now(timezone(timedelta(hours=-5))))
    now_dates.append(datetime.now(timezone.utc))
    now_dates.append(datetime.now())

    for dt in now_dates:
        m_2 = dt.strftime("%m")
        d_2 = dt.strftime("%d")
        y_2 = dt.strftime("%y")
        y_4 = dt.strftime("%Y")
        m_1 = str(dt.month)
        d_1 = str(dt.day)

        candidates = [
            f"{m_2}/{d_2}/{y_2}",
            f"{m_2}/{d_2}/{y_4}",
            f"{m_1}/{d_1}/{y_2}",
            f"{m_1}/{d_1}/{y_4}",
            f"{m_2}/{d_1}/{y_2}",
            f"{m_1}/{d_2}/{y_2}",
            f"{y_4}-{m_2}-{d_2}",
            f"{m_2}-{d_2}-{y_4}",
        ]
        for c in candidates:
            if c.lower() in text_clean:
                return True
    return False


async def apply_today_job_filter(page):
    """Apply 'Today' date filter on the 'Job Created' column in Ceipal JobPosts table only after 8:30 AM EST."""
    if not is_after_daily_830am_est():
        print("Before 8:30 AM EST: keeping all Active/Open jobs without 'Today' date filter.", flush=True)
        return
    try:
        if "/JobPosts" not in page.url:
            return

        print("Checking for 'Job Created' column to apply Today date filter...", flush=True)
        th_locator = page.locator('th').filter(has_text=re.compile(r"Job\s+Created", re.I)).first
        if not await th_locator.count():
            print("Job Created column header not found.", flush=True)
            return

        # 1. Click filter icon on Job Created header
        filter_icon = th_locator.locator('.fa-filter, [class*="filter"], svg, i, span, a, button').last
        clicked = False
        if await filter_icon.count() and await filter_icon.is_visible():
            try:
                await filter_icon.click()
                clicked = True
            except Exception:
                pass

        if not clicked:
            clicked = await page.evaluate("""() => {
                const ths = Array.from(document.querySelectorAll('th'));
                const th = ths.find(el => /Job\\s+Created/i.test(el.innerText));
                if (!th) return false;
                const icon = th.querySelector('.fa-filter, [class*="filter"], svg, i, span, a, button');
                if (icon) {
                    icon.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                    return true;
                }
                th.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                return true;
            }""")

        await asyncio.sleep(1)

        # 2. Look for the search input under 'Search Job Created'
        input_locator = page.locator(
            'div:has-text("Search Job Created") input, '
            'input[placeholder*="Job Created" i], '
            '.dropdown-menu:visible input, '
            '.popover:visible input'
        ).first

        if not await input_locator.count() or not await input_locator.is_visible():
            try:
                await th_locator.click()
                await asyncio.sleep(1)
            except Exception:
                pass

        # 3. Focus and click the input to ensure the calendar datepicker is displayed
        if await input_locator.count() and await input_locator.is_visible():
            try:
                await input_locator.click()
                await asyncio.sleep(0.5)
            except Exception:
                pass

        # 4. Click 'Today' button in the datepicker
        today_clicked = await page.evaluate("""() => {
            const candidates = Array.from(document.querySelectorAll(
                '.datepicker :not(script), .bootstrap-datetimepicker-widget :not(script), ' +
                '[class*="datepicker"] :not(script), [class*="calendar"] :not(script), ' +
                'th, td, button, a, span, div'
            ));
            for (const el of candidates) {
                if (el.offsetWidth <= 0 || el.offsetHeight <= 0) continue;
                const text = (el.innerText || el.textContent || '').trim().toLowerCase();
                if (text === 'today') {
                    el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                    return true;
                }
            }
            return false;
        }""")

        if not today_clicked:
            today_btn = page.locator(
                '.datepicker:visible :text("Today"), '
                '.bootstrap-datetimepicker-widget:visible :text("Today"), '
                'button:has-text("Today"):visible, '
                'a:has-text("Today"):visible, '
                'th:has-text("Today"):visible, '
                ':text("Today"):visible'
            ).last
            if await today_btn.count() and await today_btn.is_visible():
                await today_btn.click()
                today_clicked = True

        # 5. Ensure input has today's date filled
        if await input_locator.count() and await input_locator.is_visible():
            val = (await input_locator.input_value()).strip()
            if not val:
                try:
                    from zoneinfo import ZoneInfo
                    now_est = datetime.now(ZoneInfo("America/New_York"))
                except Exception:
                    now_est = datetime.now(timezone(timedelta(hours=-5)))
                today_formatted = now_est.strftime("%m/%d/%Y")
                await input_locator.fill(today_formatted)
                print(f"Filled date input with {today_formatted}", flush=True)

            await input_locator.press("Enter")
            await input_locator.dispatch_event("change")

        # 6. Fallback submit via JS
        await page.evaluate("""() => {
            const inputs = Array.from(document.querySelectorAll('div:has-text("Search Job Created") input, .dropdown-menu input, .popover input'));
            for (const inp of inputs) {
                inp.dispatchEvent(new Event('change', { bubbles: true }));
                inp.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true }));
            }
        }""")

        print("Applied 'Today' filter on 'Job Created' column. Waiting for table reload...", flush=True)
        await asyncio.sleep(2.5)

        # Close popover if still open
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass

    except Exception as exc:
        print(f"Notice: apply_today_job_filter error: {exc}; proceeding.", flush=True)


async def open_job_list(page):
    print("Opening the Ceipal job list...", flush=True)
    await page.goto("https://talenthirecls12.ceipal.com/JobPosts/index",
                    wait_until="domcontentloaded", timeout=180000)
    print("Ceipal job list URL: " + page.url, flush=True)
    selector = os.getenv("CEIPAL_JOB_LINK_SELECTOR") or "tbody tr td:visible"
    try:
        await page.locator(selector).first.wait_for(state="visible", timeout=180000)
    except Exception:
        print("Ceipal job list never rendered. URL: " + page.url, flush=True)
        raise
    print("Ceipal job list loaded.", flush=True)
    try:
        await apply_today_job_filter(page)
    except Exception as exc:
        print(f"Notice: apply_today_job_filter error: {exc}; continuing.", flush=True)


def boolean_from_groups(groups):
    if not isinstance(groups, list) or not 1 <= len(groups) <= 6:
        raise ValueError("AI must return 1â€“6 skill groups")
    clauses = []
    for group in groups:
        if not isinstance(group, list) or not 1 <= len(group) <= 6:
            raise ValueError("Invalid AI skill group")
        terms = []
        for term in group:
            if not isinstance(term, str) or not term.strip() or len(term) > 100:
                raise ValueError("Invalid AI skill term")
            if any(c in term for c in '"\n\r()'):
                raise ValueError("Unexpected Boolean syntax inside a skill")
            terms.append('"' + term.strip() + '"')
        clauses.append("(" + " OR ".join(dict.fromkeys(terms)) + ")")
    return " AND ".join(clauses)


def generate_boolean(description):
    from recruiting_ai import extract_json
    result = extract_json(
        'Extract required professional skills from this job description. '
        'Return {"groups": [["skill", "synonym"], ["another required skill"]]}. '
        'Use 1 to 6 groups, at most 6 synonyms each. Include only job-related skills, '
        'no demographic attributes, locations, salary, or invented requirements.',
        {"job_description": description},
    )
    return boolean_from_groups(result.get("groups"))

def normalize_ceipal_location_text(location):
    location = " ".join((location or "").replace("\xa0", " ").split())
    location = re.sub(r"^\[([^\]]+)\]\s*,?\s*(.+)$", r"\1, \2", location)
    location = re.sub(r"\s*,\s*", ", ", location)
    location = re.sub(r",\s*,+", ", ", location)
    return location.strip(" ,")


def get_cache_entry(job_key):
    return CLEAN_CACHE.setdefault(job_key, {})


def get_job_key(args, page=None):
    if getattr(args, "job_url", None):
        return args.job_url
    if page is not None:
        try:
            return page.url
        except Exception:
            return "manual_job"
    return "manual_job"


def parse_datetime_flexible(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    for candidate in (text, text.replace("Z", "+00:00")):
        try:
            dt = datetime.fromisoformat(candidate)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


def parse_relative_date_text(value, now=None):
    if not value:
        return None
    now = now or datetime.now(timezone.utc)
    text = str(value).strip().lower()
    if not text:
        return None
    if "today" in text:
        return now
    if "yesterday" in text:
        return now - timedelta(days=1)
    match = re.search(r"(\d+)\s+(minute|minutes|hour|hours|day|days)\s+ago", text)
    if match:
        qty = int(match.group(1))
        unit = match.group(2)
        if "minute" in unit:
            return now - timedelta(minutes=qty)
        if "hour" in unit:
            return now - timedelta(hours=qty)
        if "day" in unit:
            return now - timedelta(days=qty)
    return None


def job_is_older_than_3_days(job_entry, now=None):
    now = now or datetime.now(timezone.utc)
    candidates = [
        parse_datetime_flexible(job_entry.get("extracted_at")),
        parse_datetime_flexible(job_entry.get("date_posted")),
        parse_relative_date_text(job_entry.get("date_posted"), now),
    ]
    valid = [dt for dt in candidates if dt is not None]
    if not valid:
        return False
    latest = max(valid)
    return latest < (now - timedelta(days=3))


def cleanup_old_job_data(job_path, profile_path, history_path):
    now = datetime.now(timezone.utc)

    jobs = {}
    profiles = {}
    history = {}

    from state_store import read_json_object
    jobs = read_json_object(job_path)
    profiles = read_json_object(profile_path)
    history = read_json_object(history_path)

    pending_pdf_codes = {
        code for row in profiles.values() if isinstance(row, dict)
        and row.get("status") not in {"saved", "duplicate", "form_ready", "skipped_db_exists",
                                     "skipped_linkedin_member", "skipped_india_location",
                                     "skipped_resume_parser_failed"}
        and Path(row.get("resume_path") or "").is_file()
        for code in (row.get("job_codes") or [])
    }
    old_codes = {
        code for code, job in jobs.items()
        if isinstance(job, dict) and job_is_older_than_3_days(job, now)
        and code not in pending_pdf_codes
        and not (
            job.get("analysis_status") == "completed"
            and job.get("linkedin_processing") != "completed"
            and history.get(code, {}).get("status") != "completed"
        )
    }

    if not old_codes:
        print("No jobs older than 3 days found in JSON cache.", flush=True)
        return

    for code in list(old_codes):
        jobs.pop(code, None)
        history.pop(code, None)

    for url, row in list(profiles.items()):
        if not isinstance(row, dict):
            continue
        job_codes = row.get("job_codes") or []
        if not isinstance(job_codes, list):
            job_codes = []
        remaining_codes = [code for code in job_codes if code not in old_codes]
        if not remaining_codes:
            profiles.pop(url, None)
        else:
            row["job_codes"] = remaining_codes
            profiles[url] = row

    save_state(job_path, jobs)
    save_state(profile_path, profiles)
    save_state(history_path, history)
    print(f"Deleted old data for {len(old_codes)} job(s) older than 3 days: {sorted(old_codes)}", flush=True)


async def extract_ceipal_location_from_detail(page):
    for selector in CEIPAL_LOCATION_XPATHS:
        try:
            field = page.locator(selector).first
            if await field.count() and await field.is_visible():
                location = await field.evaluate("el => el.value === undefined ? el.innerText : el.value")
                location = normalize_ceipal_location_text(location)
                if location:
                    print("Ceipal job location read from detail page element: " + location, flush=True)
                    return location
        except Exception:
            continue
    return ""


async def job_location(page, args):
    job_key = get_job_key(args, page)
    cache_entry = get_cache_entry(job_key)
    if cache_entry.get("clean_location"):
        print("Reusing cached cleaned location: " + cache_entry["clean_location"], flush=True)
        return cache_entry["clean_location"]

    if args.location:
        cleaned = clean_job_location(args.location.strip())
        cache_entry["clean_location"] = cleaned
        return cleaned
    if args.job_url and page.url != args.job_url:
        await page.goto(args.job_url, wait_until="domcontentloaded", timeout=180000)
    if args.jd_file and not args.job_url:
        manual = (await prompt("Enter the job location from Ceipal (blank if unspecified):")).strip()
        cleaned = clean_job_location(manual) if manual else ""
        cache_entry["clean_location"] = cleaned
        return cleaned
    if args.jd_file and args.job_url:
        await page.goto(args.job_url, wait_until="domcontentloaded")
    selector = os.getenv("CEIPAL_JOB_LOCATION_SELECTOR")
    if selector:
        field = await visible(page, (selector,))
        location = await field.evaluate("el => el.value === undefined ? el.innerText : el.value")
        location = normalize_ceipal_location_text(location)
        if not location:
            raise ValueError("Configured Ceipal location field is empty")
        cleaned = clean_job_location(location)
        cache_entry["clean_location"] = cleaned
        return cleaned
    location = await extract_ceipal_location_from_detail(page)
    if location:
        cleaned = clean_job_location(location)
        cache_entry["clean_location"] = cleaned
        return cleaned
    field = page.get_by_label(re.compile(r"^(Job )?Location(s)?\s*[:*]?$", re.I))
    if await field.count() == 1 and await field.is_visible():
        location = await field.evaluate("el => el.value === undefined ? el.innerText : el.value")
        location = " ".join(location.split())
        if location:
            cleaned = clean_job_location(location)
            cache_entry["clean_location"] = cleaned
            return cleaned
    await page.bring_to_front()
    await page.evaluate("window.getSelection().removeAllRanges()")
    await prompt("Highlight only this job's location in Ceipal, then press Enter (leave no selection if unspecified).")
    location = " ".join((await page.evaluate("window.getSelection().toString()")).split())
    if len(location) > 200:
        raise ValueError("Selected location is too long; select only the job location")
    cleaned = clean_job_location(location) if location else ""
    cache_entry["clean_location"] = cleaned
    return cleaned


TITLE_TRUNCATION_RE = re.compile(r"\s*(?:\.\.+|â€¦)+\s*$")


async def job_title(page, args):
    job_key = get_job_key(args, page)
    cache_entry = get_cache_entry(job_key)
    if cache_entry.get("clean_title"):
        print("Reusing cached cleaned title: " + cache_entry["clean_title"], flush=True)
        return cache_entry["clean_title"]

    listed = JOB_METADATA.get(args.job_url, {}).get("title")
    if listed:
        cleaned_title = TITLE_TRUNCATION_RE.sub("", listed).strip()
        cache_entry["clean_title"] = cleaned_title
        return cleaned_title
    if args.job_url:
        await page.goto(args.job_url, wait_until="domcontentloaded", timeout=180000)
    selector = os.getenv("CEIPAL_JOB_TITLE_SELECTOR")
    if selector:
        field = await visible(page, (selector,))
        title = await field.evaluate("el => el.value === undefined ? el.innerText : el.value")
    else:
        field = page.get_by_label(re.compile(r"^Job Title\s*[:*]?$", re.I))
        if await field.count() == 1 and await field.is_visible():
            title = await field.evaluate("el => el.value === undefined ? el.innerText : el.value")
        else:
            await page.bring_to_front()
            await page.evaluate("window.getSelection().removeAllRanges()")
            await prompt("Highlight only the Job Title in Ceipal, then press Enter.")
            title = await page.evaluate("window.getSelection().toString()")
    title = " ".join(title.split())
    if not title or len(title) > 200:
        raise ValueError("Select a valid Ceipal job title (1â€“200 characters)")
    cleaned_title = TITLE_TRUNCATION_RE.sub("", title).strip()
    cache_entry["clean_title"] = cleaned_title
    return cleaned_title


async def apply_title(page, title):
    from linkedin_search import apply_job_title

    print("Applying LinkedIn job title filter: " + str(title), flush=True)
    await apply_job_title(page, title)

async def clear_existing_location_filters(page):
    remove_buttons = page.locator('button[aria-label^="Remove "]')
    count = await remove_buttons.count()
    removed = 0
    for i in range(count):
        try:
            button = remove_buttons.nth(i - removed)
            label = await button.get_attribute("aria-label") or ""
            if "location" in label.lower() or "united states" in label.lower() or "city" in label.lower():
                await button.click(timeout=3000)
                removed += 1
                await asyncio.sleep(0.5)
        except Exception:
            continue


async def apply_location(page, location):
    from linkedin_search import apply_location_paste

    print("Applying LinkedIn location filter: " + str(location), flush=True)
    await clear_existing_location_filters(page)
    await apply_location_paste(page, location)

def _unique_nonempty(values):
    out = []
    seen = set()
    for value in values:
        compact = re.sub(r"\s*,\s*", ", ", " ".join(str(value or "").split())).strip(" ,")
        if not compact:
            continue
        key = re.sub(r"\s*,\s*", " ", compact).casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(compact)
    return out


def normalize_location_for_business_unit(location, business_unit=""):
    original = ", ".join(clean_location_parts(location))
    if not original:
        return ""

    return original


def split_location_parts(location):
    text = " ".join(str(location or "").split())
    if not text:
        return []
    parts = clean_location_parts(text)
    return parts


def generate_location_variants(location, business_unit=""):
    normalized = normalize_location_for_business_unit(location, business_unit)
    parts = split_location_parts(normalized)
    variants = []

    if len(parts) >= 3:
        city = parts[0]
        state = parts[-2]
        country = parts[-1]
        variants.extend([
            f"{city}, {state}, {country}",
            f"{state}, {country}",
            country,
        ])
    elif len(parts) == 2:
        first, second = parts
        variants.extend([
            f"{first}, {second}",
            second,
        ])
    elif len(parts) == 1:
        variants.append(parts[0])


    unique = _unique_nonempty(variants)
    print(f"Location variants generated from {location!r} / business_unit={business_unit!r}: {unique}", flush=True)
    return unique


async def column_index_by_header(page, header_name):
    header = page.locator(f'th:has-text("{header_name}")').first
    if not await header.count():
        print(f"No {header_name} column found.", flush=True)
        return None
    index = await header.evaluate("el => Array.prototype.indexOf.call(el.parentElement.children, el)")
    print(f"Reading {header_name} from column {index} of the job table.", flush=True)
    return index


async def job_code_column(page):
    index = await column_index_by_header(page, "Job Code")
    if index is None:
        print("Using the job_snapshot id instead.", flush=True)
    return index


def job_code_for(url):
    code = JOB_METADATA.get(url, {}).get("code")
    if code:
        return code
    return urlsplit(url).path.rstrip("/").rsplit("/", 1)[-1]


async def go_to_next_ceipal_job_page(page, page_number):
    candidates = [
        f'a[aria-label="Page {page_number}"]:visible',
        f'button[aria-label="Page {page_number}"]:visible',
        'a[aria-label*="Next"]:visible',
        'button[aria-label*="Next"]:visible',
        'a:has-text("Next"):visible',
        'button:has-text("Next"):visible',
        'li.next a:visible',
    ]
    for candidate in candidates:
        control = page.locator(candidate).first
        try:
            if await control.count() and await control.is_visible():
                await control.click()
                await page.wait_for_load_state("domcontentloaded")
                await asyncio.sleep(2)
                return True
        except Exception:
            continue
    return False


async def discover_jobs(page, limit):
    await open_job_list(page)
    selector = os.getenv("CEIPAL_JOB_LINK_SELECTOR")
    if not selector:
        jobs = []
        seen = set()
        code_column = await job_code_column(page)
        business_unit_column = await column_index_by_header(page, "Business Unit")
        job_created_column = await column_index_by_header(page, "Job Created")

        for page_number in itertools.count(1):
            rows = page.locator("tbody tr:visible").filter(
                has=page.get_by_text(re.compile(r"^(Active|Open)$"))
            )
            found = await rows.all()
            print(f"Page {page_number}: {len(found)} Active/Open job row(s) visible in Ceipal.", flush=True)

            for row in found:
                links = row.locator('a[href*="/job_postings/job_snapshot/"]')
                if await links.count() < 2:
                    continue

                url = await links.first.evaluate("e => e.href")
                if url in seen:
                    continue

                title = (await links.nth(1).inner_text()).strip()
                cells = await row.locator("td").all_inner_texts()

                job_created = ""
                if job_created_column is not None and job_created_column < len(cells):
                    job_created = cells[job_created_column].strip()
                    if is_after_daily_830am_est() and job_created and not is_job_created_today(job_created):
                        print(f"  [After 8:30 AM EST] skipping job {title!r}: created {job_created!r}, not today.", flush=True)
                        continue

                location = ""
                for index, cell in enumerate(cells[:-1]):
                    match = re.search(r"\[([^\]]+)\]", cell)
                    if match:
                        city = match.group(1).split(",")[0].strip()
                        state = cells[index + 1].strip()
                        if city and state:
                            location = city + ", " + state
                        break

                code = ""
                if code_column is not None and code_column < len(cells):
                    code = " ".join(cells[code_column].split())

                business_unit = ""
                if business_unit_column is not None and business_unit_column < len(cells):
                    business_unit = " ".join(cells[business_unit_column].split())

                date_posted = ""
                for cell in cells:
                    compact = " ".join(cell.split())
                    if re.search(r"\b(?:posted|today|yesterday|\d+\s+(?:day|days|hour|hours|minute|minutes)\s+ago)\b", compact, re.I):
                        date_posted = compact
                        break

                JOB_METADATA[url] = {
                    "title": title,
                    "location": location,
                    "code": code,
                    "business_unit": business_unit,
                    "date_posted": job_created or date_posted,
                    "job_created": job_created,
                }
                print(
                    f"  job found: code={job_code_for(url)!r} title={title!r} "
                    f"location={location!r} business_unit={business_unit!r} date_posted={JOB_METADATA[url]['date_posted']!r} job_created={job_created!r}",
                    flush=True,
                )

                jobs.append(url)
                seen.add(url)

                if limit and len(jobs) >= limit:
                    return jobs

            if not await go_to_next_ceipal_job_page(page, page_number + 1):
                print(f"No more Ceipal job list pages after page {page_number}.", flush=True)
                break

        if not jobs:
            print("No visible Active/Open jobs found in Ceipal.", flush=True)
            return []
        return jobs

    await prompt("In Job Posts, filter to OPEN jobs and show all 25 rows, then press Enter.")
    await page.locator(selector).first.wait_for(state="visible")
    hrefs = await page.locator(selector).evaluate_all("els => els.map(el => el.href)")
    jobs = []
    for href in hrefs:
        parts = urlsplit(href or "")
        if parts.scheme == "https" and parts.hostname == "talenthirecls12.ceipal.com" and href not in jobs:
            jobs.append(href)
    if not jobs:
        raise ValueError("No job links found in the filtered list")
    return jobs[:limit]


def canonical_url(url):
    parts = urlsplit(url)
    if parts.scheme != "https" or not (parts.hostname or "").endswith(".linkedin.com"):
        return None
    if not re.match(r"^/talent/profile/[^/]+", parts.path):
        return None
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path.rstrip("/"), "", ""))


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


async def wait_for_search_results(page):
    print("Waiting for LinkedIn search results to finish loading...", flush=True)
    await asyncio.sleep(random.uniform(3, 5))
    try:
        await page.wait_for_function(
            """() => {
                const text = document.body.innerText;
                return text.includes('Search results loaded') &&
                       !text.includes('Loading search results');
            }""", timeout=90000)
        print("LinkedIn results loaded; collecting profile links.", flush=True)
    except Exception:
        print("LinkedIn did not report the results as loaded; collecting what rendered.", flush=True)


FIND_RESULTS_PANE = r"""
  const links = Array.from(document.querySelectorAll('a[href*="/talent/profile/"]'));
  let pane = null;
  let held = 0;
  for (const link of links) {
    for (let el = link.parentElement; el && el !== document.body; el = el.parentElement) {
      const style = getComputedStyle(el);
      if (!/(auto|scroll)/.test(style.overflowY)) continue;
      if (el.scrollHeight <= el.clientHeight + 40) continue;
      const count = links.filter(a => el.contains(a)).length;
      if (count > held) { pane = el; held = count; }
    }
  }
"""

SCROLL_RESULTS_JS = "(ratio) => {" + FIND_RESULTS_PANE + r"""
  if (pane) {
    pane.scrollTop = Math.min(pane.scrollTop + pane.clientHeight * ratio, pane.scrollHeight);
    return {atEnd: pane.scrollTop + pane.clientHeight >= pane.scrollHeight - 4,
            top: Math.round(pane.scrollTop), links: held};
  }
  const before = window.scrollY;
  window.scrollBy(0, window.innerHeight * ratio);
  return {atEnd: before === window.scrollY, top: window.scrollY, links: 0};
}"""

SCROLL_RESULTS_TO_TOP_JS = "() => {" + FIND_RESULTS_PANE + r"""
  if (pane) {
    pane.scrollTop = 0;
    return true;
  }
  window.scrollTo(0, 0);
  return false;
}"""


async def scroll_results_pane(page, ratio=0.8):
    try:
        return await page.evaluate(SCROLL_RESULTS_JS, ratio)
    except Exception:
        return {"atEnd": True, "top": 0}


async def scroll_results_to_top(page):
    try:
        return await page.evaluate(SCROLL_RESULTS_TO_TOP_JS)
    except Exception:
        return False


RESULT_PRESENT_JS = r"""(identifier) => Array.from(
  document.querySelectorAll('a[href*="/talent/profile/"]')
).some(a => (a.getAttribute('href') || a.href || '').includes(identifier))"""


async def ensure_result_visible(page, identifier, steps=40):
    if await page.evaluate(RESULT_PRESENT_JS, identifier):
        return True
    await scroll_results_to_top(page)
    await asyncio.sleep(1.5)
    for _ in range(steps):
        if await page.evaluate(RESULT_PRESENT_JS, identifier):
            return True
        state = await scroll_results_pane(page)
        await asyncio.sleep(random.uniform(0.8, 1.6))
        if state and state.get("atEnd"):
            break
    return await page.evaluate(RESULT_PRESENT_JS, identifier)


COLLECT_PAGE_PROFILES_JS = r"""els => {
  const normalize = s => (s || '').trim().replace(/\s+/g, ' ').toLowerCase();
  const isLinkedInMember = el => {
    const text = normalize(el.innerText || el.textContent);
    if (text === 'linkedin member') return true;
    const aria = normalize(el.getAttribute('aria-label'));
    if (aria === 'linkedin member' || aria.includes('linkedin member')) return true;
    const title = normalize(el.getAttribute('title'));
    if (title === 'linkedin member' || title.includes('linkedin member')) return true;

    const card = el.closest('li, [data-test-search-result], [data-live-test-search-result], .search-result, .artdeco-list__item, .re-search-result, [data-test-row-lockup]') || el.parentElement?.parentElement;
    if (card) {
      const nameEls = card.querySelectorAll('a[href*="/talent/profile/"], [data-test-row-lockup-full-name], .artdeco-entity-lockup__title, .search-result__title, h3, h4, [data-control-name="view_profile"]');
      for (const ne of nameEls) {
        const nt = normalize(ne.innerText || ne.textContent);
        if (nt === 'linkedin member') return true;
        const na = normalize(ne.getAttribute('aria-label'));
        if (na === 'linkedin member' || na.includes('linkedin member')) return true;
      }
      if (card.querySelector('img[alt*="LinkedIn Member" i]')) return true;
      const lines = (card.innerText || '').split('\n').map(l => normalize(l)).filter(Boolean);
      if (lines.slice(0, 3).includes('linkedin member')) return true;
    }
    return false;
  };

  return els.map(el => ({
    href: el.href || el.getAttribute('href') || '',
    is_member: isLinkedInMember(el)
  }));
}"""


async def collect_page_profiles(page, selector, urls, limit):
    previous = -1
    at_end = False
    skipped_members = set()
    for _ in range(60):
        items = await page.locator(selector).evaluate_all(COLLECT_PAGE_PROFILES_JS)
        if items and isinstance(items[0], str):
            for href in items:
                url = canonical_url(href)
                if url and url not in urls:
                    urls.append(url)
        else:
            member_urls = set()
            candidates = []
            for item in (items or []):
                url = canonical_url(item.get("href") if isinstance(item, dict) else str(item))
                if not url:
                    continue
                if isinstance(item, dict) and item.get("is_member"):
                    member_urls.add(url)
                else:
                    candidates.append(url)

            new_skipped = member_urls - skipped_members
            if new_skipped:
                print(f"Skipped {len(new_skipped)} 'LinkedIn Member' profile(s) from collection.", flush=True)
                skipped_members.update(new_skipped)

            for url in candidates:
                if url not in member_urls and url not in urls:
                    urls.append(url)

        print(f"Profiles collected so far: {len(urls)}", flush=True)
        if len(urls) >= limit or (at_end and len(urls) == previous):
            break
        previous = len(urls)
        state = await scroll_results_pane(page)
        at_end = bool(state and state.get("atEnd"))
        await asyncio.sleep(random.uniform(1.5, 3))
    return urls


async def go_to_next_results_page(page, page_number):
    selector = os.getenv("LINKEDIN_NEXT_PAGE_SELECTOR")
    candidates = ([selector] if selector else []) + [
        'a[aria-label^="Go to next page"]:visible',
        'button[aria-label^="Go to next page"]:visible',
        'a[aria-label="Next"]:visible',
        'button[aria-label="Next"]:visible',
        'a:has-text("Next"):visible',
        'button:has-text("Next"):visible',
    ]
    for candidate in candidates:
        control = page.locator(candidate).first
        try:
            if await control.count() and await control.is_visible():
                await control.click()
                return True
        except Exception:
            continue
    numbered = page.locator(f'[aria-label$="Page {page_number}"]:visible').first
    try:
        if await numbered.count() and await numbered.is_visible():
            await numbered.click()
            return True
    except Exception:
        pass
    return False


async def describe_pager(page):
    try:
        summary = await page.evaluate(r"""() => {
          const text = document.body.innerText || '';
          const hits = text.match(/[^\n]{0,50}(?:results?|\bof\s[\d,]+)[^\n]{0,50}/gi) || [];
          return Array.from(new Set(hits.map(s => s.trim().replace(/\s+/g, ' ')))).slice(0, 6);
        }""")
        print("Search summary: " + " | ".join(summary), flush=True)
    except Exception as exc:
        print(f"Could not read the search summary: {type(exc).__name__}", flush=True)
    try:
        controls = await page.evaluate(r"""() => {
          const out = [];
          for (const el of document.querySelectorAll('button, a, [role="button"]')) {
            if (!el.getClientRects().length) continue;
            const label = (el.getAttribute('aria-label') || el.innerText || '').trim().replace(/\s+/g, ' ');
            if (!label || label.length > 40) continue;
            if (/next|previous|prev\b|page \d|\d+ of \d+/i.test(label)) out.push(el.tagName + ':' + label);
          }
          return Array.from(new Set(out)).slice(0, 30);
        }""")
        print("Pager controls: " + (" | ".join(controls) or "none found"), flush=True)
    except Exception as exc:
        print(f"Could not read the pager controls: {type(exc).__name__}", flush=True)


async def collect_profiles(page, limit):
    selector = os.getenv("LINKEDIN_RESULT_SELECTOR", 'a[href*="/talent/profile/"]')
    urls = []
    for page_number in range(1, 21):
        await collect_page_profiles(page, selector, urls, limit)
        if len(urls) >= limit:
            break
        if not await go_to_next_results_page(page, page_number + 1):
            print(f"No further result pages after page {page_number}.", flush=True)
            await describe_pager(page)
            break
        print(f"Opening result page {page_number + 1}.", flush=True)
        await asyncio.sleep(random.uniform(3, 5))
        await page.evaluate("window.scrollTo(0, 0)")
    if not urls:
        limit_reason = await check_linkedin_limit_reached(page)
        if limit_reason:
            print(f"⚠️ LinkedIn search/profile view limit detected: {limit_reason}", flush=True)
    return urls[:limit]


def save_state(path, state):
    data = json.dumps(state, indent=2)
    temp = path.with_suffix(f".tmp_{os.getpid()}_{uuid.uuid4().hex[:8]}")
    for attempt in range(10):
        try:
            temp.write_text(data, encoding="utf-8")
            temp.replace(path)
            return
        except (PermissionError, OSError):
            time.sleep(0.05 * (attempt + 1))
    try:
        path.write_text(data, encoding="utf-8")
    finally:
        if temp.exists():
            try:
                temp.unlink()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs-limit", type=int, default=None,
                        help="Maximum jobs to collect; default collects all Active/Open jobs")
    parser.add_argument("--job-url", help="Ceipal open-job detail URL")
    parser.add_argument("--location", help="Override the Ceipal job location")
    parser.add_argument("--search-only", action="store_true", help="Test title/location search without uploading applicants")
    parser.add_argument("--jobs-only", action="store_true", help="Collect/cache jobs without waiting for LinkedIn or AI")
    parser.set_defaults(jd_file=None)
    parser.add_argument("--limit", type=int, choices=range(1, 21), default=30)
    args = parser.parse_args()
    if args.jobs_limit is not None and args.jobs_limit < 1:
        parser.error("--jobs-limit must be greater than zero")

    job_path = ROOT / "extract_job.json"
    profile_path = ROOT / "profile_data.json"
    history_path = ROOT / "job_history.json"
    from state_store import check_and_perform_daily_830am_est_cleanup
    check_and_perform_daily_830am_est_cleanup(ROOT)
    cleanup_old_job_data(job_path, profile_path, history_path)

    try:
        from recruit_manual import run as run_manual
        return asyncio.run(run_manual(sys.modules[__name__], args))
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"Stopped: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

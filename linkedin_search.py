"""JD-based LinkedIn Recruiter search filters; no inferred job requirements."""
import os
import asyncio
import re


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


def advanced_search_enabled():
    """Advance toggles the extra JD-first pass; default preserves existing behavior."""
    value = os.getenv("Advance", "true").strip().lower()
    if value in {"true", "1", "yes", "on"}:
        return True
    if value in {"false", "0", "no", "off"}:
        return False
    raise ValueError("Set Advance=true or Advance=false")


def advanced_search_url():
    return (os.getenv("ADVANCED_SEARCH_URL", "").strip()
            or os.getenv("LINKEDIN_ADVANCED_SEARCH_URL", "").strip())

def job_search_requirements(job):
    has_jd = bool(job.get("raw_jd") or job.get("job_description") or job.get("jd"))
    skills = list(dict.fromkeys(str(s).strip() for s in (job.get("skills") or []) if str(s).strip())) if has_jd else []
    experience = str(job.get("experience") or "").strip() if has_jd else ""
    return dict(has_jd=has_jd, skills=skills, experience=experience)


def experience_bounds(text):
    """Only auto-map an explicit single years requirement; complex prose needs UI review."""
    text = text.strip().lower().replace("\u2013", "-").replace("\u2014", "-")
    if any(word in text for word in ("preferred", "months", "month")):
        return None
    numbers = re.findall(r"\d+(?:\.\d+)?", text)
    if not numbers:
        return None
    range_match = re.search(r"(\d+)\s*(?:-|to)\s*(\d+)\s*(?:years?|yrs?)", text)
    if range_match and len(numbers) == 2:
        low, high = map(int, range_match.groups())
        return (low, high) if 0 <= low <= high <= 60 else None
    if len(numbers) != 1 or not re.search(r"\b(?:years?|yrs?)\b", text):
        return None
    number = float(numbers[0])
    if not number.is_integer() or not 0 <= number <= 60:
        return None
    value = int(number)
    if re.search(r"up to|at most|maximum|no more than", text):
        return (None, value)
    if re.search(r"\+|at least|minimum|more than|over", text):
        if re.search(r"more than|over", text):
            return None
        return (value, None)
    if re.fullmatch(r"(?:exactly\s+)?\d+\s*(?:years?|yrs?)(?:\s+of\s+experience|\s+experience)?[.]?", text):
        return (value, value) if text.startswith("exactly") else (value, None)
    return None


async def first_visible(locator):
    for field in await locator.all():
        if await field.is_visible():
            return field
    return None


async def clear_search_filters(page):
    """Remove old chips so Advanced Search cannot inherit a previous job title."""
    for _ in range(60):
        button = await first_visible(page.get_by_role("button", name=re.compile(r"^Remove ", re.I)))
        if not button:
            return
        await button.click()
    raise RuntimeError("Could not clear old search filters")

async def open_advanced_search(page, api):
    url = advanced_search_url()
    await page.goto(url or api.SEARCH_URL, wait_until="domcontentloaded")
    for _ in range(60):
        if await first_visible(page.get_by_text("Recruiter Advanced Search", exact=False)):
            return
        if await first_visible(page.locator(".collapsible-facet-wrapper__trigger-label").filter(
                has_text=re.compile(r"Years of experience", re.I))):
            return
        if await first_visible(page.get_by_text("Skill keywords anywhere on profile", exact=True)):
            return
        for role in ("button", "link"):
            control = await first_visible(page.get_by_role(role, name=re.compile(r"advanced search|all filters", re.I)))
            if control:
                await control.click()
                await page.locator(".collapsible-facet-wrapper__trigger-label").filter(
                    has_text=re.compile(r"Years of experience", re.I)
                ).first.wait_for(state="visible", timeout=15000)
                return
        await asyncio.sleep(.25)
    raise RuntimeError("Advanced Search page controls did not load; check ADVANCED_SEARCH_URL/session")

async def apply_skills(page, skills):
    if not skills:
        return
    selector = os.getenv("LINKEDIN_SKILLS_SELECTOR", "").strip()

    async def get_field():
        if selector:
            return page.locator(selector).first
        control = await first_visible(page.get_by_text("Skill keywords anywhere on profile", exact=True))
        if control:
            await control.click()
        field = await first_visible(page.get_by_placeholder(re.compile(r"skill|keyword", re.I)))
        if not field:
            field = await first_visible(page.get_by_role("combobox", name=re.compile(r"skill", re.I)))
        return field

    applied_skills = []
    for skill in skills:
        clean_skill = str(skill).strip().replace('"', '')
        if not clean_skill:
            continue
        field = await get_field()
        if not field:
            raise RuntimeError("Skills input not found; set LINKEDIN_SKILLS_SELECTOR")
        await field.click()
        await field.fill(clean_skill)
        await asyncio.sleep(0.5)
        await field.press("Enter")
        await asyncio.sleep(1)
        applied_skills.append(clean_skill)
        print(f"LinkedIn skill applied: {clean_skill}", flush=True)

    if applied_skills:
        chip = await first_visible(page.get_by_role("button", name=re.compile(r"^Remove .*" + re.escape(applied_skills[0]), re.I)))
        if not chip:
            print(f"LinkedIn skills applied ({len(applied_skills)} skills); chip check warning.", flush=True)
    print(f"LinkedIn all skills applied: {applied_skills}", flush=True)


async def visible_experience_panel(page):
    """The editor may be a portal: its heading/trigger need not be inside it."""
    # Live Recruiter DOM: use the experience form's stable field names.
    for form in await page.locator("form.range-slider-facet__form").all():
        if not await form.is_visible():
            continue
        lower = form.locator('input[name="range-from"]')
        upper = form.locator('input[name="range-to"]')
        update = form.get_by_role("button", name="Update", exact=True)
        if await lower.count() and await upper.count() and await update.count():
            return lower.first, upper.first, update.first
    updates = page.get_by_role("button", name="Update", exact=True)
    for update in await updates.all():
        if not await update.is_visible():
            continue
        ancestors = update.locator(
            "xpath=ancestor::*[count(.//input[not(@type='range') and not(@type='hidden')]) >= 2][1]"
        )
        for panel in await ancestors.all():
            inputs = []
            for field in await panel.locator(
                "input:not([type='range']):not([type='hidden']):not([type='checkbox']):not([type='radio'])"
            ).all():
                if await field.is_visible() and await field.is_editable():
                    inputs.append(field)
            if len(inputs) == 2:
                return inputs[0], inputs[1], update
    return None


async def experience_controls(page):
    """Hover the experience plus icon, click it, then use From/to and Update."""
    controls = await visible_experience_panel(page)
    if controls is None:
        triggers = page.locator(".collapsible-facet-wrapper__trigger-label[data-test-toggle-label]").filter(
            has_text=re.compile(r"^\s*Years of experience\s*$", re.I)
        )
        buttons = page.locator("button.collapsible-facet-wrapper__trigger").filter(has_text=re.compile(r"Years of experience", re.I))
        trigger = await first_visible(buttons)
        if trigger is None:
            trigger = await first_visible(triggers)
        if trigger is None:
            trigger = await first_visible(page.get_by_text("Years of experience", exact=True))
        if trigger is None:
            await triggers.first.wait_for(state="visible", timeout=15000)
            trigger = await first_visible(buttons)
            if trigger is None:
                trigger = await first_visible(triggers)
        if trigger is None:
            raise RuntimeError("Years of experience trigger text not found")
        await trigger.scroll_into_view_if_needed()
        await trigger.hover()
        await asyncio.sleep(.5)
        plus = await first_visible(trigger.locator(
            'li-icon[type="plus-icon"], .collapsible-facet-wrapper__trigger-icon'
        ))
        if plus is not None:
            await plus.hover()
            await asyncio.sleep(.5)
        # Live testing: clicking the wide button centre can leave it collapsed;
        # clicking the visible trigger-label opens the actual Ember facet.
        label = await first_visible(trigger.locator(".collapsible-facet-wrapper__trigger-label"))
        click_target = label if label is not None else trigger
        for attempt in range(3):
            if attempt == 0 or await trigger.get_attribute("aria-expanded") != "true":
                await click_target.click()
            for _ in range(20):
                controls = await visible_experience_panel(page)
                if controls is not None:
                    break
                await asyncio.sleep(.25)
            if controls is not None:
                break
    if controls is None:
        raise RuntimeError("Years of experience popup did not expose From/to and Update")
    lower, upper, update = controls
    min_selector = os.getenv("LINKEDIN_EXPERIENCE_MIN_SELECTOR", "").strip()
    max_selector = os.getenv("LINKEDIN_EXPERIENCE_MAX_SELECTOR", "").strip()
    return (page.locator(min_selector).first if min_selector else lower,
            page.locator(max_selector).first if max_selector else upper, update)

async def apply_experience(page, text):
    if not text:
        return
    bounds = experience_bounds(text)
    if bounds is None:
        raise RuntimeError(f"JD experience cannot be safely mapped to a years range: {text}")
    minimum = 0 if bounds[0] is None else bounds[0]
    maximum = 30 if bounds[1] is None else bounds[1]
    if minimum > 30 or maximum > 30:
        raise RuntimeError(f"Experience requirement exceeds the Recruiter 0-30+ editor: {text}")
    lower, upper, update = await experience_controls(page)
    # Widen first to avoid a stale range clamping the new From/to values.
    for field, value in ((lower, 0), (upper, 30), (lower, minimum), (upper, maximum)):
        await field.fill(str(value))
        await field.press("Tab")
        await asyncio.sleep(.3)
    actual = ((await lower.input_value()).strip(), (await upper.input_value()).strip())
    if actual != (str(minimum), str(maximum)):
        raise RuntimeError("Experience From/to inputs did not retain requested years")
    await update.click()
    print(f"LinkedIn experience updated: From {minimum} to {maximum} (30 means 30+); JD: {text}", flush=True)

async def submit_search(page, required=False):
    selector = os.getenv("LINKEDIN_SEARCH_SUBMIT_SELECTOR", "").strip()
    buttons = page.locator(selector) if selector else page.get_by_role("button", name=re.compile(r"^search$", re.I))
    for _ in range(40 if required else 1):
        button = await first_visible(buttons)
        if button:
            await button.click()
            print("LinkedIn Search clicked; waiting 2 seconds before results processing.", flush=True)
            await asyncio.sleep(2)
            return
        if required:
            await asyncio.sleep(.25)
    if required:
        raise RuntimeError("Search button not visible after experience Update; search was not submitted")

async def wait_for_filter_step(message):
    print(f"LinkedIn: {message}; waiting 2 seconds...", flush=True)
    await asyncio.sleep(2)

async def apply_jd_filters(page, requirements, wait_between=False):
    # On result pages, expose the screenshot's full filter panel when necessary.
    if requirements["experience"]:
        years = await first_visible(page.get_by_text("Years of experience", exact=True))
        if not years:
            for role in ("button", "link"):
                panel = await first_visible(page.get_by_role(role, name=re.compile(r"^all filters$|advanced search", re.I)))
                if panel:
                    await panel.click()
                    break
    await apply_skills(page, requirements["skills"])
    if requirements["skills"]:
        await wait_for_filter_step("skills selected")
    if requirements.get("experience"):
        try:
            await apply_experience(page, requirements["experience"])
            if wait_between:
                await wait_for_filter_step("experience Update completed")
        except Exception as exc:
            print(f"LinkedIn experience filter skipped ({exc}); continuing search with skills.", flush=True)

async def run_advanced_search(api, args, linkedin, code, locations, requirements, saved_job, persist_history, title=""):
    """Run/cache JD-first pass(es) across all location variations; return tracking record and URLs."""
    advanced = None
    advanced_urls = []
    if requirements["has_jd"] and advanced_search_enabled():
        if isinstance(locations, str):
            location_variants = [locations]
        else:
            location_variants = list(locations or [])
        location_variants = list(dict.fromkeys(
            " ".join(str(v or "").split()) for v in location_variants if " ".join(str(v or "").split())
        ))[:3]
        if not location_variants:
            return None, []

        signature = dict(
            title=title,
            location=location_variants[0] if location_variants else "",
            locations=location_variants,
            skills=requirements["skills"],
            experience=requirements["experience"],
            version=3
        )
        advanced = saved_job.get("advanced_search") or {}
        if (not args.search_only and advanced.get("filters") == signature
                and advanced.get("search_status") == "completed"):
            advanced_urls = advanced.get("profiles", [])[:20]
            print(f"Job {code}: reusing Advanced Search URLs ({len(advanced_urls)}).", flush=True)
            return advanced, advanced_urls

        advanced = dict(
            filters=signature,
            status="pending",
            profiles=[],
            location_searches=advanced.get("location_searches", []) if advanced.get("filters") == signature else []
        )
        saved_job["advanced_search"] = advanced
        persist_history()
        adv_location_searches = advanced.setdefault("location_searches", [])
        all_advanced_candidates = []

        for pass_number, variant in enumerate(location_variants, start=1):
            if len(all_advanced_candidates) >= 20 and not args.search_only:
                print(f"Job {code}: Already reached 20 Advanced Search profiles; skipping remaining passes.", flush=True)
                break

            record = next(
                (item for item in adv_location_searches if item.get("pass") == pass_number),
                None,
            )
            if (record and record.get("filters") == signature and record.get("location") == variant
                    and (record.get("status") in {"processing", "collected", "completed"}
                         or record.get("search_status") == "completed")
                    and not args.search_only):
                candidates = record.get("profiles", [])
                print(f"Job {code}: reusing Advanced Search pass {pass_number} for {variant!r} ({len(candidates)} profile(s)).", flush=True)
            else:
                candidates = None

            if candidates is None:
                try:
                    print(f"Job {code}: next step - running Advanced Search pass {pass_number} for {variant!r}...", flush=True)
                    await open_advanced_search(linkedin, api)
                    await wait_for_filter_step("Advanced Search opened")
                    await clear_search_filters(linkedin)
                    await wait_for_filter_step("old filters cleared")
                    if title:
                        await api.apply_title(linkedin, title)
                        await wait_for_filter_step("job title selected")
                    await api.apply_location(linkedin, variant)
                    await wait_for_filter_step(f"location variant {pass_number} ({variant}) selected")
                    await apply_jd_filters(linkedin, requirements, wait_between=True)
                    await wait_for_filter_step("filters ready before Search")
                    await submit_search(linkedin, required=True)
                    await api.wait_for_search_results(linkedin)
                    needed = max(1, 20 - len(all_advanced_candidates)) if not args.search_only else 20
                    candidates = (await api.collect_profiles(linkedin, needed))[:needed]
                    if not candidates:
                        limit_fn = getattr(api, "check_linkedin_limit_reached", None)
                        limit_reason = await limit_fn(linkedin) if limit_fn else None
                        if limit_reason:
                            print(f"⚠️ Job {code}: LinkedIn profile view / search limit detected on Advanced Search pass {pass_number}: {limit_reason!r}", flush=True)
                            saved_job["limit_reached"] = True
                            saved_job["limit_reason"] = limit_reason
                            advanced["limit_reached"] = True
                            advanced["limit_reason"] = limit_reason
                except Exception as exc:
                    if is_browser_closed_error(exc):
                        print(f"Job {code}: Browser closed suddenly during Advanced Search pass {pass_number}; leaving pending.", flush=True)
                        raise
                    if record is None:
                        record = {"pass": pass_number, "location": variant, "profiles": []}
                        adv_location_searches.append(record)
                    record.update(filters=signature, search_status="failed", status="failed", error=str(exc))
                    persist_history()
                    print(f"Job {code}: Advanced Search pass {pass_number} ({variant}) failed: {exc}; continuing to next variant.", flush=True)
                    continue

            # Cap candidates to only what is needed up to 20
            remaining_slots = max(0, 20 - len(all_advanced_candidates)) if not args.search_only else len(candidates)
            needed_candidates = candidates[:remaining_slots]

            if record is None:
                record = {"pass": pass_number, "location": variant}
                adv_location_searches.append(record)
            record["filters"] = signature
            record.pop("error", None)
            record["location"] = variant
            record["profiles"] = needed_candidates
            record["search_status"] = "limit_reached" if advanced.get("limit_reached") and not needed_candidates else "completed"
            record["status"] = "pending" if advanced.get("limit_reached") and not needed_candidates else "collected"
            all_advanced_candidates.extend(needed_candidates)
            persist_history()
            print(f"Job {code}: Advanced Search pass {pass_number} using {variant!r} returned {len(needed_candidates)} profile(s) (total: {len(all_advanced_candidates)}/20).", flush=True)

        advanced_urls = list(dict.fromkeys(all_advanced_candidates))[:20]
        if advanced.get("limit_reached") and not advanced_urls:
            advanced.update(profiles=[], search_status="limit_reached", status="pending")
            print(f"⚠️ Job {code}: Advanced Search hit LinkedIn limit; keeping status pending.", flush=True)
        elif any(r.get("search_status") == "completed" for r in adv_location_searches):
            advanced.update(profiles=advanced_urls, search_status="completed", status="collected")
            print(f"Job {code}: Advanced Search location pass(es) finished; {len(advanced_urls)} profile(s) collected (max 20).", flush=True)
        else:
            advanced.update(profiles=[], search_status="failed", status="failed")
            print(f"Job {code}: Advanced Search all passes failed; continuing with normal searches.", flush=True)
        persist_history()


    return advanced, advanced_urls

async def paste_filter_value(page, field, value, blur=True):
    """Use the focused keyboard so a rerendered typeahead cannot stale a locator."""
    from urllib.parse import urlsplit
    parts = urlsplit(page.url)
    origin = f"{parts.scheme}://{parts.netloc}"
    await page.context.grant_permissions(["clipboard-read", "clipboard-write"], origin=origin)
    await page.evaluate("text => navigator.clipboard.writeText(text)", value)
    await field.scroll_into_view_if_needed()
    await field.click()
    await page.keyboard.press("Control+A")
    await page.keyboard.press("Control+V")
    await asyncio.sleep(1)
    if blur:
        await page.keyboard.press("Tab")

async def apply_location_paste(page, location):
    """Paste location and choose its first suggestion; no post-selection token checks."""
    location = " ".join(str(location or "").split())
    if not location:
        raise ValueError("Location is required before searching")
    selector = os.getenv("LINKEDIN_LOCATION_SELECTOR", "").strip()

    async def find_field():
        if selector:
            return await first_visible(page.locator(selector))
        field = await first_visible(page.get_by_placeholder(re.compile(r"enter a location|search locations?", re.I)))
        return field or await first_visible(page.get_by_role("combobox", name=re.compile(r"^Locations?$", re.I)))

    field = await find_field()
    if field is None:
        trigger = await first_visible(page.get_by_text("Candidate geographic locations", exact=True))
        if trigger is None:
            trigger = await first_visible(page.locator(".collapsible-facet-wrapper__trigger-label").filter(
                has_text=re.compile(r"^\s*Locations?\s*$", re.I)))
        if trigger is None:
            raise RuntimeError("Location input/trigger not found; set LINKEDIN_LOCATION_SELECTOR")
        await trigger.scroll_into_view_if_needed()
        await trigger.click()
        for _ in range(40):
            field = await find_field()
            if field is not None:
                break
            await asyncio.sleep(.25)
    if field is None:
        raise RuntimeError("Location input did not open")
    await paste_filter_value(page, field, location, blur=False)
    print(f"LinkedIn location pasted: {location}; waiting for first suggestion...", flush=True)
    await asyncio.sleep(2)
    option_selector = os.getenv("LINKEDIN_LOCATION_OPTION_SELECTOR", "").strip()
    for _ in range(60):
        current_field = await find_field()
        options = page.locator(option_selector) if option_selector else None
        if options is None and current_field is not None:
            controlled = (await current_field.get_attribute("aria-controls") or await current_field.get_attribute("aria-owns") or "").split()
            for identifier in controlled:
                container = page.locator("id=" + identifier)
                if await container.count() and await container.first.is_visible():
                    options = container.locator('[role="option"], li, .basic-typeahead__selectable')
                    break
        if options is None:
            options = page.locator('[role="listbox"]:visible [role="option"], .basic-typeahead__selectable:visible, .typeahead-results:visible li')
        first = await first_visible(options)
        if first is not None:
            await first.scroll_into_view_if_needed()
            await first.click()
            await asyncio.sleep(1)
            print("LinkedIn first location suggestion selected.", flush=True)
            return
        await asyncio.sleep(.25)
    raise RuntimeError(f"No location suggestion appeared for {location}")


async def apply_job_title(page, title):
    """Paste the exact title via Ctrl+V; no suggestion clicks or token checks."""
    title = " ".join(str(title or "").split())
    if not title:
        raise ValueError("Job title is empty")
    await page.wait_for_load_state("domcontentloaded")
    await asyncio.sleep(2)
    selector = os.getenv("LINKEDIN_JOB_TITLE_SELECTOR", "").strip()

    async def find_input():
        if selector:
            return await first_visible(page.locator(selector))
        return await first_visible(page.locator(
            'input[role="combobox"][placeholder*="enter a job title or boolean" i]'
        ))

    field = await find_input()
    if field is None:
        trigger = await first_visible(page.get_by_text("Job titles or boolean", exact=True))
        if trigger:
            await trigger.scroll_into_view_if_needed()
            await trigger.click()
        for _ in range(60):
            field = await find_input()
            if field is not None:
                break
            await asyncio.sleep(.25)
    if field is None:
        raise RuntimeError("Job-title typeahead input not found after page load")
    await paste_filter_value(page, field, title)
    print(f"LinkedIn job title pasted: {title}", flush=True)

import os
import json
import time
import requests
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 1000


def _post_waiting_for_quota(*args, **kwargs):
    """Keep the same AI request pending until provider quota is available."""
    delay = 60.0
    while True:
        response = requests.post(*args, **kwargs)
        quota = response.status_code == 429 or (
            response.status_code in (402, 403)
            and any(term in response.text.casefold()
                    for term in ("quota", "rate limit", "ratelimit", "resource_exhausted"))
        )
        if not quota:
            return response
        wait = delay
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                wait = max(1.0, float(retry_after))
            except ValueError:
                try:
                    reset = parsedate_to_datetime(retry_after)
                    if reset.tzinfo is None:
                        reset = reset.replace(tzinfo=timezone.utc)
                    wait = max(1.0, (reset - datetime.now(timezone.utc)).total_seconds())
                except (ValueError, TypeError, OverflowError):
                    pass
        response.close()
        print(f"AI quota/rate limit reached; waiting {wait:.0f}s before retrying the same request.", flush=True)
        remaining = wait
        while remaining > 0:
            pause = min(remaining, 60)
            time.sleep(pause)
            remaining -= pause
        delay = min(delay * 2, 600)


def _post_with_retry(body: dict) -> dict:
    """Route all pipeline AI calls through the provider selected in .env."""
    provider = os.getenv("AI_PROVIDER", "ollama").strip().lower()
    if provider in ("azure_ai", "azure"):
        return _post_azure(body)
    if provider not in ("ollama", "olama"):
        raise ValueError("Set AI_PROVIDER=azure_ai or AI_PROVIDER=ollama (choose one)")
    return _post_ollama(body)


def _post_azure(body: dict) -> dict:
    endpoint = os.getenv("AZURE_AI_ENDPOINT", "").strip()
    key = os.getenv("AZURE_AI_API_KEY", "").strip()
    if not endpoint or not key:
        raise ValueError("Azure requires AZURE_AI_ENDPOINT and AZURE_AI_API_KEY in .env")
    for attempt in range(1, MAX_RETRIES + 1):
        response = _post_waiting_for_quota(
            endpoint, json=body,
            headers={"Content-Type": "application/json", "api-key": key},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_SECONDS)
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError("Azure AI request failed")


def _post_ollama(body: dict) -> dict:
    """Adapt Ollama responses to the common chat completion response shape."""
    endpoint = os.getenv("OLLAMA_URL", "http://localhost:11434/api/chat").rstrip("/")
    for suffix in ("/api/generate", "/api/chat"):
        if endpoint.endswith(suffix):
            endpoint = endpoint[:-len(suffix)]
            break
    endpoint += "/api/chat"
    payload = {
        "model": os.getenv("OLLAMA_MODEL", "qwen2.5:3b"),
        "messages": body.get("messages", []),
        "stream": False,
        "options": {
            "temperature": body.get("temperature", 0),
            "num_predict": max(body.get("max_tokens", 2048), 256),
            "num_ctx": int(os.getenv("OLLAMA_CONTEXT_LENGTH", "16384")),
        },
    }
    if body.get("response_format", {}).get("type") == "json_object":
        payload["format"] = "json"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = _post_waiting_for_quota(endpoint, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            data = response.json()
            if data.get("done_reason") == "length":
                raise ValueError("Ollama output was truncated; increase the output token budget")
            content = data.get("message", {}).get("content", "").strip()
            if not content:
                raise ValueError("Ollama returned empty content")
            return {"choices": [{"message": {"content": content}}]}
        except requests.ConnectionError as exc:
            raise RuntimeError("Cannot connect to Ollama. Start it with: ollama serve") from exc
        except requests.HTTPError as exc:
            if response.status_code == 404:
                raise RuntimeError(
                    f"Ollama model not found. Run: ollama pull {payload['model']}"
                ) from exc
            if response.status_code not in (429, 500, 502, 503, 504) or attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY_SECONDS)
    raise RuntimeError("Ollama request failed")

def _extract_message_content(data: dict) -> str:
    choices = data.get("choices", [])
    if not choices:
        raise ValueError("AI provider returned no response.")

    content = choices[0].get("message", {}).get("content", "").strip()
    if not content:
        raise ValueError("AI provider returned empty content.")

    return content


def validate_city_location(location: str) -> bool:
    """Check that a location names a real city and includes its matching country."""
    candidate = " ".join(str(location or "").split())
    components = [part.strip() for part in candidate.split(",")]
    if len(components) < 2 or not all(components):
        print("City validation (AI): False (country is required; AI not called)")
        return False

    prompt = (
        "Determine whether the supplied location identifies a real city and includes "
        "that city's correct country. The first comma-separated component must be a "
        "geographic city name, not a person's name, degree, job title, or other text. "
        "The last comma-separated component must be the real country containing that "
        "city. If there are middle components, they must be the matching state or "
        "province. Return valid=true only when the city, any state/province, and "
        "country are consistent and you are confident. If the country is missing, "
        "uncertain, or any component is not a real geographic location, return "
        "valid=false. "
        "Return JSON only with a boolean key named valid."
    )
    body = {
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps({"location": candidate}, ensure_ascii=False)},
        ],
        "temperature": 0,
        "max_tokens": 30,
        "response_format": {"type": "json_object"},
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = _post_with_retry(body)
            content = _extract_message_content(data)
            try:
                parsed = json.loads(content)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Ollama returned non-JSON location validation content: {content}"
                ) from exc

            valid = parsed.get("valid")
            if not isinstance(valid, bool):
                raise ValueError(
                    f"Ollama returned an invalid location validation result: {content}"
                )

            print(f"City validation (AI): {valid}")
            return valid
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            if attempt == MAX_RETRIES:
                print(
                    "City validation (AI): False "
                    f"(failed after {MAX_RETRIES} attempts: {exc})"
                )
                return False
            print(
                f"City validation AI attempt {attempt}/{MAX_RETRIES} failed: {exc}. "
                f"Retrying in {RETRY_DELAY_SECONDS} seconds..."
            )
            time.sleep(RETRY_DELAY_SECONDS)

    return False


def clean_job_title(raw_title: str) -> str:
    """Return a simple cleaned job title without client name, business unit, location, or noise."""
    if raw_title is None:
        return ""

    original = " ".join(str(raw_title).split())
    if not original:
        return ""

    prompt = (
        "You clean messy job titles extracted from ATS, LinkedIn, or job boards into simple standard job titles. "
        "Return JSON only with keys original_title and cleaned_title. "
        "Strict rules for cleaned_title: "
        "1. Strictly remove all client names, customer names, agency or department names/acronyms (e.g. NJAOC, SCCO, DOT, NYC, State of NJ, etc.), business unit names, and requisition codes. "
        "2. Strictly remove all location names (cities, states, countries). "
        "3. Keep only the core, standard professional role/job title (maximum 1 to 2 standard titles, e.g. 'Help Desk Analyst', 'Java Developer', 'Data Analyst'). "
        "4. Remove level indicators like L2, L3, II, III, Level 2, numeric grades, and trailing punctuation like ' -..', '...', etc. "
        "5. Example: 'NJAOC SCCO Help Desk Analyst -..' must be cleaned strictly to 'Help Desk Analyst'. "
        "Do not include any client name, business unit, or location in cleaned_title."
    )

    body = {
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps({"original_title": original}, ensure_ascii=False)},
        ],
        "temperature": 0,
        "max_tokens": 60,
        "response_format": {"type": "json_object"},
    }

    data = _post_with_retry(body)
    content = _extract_message_content(data)

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"AI returned non-JSON content: {content}") from exc

    cleaned = " ".join(str(parsed.get("cleaned_title", "")).split())
    cleaned = re.sub(r"[\s\-_.:;]+$", "", cleaned).strip()
    return cleaned or original


def clean_job_location(raw_location: str) -> str:
    """Return a cleaned location with country always included."""
    if raw_location is None:
        return ""

    original = " ".join(str(raw_location).split())
    if not original:
        return ""

    prompt = (
        "You are an expert location cleaning agent. Your task is to take raw, messy location data and convert it into a clean, fully expanded, and standardized format. "
        "Strictly follow these rules: "
        "1. Always return full location names. Never use abbreviations or short forms. For example, use California instead of CA, Virginia instead of VA, and United States instead of US or USA. "
        "2. Always include the country in the final output whenever it is present or can be clearly inferred from the input. "
        "3. Format the cleaned location as comma-separated components: City, State or Province, Country. "
        "Omit components that are not present and do not guess missing cities or states. "
        "4. Remove noise such as Remote, Hybrid, Onsite, Location:, job IDs, extra punctuation, and duplicate text. "
        "5. Preserve the true location and do not invent details unless the country is obvious from the city and state combination. "
        "6. If the input is a U.S. city and state, always return United States as the country. "
        "7. Return JSON only with keys original_location and cleaned_location. Do not return any extra text."
    )

    body = {
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps({"original_location": original}, ensure_ascii=False)},
        ],
        "temperature": 0,
        "max_tokens": 80,
        "response_format": {"type": "json_object"},
    }

    data = _post_with_retry(body)
    content = _extract_message_content(data)

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Ollama returned non-JSON content: {content}") from exc

    cleaned = " ".join(str(parsed.get("cleaned_location", "")).split())
    return cleaned or original

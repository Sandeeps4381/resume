"""Read optional JSON state without failing on a new or emptied file."""
import json


def read_json_object(path):
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8-sig").strip()
    if not text:
        print(f"State file {path.name} is empty; starting with an empty object.", flush=True)
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in {path.name}, line {exc.lineno}, column {exc.colno}. "
            "File preserved; correct its JSON before restarting."
        ) from exc
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object in {path.name}; file preserved")
    return data


def check_and_perform_daily_830am_est_cleanup(root_dir, jobs=None, profiles=None, history=None):
    """
    Daily delete all data of JSON files (extract_job.json, job_history.json, profile_data.json)
    at 8:30 AM EST (America/New_York timezone).
    """
    import os
    from datetime import datetime, timezone, timedelta
    from pathlib import Path

    try:
        from zoneinfo import ZoneInfo
        est_tz = ZoneInfo("America/New_York")
    except Exception:
        est_tz = timezone(timedelta(hours=-5))

    now_est = datetime.now(est_tz)
    today_830am_est = now_est.replace(hour=8, minute=30, second=0, microsecond=0)

    # Check if we have reached or passed 8:30 AM EST today
    if now_est < today_830am_est:
        return False

    today_str = now_est.date().isoformat()
    marker_path = Path(root_dir) / ".last_daily_cleanup.json"

    last_cleaned = None
    if marker_path.exists():
        try:
            marker_data = json.loads(marker_path.read_text(encoding="utf-8"))
            last_cleaned = marker_data.get("last_cleaned_date")
        except Exception:
            last_cleaned = None

    if last_cleaned == today_str:
        return False

    print(f"\n=======================================================", flush=True)
    print(f"⏰ [8:30 AM EST DAILY CLEANUP TRIGGERED at {now_est.strftime('%Y-%m-%d %H:%M:%S %Z')}]", flush=True)
    print("Deleting all previous date data (extract_job.json, job_history.json, profile_data.json, and resumes)...", flush=True)

    job_path = Path(root_dir) / "extract_job.json"
    profile_path = Path(root_dir) / "profile_data.json"
    history_path = Path(root_dir) / "job_history.json"

    for path in (job_path, profile_path, history_path):
        try:
            path.write_text("{}", encoding="utf-8")
        except Exception as exc:
            print(f"Could not reset {path.name}: {exc}", flush=True)

    if isinstance(jobs, dict):
        jobs.clear()
    if isinstance(profiles, dict):
        profiles.clear()
    if isinstance(history, dict):
        history.clear()

    resumes_dir = Path(root_dir) / "resumes"
    if resumes_dir.is_dir():
        for pdf_file in resumes_dir.glob("*.pdf"):
            try:
                pdf_file.unlink()
            except Exception:
                pass

    try:
        marker_path.write_text(
            json.dumps({
                "last_cleaned_date": today_str,
                "timestamp_est": now_est.isoformat(),
            }, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        print(f"Could not write cleanup marker file: {exc}", flush=True)

    print("✅ Daily 8:30 AM EST cleanup completed successfully. State files reset to empty.", flush=True)
    print(f"=======================================================\n", flush=True)
    return True


# Backward compatibility alias
check_and_perform_daily_8am_est_cleanup = check_and_perform_daily_830am_est_cleanup



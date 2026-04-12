#!/usr/bin/env python
"""Backend API for Clip Studio ES (Railway/Fly/Render).

Exposes:
- POST /api/discover
- POST /api/jobs
- GET  /api/jobs/{job_id}
- GET  /api/health
- Static media under /output
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import traceback
import uuid
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from clip_dashboard import (  # noqa: E402
    DashboardConfig,
    backfill_recent_used_videos_from_output,
    build_daily_post_plan,
    discover_creator_videos,
    generate_dashboard,
    recent_used_video_keys,
    record_used_video,
)
from tiktok_direct_post_api import (  # noqa: E402
    TikTokApiError,
    TikTokDesktopOAuth,
    TikTokDirectPostClient,
    load_tokens,
    save_tokens,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_origins() -> list[str]:
    raw = os.getenv("CORS_ORIGINS", "*").strip()
    if raw == "*":
        return ["*"]
    return [x.strip() for x in raw.split(",") if x.strip()]


def _normalize_public_base_url(raw: str) -> str:
    value = (raw or "").strip().rstrip("/")
    if not value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return value


OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", str(REPO_ROOT / "output"))).resolve()
WORK_DIR = Path(os.getenv("WORK_DIR", str(REPO_ROOT / "work"))).resolve()
DATA_DIR = Path(os.getenv("DATA_DIR", str(REPO_ROOT / "data"))).resolve()
TIKTOK_VERIFICATION_DIR = Path(os.getenv("TIKTOK_VERIFICATION_DIR", str(REPO_ROOT / "tiktok_verification"))).resolve()
FRONTEND_INDEX = REPO_ROOT / "index.html"
BACKEND_PUBLIC_URL = (
    _normalize_public_base_url(os.getenv("RAILWAY_PUBLIC_DOMAIN", ""))
    or _normalize_public_base_url(os.getenv("RAILWAY_SERVICE_WEB_URL", ""))
    or _normalize_public_base_url(os.getenv("BACKEND_PUBLIC_URL", ""))
)
YTDLP_COOKIES_FILE = os.getenv("YTDLP_COOKIES_FILE", "").strip()
YTDLP_COOKIES_TEXT = os.getenv("YTDLP_COOKIES_TEXT", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_MESSAGE_THREAD_ID = os.getenv("TELEGRAM_MESSAGE_THREAD_ID", "").strip()
TELEGRAM_APPROVALS_ENABLED = os.getenv("TELEGRAM_APPROVALS_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
TIKTOK_CLIENT_KEY = os.getenv("TIKTOK_CLIENT_KEY", "").strip()
TIKTOK_CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET", "").strip()
TIKTOK_REDIRECT_URI = os.getenv("TIKTOK_REDIRECT_URI", "http://127.0.0.1:8765/callback/").strip()
TIKTOK_TOKENS_FILE = os.getenv("TIKTOK_TOKENS_FILE", str(REPO_ROOT / ".tiktok_tokens.json")).strip()
TIKTOK_DEFAULT_PRIVACY = os.getenv("TIKTOK_DEFAULT_PRIVACY", "PUBLIC_TO_EVERYONE").strip() or "PUBLIC_TO_EVERYONE"
TIKTOK_EXPECTED_USERNAME = os.getenv("TIKTOK_EXPECTED_USERNAME", "").strip().lstrip("@").lower()
TIKTOK_BROWSER_FALLBACK_ENABLED = (
    os.getenv("TIKTOK_BROWSER_FALLBACK_ENABLED", "0").strip().lower() not in {"0", "false", "no", "off", ""}
)
TIKTOK_BROWSER_CONNECT_CDP = os.getenv("TIKTOK_BROWSER_CONNECT_CDP", "").strip()
TIKTOK_BROWSER_CHANNEL = os.getenv("TIKTOK_BROWSER_CHANNEL", "brave").strip() or "brave"
TIKTOK_BROWSER_EXECUTABLE = os.getenv("TIKTOK_BROWSER_EXECUTABLE", "").strip()
TIKTOK_BROWSER_PROFILE_DIR = os.getenv("TIKTOK_BROWSER_PROFILE_DIR", str(REPO_ROOT / ".tiktok_profile")).strip()
TIKTOK_BROWSER_MANUAL_WAIT = int(os.getenv("TIKTOK_BROWSER_MANUAL_WAIT", "45").strip() or "45")
TIKTOK_BROWSER_AUTO_POST = os.getenv("TIKTOK_BROWSER_AUTO_POST", "1").strip().lower() not in {"0", "false", "no", "off"}
TIKTOK_BROWSER_USE_SYSTEM_PROFILE = (
    os.getenv("TIKTOK_BROWSER_USE_SYSTEM_PROFILE", "1").strip().lower() not in {"0", "false", "no", "off"}
)
TIKTOK_BROWSER_USER_DATA_DIR = os.getenv("TIKTOK_BROWSER_USER_DATA_DIR", "").strip()
TIKTOK_BROWSER_PROFILE_DIRECTORY = os.getenv("TIKTOK_BROWSER_PROFILE_DIRECTORY", "Default").strip() or "Default"
TIKTOK_BROWSER_TIMEOUT_SEC = int(os.getenv("TIKTOK_BROWSER_TIMEOUT_SEC", "900").strip() or "900")
TIKTOK_BROWSER_HELPER_URL = os.getenv("TIKTOK_BROWSER_HELPER_URL", "").strip().rstrip("/")
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Europe/Madrid").strip() or "Europe/Madrid"
SCHEDULER_ENABLED = os.getenv("DAILY_REVIEW_SCHEDULER_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
SCHEDULER_SLOT_TIMES_RAW = os.getenv("DAILY_REVIEW_SLOT_TIMES", "09:30,13:30,18:30,21:30,23:00").strip()
SCHEDULER_PREP_MINUTES = int(os.getenv("DAILY_REVIEW_PREP_MINUTES", "20").strip() or "20")
SCHEDULER_PREP_MINUTES_BY_SLOT_RAW = os.getenv("DAILY_REVIEW_PREP_MINUTES_BY_SLOT", "09:30=15").strip()
SCHEDULER_PER_CHANNEL_SCAN = int(os.getenv("DAILY_REVIEW_PER_CHANNEL_SCAN", "12").strip() or "12")
SCHEDULER_MAX_RESULTS = int(os.getenv("DAILY_REVIEW_MAX_RESULTS", "18").strip() or "18")
SCHEDULER_MIN_SOURCE_DURATION = int(os.getenv("DAILY_REVIEW_MIN_SOURCE_DURATION", "90").strip() or "90")
SCHEDULER_RESERVE_COUNT = int(os.getenv("DAILY_REVIEW_RESERVE_COUNT", "2").strip() or "2")
SCHEDULER_DURATION = int(os.getenv("DAILY_REVIEW_DURATION", "60").strip() or "60")
SCHEDULER_STRIDE = int(os.getenv("DAILY_REVIEW_STRIDE", "10").strip() or "10")
SCHEDULER_OVERLAP_RATIO = float(os.getenv("DAILY_REVIEW_OVERLAP_RATIO", "0.40").strip() or "0.40")
SCHEDULER_OPTIONS_PER_SLOT = int(os.getenv("DAILY_REVIEW_OPTIONS_PER_SLOT", "3").strip() or "3")
SCHEDULER_LANGUAGE = os.getenv("DAILY_REVIEW_LANGUAGE", "es").strip() or "es"
SCHEDULER_MODE = os.getenv("DAILY_REVIEW_MODE", "creators_es").strip() or "creators_es"
SCHEDULER_STATE_FILE = WORK_DIR / "daily_review_scheduler_state.json"
PUBLISH_REQUESTS_FILE = DATA_DIR / "publish_requests.json"
RETENTION_ENABLED = os.getenv("GENERATED_MEDIA_RETENTION_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
RETENTION_HOURS = max(1, int(os.getenv("GENERATED_MEDIA_RETENTION_HOURS", "24").strip() or "24"))
RETENTION_OUTPUT_HOURS = max(
    1,
    int(os.getenv("GENERATED_MEDIA_OUTPUT_RETENTION_HOURS", str(RETENTION_HOURS)).strip() or str(RETENTION_HOURS)),
)
RETENTION_WORK_HOURS = max(
    1,
    int(os.getenv("GENERATED_MEDIA_WORK_RETENTION_HOURS", "6").strip() or "6"),
)
RETENTION_INTERVAL_MINUTES = max(5, int(os.getenv("GENERATED_MEDIA_RETENTION_INTERVAL_MINUTES", "60").strip() or "60"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
WORK_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
TIKTOK_VERIFICATION_DIR.mkdir(parents=True, exist_ok=True)

try:
    LOCAL_TZ = ZoneInfo(APP_TIMEZONE)
except Exception:
    LOCAL_TZ = ZoneInfo("Europe/Madrid")


def _parse_scheduler_slot_times(raw: str) -> list[str]:
    parsed: list[tuple[int, str]] = []
    seen: set[str] = set()
    for part in (raw or "").split(","):
        value = part.strip()
        if not value:
            continue
        try:
            hour_str, minute_str = value.split(":", 1)
            hour = int(hour_str)
            minute = int(minute_str)
        except Exception:
            continue
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            continue
        normalized = f"{hour:02d}:{minute:02d}"
        if normalized in seen:
            continue
        seen.add(normalized)
        parsed.append((hour * 60 + minute, normalized))
    parsed.sort(key=lambda row: row[0])
    return [value for _, value in parsed]


SCHEDULER_SLOT_TIMES = _parse_scheduler_slot_times(SCHEDULER_SLOT_TIMES_RAW) or ["09:30", "13:30", "18:30", "21:30", "23:00"]


def _parse_scheduler_prep_minutes_by_slot(raw: str) -> dict[str, int]:
    parsed: dict[str, int] = {}
    for part in (raw or "").split(","):
        value = part.strip()
        if not value or "=" not in value:
            continue
        slot_raw, minutes_raw = value.split("=", 1)
        slot = slot_raw.strip()
        try:
            minutes = max(1, int(minutes_raw.strip()))
        except Exception:
            continue
        if slot:
            parsed[slot] = minutes
    return parsed


SCHEDULER_PREP_MINUTES_BY_SLOT = _parse_scheduler_prep_minutes_by_slot(SCHEDULER_PREP_MINUTES_BY_SLOT_RAW)


def _prep_minutes_for_slot(slot_label: str) -> int:
    return int(SCHEDULER_PREP_MINUTES_BY_SLOT.get(slot_label, SCHEDULER_PREP_MINUTES))


def _cookie_file_is_usable(path: str) -> bool:
    if not path:
        return False
    p = Path(path)
    return p.exists() and p.is_file() and p.stat().st_size > 0


def _resolve_cookies_file() -> str:
    """Resolve cookie file path from file var or inline text var."""
    global YTDLP_COOKIES_FILE
    file_path = YTDLP_COOKIES_FILE.strip()
    if _cookie_file_is_usable(file_path):
        return file_path

    if YTDLP_COOKIES_TEXT.strip():
        text = YTDLP_COOKIES_TEXT
        # Support both real newlines and escaped "\\n".
        if "\\n" in text and "\n" not in text:
            text = text.replace("\\n", "\n")

        # Prefer writing to configured cookie path (e.g. persistent /data/cookies.txt).
        if file_path:
            try:
                target = Path(file_path)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text, encoding="utf-8")
                if _cookie_file_is_usable(str(target)):
                    YTDLP_COOKIES_FILE = str(target)
                    os.environ["YTDLP_COOKIES_FILE"] = YTDLP_COOKIES_FILE
                    return YTDLP_COOKIES_FILE
            except Exception:
                pass

        # Fallback to temporary file if target path is unavailable.
        inline_path = Path("/tmp/ytdlp_cookies_from_env.txt")
        inline_path.write_text(text, encoding="utf-8")
        YTDLP_COOKIES_FILE = str(inline_path)
        os.environ["YTDLP_COOKIES_FILE"] = YTDLP_COOKIES_FILE
        return YTDLP_COOKIES_FILE

    return file_path


YTDLP_COOKIES_FILE = _resolve_cookies_file()


class DiscoverRequest(BaseModel):
    mode: str = Field(default="viral_es", min_length=4, max_length=32)
    channels: list[str] | None = None
    per_channel_scan: int = Field(default=12, ge=5, le=50)
    this_week_only: bool = True
    max_results: int = Field(default=12, ge=1, le=50)
    min_source_duration: int = Field(default=90, ge=30, le=3600)


class DailyPlanRequest(BaseModel):
    mode: str = Field(default="creators_es", min_length=4, max_length=32)
    channels: list[str] | None = None
    per_channel_scan: int = Field(default=12, ge=5, le=50)
    this_week_only: bool = True
    max_results: int = Field(default=18, ge=4, le=50)
    min_source_duration: int = Field(default=90, ge=30, le=3600)
    posts_per_day: int = Field(default=4, ge=1, le=4)
    reserve_count: int = Field(default=2, ge=0, le=4)
    slot_option_count: int = Field(default=3, ge=1, le=3)


class DailyReviewBatchRequest(DailyPlanRequest):
    duration: int = Field(default=60, ge=20, le=90)
    stride: int = Field(default=10, ge=5, le=30)
    overlap_ratio: float = Field(default=0.40, ge=0.10, le=0.80)
    language: str = Field(default="es", min_length=2, max_length=8)
    options_per_slot: int = Field(default=3, ge=1, le=3)


class CreateJobRequest(BaseModel):
    url: str = Field(min_length=8, max_length=500)
    duration: int = Field(default=60, ge=20, le=90)
    options: int = Field(default=6, ge=1, le=12)
    stride: int = Field(default=10, ge=5, le=30)
    overlap_ratio: float = Field(default=0.40, ge=0.10, le=0.80)
    language: str = Field(default="es", min_length=2, max_length=8)
    fast_render: bool = False


class ShareTelegramRequest(BaseModel):
    job_id: str = Field(min_length=8, max_length=80)
    option_id: int = Field(ge=1, le=99)


class PrepareTikTokReviewRequest(BaseModel):
    job_id: str = Field(min_length=8, max_length=80)
    option_id: int = Field(ge=1, le=99)
    privacy_level: str | None = Field(default=None, min_length=3, max_length=64)


class PrepareTikTokReviewFromOutputRequest(BaseModel):
    output_slug: str = Field(min_length=3, max_length=200)
    option_id: int = Field(ge=1, le=99)
    privacy_level: str | None = Field(default=None, min_length=3, max_length=64)


class SchedulerTriggerRequest(BaseModel):
    slot_label: str | None = Field(default=None, min_length=4, max_length=16)


app = FastAPI(title="Clip Studio ES API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_read_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/output", StaticFiles(directory=str(OUTPUT_DIR), check_dir=False), name="output")

_jobs_lock = threading.Lock()
_jobs: dict[str, dict[str, Any]] = {}
_publish_lock = threading.Lock()
_publish_requests: dict[str, dict[str, Any]] = {}
_daily_batches_lock = threading.Lock()


def _persist_publish_requests_locked() -> None:
    snapshot = {request_id: dict(payload) for request_id, payload in _publish_requests.items()}
    tmp_path = PUBLISH_REQUESTS_FILE.with_suffix(PUBLISH_REQUESTS_FILE.suffix + ".tmp")
    tmp_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(PUBLISH_REQUESTS_FILE)


def _load_publish_requests() -> None:
    global _publish_requests
    if not PUBLISH_REQUESTS_FILE.exists():
        return
    try:
        payload = json.loads(PUBLISH_REQUESTS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(payload, dict):
        return
    restored: dict[str, dict[str, Any]] = {}
    for request_id, req in payload.items():
        if not isinstance(req, dict):
            continue
        rid = str(req.get("request_id") or request_id or "").strip()
        if not rid:
            continue
        req["request_id"] = rid
        restored[rid] = req
    with _publish_lock:
        _publish_requests = restored


def _get_publish_request(request_id: str, *, reload_if_missing: bool = False) -> dict[str, Any] | None:
    with _publish_lock:
        req = _publish_requests.get(request_id)
    if req or not reload_if_missing:
        return req
    _load_publish_requests()
    with _publish_lock:
        return _publish_requests.get(request_id)

_daily_review_batches: dict[str, dict[str, Any]] = {}
_tiktok_oauth_lock = threading.Lock()
_tiktok_oauth_states: dict[str, dict[str, Any]] = {}
_telegram_session = requests.Session()
_telegram_poller_started = False
_telegram_update_offset = 0
_scheduler_lock = threading.Lock()
_scheduler_thread_started = False
_scheduler_state: dict[str, Any] = {
    "date": "",
    "triggered": {},
    "last_check": None,
    "last_error": None,
    "next_trigger_at": None,
    "last_batch_id": None,
}
_cleanup_lock = threading.Lock()
_cleanup_thread_started = False
_cleanup_state: dict[str, Any] = {
    "last_run_at": None,
    "last_error": None,
    "deleted_output_entries": 0,
    "deleted_work_entries": 0,
    "purged_jobs": 0,
    "purged_publish_requests": 0,
    "purged_daily_batches": 0,
}


def _local_now() -> datetime:
    return datetime.now(LOCAL_TZ)


def _load_scheduler_state() -> None:
    global _scheduler_state
    state = {
        "date": "",
        "triggered": {},
        "last_check": None,
        "last_error": None,
        "next_trigger_at": None,
        "last_batch_id": None,
    }
    try:
        if SCHEDULER_STATE_FILE.exists():
            payload = json.loads(SCHEDULER_STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                state.update(payload)
                if not isinstance(state.get("triggered"), dict):
                    state["triggered"] = {}
    except Exception as exc:
        state["last_error"] = f"state_load_failed: {exc}"
    with _scheduler_lock:
        _scheduler_state = state


def _save_scheduler_state() -> None:
    with _scheduler_lock:
        payload = dict(_scheduler_state)
    try:
        SCHEDULER_STATE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _scheduler_window_for(day: datetime, slot_label: str) -> dict[str, str]:
    hour_str, minute_str = slot_label.split(":", 1)
    publish_at = day.replace(hour=int(hour_str), minute=int(minute_str), second=0, microsecond=0)
    prepare_at = publish_at - timedelta(minutes=_prep_minutes_for_slot(slot_label))
    return {
        "slot_label": slot_label,
        "prepare_at": prepare_at.isoformat(),
        "publish_at": publish_at.isoformat(),
    }


def _get_scheduler_status() -> dict[str, Any]:
    now = _local_now()
    with _scheduler_lock:
        state = dict(_scheduler_state)
    return {
        "enabled": SCHEDULER_ENABLED,
        "timezone": APP_TIMEZONE,
        "slot_times": list(SCHEDULER_SLOT_TIMES),
        "prep_minutes": SCHEDULER_PREP_MINUTES,
        "prep_minutes_by_slot": dict(SCHEDULER_PREP_MINUTES_BY_SLOT),
        "now_local": now.isoformat(),
        "state": state,
        "windows_today": [_scheduler_window_for(now, slot) for slot in SCHEDULER_SLOT_TIMES],
    }


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_payload_older_than(payload: dict[str, Any], cutoff: datetime) -> bool:
    for key in ("updated_at", "created_at"):
        dt = _parse_iso_datetime(payload.get(key))
        if dt is not None:
            return dt < cutoff
    return False


def _path_last_modified_ts(path: Path) -> float:
    latest = path.stat().st_mtime
    if not path.is_dir():
        return latest
    for root, dirs, files in os.walk(path):
        for name in dirs:
            try:
                latest = max(latest, (Path(root) / name).stat().st_mtime)
            except Exception:
                continue
        for name in files:
            try:
                latest = max(latest, (Path(root) / name).stat().st_mtime)
            except Exception:
                continue
    return latest


def _delete_old_entries(root: Path, cutoff_ts: float) -> int:
    deleted = 0
    if not root.exists():
        return deleted
    for child in root.iterdir():
        try:
            if _path_last_modified_ts(child) >= cutoff_ts:
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=False)
            else:
                child.unlink(missing_ok=True)
            deleted += 1
        except FileNotFoundError:
            continue
    return deleted


def _purge_old_memory(cutoff: datetime) -> dict[str, int]:
    purged = {"jobs": 0, "publish_requests": 0, "daily_batches": 0}

    with _jobs_lock:
        removable = [
            job_id
            for job_id, payload in _jobs.items()
            if payload.get("status") not in {"queued", "pending", "running"}
            and _is_payload_older_than(payload, cutoff)
        ]
        for job_id in removable:
            _jobs.pop(job_id, None)
        purged["jobs"] = len(removable)

    with _publish_lock:
        removable = [
            request_id
            for request_id, payload in _publish_requests.items()
            if payload.get("status") not in {"pending_review", "publishing"}
            and _is_payload_older_than(payload, cutoff)
        ]
        for request_id in removable:
            _publish_requests.pop(request_id, None)
        _persist_publish_requests_locked()
        purged["publish_requests"] = len(removable)

    with _daily_batches_lock:
        removable = [
            batch_id
            for batch_id, payload in _daily_review_batches.items()
            if payload.get("status") not in {"queued", "running"}
            and _is_payload_older_than(payload, cutoff)
        ]
        for batch_id in removable:
            _daily_review_batches.pop(batch_id, None)
        purged["daily_batches"] = len(removable)

    return purged


def _run_retention_cleanup() -> None:
    now_utc = datetime.now(timezone.utc)
    output_cutoff = now_utc - timedelta(hours=RETENTION_OUTPUT_HOURS)
    work_cutoff = now_utc - timedelta(hours=RETENTION_WORK_HOURS)
    memory_cutoff = now_utc - timedelta(hours=min(RETENTION_OUTPUT_HOURS, RETENTION_WORK_HOURS))
    deleted_output_entries = _delete_old_entries(OUTPUT_DIR, output_cutoff.timestamp())
    deleted_work_entries = _delete_old_entries(WORK_DIR, work_cutoff.timestamp())
    purged = _purge_old_memory(memory_cutoff)
    with _cleanup_lock:
        _cleanup_state.update(
            {
                "last_run_at": now_utc.isoformat(),
                "last_error": None,
                "deleted_output_entries": deleted_output_entries,
                "deleted_work_entries": deleted_work_entries,
                "purged_jobs": purged["jobs"],
                "purged_publish_requests": purged["publish_requests"],
                "purged_daily_batches": purged["daily_batches"],
            }
        )


def _retention_cleanup_loop() -> None:
    while True:
        try:
            if RETENTION_ENABLED:
                _run_retention_cleanup()
        except Exception as exc:
            with _cleanup_lock:
                _cleanup_state.update(
                    {
                        "last_run_at": datetime.now(timezone.utc).isoformat(),
                        "last_error": str(exc),
                    }
                )
        threading.Event().wait(RETENTION_INTERVAL_MINUTES * 60)


def _ensure_retention_cleanup_started() -> None:
    global _cleanup_thread_started
    if _cleanup_thread_started or not RETENTION_ENABLED:
        return
    _cleanup_thread_started = True
    threading.Thread(target=_retention_cleanup_loop, daemon=True).start()


@app.on_event("startup")
def _startup_tasks() -> None:
    backfill_recent_used_videos_from_output(OUTPUT_DIR)
    _load_scheduler_state()
    _load_publish_requests()
    _ensure_telegram_poller_started()
    _ensure_daily_review_scheduler_started()
    _ensure_retention_cleanup_started()


def _safe_rel_to_output(file_path: str) -> str | None:
    try:
        p = Path(file_path).resolve()
        rel = p.relative_to(OUTPUT_DIR)
        return rel.as_posix()
    except Exception:
        return None


def _file_to_url(file_path: str) -> str | None:
    rel = _safe_rel_to_output(file_path)
    if not rel:
        return None
    return f"/output/{rel}"


def _public_base_url(request: Request | None = None) -> str:
    if BACKEND_PUBLIC_URL:
        return BACKEND_PUBLIC_URL.rstrip("/")
    if request is not None:
        return str(request.base_url).rstrip("/")
    return "http://127.0.0.1:8780"


def _serialize_candidate(c: Any) -> dict[str, Any]:
    return {
        "title": c.title,
        "url": c.url,
        "view_count": int(c.view_count or 0),
        "duration": c.duration,
        "channel": c.channel,
        "video_id": c.video_id,
        "upload_date": c.upload_date,
        "views_per_day": float(c.views_per_day or 0.0),
        "ai_score": float(c.ai_score or 0.0),
        "ai_reason": c.ai_reason or "",
    }


def _serialize_plan_entry(entry: dict[str, Any]) -> dict[str, Any]:
    candidate = entry.get("candidate")
    return {
        "slot_key": entry.get("slot_key"),
        "slot_label": entry.get("slot_label"),
        "publish_time": entry.get("publish_time"),
        "strategy": entry.get("strategy"),
        "role": entry.get("role"),
        "plan_score": entry.get("plan_score"),
        "reason": entry.get("reason"),
        "summary": entry.get("summary"),
        "candidate": _serialize_candidate(candidate) if candidate else None,
        "alternatives": [
            _serialize_candidate(alt.get("candidate"))
            for alt in (entry.get("alternatives") or [])
            if alt.get("candidate") is not None
        ],
    }


def _serialize_result(result: Any) -> dict[str, Any]:
    options: list[dict[str, Any]] = []
    for opt in result.options:
        preview_path = str((Path(opt.manual_upload_file).resolve().parent / opt.preview_file).resolve())
        poster_path = (
            str((Path(opt.manual_upload_file).resolve().parent / opt.poster_file).resolve())
            if getattr(opt, "poster_file", "")
            else ""
        )
        preview_url = _file_to_url(preview_path)
        poster_url = _file_to_url(poster_path) if poster_path else None
        manual_url = _file_to_url(opt.manual_upload_file)
        options.append(
            {
                "option_id": opt.option_id,
                "start": opt.start,
                "end": opt.end,
                "duration": opt.duration,
                "score": opt.score,
                "interest_score": opt.interest_score,
                "reach_score": opt.reach_score,
                "audio_score": opt.audio_score,
                "visual_score": opt.visual_score,
                "hook": opt.hook,
                "short_description": opt.short_description,
                "why_it_may_work": opt.why_it_may_work,
                "transcript_preview": getattr(opt, "transcript_preview", ""),
                "cue_count": getattr(opt, "cue_count", 0),
                "speech_density": getattr(opt, "speech_density", 0.0),
                "question_hits": getattr(opt, "question_hits", 0),
                "exclaim_hits": getattr(opt, "exclaim_hits", 0),
                "number_hits": getattr(opt, "number_hits", 0),
                "scene_cut_count": getattr(opt, "scene_cut_count", 0),
                "signal_tags": getattr(opt, "signal_tags", []),
                "tiktok_title": getattr(opt, "tiktok_title", ""),
                "tiktok_caption": getattr(opt, "tiktok_caption", ""),
                "tiktok_hashtags": getattr(opt, "tiktok_hashtags", []),
                "preview_file": opt.preview_file,
                "poster_file": getattr(opt, "poster_file", ""),
                "manual_upload_file": opt.manual_upload_file,
                "preview_url": preview_url,
                "poster_url": poster_url,
                "manual_upload_url": manual_url,
            }
        )
    return {
        "dashboard_dir": result.dashboard_dir,
        "dashboard_html": result.dashboard_html,
        "dashboard_html_url": _file_to_url(result.dashboard_html),
        "manifest_path": result.manifest_path,
        "manifest_url": _file_to_url(result.manifest_path),
        "source_title": result.source_title,
        "source_url": result.source_url,
        "options": options,
    }


def _telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _tiktok_enabled() -> bool:
    return bool(TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET and Path(TIKTOK_TOKENS_FILE).exists())


def _tiktok_credentials_configured() -> bool:
    return bool(TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET)


def _build_telegram_caption(job: dict[str, Any], option: dict[str, Any], result: dict[str, Any]) -> str:
    source_title = str(result.get("source_title") or "Clip Studio ES").strip()
    source_url = str(result.get("source_url") or "").strip()
    short_desc = str(option.get("short_description") or "").strip()
    why = str(option.get("why_it_may_work") or "").strip()
    hook = str(option.get("hook") or "").strip()
    tags = option.get("signal_tags") or []
    tag_text = ", ".join(str(tag) for tag in tags[:5]) if tags else "sin senales"

    lines = [
        "Clip listo para revisar",
        f"video: {source_title}",
        f"opcion: {option.get('option_id')}",
        f"tramo: {float(option.get('start') or 0):.1f}s -> {float(option.get('end') or 0):.1f}s",
    ]
    if hook:
        lines.append(f"hook: {hook}")
    if short_desc:
        lines.append(f"descripcion: {short_desc}")
    if why:
        lines.append(f"por que: {why}")
    lines.append(f"senales: {tag_text}")
    if source_url:
        lines.append(f"fuente: {source_url}")
    return "\n".join(lines)[:1000]


def _compose_tiktok_post_text(option: dict[str, Any]) -> str:
    caption = str(option.get("tiktok_caption") or option.get("short_description") or "").strip()
    hashtags = option.get("tiktok_hashtags") or []
    hash_line = " ".join(str(tag).strip() for tag in hashtags if str(tag).strip())
    post_text = "\n".join(part for part in [caption, hash_line] if part).strip()
    return post_text[:2200]


def _should_try_browser_fallback(exc: Exception) -> bool:
    message = str(exc).lower()
    if not TIKTOK_BROWSER_FALLBACK_ENABLED:
        return False
    return any(
        needle in message
        for needle in (
            "content-sharing-guidelines",
            "integration guidelines",
            "review our integration guidelines",
        )
    )


def _extract_browser_fallback_result(stdout: str) -> dict[str, Any]:
    raw = (stdout or "").strip()
    if not raw:
        return {}
    for line in reversed(raw.splitlines()):
        candidate = line.strip()
        if not candidate.startswith("{"):
            continue
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return {}


def _clean_browser_fallback_stderr(stderr: str) -> str:
    lines = []
    for line in (stderr or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if "[DEP0169]" in stripped:
            continue
        if "Use `node --trace-deprecation" in stripped:
            continue
        lines.append(stripped)
    return "\n".join(lines).strip()


def _run_tiktok_browser_fallback(video_path: Path, caption: str, privacy_level: str) -> dict[str, Any]:
    helper_url = TIKTOK_BROWSER_HELPER_URL
    if helper_url:
        public_video_url = _build_absolute_asset_url(_file_to_url(str(video_path)) or "", base_url=_public_base_url(None))
        if not public_video_url:
            raise RuntimeError("Browser fallback fallo: no se pudo construir una URL publica del video.")
        try:
            response = requests.post(
                helper_url,
                json={
                    "video_url": public_video_url,
                    "caption": caption[:2200],
                    "privacy_level": privacy_level,
                    "manual_wait": TIKTOK_BROWSER_MANUAL_WAIT,
                    "auto_post": TIKTOK_BROWSER_AUTO_POST,
                },
                timeout=max(120, TIKTOK_BROWSER_TIMEOUT_SEC),
            )
            response.raise_for_status()
            helper_payload = response.json()
        except requests.RequestException as exc:
            raise RuntimeError(f"Browser fallback helper fallo: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Browser fallback helper devolvio respuesta invalida: {exc}") from exc
        if not helper_payload.get("ok"):
            detail = (
                str(helper_payload.get("error") or "").strip()
                or str((helper_payload.get("result") or {}).get("error") or "").strip()
                or str((helper_payload.get("result") or {}).get("status") or "").strip()
                or str(helper_payload.get("stderr") or "").strip()
                or "helper_failed"
            )
            raise RuntimeError(f"Browser fallback helper fallo: {detail[:1200]}")
        return {"result": helper_payload.get("result") or helper_payload}

    script = SCRIPTS_DIR / "upload_to_tiktok.py"
    if not script.exists():
        raise RuntimeError(f"No existe el uploader browser fallback: {script}")

    cmd = [
        sys.executable,
        str(script),
        "--video",
        str(video_path),
        "--caption",
        caption[:2200],
        "--privacy-level",
        privacy_level,
        "--manual-wait",
        str(TIKTOK_BROWSER_MANUAL_WAIT),
        "--browser-channel",
        TIKTOK_BROWSER_CHANNEL,
        "--json",
    ]
    if TIKTOK_BROWSER_AUTO_POST:
        cmd.append("--auto-post")
    if TIKTOK_BROWSER_CONNECT_CDP:
        cmd.extend(["--connect-cdp", TIKTOK_BROWSER_CONNECT_CDP])
    if TIKTOK_BROWSER_EXECUTABLE:
        cmd.extend(["--browser-executable", TIKTOK_BROWSER_EXECUTABLE])
    if TIKTOK_BROWSER_PROFILE_DIR:
        cmd.extend(["--profile-dir", TIKTOK_BROWSER_PROFILE_DIR])
    if TIKTOK_BROWSER_USE_SYSTEM_PROFILE:
        cmd.append("--use-system-chrome-profile")
    if TIKTOK_BROWSER_USER_DATA_DIR:
        cmd.extend(["--chrome-user-data-dir", TIKTOK_BROWSER_USER_DATA_DIR])
    if TIKTOK_BROWSER_PROFILE_DIRECTORY:
        cmd.extend(["--chrome-profile-directory", TIKTOK_BROWSER_PROFILE_DIRECTORY])

    env = os.environ.copy()
    env.setdefault("NODE_NO_WARNINGS", "1")
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=TIKTOK_BROWSER_TIMEOUT_SEC,
        env=env,
    )
    stdout = (proc.stdout or "").strip()
    stderr = _clean_browser_fallback_stderr(proc.stderr or "")
    result = _extract_browser_fallback_result(stdout)
    if proc.returncode != 0:
        detail = (
            str(result.get("error") or "").strip()
            or str(result.get("status") or "").strip()
            or stderr
            or stdout
            or f"codigo {proc.returncode}"
        )
        raise RuntimeError(f"Browser fallback fallo: {detail[:1200]}")

    if not result and stdout:
        last_line = stdout.splitlines()[-1].strip()
        try:
            result = json.loads(last_line)
        except Exception:
            result = {"ok": "true", "status": "unknown", "raw": last_line}
    if not result:
        result = {"ok": "true", "status": "unknown"}
    return {
        "result": result,
        "stdout": stdout[-2000:],
        "stderr": stderr[-1000:],
    }


def _build_telegram_review_caption(job: dict[str, Any], option: dict[str, Any], result: dict[str, Any]) -> str:
    source_title = str(result.get("source_title") or "Clip Studio ES").strip()
    source_channel = str(result.get("source_channel") or result.get("channel") or "").strip()
    tiktok_title = str(option.get("tiktok_title") or option.get("short_description") or "").strip()
    tiktok_caption = str(option.get("tiktok_caption") or "").strip()
    why = str(option.get("why_it_may_work") or "").strip()
    hashtags = " ".join(option.get("tiktok_hashtags") or [])
    lines = [
        "Revision TikTok pendiente",
        f"video: {source_title}",
        f"opcion: {option.get('option_id')}",
    ]
    if source_channel:
        lines.append(f"canal: {source_channel}")
    if tiktok_title:
        lines.append(f"titulo: {tiktok_title}")
    if tiktok_caption:
        lines.append(f"caption: {tiktok_caption}")
    if why:
        lines.append(f"por que puede funcionar: {why}")
    if hashtags:
        lines.append(f"hashtags: {hashtags}")
    lines.append("Pulsa OK para subir a TikTok.")
    return "\n".join(lines)[:1000]


def _build_review_reply_markup(request_id: str) -> str:
    return json.dumps(
        {
            "inline_keyboard": [
                [
                    {"text": "OK TikTok", "callback_data": f"ttok:{request_id}:ok"},
                    {"text": "Cancelar", "callback_data": f"ttok:{request_id}:no"},
                ]
            ]
        },
        ensure_ascii=False,
    )


def _build_absolute_asset_url(relative_or_absolute_url: str, *, request: Request | None = None, base_url: str | None = None) -> str:
    raw = str(relative_or_absolute_url or "").strip()
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        return raw
    base = (base_url or _public_base_url(request)).rstrip("/")
    if raw.startswith("/"):
        return f"{base}{raw}"
    return f"{base}/{raw}"


def _request_public_base_url(request: Request) -> str:
    return _public_base_url(request)


def _build_tiktok_redirect_uri(request: Request) -> str:
    return f"{_request_public_base_url(request)}/api/tiktok/connect/callback/"


def _telegram_call(method: str, *, data: dict[str, Any], files: dict[str, Any] | None = None, timeout: int = 180) -> dict[str, Any]:
    try:
        response = _telegram_session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
            data=data,
            files=files,
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"telegram_send_failed: {exc}") from exc

    body = response.json()
    if not body.get("ok"):
        raise HTTPException(status_code=502, detail=f"telegram_send_failed: {body}")
    return body


def _telegram_api_get(method: str, *, params: dict[str, Any], timeout: int = 60) -> dict[str, Any]:
    try:
        response = _telegram_session.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
            params=params,
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"telegram_get_failed: {exc}") from exc

    body = response.json()
    if not body.get("ok"):
        raise HTTPException(status_code=502, detail=f"telegram_get_failed: {body}")
    return body


def _send_option_to_telegram(
    job: dict[str, Any],
    option: dict[str, Any],
    result: dict[str, Any],
    request: Request | None = None,
    *,
    base_url: str | None = None,
) -> dict[str, Any]:
    if not _telegram_enabled():
        raise HTTPException(status_code=503, detail="telegram_not_configured")

    video_path = Path(str(option.get("manual_upload_file") or "")).resolve()
    if not video_path.exists() or not video_path.is_file():
        raise HTTPException(status_code=404, detail="video_file_not_found")

    caption = _build_telegram_caption(job, option, result)
    data: dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "caption": caption,
        "supports_streaming": True,
    }
    if TELEGRAM_MESSAGE_THREAD_ID:
        data["message_thread_id"] = TELEGRAM_MESSAGE_THREAD_ID
    errors: list[str] = []
    public_url = _build_absolute_asset_url(str(option.get("manual_upload_url") or ""), request=request, base_url=base_url)

    if public_url:
        try:
            body = _telegram_call("sendVideo", data={**data, "video": public_url}, timeout=120)
            result_obj = body.get("result") or {}
            return {
                "ok": True,
                "message_id": result_obj.get("message_id"),
                "chat_id": TELEGRAM_CHAT_ID,
                "method": "sendVideo:url",
            }
        except HTTPException as exc:
            errors.append(str(exc.detail))

    try:
        with video_path.open("rb") as fh:
            body = _telegram_call(
                "sendVideo",
                data=data,
                files={"video": (video_path.name, fh, "video/mp4")},
                timeout=240,
            )
        result_obj = body.get("result") or {}
        return {
            "ok": True,
            "message_id": result_obj.get("message_id"),
            "chat_id": TELEGRAM_CHAT_ID,
            "method": "sendVideo:file",
        }
    except HTTPException as exc:
        errors.append(str(exc.detail))

    with video_path.open("rb") as fh:
        body = _telegram_call(
            "sendDocument",
            data=data,
            files={"document": (video_path.name, fh, "video/mp4")},
            timeout=240,
        )
    result_obj = body.get("result") or {}
    return {
        "ok": True,
        "message_id": result_obj.get("message_id"),
        "chat_id": TELEGRAM_CHAT_ID,
        "method": "sendDocument:file",
        "fallback_errors": errors,
    }


def _send_tiktok_review_to_telegram(
    job: dict[str, Any],
    option: dict[str, Any],
    result: dict[str, Any],
    request: Request | None = None,
    *,
    request_id: str,
    base_url: str | None = None,
) -> dict[str, Any]:
    if not _telegram_enabled():
        raise HTTPException(status_code=503, detail="telegram_not_configured")
    if not _tiktok_enabled():
        raise HTTPException(status_code=503, detail="tiktok_not_connected")

    video_path = Path(str(option.get("manual_upload_file") or "")).resolve()
    if not video_path.exists() or not video_path.is_file():
        raise HTTPException(status_code=404, detail="video_file_not_found")

    caption = _build_telegram_review_caption(job, option, result)
    data: dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "caption": caption,
        "supports_streaming": True,
        "reply_markup": _build_review_reply_markup(request_id),
    }
    if TELEGRAM_MESSAGE_THREAD_ID:
        data["message_thread_id"] = TELEGRAM_MESSAGE_THREAD_ID

    errors: list[str] = []
    public_url = _build_absolute_asset_url(str(option.get("manual_upload_url") or ""), request=request, base_url=base_url)

    if public_url:
        try:
            body = _telegram_call("sendVideo", data={**data, "video": public_url}, timeout=120)
            result_obj = body.get("result") or {}
            return {
                "ok": True,
                "message_id": result_obj.get("message_id"),
                "chat_id": TELEGRAM_CHAT_ID,
                "method": "sendVideo:url",
            }
        except HTTPException as exc:
            errors.append(str(exc.detail))

    try:
        with video_path.open("rb") as fh:
            body = _telegram_call(
                "sendVideo",
                data=data,
                files={"video": (video_path.name, fh, "video/mp4")},
                timeout=240,
            )
        result_obj = body.get("result") or {}
        return {
            "ok": True,
            "message_id": result_obj.get("message_id"),
            "chat_id": TELEGRAM_CHAT_ID,
            "method": "sendVideo:file",
        }
    except HTTPException as exc:
        errors.append(str(exc.detail))

    with video_path.open("rb") as fh:
        body = _telegram_call(
            "sendDocument",
            data=data,
            files={"document": (video_path.name, fh, "video/mp4")},
            timeout=240,
        )
    result_obj = body.get("result") or {}
    return {
        "ok": True,
        "message_id": result_obj.get("message_id"),
        "chat_id": TELEGRAM_CHAT_ID,
        "method": "sendDocument:file",
        "fallback_errors": errors,
    }


def _append_log(job_id: str, message: str) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job["logs"].append(f"{datetime.now().strftime('%H:%M:%S')} {message}")
        if len(job["logs"]) > 1200:
            job["logs"] = job["logs"][-1200:]
        job["updated_at"] = _utc_now_iso()


def _safe_output_slug(output_slug: str) -> str:
    slug = str(output_slug or "").strip().strip("/\\")
    if not slug or slug in {".", ".."}:
        raise HTTPException(status_code=400, detail="invalid_output_slug")
    if any(part in {"..", ""} for part in Path(slug).parts):
        raise HTTPException(status_code=400, detail="invalid_output_slug")
    return slug


def _load_result_from_output_slug(output_slug: str) -> tuple[str, dict[str, Any]]:
    safe_slug = _safe_output_slug(output_slug)
    manifest_path = (OUTPUT_DIR / safe_slug / "options_manifest.json").resolve()
    output_root = OUTPUT_DIR.resolve()
    try:
        manifest_path.relative_to(output_root)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="invalid_output_slug") from exc
    if not manifest_path.exists() or not manifest_path.is_file():
        raise HTTPException(status_code=404, detail="output_manifest_not_found")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"output_manifest_invalid: {exc}") from exc

    options = []
    for raw_option in payload.get("options") or []:
        option = dict(raw_option)
        preview_file = str(option.get("preview_file") or "").strip()
        poster_file = str(option.get("poster_file") or "").strip()
        manual_upload_file = str(option.get("manual_upload_file") or "").strip()
        if not manual_upload_file and preview_file:
            manual_upload_file = str((OUTPUT_DIR / safe_slug / preview_file).resolve())
        option["manual_upload_file"] = manual_upload_file
        if preview_file:
            option["manual_upload_url"] = f"/output/{safe_slug}/{preview_file}"
        if poster_file:
            option["poster_url"] = f"/output/{safe_slug}/{poster_file}"
        options.append(option)

    result = {
        "source_title": payload.get("source_title"),
        "source_url": payload.get("source_url"),
        "source_duration": payload.get("source_duration"),
        "source_channel": payload.get("source_channel") or payload.get("channel") or "",
        "output_dir": str((OUTPUT_DIR / safe_slug).resolve()),
        "dashboard_url": f"/output/{safe_slug}/dashboard.html",
        "manifest_url": f"/output/{safe_slug}/options_manifest.json",
        "options": options,
    }
    return safe_slug, result


def _set_job_state(job_id: str, **updates: Any) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job.update(updates)
        job["updated_at"] = _utc_now_iso()


def _set_publish_state(request_id: str, **updates: Any) -> None:
    with _publish_lock:
        req = _publish_requests.get(request_id)
        if not req:
            return
        req.update(updates)
        req["updated_at"] = _utc_now_iso()
        _persist_publish_requests_locked()


def _append_publish_log(request_id: str, message: str) -> None:
    with _publish_lock:
        req = _publish_requests.get(request_id)
        if not req:
            return
        req.setdefault("logs", []).append(f"{datetime.now().strftime('%H:%M:%S')} {message}")
        req["logs"] = req["logs"][-200:]
        req["updated_at"] = _utc_now_iso()
        _persist_publish_requests_locked()


def _serialize_publish_request(req: dict[str, Any]) -> dict[str, Any]:
    return {
        "request_id": req["request_id"],
        "job_id": req["job_id"],
        "option_id": req["option_id"],
        "status": req["status"],
        "created_at": req["created_at"],
        "updated_at": req["updated_at"],
        "telegram_message_id": req.get("telegram_message_id"),
        "privacy_level": req.get("privacy_level"),
        "method": req.get("method"),
        "tiktok_status": req.get("tiktok_status"),
        "creator_username": req.get("creator_username"),
        "publish_id": req.get("publish_id"),
        "error": req.get("error"),
        "logs": req.get("logs", []),
        "option": req.get("option"),
    }


def _append_daily_batch_log(batch_id: str, message: str) -> None:
    with _daily_batches_lock:
        batch = _daily_review_batches.get(batch_id)
        if not batch:
            return
        batch.setdefault("logs", []).append(f"{datetime.now().strftime('%H:%M:%S')} {message}")
        batch["logs"] = batch["logs"][-400:]
        batch["updated_at"] = _utc_now_iso()


def _set_daily_batch_state(batch_id: str, **updates: Any) -> None:
    with _daily_batches_lock:
        batch = _daily_review_batches.get(batch_id)
        if not batch:
            return
        batch.update(updates)
        batch["updated_at"] = _utc_now_iso()


def _update_daily_batch_item(batch_id: str, item_key: str, **updates: Any) -> None:
    with _daily_batches_lock:
        batch = _daily_review_batches.get(batch_id)
        if not batch:
            return
        for item in batch.get("items", []):
            if item.get("item_key") == item_key:
                item.update(updates)
                break
        batch["updated_at"] = _utc_now_iso()


def _serialize_daily_batch(batch: dict[str, Any]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    with _jobs_lock:
        jobs_snapshot = {
            job_id: {
                "status": job.get("status"),
                "logs": list(job.get("logs", []))[-4:],
                "error": job.get("error"),
            }
            for job_id, job in _jobs.items()
        }
    with _publish_lock:
        publish_snapshot = {
            request_id: {
                "status": req.get("status"),
                "error": req.get("error"),
                "telegram_message_id": req.get("telegram_message_id"),
                "tiktok_status": req.get("tiktok_status"),
            }
            for request_id, req in _publish_requests.items()
        }
    for item in batch.get("items", []):
        enriched = dict(item)
        job_id = str(enriched.get("job_id") or "").strip()
        if job_id and job_id in jobs_snapshot:
            job = jobs_snapshot[job_id]
            enriched["job_status"] = job.get("status")
            enriched["job_logs_tail"] = job.get("logs", [])
            enriched["job_error"] = job.get("error")
        request_id = str(enriched.get("request_id") or "").strip()
        if request_id and request_id in publish_snapshot:
            publish_req = publish_snapshot[request_id]
            enriched["review_status"] = publish_req.get("status")
            enriched["review_error"] = publish_req.get("error")
            enriched["tiktok_status"] = publish_req.get("tiktok_status")
            if not enriched.get("telegram_message_id"):
                enriched["telegram_message_id"] = publish_req.get("telegram_message_id")
        items.append(enriched)
    return {
        "batch_id": batch["batch_id"],
        "status": batch["status"],
        "created_at": batch["created_at"],
        "updated_at": batch["updated_at"],
        "source": batch.get("source"),
        "scheduled_slot": batch.get("scheduled_slot"),
        "scheduled_prepare_time": batch.get("scheduled_prepare_time"),
        "scheduled_publish_time": batch.get("scheduled_publish_time"),
        "mode": batch.get("mode"),
        "posts_per_day": batch.get("posts_per_day"),
        "reserve_count": batch.get("reserve_count"),
        "options_per_slot": batch.get("options_per_slot"),
        "notes": batch.get("notes"),
        "plan": batch.get("plan"),
        "items": items,
        "logs": batch.get("logs", []),
        "error": batch.get("error"),
    }


def _clear_publish_reply_markup_for_request(req: dict[str, Any]) -> None:
    message_id = req.get("telegram_message_id")
    if not message_id:
        return
    data: dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "message_id": message_id,
        "reply_markup": json.dumps({"inline_keyboard": []}),
    }
    try:
        _telegram_call("editMessageReplyMarkup", data=data, timeout=60)
    except HTTPException:
        pass


def _enqueue_daily_review_batch(
    req: DailyReviewBatchRequest,
    *,
    source: str = "manual",
    scheduled_slot: str | None = None,
    scheduled_prepare_time: str | None = None,
    scheduled_publish_time: str | None = None,
) -> dict[str, Any]:
    batch_id = uuid.uuid4().hex[:24]
    payload = {
        "batch_id": batch_id,
        "status": "queued",
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "source": source,
        "scheduled_slot": scheduled_slot,
        "scheduled_prepare_time": scheduled_prepare_time,
        "scheduled_publish_time": scheduled_publish_time,
        "mode": req.mode,
        "posts_per_day": req.posts_per_day,
        "reserve_count": req.reserve_count,
        "options_per_slot": req.options_per_slot,
        "notes": None,
        "plan": None,
        "items": [],
        "logs": ["Preparando batch diario de revisiones..."],
        "error": None,
    }
    with _daily_batches_lock:
        _daily_review_batches[batch_id] = payload
    threading.Thread(target=_run_daily_review_batch, args=(batch_id, req), daemon=True).start()
    return payload


def _scheduler_mark_triggered(date_key: str, slot_label: str, batch_id: str, publish_at: str, prepare_at: str) -> None:
    with _scheduler_lock:
        if _scheduler_state.get("date") != date_key:
            _scheduler_state["date"] = date_key
            _scheduler_state["triggered"] = {}
        triggered = _scheduler_state.setdefault("triggered", {})
        triggered[slot_label] = {
            "batch_id": batch_id,
            "publish_at": publish_at,
            "prepare_at": prepare_at,
            "triggered_at": _local_now().isoformat(),
        }
        _scheduler_state["last_batch_id"] = batch_id
        _scheduler_state["last_error"] = None
    _save_scheduler_state()


def _scheduler_set_last_error(message: str | None) -> None:
    with _scheduler_lock:
        _scheduler_state["last_error"] = message
        _scheduler_state["last_check"] = _local_now().isoformat()
    _save_scheduler_state()


def _scheduler_set_next_trigger(next_trigger_at: str | None) -> None:
    with _scheduler_lock:
        _scheduler_state["next_trigger_at"] = next_trigger_at
        _scheduler_state["last_check"] = _local_now().isoformat()
    _save_scheduler_state()


def _start_scheduled_daily_review_batch(slot_label: str, prepare_at: datetime, publish_at: datetime) -> str:
    req = DailyReviewBatchRequest(
        mode=SCHEDULER_MODE,
        per_channel_scan=SCHEDULER_PER_CHANNEL_SCAN,
        max_results=SCHEDULER_MAX_RESULTS,
        this_week_only=True,
        min_source_duration=SCHEDULER_MIN_SOURCE_DURATION,
        posts_per_day=1,
        reserve_count=SCHEDULER_RESERVE_COUNT,
        options_per_slot=SCHEDULER_OPTIONS_PER_SLOT,
        duration=SCHEDULER_DURATION,
        stride=SCHEDULER_STRIDE,
        overlap_ratio=SCHEDULER_OVERLAP_RATIO,
        language=SCHEDULER_LANGUAGE,
    )
    payload = _enqueue_daily_review_batch(
        req,
        source="scheduler",
        scheduled_slot=slot_label,
        scheduled_prepare_time=prepare_at.isoformat(),
        scheduled_publish_time=publish_at.isoformat(),
    )
    return str(payload["batch_id"])


def _resolve_scheduler_slot(slot_label: str | None = None) -> tuple[str, datetime, datetime]:
    now = _local_now()
    normalized = (slot_label or "").strip()
    if normalized and normalized not in SCHEDULER_SLOT_TIMES:
        raise HTTPException(status_code=400, detail="scheduler_slot_not_supported")

    if not normalized:
        future_candidates: list[tuple[datetime, str]] = []
        for candidate in SCHEDULER_SLOT_TIMES:
            hour_str, minute_str = candidate.split(":", 1)
            publish_at = now.replace(hour=int(hour_str), minute=int(minute_str), second=0, microsecond=0)
            if publish_at >= now:
                future_candidates.append((publish_at, candidate))
        if future_candidates:
            publish_at, normalized = min(future_candidates, key=lambda row: row[0])
        else:
            normalized = SCHEDULER_SLOT_TIMES[0]
            hour_str, minute_str = normalized.split(":", 1)
            publish_at = (now + timedelta(days=1)).replace(
                hour=int(hour_str), minute=int(minute_str), second=0, microsecond=0
            )
    else:
        hour_str, minute_str = normalized.split(":", 1)
        publish_at = now.replace(hour=int(hour_str), minute=int(minute_str), second=0, microsecond=0)
        if publish_at < now:
            publish_at = publish_at + timedelta(days=1)

    prepare_at = publish_at - timedelta(minutes=_prep_minutes_for_slot(normalized))
    return normalized, prepare_at, publish_at


def _daily_review_scheduler_loop() -> None:
    while True:
        try:
            if not (SCHEDULER_ENABLED and _telegram_enabled()):
                _scheduler_set_next_trigger(None)
                threading.Event().wait(30)
                continue

            now = _local_now()
            date_key = now.date().isoformat()
            with _scheduler_lock:
                if _scheduler_state.get("date") != date_key:
                    _scheduler_state["date"] = date_key
                    _scheduler_state["triggered"] = {}
                triggered_today = dict(_scheduler_state.get("triggered") or {})

            next_trigger_at: str | None = None
            for slot_label in SCHEDULER_SLOT_TIMES:
                if slot_label in triggered_today:
                    continue
                hour_str, minute_str = slot_label.split(":", 1)
                publish_at = now.replace(hour=int(hour_str), minute=int(minute_str), second=0, microsecond=0)
                prepare_at = publish_at - timedelta(minutes=_prep_minutes_for_slot(slot_label))
                if next_trigger_at is None and now <= prepare_at:
                    next_trigger_at = prepare_at.isoformat()
                latest_useful = publish_at - timedelta(minutes=1)
                if prepare_at <= now <= latest_useful:
                    batch_id = _start_scheduled_daily_review_batch(slot_label, prepare_at, publish_at)
                    _scheduler_mark_triggered(date_key, slot_label, batch_id, publish_at.isoformat(), prepare_at.isoformat())
                    triggered_today[slot_label] = {"batch_id": batch_id}
                    next_trigger_at = None

            if next_trigger_at is None:
                future_prepares = []
                for slot_label in SCHEDULER_SLOT_TIMES:
                    if slot_label in triggered_today:
                        continue
                    hour_str, minute_str = slot_label.split(":", 1)
                    publish_at = now.replace(hour=int(hour_str), minute=int(minute_str), second=0, microsecond=0)
                    prepare_at = publish_at - timedelta(minutes=_prep_minutes_for_slot(slot_label))
                    if now <= prepare_at:
                        future_prepares.append(prepare_at)
                if future_prepares:
                    next_trigger_at = min(future_prepares).isoformat()
            _scheduler_set_next_trigger(next_trigger_at)
        except Exception as exc:
            _scheduler_set_last_error(str(exc))
        threading.Event().wait(20)


def _ensure_daily_review_scheduler_started() -> None:
    global _scheduler_thread_started
    if _scheduler_thread_started or not SCHEDULER_ENABLED:
        return
    _scheduler_thread_started = True
    threading.Thread(target=_daily_review_scheduler_loop, daemon=True).start()


def _run_daily_review_batch(batch_id: str, req: DailyReviewBatchRequest) -> None:
    _set_daily_batch_state(batch_id, status="running")
    _append_daily_batch_log(batch_id, "Construyendo plan diario...")
    try:
        plan = None
        plan_errors: list[str] = []
        plan_attempts = [
            {
                "label": "normal",
                "this_week_only": req.this_week_only,
                "per_channel_scan": req.per_channel_scan,
                "max_results": req.max_results,
            },
            {
                "label": "retry_relajado",
                "this_week_only": False,
                "per_channel_scan": max(req.per_channel_scan, 20),
                "max_results": max(req.max_results * 2, 40),
            },
        ]
        for attempt_idx, attempt in enumerate(plan_attempts, start=1):
            try:
                if attempt_idx > 1:
                    _append_daily_batch_log(
                        batch_id,
                        "Reintentando plan diario con busqueda mas amplia para evitar que una tanda se quede vacia.",
                    )
                plan = build_daily_post_plan(
                    mode=req.mode,
                    channels=req.channels,
                    per_channel_scan=int(attempt["per_channel_scan"]),
                    this_week_only=bool(attempt["this_week_only"]),
                    min_source_duration=req.min_source_duration,
                    max_results=int(attempt["max_results"]),
                    posts_per_day=req.posts_per_day,
                    reserve_count=req.reserve_count,
                    slot_option_count=req.options_per_slot,
                    log_fn=lambda msg: _append_daily_batch_log(batch_id, msg),
                )
                break
            except Exception as attempt_exc:
                err_text = str(attempt_exc)
                plan_errors.append(err_text)
                _append_daily_batch_log(
                    batch_id,
                    f"Fallo construyendo plan ({attempt['label']}): {err_text}",
                )
        if plan is None:
            raise RuntimeError(" | ".join(plan_errors) or "plan_build_failed")
        serialized_plan = {
            "date": plan.get("date"),
            "timezone": plan.get("timezone"),
            "mode": plan.get("mode"),
            "posts_per_day": plan.get("posts_per_day"),
            "reserve_count": plan.get("reserve_count"),
            "options_per_slot": plan.get("slot_option_count"),
            "notes": plan.get("notes"),
            "slots": [_serialize_plan_entry(item) for item in plan.get("slots", [])],
            "reserves": [_serialize_plan_entry(item) for item in plan.get("reserves", [])],
        }
        items: list[dict[str, Any]] = []
        for entry in serialized_plan["slots"]:
            slot_key = str(entry.get("slot_key") or "")
            slot_candidates = []
            primary = entry.get("candidate")
            if primary:
                slot_candidates.append(primary)
            slot_candidates.extend(entry.get("alternatives") or [])

            for candidate_idx, candidate in enumerate(slot_candidates, start=1):
                if not isinstance(candidate, dict):
                    _append_daily_batch_log(
                        batch_id,
                        f"Saltando candidato invalido en {slot_key or 'slot'} posicion {candidate_idx}.",
                    )
                    continue
                source_title = str(candidate.get("title") or "").strip()
                source_url = str(candidate.get("url") or "").strip()
                if not source_title or not source_url:
                    _append_daily_batch_log(
                        batch_id,
                        f"Saltando candidato incompleto en {slot_key or 'slot'} posicion {candidate_idx}.",
                    )
                    continue
                items.append(
                    {
                        "item_key": f"{slot_key}:{candidate_idx}",
                        "review_group_id": f"{batch_id}:{slot_key}",
                        "option_rank": candidate_idx,
                        "slot_key": slot_key,
                        "slot_label": entry.get("slot_label"),
                        "publish_time": entry.get("publish_time"),
                        "strategy": entry.get("strategy"),
                        "plan_score": entry.get("plan_score"),
                        "source_title": source_title,
                        "source_channel": candidate.get("channel"),
                        "source_url": source_url,
                        "status": "pending",
                        "job_id": None,
                        "option_id": None,
                        "telegram_message_id": None,
                        "preview_url": None,
                        "request_id": None,
                        "error": None,
                    }
                )
        _set_daily_batch_state(
            batch_id,
            plan=serialized_plan,
            items=items,
            notes=serialized_plan.get("notes"),
            mode=serialized_plan.get("mode"),
            posts_per_day=serialized_plan.get("posts_per_day"),
            reserve_count=serialized_plan.get("reserve_count"),
            options_per_slot=serialized_plan.get("options_per_slot"),
        )

        if not items:
            _set_daily_batch_state(batch_id, status="failed", error="Sin slots suficientes para generar revisiones hoy.")
            _append_daily_batch_log(batch_id, "No salieron slots suficientes para preparar revisiones.")
            return

        batch_public_base = _public_base_url()
        failed = 0
        for item in items:
            item_key = str(item.get("item_key") or "")
            slot_key = str(item.get("slot_key") or "")
            source_url = str(item.get("source_url") or "").strip()
            source_title = str(item.get("source_title") or source_url).strip()
            if not source_url:
                failed += 1
                _update_daily_batch_item(batch_id, item_key, status="failed", error="slot_sin_url")
                continue

            job_id = uuid.uuid4().hex
            _update_daily_batch_item(batch_id, item_key, status="rendering", job_id=job_id)
            _append_daily_batch_log(
                batch_id,
                f"Generando opcion {item.get('option_rank')}/"
                f"{req.options_per_slot} para {item.get('slot_label')}: {source_title}",
            )
            with _jobs_lock:
                _jobs[job_id] = {
                    "job_id": job_id,
                    "status": "pending",
                    "created_at": _utc_now_iso(),
                    "updated_at": _utc_now_iso(),
                    "request": {
                        "url": source_url,
                        "duration": req.duration,
                        "options": 1,
                        "stride": req.stride,
                        "overlap_ratio": req.overlap_ratio,
                        "language": req.language,
                        "fast_render": True,
                    },
                    "result": None,
                    "error": None,
                    "logs": [],
                }
            create_req = CreateJobRequest(
                url=source_url,
                duration=req.duration,
                options=1,
                stride=req.stride,
                overlap_ratio=req.overlap_ratio,
                language=req.language,
                fast_render=True,
            )
            _run_job(job_id, create_req)
            with _jobs_lock:
                job = dict(_jobs.get(job_id) or {})

            if job.get("status") != "completed" or not job.get("result"):
                failed += 1
                err = str(job.get("error") or "job_failed")
                _update_daily_batch_item(batch_id, item_key, status="failed", error=err)
                _append_daily_batch_log(batch_id, f"Fallo generando {source_title}: {err}")
                continue

            result = job["result"]
            options = result.get("options") or []
            option = options[0] if options else None
            if not option:
                failed += 1
                _update_daily_batch_item(batch_id, item_key, status="failed", error="sin_opciones")
                _append_daily_batch_log(batch_id, f"{source_title}: el render no produjo opciones.")
                continue

            try:
                if _tiktok_enabled():
                    request_id = uuid.uuid4().hex[:24]
                    sent = _send_tiktok_review_to_telegram(
                        job,
                        option,
                        result,
                        request_id=request_id,
                        base_url=batch_public_base,
                    )
                    publish_payload = {
                        "request_id": request_id,
                        "job_id": job_id,
                        "option_id": int(option.get("option_id") or 0),
                        "status": "pending_review",
                        "created_at": _utc_now_iso(),
                        "updated_at": _utc_now_iso(),
                        "telegram_message_id": sent.get("message_id"),
                        "privacy_level": None,
                        "method": sent.get("method"),
                        "option": option,
                        "logs": [f"Solicitud enviada a Telegram para {item.get('slot_label')}."],
                        "error": None,
                        "publish_id": None,
                        "tiktok_status": None,
                        "creator_username": None,
                        "review_group_id": item.get("review_group_id"),
                        "review_group_label": f"{item.get('slot_label')} {item.get('publish_time')}".strip(),
                    }
                    with _publish_lock:
                        _publish_requests[request_id] = publish_payload
                        _persist_publish_requests_locked()
                    item_status = "review_sent"
                else:
                    request_id = None
                    sent = _send_option_to_telegram(job, option, result, base_url=batch_public_base)
                    item_status = "sent"

                _update_daily_batch_item(
                    batch_id,
                    item_key,
                    status=item_status,
                    option_id=option.get("option_id"),
                    telegram_message_id=sent.get("message_id"),
                    preview_url=option.get("preview_url"),
                    request_id=request_id,
                    error=None,
                )
                record_used_video(
                    source_url=source_url,
                    source_title=source_title,
                    source_channel=str(item.get("source_channel") or ""),
                    video_id=str((option or {}).get("video_id") or ""),
                    context=f"daily_batch:{batch_id}:{slot_key}",
                )
                _append_daily_batch_log(
                    batch_id,
                    f"Revision enviada a Telegram para {source_title} "
                    f"(mensaje {sent.get('message_id')}).",
                )
            except Exception as exc:
                failed += 1
                _update_daily_batch_item(batch_id, item_key, status="failed", error=str(exc))
                _append_daily_batch_log(batch_id, f"Fallo enviando {source_title} a Telegram: {exc}")

        final_status = "completed" if failed == 0 else ("partial_failed" if failed < len(items) else "failed")
        _set_daily_batch_state(batch_id, status=final_status, error=None if failed < len(items) else "all_items_failed")
        _append_daily_batch_log(batch_id, f"Proceso terminado con estado {final_status}.")
    except Exception as exc:
        _set_daily_batch_state(batch_id, status="failed", error=str(exc))
        _append_daily_batch_log(batch_id, f"Error preparando revisiones: {exc}")


def _refresh_tiktok_tokens_if_needed() -> tuple[TikTokDirectPostClient, dict[str, Any], Any]:
    token_path = Path(TIKTOK_TOKENS_FILE).expanduser().resolve()
    if not token_path.exists():
        raise RuntimeError(f"TikTok token file no encontrado: {token_path}")

    tokens = load_tokens(token_path)
    client = TikTokDirectPostClient(tokens.access_token)
    try:
        creator_info = client.query_creator_info()
        return client, creator_info, tokens
    except TikTokApiError as exc:
        err_blob = json.dumps(exc.payload or {}, ensure_ascii=False).lower()
        needs_refresh = "access_token" in err_blob or "invalid" in err_blob or "expired" in err_blob
        if not needs_refresh:
            raise

    oauth = TikTokDesktopOAuth(
        client_key=TIKTOK_CLIENT_KEY,
        client_secret=TIKTOK_CLIENT_SECRET,
        redirect_uri=TIKTOK_REDIRECT_URI,
    )
    refreshed = oauth.refresh_access_token(tokens.refresh_token)
    save_tokens(refreshed, token_path)
    client = TikTokDirectPostClient(refreshed.access_token)
    creator_info = client.query_creator_info()
    return client, creator_info, refreshed


def _pick_privacy_level(creator_info: dict[str, Any], requested: str | None = None) -> str:
    options = (creator_info.get("data") or {}).get("privacy_level_options") or []
    requested_clean = (requested or "").strip()
    if requested_clean and requested_clean in options:
        return requested_clean
    if TIKTOK_DEFAULT_PRIVACY in options:
        return TIKTOK_DEFAULT_PRIVACY
    if "SELF_ONLY" in options:
        return "SELF_ONLY"
    if options:
        return str(options[0])
    return requested_clean or TIKTOK_DEFAULT_PRIVACY


def _notify_publish_result(request_id: str, text: str) -> None:
    with _publish_lock:
        req = _publish_requests.get(request_id)
        if not req:
            return
        message_id = req.get("telegram_message_id")
    if not message_id:
        return

    base_data: dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "message_id": message_id,
        "reply_markup": json.dumps({"inline_keyboard": []}),
    }
    try:
        _telegram_call("editMessageReplyMarkup", data=base_data, timeout=60)
    except HTTPException:
        pass

    msg_data: dict[str, Any] = {"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]}
    if TELEGRAM_MESSAGE_THREAD_ID:
        msg_data["message_thread_id"] = TELEGRAM_MESSAGE_THREAD_ID
    try:
        _telegram_call("sendMessage", data=msg_data, timeout=60)
    except HTTPException:
        pass


def _run_tiktok_publish(request_id: str) -> None:
    with _publish_lock:
        req = _publish_requests.get(request_id)
        if not req:
            return
        option = dict(req["option"])
    _set_publish_state(request_id, status="uploading")
    _append_publish_log(request_id, "Aprobado en Telegram. Iniciando subida a TikTok...")
    _append_log(req["job_id"], f"Opcion {req['option_id']} aprobada en Telegram para TikTok.")
    try:
        client, creator_info, _tokens = _refresh_tiktok_tokens_if_needed()
        creator_username = str((creator_info.get("data") or {}).get("creator_username") or "").strip()
        if TIKTOK_EXPECTED_USERNAME:
            normalized_creator = creator_username.lstrip("@").lower()
            if normalized_creator != TIKTOK_EXPECTED_USERNAME:
                raise RuntimeError(
                    f"La cuenta TikTok conectada es @{creator_username or 'desconocida'} y no @{TIKTOK_EXPECTED_USERNAME}."
                )

        video_path = Path(str(option.get("manual_upload_file") or "")).resolve()
        if not video_path.exists():
            raise FileNotFoundError(f"No existe el video a publicar: {video_path}")

        title = _compose_tiktok_post_text(option)
        max_duration = int((creator_info.get("data") or {}).get("max_video_post_duration_sec") or 0)
        option_duration = float(option.get("duration") or 0)
        if max_duration > 0 and option_duration > max_duration:
            raise RuntimeError(
                f"El clip dura {option_duration:.1f}s y la cuenta solo permite hasta {max_duration}s."
            )
        privacy_level = _pick_privacy_level(creator_info, req.get("privacy_level"))
        disable_comment = bool((creator_info.get("data") or {}).get("comment_disabled"))
        disable_duet = bool((creator_info.get("data") or {}).get("duet_disabled"))
        disable_stitch = bool((creator_info.get("data") or {}).get("stitch_disabled"))
        _set_publish_state(request_id, creator_username=creator_username, privacy_level=privacy_level)
        _append_publish_log(request_id, f"Subiendo a TikTok como @{creator_username or 'desconocido'} en modo {privacy_level}.")

        try:
            status_payload = client.direct_post_file(
                video_path=video_path,
                title=title,
                privacy_level=privacy_level,
                disable_duet=disable_duet,
                disable_comment=disable_comment,
                disable_stitch=disable_stitch,
                video_cover_timestamp_ms=1000,
            )
        except Exception as exc:
            if not _should_try_browser_fallback(exc):
                raise
            _append_publish_log(
                request_id,
                "TikTok API oficial rechazo el post por guidelines. Probando fallback por navegador...",
            )
            _append_log(
                req["job_id"],
                f"Opcion {req['option_id']} cambiando a fallback navegador por bloqueo API: {exc}",
            )
            browser_result = _run_tiktok_browser_fallback(
                video_path=video_path,
                caption=title,
                privacy_level=privacy_level,
            )
            browser_status = str((browser_result.get("result") or {}).get("status") or "browser_fallback")
            browser_status_upper = f"BROWSER_{browser_status.upper()}"
            confirmed_browser_statuses = {"publish_navigation_detected", "publish_confirmation_detected"}
            if browser_status in confirmed_browser_statuses:
                publish_state = "completed"
            elif browser_status == "manual_review":
                publish_state = "pending_manual_review"
            else:
                publish_state = "pending_browser_confirmation"
            _set_publish_state(
                request_id,
                status=publish_state,
                tiktok_status=browser_status_upper,
                publish_id=None,
                creator_username=creator_username,
            )
            _append_publish_log(request_id, f"Fallback navegador terminado con estado: {browser_status}.")
            if browser_result.get("stdout"):
                _append_publish_log(request_id, f"Uploader browser: {browser_result['stdout'][-400:]}")
            if publish_state == "completed":
                _notify_publish_result(
                    request_id,
                    f"OK TikTok web: opcion {req['option_id']} publicada en @{creator_username or 'cuenta conectada'}.",
                )
            elif publish_state == "pending_manual_review":
                _notify_publish_result(
                    request_id,
                    f"Revision TikTok necesaria en opcion {req['option_id']}: se dejo lista en TikTok Studio para @{creator_username or 'cuenta conectada'}, pero no se publico automaticamente.",
                )
            else:
                _notify_publish_result(
                    request_id,
                    f"Publicacion sin confirmar en opcion {req['option_id']}: se pulso publicar en TikTok Studio para @{creator_username or 'cuenta conectada'}, pero TikTok no confirmo aun que este visible.",
                )
            return
        status_data = status_payload.get("data") or {}
        final_status = str(status_data.get("status") or "UNKNOWN")
        publish_id = str(status_data.get("publish_id") or "")
        _set_publish_state(
            request_id,
            status="completed" if final_status == "PUBLISH_COMPLETE" else "failed",
            tiktok_status=final_status,
            publish_id=publish_id,
            creator_username=creator_username,
        )
        _append_publish_log(request_id, f"TikTok devolvio estado final: {final_status}.")
        _append_log(req["job_id"], f"Opcion {req['option_id']} TikTok status: {final_status}.")
        if final_status == "PUBLISH_COMPLETE":
            _notify_publish_result(
                request_id,
                f"OK TikTok: opcion {req['option_id']} subida en @{creator_username or 'cuenta conectada'} ({privacy_level}).",
            )
        else:
            _notify_publish_result(
                request_id,
                f"Fallo TikTok en opcion {req['option_id']}: estado {final_status}.",
            )
    except Exception as exc:
        _set_publish_state(request_id, status="failed", error=str(exc))
        _append_publish_log(request_id, f"Error TikTok: {exc}")
        _append_log(req["job_id"], f"Opcion {req['option_id']} fallo al subir a TikTok: {exc}")
        _notify_publish_result(request_id, f"Fallo TikTok en opcion {req['option_id']}: {exc}")


def _process_telegram_callback(callback_query: dict[str, Any]) -> None:
    data = str(callback_query.get("data") or "")
    callback_id = str(callback_query.get("id") or "")
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "ttok":
        return
    request_id = parts[1]
    action = parts[2]
    req = _get_publish_request(request_id, reload_if_missing=True)
    if not req:
        _telegram_call("answerCallbackQuery", data={"callback_query_id": callback_id, "text": "Solicitud no encontrada."}, timeout=30)
        return

    if action == "no":
        _set_publish_state(request_id, status="cancelled")
        _append_publish_log(request_id, "Solicitud cancelada desde Telegram.")
        _append_log(req["job_id"], f"Solicitud TikTok cancelada para opcion {req['option_id']}.")
        _telegram_call("answerCallbackQuery", data={"callback_query_id": callback_id, "text": "Cancelado."}, timeout=30)
        _notify_publish_result(request_id, f"Cancelado: no se subira la opcion {req['option_id']} a TikTok.")
        return

    with _publish_lock:
        current_status = (_publish_requests.get(request_id) or {}).get("status")
    if current_status != "pending_review":
        _telegram_call("answerCallbackQuery", data={"callback_query_id": callback_id, "text": "Ya procesado."}, timeout=30)
        return

    sibling_reqs: list[dict[str, Any]] = []
    review_group_id = str(req.get("review_group_id") or "").strip()
    if review_group_id:
        with _publish_lock:
            for other_id, other_req in _publish_requests.items():
                if other_id == request_id:
                    continue
                if str(other_req.get("review_group_id") or "").strip() != review_group_id:
                    continue
                if other_req.get("status") != "pending_review":
                    continue
                other_req["status"] = "not_selected"
                other_req["updated_at"] = _utc_now_iso()
                other_req.setdefault("logs", []).append("Descartada al aprobar otra opcion del mismo bloque.")
                sibling_reqs.append(dict(other_req))
            _persist_publish_requests_locked()

    for sibling_req in sibling_reqs:
        _clear_publish_reply_markup_for_request(sibling_req)

    _set_publish_state(request_id, status="approved")
    _append_publish_log(request_id, "OK recibido desde Telegram.")
    _telegram_call("answerCallbackQuery", data={"callback_query_id": callback_id, "text": "Subiendo a TikTok..."}, timeout=30)
    threading.Thread(target=_run_tiktok_publish, args=(request_id,), daemon=True).start()


def _telegram_poll_loop() -> None:
    global _telegram_update_offset
    while True:
        if not (_telegram_enabled() and TELEGRAM_APPROVALS_ENABLED):
            threading.Event().wait(10)
            continue
        try:
            body = _telegram_api_get(
                "getUpdates",
                params={
                    "offset": _telegram_update_offset,
                    "timeout": 30,
                    "allowed_updates": json.dumps(["callback_query"]),
                },
                timeout=35,
            )
            for update in body.get("result") or []:
                _telegram_update_offset = int(update.get("update_id", 0)) + 1
                callback_query = update.get("callback_query")
                if callback_query:
                    _process_telegram_callback(callback_query)
        except Exception:
            threading.Event().wait(4)


def _ensure_telegram_poller_started() -> None:
    global _telegram_poller_started
    if _telegram_poller_started:
        return
    if not (_telegram_enabled() and TELEGRAM_APPROVALS_ENABLED):
        return
    _telegram_poller_started = True
    threading.Thread(target=_telegram_poll_loop, daemon=True).start()


def _run_job(job_id: str, req: CreateJobRequest) -> None:
    _set_job_state(job_id, status="running")
    _append_log(job_id, "Generacion iniciada.")
    try:
        config = DashboardConfig(
            url=req.url,
            language=req.language,
            duration=req.duration,
            options=req.options,
            stride=req.stride,
            overlap_ratio=req.overlap_ratio,
            fast_render=req.fast_render,
            output_dir=str(OUTPUT_DIR),
            work_dir=str(WORK_DIR),
        )
        result = generate_dashboard(config, log_fn=lambda m: _append_log(job_id, m))
        payload = _serialize_result(result)
        work_job_dir = Path(getattr(result, "work_job_dir", "") or "")
        if work_job_dir:
            try:
                shutil.rmtree(work_job_dir, ignore_errors=False)
                _append_log(job_id, f"Temporales borrados: {work_job_dir.name}")
            except FileNotFoundError:
                pass
            except Exception as cleanup_exc:
                _append_log(job_id, f"No se pudieron borrar temporales ({work_job_dir}): {cleanup_exc}")
        _set_job_state(job_id, status="completed", result=payload)
        _append_log(job_id, "Generacion completada.")
    except Exception as exc:
        err_text = f"{type(exc).__name__}: {exc}"
        if ("Sign in to confirm you" in str(exc) or "not a bot" in str(exc)) and not YTDLP_COOKIES_FILE:
            err_text += (
                " | AYUDA: en Railway define YTDLP_COOKIES_FILE=/data/cookies.txt "
                "y sube un cookies.txt valido de YouTube."
            )
        _set_job_state(
            job_id,
            status="failed",
            error=err_text,
            traceback=traceback.format_exc(limit=4),
        )
        _append_log(job_id, f"Error: {exc}")


@app.get("/api/health")
def health() -> dict[str, Any]:
    scheduler = _get_scheduler_status()
    with _cleanup_lock:
        cleanup = dict(_cleanup_state)
    recent_used_count = len(recent_used_video_keys())
    return {
        "ok": True,
        "service": "clip-studio-es-api",
        "time": _utc_now_iso(),
        "output_dir": str(OUTPUT_DIR),
        "work_dir": str(WORK_DIR),
        "public_base_url": BACKEND_PUBLIC_URL,
        "cookies_configured": bool(YTDLP_COOKIES_FILE),
        "cookies_file_exists": _cookie_file_is_usable(YTDLP_COOKIES_FILE),
        "cookies_inline_configured": bool(YTDLP_COOKIES_TEXT.strip()),
        "youtube_api_configured": bool(os.getenv("YOUTUBE_API_KEY", "").strip()),
        "telegram_configured": _telegram_enabled(),
        "telegram_approvals_enabled": TELEGRAM_APPROVALS_ENABLED,
        "tiktok_credentials_configured": _tiktok_credentials_configured(),
        "tiktok_tokens_file_exists": Path(TIKTOK_TOKENS_FILE).expanduser().exists(),
        "tiktok_ready": _tiktok_enabled(),
        "tiktok_browser_fallback_enabled": TIKTOK_BROWSER_FALLBACK_ENABLED,
        "tiktok_browser_connect_cdp": bool(TIKTOK_BROWSER_CONNECT_CDP),
        "scheduler_enabled": scheduler["enabled"],
        "scheduler_slot_times": scheduler["slot_times"],
        "scheduler_prep_minutes": scheduler["prep_minutes"],
        "scheduler_next_trigger_at": scheduler["state"].get("next_trigger_at"),
        "scheduler_last_error": scheduler["state"].get("last_error"),
        "retention_enabled": RETENTION_ENABLED,
        "retention_hours": RETENTION_HOURS,
        "retention_output_hours": RETENTION_OUTPUT_HOURS,
        "retention_work_hours": RETENTION_WORK_HOURS,
        "retention_interval_minutes": RETENTION_INTERVAL_MINUTES,
        "retention_last_run_at": cleanup.get("last_run_at"),
        "retention_last_error": cleanup.get("last_error"),
        "retention_deleted_output_entries": cleanup.get("deleted_output_entries"),
        "retention_deleted_work_entries": cleanup.get("deleted_work_entries"),
        "used_video_cooldown_active_count": recent_used_count,
    }


@app.get("/api/scheduler/status")
def scheduler_status() -> dict[str, Any]:
    return {"ok": True, **_get_scheduler_status()}


@app.post("/api/scheduler/trigger")
def scheduler_trigger(req: SchedulerTriggerRequest | None = None) -> dict[str, Any]:
    if not _telegram_enabled():
        raise HTTPException(status_code=503, detail="telegram_not_configured")
    slot_label, _prepare_at, publish_at = _resolve_scheduler_slot((req.slot_label if req else None))
    now = _local_now()
    payload = _enqueue_daily_review_batch(
        DailyReviewBatchRequest(
            mode=SCHEDULER_MODE,
            per_channel_scan=SCHEDULER_PER_CHANNEL_SCAN,
            max_results=SCHEDULER_MAX_RESULTS,
            this_week_only=True,
            min_source_duration=SCHEDULER_MIN_SOURCE_DURATION,
            posts_per_day=1,
            reserve_count=SCHEDULER_RESERVE_COUNT,
            options_per_slot=SCHEDULER_OPTIONS_PER_SLOT,
            duration=SCHEDULER_DURATION,
            stride=SCHEDULER_STRIDE,
            overlap_ratio=SCHEDULER_OVERLAP_RATIO,
            language=SCHEDULER_LANGUAGE,
        ),
        source="scheduler_manual",
        scheduled_slot=slot_label,
        scheduled_prepare_time=now.isoformat(),
        scheduled_publish_time=publish_at.isoformat(),
    )
    if publish_at.date() == now.date():
        _scheduler_mark_triggered(now.date().isoformat(), slot_label, payload["batch_id"], publish_at.isoformat(), now.isoformat())
    return {"ok": True, **_serialize_daily_batch(payload)}


@app.get("/api/tiktok/connect/start")
def tiktok_connect_start(request: Request) -> dict[str, Any]:
    if not _tiktok_credentials_configured():
        raise HTTPException(status_code=503, detail="tiktok_credentials_not_configured")
    redirect_uri = _build_tiktok_redirect_uri(request)
    oauth = TikTokDesktopOAuth(
        client_key=TIKTOK_CLIENT_KEY,
        client_secret=TIKTOK_CLIENT_SECRET,
        redirect_uri=redirect_uri,
    )
    state = oauth.generate_state()
    auth_url = oauth.build_authorize_url(
        scopes=["video.publish"],
        state=state,
    )
    with _tiktok_oauth_lock:
        _tiktok_oauth_states[state] = {
            "redirect_uri": redirect_uri,
            "created_at": _utc_now_iso(),
        }
    return {"ok": True, "auth_url": auth_url, "redirect_uri": redirect_uri}


@app.get("/api/tiktok/connect/callback/{verification_name}")
def tiktok_connect_callback_verification(verification_name: str) -> FileResponse:
    safe_name = Path(verification_name).name
    target = (TIKTOK_VERIFICATION_DIR / safe_name).resolve()
    try:
        target.relative_to(TIKTOK_VERIFICATION_DIR)
    except Exception as exc:
        raise HTTPException(status_code=404, detail="verification_file_not_found") from exc
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="verification_file_not_found")
    return FileResponse(str(target), media_type="text/plain; charset=utf-8")


@app.get("/api/tiktok/connect/callback")
@app.get("/api/tiktok/connect/callback/")
def tiktok_connect_callback(code: str | None = None, state: str | None = None, error: str | None = None) -> HTMLResponse:
    if error:
        return HTMLResponse(f"<html><body><h2>TikTok autorizacion fallida</h2><p>{error}</p></body></html>", status_code=400)
    if not code or not state:
        return HTMLResponse("<html><body><h2>Falta code/state</h2></body></html>", status_code=400)
    with _tiktok_oauth_lock:
        flow = _tiktok_oauth_states.pop(state, None)
    if not flow:
        return HTMLResponse("<html><body><h2>Sesion OAuth invalida o expirada</h2></body></html>", status_code=400)

    oauth = TikTokDesktopOAuth(
        client_key=TIKTOK_CLIENT_KEY,
        client_secret=TIKTOK_CLIENT_SECRET,
        redirect_uri=str(flow["redirect_uri"]),
    )
    try:
        code_verifier = str(flow.get("code_verifier") or "").strip() or None
        tokens = oauth.exchange_code_for_tokens(code=code, code_verifier=code_verifier)
        save_tokens(tokens, Path(TIKTOK_TOKENS_FILE).expanduser().resolve())
        client = TikTokDirectPostClient(tokens.access_token)
        creator_info = client.query_creator_info()
        creator_username = str((creator_info.get("data") or {}).get("creator_username") or "").strip()
        expected_note = ""
        if TIKTOK_EXPECTED_USERNAME:
            normalized = creator_username.lstrip("@").lower()
            expected_note = (
                "<p>Cuenta esperada confirmada.</p>"
                if normalized == TIKTOK_EXPECTED_USERNAME
                else f"<p>Atencion: conectaste @{creator_username}, esperabamos @{TIKTOK_EXPECTED_USERNAME}.</p>"
            )
        return HTMLResponse(
            f"<html><body><h2>TikTok conectado</h2><p>Cuenta: @{creator_username or 'desconocida'}</p>{expected_note}<p>Ya puedes volver a Clip Studio.</p></body></html>"
        )
    except Exception as exc:
        return HTMLResponse(f"<html><body><h2>Error conectando TikTok</h2><p>{exc}</p></body></html>", status_code=500)


@app.get("/api/tiktok/account")
def tiktok_account() -> dict[str, Any]:
    if not _tiktok_enabled():
        raise HTTPException(status_code=503, detail="tiktok_not_connected")
    client, creator_info, _tokens = _refresh_tiktok_tokens_if_needed()
    data = creator_info.get("data") or {}
    return {
        "ok": True,
        "creator_username": data.get("creator_username"),
        "creator_nickname": data.get("creator_nickname"),
        "privacy_level_options": data.get("privacy_level_options") or [],
        "comment_disabled": bool(data.get("comment_disabled")),
        "duet_disabled": bool(data.get("duet_disabled")),
        "stitch_disabled": bool(data.get("stitch_disabled")),
        "max_video_post_duration_sec": data.get("max_video_post_duration_sec"),
    }


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "clip-studio-es-api",
        "health": "/api/health",
        "discover": "/api/discover",
        "jobs": "/api/jobs",
        "output": "/output",
    }


@app.get("/studio")
def studio() -> FileResponse:
    if not FRONTEND_INDEX.exists():
        raise HTTPException(status_code=404, detail="studio_not_found")
    return FileResponse(str(FRONTEND_INDEX), media_type="text/html")


@app.post("/api/discover")
def discover(req: DiscoverRequest) -> dict[str, Any]:
    try:
        candidates = discover_creator_videos(
            mode=req.mode,
            channels=req.channels,
            per_channel_scan=req.per_channel_scan,
            this_week_only=req.this_week_only,
            min_source_duration=req.min_source_duration,
            max_results=req.max_results,
        )
        return {"items": [_serialize_candidate(c) for c in candidates]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"discover_failed: {exc}") from exc


@app.post("/api/plan/daily")
def daily_plan(req: DailyPlanRequest) -> dict[str, Any]:
    try:
        plan = build_daily_post_plan(
            mode=req.mode,
            channels=req.channels,
            per_channel_scan=req.per_channel_scan,
            this_week_only=req.this_week_only,
            min_source_duration=req.min_source_duration,
            max_results=req.max_results,
            posts_per_day=req.posts_per_day,
            reserve_count=req.reserve_count,
            slot_option_count=req.slot_option_count,
        )
        return {
            "date": plan.get("date"),
            "timezone": plan.get("timezone"),
            "mode": plan.get("mode"),
            "posts_per_day": plan.get("posts_per_day"),
            "reserve_count": plan.get("reserve_count"),
            "slot_option_count": plan.get("slot_option_count"),
            "notes": plan.get("notes"),
            "slots": [_serialize_plan_entry(item) for item in plan.get("slots", [])],
            "reserves": [_serialize_plan_entry(item) for item in plan.get("reserves", [])],
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"daily_plan_failed: {exc}") from exc


@app.post("/api/plan/daily/reviews")
def create_daily_review_batch(req: DailyReviewBatchRequest) -> dict[str, Any]:
    if not _telegram_enabled():
        raise HTTPException(status_code=503, detail="telegram_not_configured")
    payload = _enqueue_daily_review_batch(req, source="manual")
    return {"ok": True, **_serialize_daily_batch(payload)}


@app.get("/api/plan/daily/reviews/{batch_id}")
def get_daily_review_batch(batch_id: str) -> dict[str, Any]:
    with _daily_batches_lock:
        batch = _daily_review_batches.get(batch_id)
        if not batch:
            raise HTTPException(status_code=404, detail="daily_review_batch_not_found")
        return _serialize_daily_batch(batch)


@app.post("/api/jobs")
def create_job(req: CreateJobRequest) -> dict[str, Any]:
    if "youtube.com" not in req.url and "youtu.be" not in req.url:
        raise HTTPException(status_code=400, detail="url must be a valid YouTube URL")

    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
            "request": req.model_dump(),
            "logs": [],
            "result": None,
            "error": None,
            "traceback": None,
        }
    worker = threading.Thread(target=_run_job, args=(job_id, req), daemon=True)
    worker.start()
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, since_log: int = Query(default=0, ge=0)) -> dict[str, Any]:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job_not_found")
        logs = job.get("logs", [])
        new_logs = logs[since_log:]
        return {
            "job_id": job["job_id"],
            "status": job["status"],
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "logs": new_logs,
            "next_log_cursor": len(logs),
            "result": job["result"],
            "error": job["error"],
        }


@app.post("/api/share/telegram")
def share_option_to_telegram(req: ShareTelegramRequest, request: Request) -> dict[str, Any]:
    with _jobs_lock:
        job = _jobs.get(req.job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job_not_found")
        result = job.get("result")
        if not result:
            raise HTTPException(status_code=409, detail="job_not_completed")
        options = result.get("options") or []
        option = next((opt for opt in options if int(opt.get("option_id") or 0) == req.option_id), None)
        if not option:
            raise HTTPException(status_code=404, detail="option_not_found")

    sent = _send_option_to_telegram(job, option, result, request)
    _append_log(req.job_id, f"Opcion {req.option_id} enviada a Telegram ({sent.get('method', 'unknown')}).")
    return {
        "ok": True,
        "job_id": req.job_id,
        "option_id": req.option_id,
        **sent,
    }


@app.post("/api/publish/telegram-review")
def prepare_tiktok_review(req: PrepareTikTokReviewRequest, request: Request) -> dict[str, Any]:
    with _jobs_lock:
        job = _jobs.get(req.job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job_not_found")
        result = job.get("result")
        if not result:
            raise HTTPException(status_code=409, detail="job_not_completed")
        options = result.get("options") or []
        option = next((opt for opt in options if int(opt.get("option_id") or 0) == req.option_id), None)
        if not option:
            raise HTTPException(status_code=404, detail="option_not_found")

    request_id = uuid.uuid4().hex[:24]
    sent = _send_tiktok_review_to_telegram(job, option, result, request, request_id=request_id)
    payload = {
        "request_id": request_id,
        "job_id": req.job_id,
        "option_id": req.option_id,
        "status": "pending_review",
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "telegram_message_id": sent.get("message_id"),
        "privacy_level": req.privacy_level,
        "method": sent.get("method"),
        "option": option,
        "logs": ["Solicitud enviada a Telegram para revision."],
        "error": None,
        "publish_id": None,
        "tiktok_status": None,
        "creator_username": None,
    }
    with _publish_lock:
        _publish_requests[request_id] = payload
        _persist_publish_requests_locked()
    _append_log(req.job_id, f"Solicitud TikTok enviada a Telegram para opcion {req.option_id}.")
    return {"ok": True, **_serialize_publish_request(payload)}


@app.post("/api/publish/telegram-review-from-output")
def prepare_tiktok_review_from_output(req: PrepareTikTokReviewFromOutputRequest, request: Request) -> dict[str, Any]:
    safe_slug, result = _load_result_from_output_slug(req.output_slug)
    options = result.get("options") or []
    option = next((opt for opt in options if int(opt.get("option_id") or 0) == req.option_id), None)
    if not option:
        raise HTTPException(status_code=404, detail="option_not_found")

    synthetic_job_id = f"output:{safe_slug}"
    synthetic_job = {
        "job_id": synthetic_job_id,
        "status": "completed",
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "request": {
            "url": result.get("source_url"),
            "duration": int(round(float(option.get("duration") or 60))),
            "options": len(options),
        },
        "logs": [f"Revision reabierta desde output/{safe_slug}."],
        "result": result,
        "error": None,
        "traceback": None,
    }
    with _jobs_lock:
        _jobs[synthetic_job_id] = synthetic_job

    request_id = uuid.uuid4().hex[:24]
    sent = _send_tiktok_review_to_telegram(synthetic_job, option, result, request, request_id=request_id)
    payload = {
        "request_id": request_id,
        "job_id": synthetic_job_id,
        "option_id": req.option_id,
        "status": "pending_review",
        "created_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "telegram_message_id": sent.get("message_id"),
        "privacy_level": req.privacy_level,
        "method": sent.get("method"),
        "option": option,
        "logs": [f"Solicitud reabierta desde output/{safe_slug}."],
        "error": None,
        "publish_id": None,
        "tiktok_status": None,
        "creator_username": None,
        "review_group_id": f"output:{safe_slug}",
    }
    with _publish_lock:
        _publish_requests[request_id] = payload
        _persist_publish_requests_locked()
    _append_log(synthetic_job_id, f"Solicitud TikTok reenviada a Telegram para opcion {req.option_id}.")
    return {"ok": True, **_serialize_publish_request(payload)}


@app.get("/api/publish/requests/{request_id}")
def get_publish_request(request_id: str) -> dict[str, Any]:
    req = _get_publish_request(request_id, reload_if_missing=True)
    if not req:
        raise HTTPException(status_code=404, detail="publish_request_not_found")
    return _serialize_publish_request(req)

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
import sys
import threading
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from clip_dashboard import DashboardConfig, discover_creator_videos, generate_dashboard  # noqa: E402


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
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
WORK_DIR.mkdir(parents=True, exist_ok=True)


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
    channels: list[str] | None = None
    per_channel_scan: int = Field(default=12, ge=5, le=50)
    this_week_only: bool = True
    max_results: int = Field(default=12, ge=1, le=50)
    min_source_duration: int = Field(default=90, ge=30, le=3600)


class CreateJobRequest(BaseModel):
    url: str = Field(min_length=8, max_length=500)
    duration: int = Field(default=60, ge=20, le=90)
    options: int = Field(default=6, ge=2, le=12)
    stride: int = Field(default=10, ge=5, le=30)
    overlap_ratio: float = Field(default=0.40, ge=0.10, le=0.80)
    language: str = Field(default="es", min_length=2, max_length=8)


class ShareTelegramRequest(BaseModel):
    job_id: str = Field(min_length=8, max_length=80)
    option_id: int = Field(ge=1, le=99)


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
_telegram_session = requests.Session()


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


def _send_option_to_telegram(job: dict[str, Any], option: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
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

    with video_path.open("rb") as fh:
        files = {
            "video": (video_path.name, fh, "video/mp4"),
        }
        try:
            response = _telegram_session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendVideo",
                data=data,
                files=files,
                timeout=180,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise HTTPException(status_code=502, detail=f"telegram_send_failed: {exc}") from exc

    body = response.json()
    if not body.get("ok"):
        raise HTTPException(status_code=502, detail=f"telegram_send_failed: {body}")

    result_obj = body.get("result") or {}
    return {
        "ok": True,
        "message_id": result_obj.get("message_id"),
        "chat_id": TELEGRAM_CHAT_ID,
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


def _set_job_state(job_id: str, **updates: Any) -> None:
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job.update(updates)
        job["updated_at"] = _utc_now_iso()


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
            output_dir=str(OUTPUT_DIR),
            work_dir=str(WORK_DIR),
        )
        result = generate_dashboard(config, log_fn=lambda m: _append_log(job_id, m))
        payload = _serialize_result(result)
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
            channels=req.channels,
            per_channel_scan=req.per_channel_scan,
            this_week_only=req.this_week_only,
            min_source_duration=req.min_source_duration,
            max_results=req.max_results,
        )
        return {"items": [_serialize_candidate(c) for c in candidates]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"discover_failed: {exc}") from exc


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
def share_option_to_telegram(req: ShareTelegramRequest) -> dict[str, Any]:
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

    sent = _send_option_to_telegram(job, option, result)
    _append_log(req.job_id, f"Opcion {req.option_id} enviada a Telegram.")
    return {
        "ok": True,
        "job_id": req.job_id,
        "option_id": req.option_id,
        **sent,
    }

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
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

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

from clip_dashboard import DashboardConfig, discover_creator_videos, generate_dashboard  # noqa: E402
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
TIKTOK_DEFAULT_PRIVACY = os.getenv("TIKTOK_DEFAULT_PRIVACY", "SELF_ONLY").strip() or "SELF_ONLY"
TIKTOK_EXPECTED_USERNAME = os.getenv("TIKTOK_EXPECTED_USERNAME", "").strip().lstrip("@").lower()
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
    mode: str = Field(default="viral_es", min_length=4, max_length=32)
    channels: list[str] | None = None
    per_channel_scan: int = Field(default=12, ge=5, le=50)
    this_week_only: bool = True
    max_results: int = Field(default=12, ge=1, le=50)
    min_source_duration: int = Field(default=90, ge=30, le=3600)


class CreateJobRequest(BaseModel):
    url: str = Field(min_length=8, max_length=500)
    duration: int = Field(default=60, ge=20, le=90)
    options: int = Field(default=6, ge=1, le=12)
    stride: int = Field(default=10, ge=5, le=30)
    overlap_ratio: float = Field(default=0.40, ge=0.10, le=0.80)
    language: str = Field(default="es", min_length=2, max_length=8)


class ShareTelegramRequest(BaseModel):
    job_id: str = Field(min_length=8, max_length=80)
    option_id: int = Field(ge=1, le=99)


class PrepareTikTokReviewRequest(BaseModel):
    job_id: str = Field(min_length=8, max_length=80)
    option_id: int = Field(ge=1, le=99)
    privacy_level: str | None = Field(default=None, min_length=3, max_length=64)


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
_tiktok_oauth_lock = threading.Lock()
_tiktok_oauth_states: dict[str, dict[str, Any]] = {}
_telegram_session = requests.Session()
_telegram_poller_started = False
_telegram_update_offset = 0


@app.on_event("startup")
def _startup_tasks() -> None:
    _ensure_telegram_poller_started()


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


def _build_telegram_review_caption(job: dict[str, Any], option: dict[str, Any], result: dict[str, Any]) -> str:
    source_title = str(result.get("source_title") or "Clip Studio ES").strip()
    tiktok_title = str(option.get("tiktok_title") or option.get("short_description") or "").strip()
    tiktok_caption = str(option.get("tiktok_caption") or "").strip()
    hashtags = " ".join(option.get("tiktok_hashtags") or [])
    lines = [
        "Revision TikTok pendiente",
        f"video: {source_title}",
        f"opcion: {option.get('option_id')}",
    ]
    if tiktok_title:
        lines.append(f"titulo: {tiktok_title}")
    if tiktok_caption:
        lines.append(f"caption: {tiktok_caption}")
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


def _build_absolute_asset_url(request: Request, relative_or_absolute_url: str) -> str:
    raw = str(relative_or_absolute_url or "").strip()
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        return raw
    base = str(request.base_url).rstrip("/")
    if raw.startswith("/"):
        return f"{base}{raw}"
    return f"{base}/{raw}"


def _request_public_base_url(request: Request) -> str:
    if BACKEND_PUBLIC_URL:
        return BACKEND_PUBLIC_URL.rstrip("/")
    return str(request.base_url).rstrip("/")


def _build_tiktok_redirect_uri(request: Request) -> str:
    return f"{_request_public_base_url(request)}/api/tiktok/connect/callback"


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


def _send_option_to_telegram(job: dict[str, Any], option: dict[str, Any], result: dict[str, Any], request: Request) -> dict[str, Any]:
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
    public_url = _build_absolute_asset_url(request, str(option.get("manual_upload_url") or ""))

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
    request: Request,
    *,
    request_id: str,
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
    public_url = _build_absolute_asset_url(request, str(option.get("manual_upload_url") or ""))

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


def _append_publish_log(request_id: str, message: str) -> None:
    with _publish_lock:
        req = _publish_requests.get(request_id)
        if not req:
            return
        req.setdefault("logs", []).append(f"{datetime.now().strftime('%H:%M:%S')} {message}")
        req["logs"] = req["logs"][-200:]
        req["updated_at"] = _utc_now_iso()


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
    }


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

        status_payload = client.direct_post_file(
            video_path=video_path,
            title=title,
            privacy_level=privacy_level,
            disable_duet=disable_duet,
            disable_comment=disable_comment,
            disable_stitch=disable_stitch,
            video_cover_timestamp_ms=1000,
        )
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
    with _publish_lock:
        req = _publish_requests.get(request_id)
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
        "telegram_approvals_enabled": TELEGRAM_APPROVALS_ENABLED,
        "tiktok_credentials_configured": _tiktok_credentials_configured(),
        "tiktok_tokens_file_exists": Path(TIKTOK_TOKENS_FILE).expanduser().exists(),
        "tiktok_ready": _tiktok_enabled(),
    }


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
    code_verifier = oauth.generate_code_verifier()
    code_challenge = oauth.code_challenge_from_verifier(code_verifier)
    auth_url = oauth.build_authorize_url(
        scopes=["video.publish"],
        state=state,
        code_challenge=code_challenge,
    )
    with _tiktok_oauth_lock:
        _tiktok_oauth_states[state] = {
            "code_verifier": code_verifier,
            "redirect_uri": redirect_uri,
            "created_at": _utc_now_iso(),
        }
    return {"ok": True, "auth_url": auth_url, "redirect_uri": redirect_uri}


@app.get("/api/tiktok/connect/callback")
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
        tokens = oauth.exchange_code_for_tokens(code=code, code_verifier=str(flow["code_verifier"]))
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
    _append_log(req.job_id, f"Solicitud TikTok enviada a Telegram para opcion {req.option_id}.")
    return {"ok": True, **_serialize_publish_request(payload)}


@app.get("/api/publish/requests/{request_id}")
def get_publish_request(request_id: str) -> dict[str, Any]:
    with _publish_lock:
        req = _publish_requests.get(request_id)
        if not req:
            raise HTTPException(status_code=404, detail="publish_request_not_found")
        return _serialize_publish_request(req)

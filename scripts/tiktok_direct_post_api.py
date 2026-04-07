#!/usr/bin/env python
"""TikTok Content Posting API (Direct Post) integration for desktop apps.

Implements:
1) OAuth 2.0 authorization code flow (Desktop PKCE).
2) Direct Post initialize request.
3) Chunked file upload to upload_url.
4) Publish status polling.

Environment variables required:
- TIKTOK_CLIENT_KEY
- TIKTOK_CLIENT_SECRET

Optional:
- TIKTOK_REDIRECT_URI (default: http://127.0.0.1:8765/callback/)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import secrets
import threading
import time
import webbrowser
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Iterable, Optional
from urllib.parse import parse_qs, quote, urlencode, urlparse

import requests


AUTH_URL = "https://www.tiktok.com/v2/auth/authorize/"
TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"
CREATOR_INFO_URL = "https://open.tiktokapis.com/v2/post/publish/creator_info/query/"
INBOX_UPLOAD_INIT_URL = "https://open.tiktokapis.com/v2/post/publish/inbox/video/init/"
DIRECT_POST_INIT_URL = "https://open.tiktokapis.com/v2/post/publish/video/init/"
POST_STATUS_FETCH_URL = "https://open.tiktokapis.com/v2/post/publish/status/fetch/"


class TikTokApiError(RuntimeError):
    """Raised when TikTok API returns non-success response."""

    def __init__(self, message: str, status_code: int | None = None, payload: dict | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


@dataclass
class OAuthTokens:
    access_token: str
    refresh_token: str
    open_id: str
    scope: str
    expires_in: int
    refresh_expires_in: int
    token_type: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OAuthTokens":
        return cls(
            access_token=str(data["access_token"]),
            refresh_token=str(data["refresh_token"]),
            open_id=str(data["open_id"]),
            scope=str(data.get("scope", "")),
            expires_in=int(data.get("expires_in", 0)),
            refresh_expires_in=int(data.get("refresh_expires_in", 0)),
            token_type=str(data.get("token_type", "Bearer")),
        )


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _safe_json(response: requests.Response) -> dict:
    try:
        return response.json()
    except Exception:
        return {"raw": response.text}


def _extract_tiktok_error(payload: dict[str, Any]) -> str:
    return str(
        payload.get("error_description")
        or payload.get("error", {}).get("message")
        or payload.get("error", {}).get("code")
        or payload.get("error")
        or ""
    )


def _raise_for_tiktok_error(response: requests.Response, default_message: str) -> None:
    if response.ok:
        return
    payload = _safe_json(response)
    message = _extract_tiktok_error(payload) or default_message
    raise TikTokApiError(str(message), status_code=response.status_code, payload=payload)


def _raise_for_api_payload_error(payload: dict[str, Any], default_message: str) -> None:
    code = str(payload.get("error", {}).get("code") or "")
    if code in {"ok", "", "0"}:
        return
    message = _extract_tiktok_error(payload) or default_message
    raise TikTokApiError(message, payload=payload)


def _request_with_retries(
    method: str,
    url: str,
    *,
    retries: int = 3,
    retry_delay: float = 1.2,
    timeout: float = 45.0,
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504),
    **kwargs: Any,
) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)
            if response.status_code in retry_statuses and attempt < retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            return response
        except requests.RequestException as exc:
            last_exc = exc
            if attempt == retries - 1:
                break
            time.sleep(retry_delay * (attempt + 1))
    raise RuntimeError(f"Network request failed: {method} {url}. Last error: {last_exc}") from last_exc


def _guess_mime_type(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".mp4":
        return "video/mp4"
    if ext == ".mov":
        return "video/quicktime"
    if ext == ".webm":
        return "video/webm"
    return "video/mp4"


def _build_chunk_plan(file_size: int, preferred_chunk_size: int = 10 * 1024 * 1024) -> tuple[int, int]:
    # TikTok media transfer guide:
    # - Each chunk 5MB..64MB, except final chunk may exceed configured chunk_size (up to 128MB).
    # - For files <5MB upload as a whole.
    if file_size <= 0:
        raise ValueError("File size must be > 0 bytes.")

    min_chunk = 5 * 1024 * 1024
    max_chunk = 64 * 1024 * 1024
    if file_size < min_chunk:
        chunk_size = file_size
    else:
        chunk_size = max(min(preferred_chunk_size, max_chunk), min_chunk)
    total_chunks = int(math.ceil(file_size / float(chunk_size)))
    return chunk_size, total_chunks


class TikTokDesktopOAuth:
    """Desktop OAuth 2.0 PKCE helper (manual + localhost callback modes)."""

    def __init__(self, client_key: str, client_secret: str, redirect_uri: str) -> None:
        self.client_key = client_key
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri

    @staticmethod
    def generate_state(length: int = 40) -> str:
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return "".join(secrets.choice(alphabet) for _ in range(length))

    @staticmethod
    def generate_code_verifier(length: int = 64) -> str:
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
        return "".join(secrets.choice(alphabet) for _ in range(length))

    @staticmethod
    def code_challenge_from_verifier(code_verifier: str) -> str:
        # TikTok desktop docs require SHA256 hex encoding.
        return hashlib.sha256(code_verifier.encode("utf-8")).hexdigest()

    def build_authorize_url(
        self,
        scopes: Iterable[str],
        state: str,
        code_challenge: str | None = None,
    ) -> str:
        params = {
            "client_key": self.client_key,
            "response_type": "code",
            "scope": ",".join(scopes),
            "redirect_uri": self.redirect_uri,
            "state": state,
        }
        if code_challenge:
            params["code_challenge"] = code_challenge
            params["code_challenge_method"] = "S256"
        return f"{AUTH_URL}?{urlencode(params, quote_via=quote)}"

    def exchange_code_for_tokens(self, code: str, code_verifier: str | None = None) -> OAuthTokens:
        body = {
            "client_key": self.client_key,
            "client_secret": self.client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": self.redirect_uri,
        }
        if code_verifier:
            body["code_verifier"] = code_verifier
        response = _request_with_retries(
            "POST",
            TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=body,
        )
        _raise_for_tiktok_error(response, "Failed to exchange authorization code for access token.")
        return OAuthTokens.from_dict(response.json())

    def refresh_access_token(self, refresh_token: str) -> OAuthTokens:
        body = {
            "client_key": self.client_key,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        response = _request_with_retries(
            "POST",
            TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=body,
        )
        _raise_for_tiktok_error(response, "Failed to refresh access token.")
        return OAuthTokens.from_dict(response.json())

    def authorize_interactive(
        self,
        *,
        scopes: Iterable[str] = ("video.upload",),
        open_browser_automatically: bool = True,
        callback_timeout_sec: int = 180,
    ) -> OAuthTokens:
        """Opens browser, catches callback on localhost, exchanges code for token."""
        parsed = urlparse(self.redirect_uri)
        if parsed.hostname not in {"127.0.0.1", "localhost"}:
            raise RuntimeError("authorize_interactive requires localhost redirect_uri.")
        if not parsed.port:
            raise RuntimeError("redirect_uri must include a port for desktop callback flow.")

        state = self.generate_state()
        code_verifier = self.generate_code_verifier()
        code_challenge = self.code_challenge_from_verifier(code_verifier)
        auth_url = self.build_authorize_url(scopes=scopes, state=state, code_challenge=code_challenge)

        auth_result: dict[str, str] = {}
        event = threading.Event()

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                query = parse_qs(urlparse(self.path).query)
                if "error" in query:
                    auth_result["error"] = query.get("error_description", query["error"])[0]
                elif "code" in query and "state" in query:
                    auth_result["code"] = query["code"][0]
                    auth_result["state"] = query["state"][0]
                else:
                    auth_result["error"] = "Missing code/state in callback."
                event.set()
                body = (
                    "<html><body><h2>TikTok authorization received.</h2>"
                    "<p>You can close this window and return to the app.</p></body></html>"
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

        server = HTTPServer((parsed.hostname or "127.0.0.1", parsed.port), _Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            if open_browser_automatically:
                webbrowser.open(auth_url)
            print(f"[oauth] Open this URL and authorize:\n{auth_url}\n")
            if not event.wait(timeout=callback_timeout_sec):
                raise TimeoutError("Timed out waiting for TikTok OAuth callback.")
            if auth_result.get("error"):
                raise RuntimeError(f"Authorization failed: {auth_result['error']}")
            if auth_result.get("state") != state:
                raise RuntimeError("Invalid OAuth state (possible CSRF mismatch).")
            code = auth_result.get("code", "").strip()
            if not code:
                raise RuntimeError("No authorization code returned.")
            return self.exchange_code_for_tokens(code=code, code_verifier=code_verifier)
        finally:
            server.shutdown()
            server.server_close()


class TikTokDirectPostClient:
    """Client for TikTok Content Posting API (upload + direct post)."""

    def __init__(self, access_token: str) -> None:
        self.access_token = access_token

    @property
    def _auth_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json; charset=UTF-8",
        }

    def query_creator_info(self) -> dict[str, Any]:
        response = _request_with_retries("POST", CREATOR_INFO_URL, headers=self._auth_headers, json={})
        _raise_for_tiktok_error(response, "Failed to query creator info.")
        data = response.json()
        _raise_for_api_payload_error(data, "Creator info response reported an error.")
        return data

    def initialize_upload(
        self,
        *,
        video_path: Path,
        preferred_chunk_size: int = 10 * 1024 * 1024,
    ) -> dict[str, Any]:
        """Step 1 (scope video.upload): initialize upload and receive upload_url + publish_id."""
        if not video_path.exists():
            raise FileNotFoundError(f"Video not found: {video_path}")
        video_size = video_path.stat().st_size
        chunk_size, total_chunk_count = _build_chunk_plan(video_size, preferred_chunk_size=preferred_chunk_size)
        payload = {
            "source_info": {
                "source": "FILE_UPLOAD",
                "video_size": int(video_size),
                "chunk_size": int(chunk_size),
                "total_chunk_count": int(total_chunk_count),
            }
        }
        response = _request_with_retries(
            "POST",
            INBOX_UPLOAD_INIT_URL,
            headers=self._auth_headers,
            json=payload,
        )
        _raise_for_tiktok_error(response, "Failed to initialize TikTok upload.")
        data = response.json()
        _raise_for_api_payload_error(data, "Upload init response reported an error.")
        return data

    def initialize_direct_post(
        self,
        *,
        video_path: Path,
        title: str,
        privacy_level: str = "SELF_ONLY",
        disable_duet: bool = False,
        disable_comment: bool = False,
        disable_stitch: bool = False,
        video_cover_timestamp_ms: int = 1000,
        brand_content_toggle: bool = False,
        brand_organic_toggle: bool = False,
        is_aigc: bool = False,
        preferred_chunk_size: int = 10 * 1024 * 1024,
    ) -> dict[str, Any]:
        if not video_path.exists():
            raise FileNotFoundError(f"Video not found: {video_path}")
        video_size = video_path.stat().st_size
        chunk_size, total_chunk_count = _build_chunk_plan(video_size, preferred_chunk_size=preferred_chunk_size)

        payload = {
            "post_info": {
                "title": title,
                "privacy_level": privacy_level,
                "disable_duet": disable_duet,
                "disable_comment": disable_comment,
                "disable_stitch": disable_stitch,
                "video_cover_timestamp_ms": int(video_cover_timestamp_ms),
                "brand_content_toggle": bool(brand_content_toggle),
                "brand_organic_toggle": bool(brand_organic_toggle),
                "is_aigc": bool(is_aigc),
            },
            "source_info": {
                "source": "FILE_UPLOAD",
                "video_size": int(video_size),
                "chunk_size": int(chunk_size),
                "total_chunk_count": int(total_chunk_count),
            },
        }
        response = _request_with_retries(
            "POST",
            DIRECT_POST_INIT_URL,
            headers=self._auth_headers,
            json=payload,
        )
        _raise_for_tiktok_error(response, "Failed to initialize TikTok direct post.")
        data = response.json()
        _raise_for_api_payload_error(data, "Direct post init response reported an error.")
        return data

    def publish_direct_post(
        self,
        *,
        video_path: Path,
        title: str,
        privacy_level: str = "SELF_ONLY",
        disable_duet: bool = False,
        disable_comment: bool = False,
        disable_stitch: bool = False,
        video_cover_timestamp_ms: int = 1000,
        preferred_chunk_size: int = 10 * 1024 * 1024,
    ) -> dict[str, Any]:
        """Step 3 helper alias.

        TikTok Direct Post sets title/privacy in `video/init` (POST). This method intentionally
        maps to that endpoint so callers can model it as a dedicated "publish" step.
        """
        return self.initialize_direct_post(
            video_path=video_path,
            title=title,
            privacy_level=privacy_level,
            disable_duet=disable_duet,
            disable_comment=disable_comment,
            disable_stitch=disable_stitch,
            video_cover_timestamp_ms=video_cover_timestamp_ms,
            preferred_chunk_size=preferred_chunk_size,
        )

    def upload_video_chunks(
        self,
        *,
        upload_url: str,
        video_path: Path,
        chunk_size: int,
        progress_cb: Optional[Callable[[int, int, int], None]] = None,
    ) -> None:
        if not video_path.exists():
            raise FileNotFoundError(f"Video not found: {video_path}")
        total_size = video_path.stat().st_size
        mime_type = _guess_mime_type(video_path)
        total_chunks = int(math.ceil(total_size / float(chunk_size)))

        with video_path.open("rb") as f:
            for idx in range(total_chunks):
                start = idx * chunk_size
                f.seek(start)
                data = f.read(chunk_size)
                if not data:
                    break
                end = start + len(data) - 1
                headers = {
                    "Content-Type": mime_type,
                    "Content-Length": str(len(data)),
                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                }
                response = _request_with_retries(
                    "PUT",
                    upload_url,
                    headers=headers,
                    data=data,
                    retries=4,
                    retry_delay=1.5,
                    timeout=120.0,
                )
                _raise_for_tiktok_error(response, f"Chunk upload failed at chunk {idx + 1}/{total_chunks}.")
                if progress_cb:
                    progress_cb(idx + 1, total_chunks, end + 1)

    def fetch_publish_status(self, publish_id: str) -> dict[str, Any]:
        payload = {"publish_id": publish_id}
        response = _request_with_retries(
            "POST",
            POST_STATUS_FETCH_URL,
            headers=self._auth_headers,
            json=payload,
        )
        _raise_for_tiktok_error(response, "Failed to fetch TikTok publish status.")
        data = response.json()
        _raise_for_api_payload_error(data, "Publish status response reported an error.")
        return data

    def wait_for_publish_completion(
        self,
        publish_id: str,
        *,
        timeout_sec: int = 600,
        poll_interval_sec: int = 4,
    ) -> dict[str, Any]:
        start = time.time()
        while True:
            data = self.fetch_publish_status(publish_id)
            status = (data.get("data") or {}).get("status", "")
            if status in {"PUBLISH_COMPLETE", "FAILED", "SEND_TO_USER_INBOX"}:
                return data
            if time.time() - start > timeout_sec:
                raise TimeoutError(f"Timed out waiting for publish status. Last status={status!r}")
            time.sleep(poll_interval_sec)

    def direct_post_file(
        self,
        *,
        video_path: Path,
        title: str,
        privacy_level: str = "SELF_ONLY",
        disable_duet: bool = False,
        disable_comment: bool = False,
        disable_stitch: bool = False,
        video_cover_timestamp_ms: int = 1000,
        preferred_chunk_size: int = 10 * 1024 * 1024,
        timeout_sec: int = 600,
        poll_interval_sec: int = 4,
        progress_cb: Optional[Callable[[int, int, int], None]] = None,
    ) -> dict[str, Any]:
        """Convenience workflow: init direct post -> PUT chunks -> poll final publish status."""
        init = self.initialize_direct_post(
            video_path=video_path,
            title=title,
            privacy_level=privacy_level,
            disable_duet=disable_duet,
            disable_comment=disable_comment,
            disable_stitch=disable_stitch,
            video_cover_timestamp_ms=video_cover_timestamp_ms,
            preferred_chunk_size=preferred_chunk_size,
        )
        data = init.get("data") or {}
        publish_id = str(data.get("publish_id") or "")
        upload_url = str(data.get("upload_url") or "")
        if not publish_id:
            raise RuntimeError("TikTok init response did not return publish_id.")
        if not upload_url:
            raise RuntimeError("TikTok init response did not return upload_url for FILE_UPLOAD.")
        chunk_size, _ = _build_chunk_plan(video_path.stat().st_size, preferred_chunk_size=preferred_chunk_size)
        self.upload_video_chunks(
            upload_url=upload_url,
            video_path=video_path,
            chunk_size=chunk_size,
            progress_cb=progress_cb,
        )
        return self.wait_for_publish_completion(
            publish_id=publish_id,
            timeout_sec=timeout_sec,
            poll_interval_sec=poll_interval_sec,
        )


def save_tokens(tokens: OAuthTokens, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(tokens.__dict__, ensure_ascii=False, indent=2), encoding="utf-8")


def load_tokens(path: Path) -> OAuthTokens:
    data = json.loads(path.read_text(encoding="utf-8"))
    return OAuthTokens.from_dict(data)


def _cmd_auth(args: argparse.Namespace) -> int:
    client_key = _require_env("TIKTOK_CLIENT_KEY")
    client_secret = _require_env("TIKTOK_CLIENT_SECRET")
    redirect_uri = os.getenv("TIKTOK_REDIRECT_URI", "http://127.0.0.1:8765/callback/").strip()
    oauth = TikTokDesktopOAuth(client_key=client_key, client_secret=client_secret, redirect_uri=redirect_uri)
    scopes = [s.strip() for s in args.scopes.split(",") if s.strip()]
    tokens = oauth.authorize_interactive(scopes=scopes, open_browser_automatically=not args.no_browser)
    out = Path(args.token_out)
    save_tokens(tokens, out)
    print(f"[auth] Access token acquired and saved at: {out}")
    return 0


def _cmd_refresh(args: argparse.Namespace) -> int:
    token_path = Path(args.token_file)
    if not token_path.exists():
        raise FileNotFoundError(f"Token file not found: {token_path}")
    old_tokens = load_tokens(token_path)
    client_key = _require_env("TIKTOK_CLIENT_KEY")
    client_secret = _require_env("TIKTOK_CLIENT_SECRET")
    redirect_uri = os.getenv("TIKTOK_REDIRECT_URI", "http://127.0.0.1:8765/callback/").strip()
    oauth = TikTokDesktopOAuth(client_key=client_key, client_secret=client_secret, redirect_uri=redirect_uri)
    new_tokens = oauth.refresh_access_token(old_tokens.refresh_token)
    save_tokens(new_tokens, token_path)
    print(f"[auth] Access token refreshed and saved at: {token_path}")
    return 0


def _cmd_upload_only(args: argparse.Namespace) -> int:
    token_path = Path(args.token_file)
    if not token_path.exists():
        raise FileNotFoundError(f"Token file not found: {token_path}")

    tokens = load_tokens(token_path)
    client = TikTokDirectPostClient(access_token=tokens.access_token)
    video_path = Path(args.video).expanduser().resolve()
    init = client.initialize_upload(
        video_path=video_path,
        preferred_chunk_size=args.chunk_size,
    )
    data = init.get("data") or {}
    publish_id = str(data.get("publish_id") or "")
    upload_url = str(data.get("upload_url") or "")
    if not publish_id:
        raise RuntimeError("TikTok upload init response did not return publish_id.")
    if not upload_url:
        raise RuntimeError("TikTok upload init response did not return upload_url.")

    file_size = video_path.stat().st_size
    chunk_size, _ = _build_chunk_plan(file_size, preferred_chunk_size=args.chunk_size)
    print(f"[upload-only] publish_id={publish_id}")
    print(f"[upload-only] Uploading {video_path.name} in chunks of {chunk_size} bytes...")
    client.upload_video_chunks(
        upload_url=upload_url,
        video_path=video_path,
        chunk_size=chunk_size,
        progress_cb=lambda done, total, sent: print(f"[upload-only] chunk {done}/{total} sent_bytes={sent}"),
    )
    print("[upload-only] Upload complete.")
    if args.check_status:
        status = client.fetch_publish_status(publish_id)
        print("[upload-only][status] " + json.dumps(status, ensure_ascii=False, indent=2))
    return 0


def _cmd_post(args: argparse.Namespace) -> int:
    token_path = Path(args.token_file)
    if not token_path.exists():
        raise FileNotFoundError(f"Token file not found: {token_path}")

    tokens = load_tokens(token_path)
    client = TikTokDirectPostClient(access_token=tokens.access_token)
    video_path = Path(args.video).expanduser().resolve()

    creator_info = client.query_creator_info()
    print(f"[post] Creator info fetched. privacy options: {(creator_info.get('data') or {}).get('privacy_level_options')}")

    status = client.direct_post_file(
        video_path=video_path,
        title=args.title,
        privacy_level=args.privacy_level,
        disable_duet=args.disable_duet,
        disable_comment=args.disable_comment,
        disable_stitch=args.disable_stitch,
        video_cover_timestamp_ms=args.cover_ms,
        preferred_chunk_size=args.chunk_size,
        timeout_sec=args.wait_timeout,
        poll_interval_sec=args.poll_interval,
        progress_cb=lambda done, total, sent: print(f"[upload] chunk {done}/{total} sent_bytes={sent}"),
    )
    print("[status] " + json.dumps(status, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TikTok Direct Post API helper for desktop apps.")
    sub = p.add_subparsers(dest="cmd", required=True)

    auth = sub.add_parser("auth", help="Run OAuth desktop flow and save access token.")
    auth.add_argument("--scopes", default="video.upload")
    auth.add_argument("--token-out", default=".tiktok_tokens.json")
    auth.add_argument("--no-browser", action="store_true")
    auth.set_defaults(func=_cmd_auth)

    refresh = sub.add_parser("refresh", help="Refresh access token using stored refresh_token.")
    refresh.add_argument("--token-file", default=".tiktok_tokens.json")
    refresh.set_defaults(func=_cmd_refresh)

    upload_only = sub.add_parser(
        "upload-only",
        help="Initialize upload (video.upload scope), PUT chunks, optionally fetch status.",
    )
    upload_only.add_argument("--token-file", default=".tiktok_tokens.json")
    upload_only.add_argument("--video", required=True)
    upload_only.add_argument("--chunk-size", type=int, default=10 * 1024 * 1024)
    upload_only.add_argument("--check-status", action="store_true")
    upload_only.set_defaults(func=_cmd_upload_only)

    post = sub.add_parser("post", help="Direct Post end-to-end (requires video.publish scope).")
    post.add_argument("--token-file", default=".tiktok_tokens.json")
    post.add_argument("--video", required=True)
    post.add_argument("--title", default="Prueba API Direct Post #test")
    post.add_argument("--privacy-level", default="SELF_ONLY")
    post.add_argument("--disable-duet", action="store_true")
    post.add_argument("--disable-comment", action="store_true")
    post.add_argument("--disable-stitch", action="store_true")
    post.add_argument("--cover-ms", type=int, default=1000)
    post.add_argument("--chunk-size", type=int, default=10 * 1024 * 1024)
    post.add_argument("--poll-interval", type=int, default=4)
    post.add_argument("--wait-timeout", type=int, default=600)
    post.set_defaults(func=_cmd_post)
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())

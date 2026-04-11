#!/usr/bin/env python
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parent.parent
UPLOAD_DIR = (REPO_ROOT / "data" / "local_uploader").resolve()
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_SCRIPT = (REPO_ROOT / "scripts" / "upload_to_tiktok.py").resolve()
DEFAULT_BIND = "0.0.0.0"
DEFAULT_PORT = 8766


def _json(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


def _download_video(video_url: str) -> Path:
    parsed = urlparse(video_url)
    name = Path(parsed.path or "/video.mp4").name or "video.mp4"
    if not name.lower().endswith(".mp4"):
        name += ".mp4"
    target = UPLOAD_DIR / name
    with urlopen(video_url, timeout=120) as response:
        target.write_bytes(response.read())
    return target


def _run_upload(video_path: Path, caption: str, privacy_level: str, manual_wait: int, auto_post: bool) -> dict:
    cmd = [
        sys.executable,
        str(UPLOAD_SCRIPT),
        "--video",
        str(video_path),
        "--caption",
        caption[:2200],
        "--privacy-level",
        privacy_level,
        "--manual-wait",
        str(max(1, manual_wait)),
        "--browser-channel",
        "brave",
        "--json",
        "--connect-cdp",
        "http://127.0.0.1:9222",
    ]
    if auto_post:
        cmd.append("--auto-post")
    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=1800,
    )
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    payload = {}
    if stdout:
        for line in reversed(stdout.splitlines()):
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                payload = json.loads(line)
                break
            except Exception:
                continue
    return {
        "returncode": proc.returncode,
        "stdout": stdout[-4000:],
        "stderr": stderr[-4000:],
        "result": payload,
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.rstrip("/") == "/health":
            return _json(self, 200, {"ok": True, "service": "tiktok-local-uploader"})
        _json(self, 404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:
        if self.path.rstrip("/") != "/upload":
            return _json(self, 404, {"ok": False, "error": "not_found"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length > 0 else b"{}"
            payload = json.loads(body.decode("utf-8"))
            video_url = str(payload.get("video_url") or "").strip()
            caption = str(payload.get("caption") or "").strip()
            privacy_level = str(payload.get("privacy_level") or "PUBLIC_TO_EVERYONE").strip()
            manual_wait = int(payload.get("manual_wait") or 45)
            auto_post = bool(payload.get("auto_post", True))
            if not video_url:
                return _json(self, 400, {"ok": False, "error": "video_url_required"})
            local_video = _download_video(video_url)
            result = _run_upload(local_video, caption, privacy_level, manual_wait, auto_post)
            status = 200 if result["returncode"] == 0 else 500
            return _json(self, status, {"ok": result["returncode"] == 0, **result, "downloaded_to": str(local_video)})
        except Exception as exc:
            return _json(self, 500, {"ok": False, "error": str(exc)})

    def log_message(self, format: str, *args) -> None:
        return


def main() -> int:
    server = ThreadingHTTPServer((DEFAULT_BIND, DEFAULT_PORT), Handler)
    print(f"[tiktok-local-uploader] http://{DEFAULT_BIND}:{DEFAULT_PORT}", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

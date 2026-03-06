#!/usr/bin/env python
"""YouTube -> TikTok shorts pipeline (Spain-focused).

Flow:
1) Discover candidate YouTube videos (search, channel list, or direct URL).
2) Pick high-view candidates and download one source video.
3) Pick the most information-dense segment from subtitles (internal only).
4) Render a vertical short with hook text (no burned subtitles).
5) Optionally upload to TikTok using Playwright with a persistent profile.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

import imageio_ffmpeg
import webvtt
import yt_dlp

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


DEFAULT_ES_CHANNELS = [
    "https://www.youtube.com/@Ibai/videos",
    "https://www.youtube.com/@elrubiusOMG/videos",
    "https://www.youtube.com/@VEGETTA777/videos",
    "https://www.youtube.com/@AuronPlay/videos",
    "https://www.youtube.com/@Mikecrack/videos",
]

IMPACT_WORDS = {
    "increible",
    "secreto",
    "error",
    "truco",
    "dinero",
    "impacto",
    "riesgo",
    "nunca",
    "siempre",
    "importante",
    "urgente",
    "historico",
    "prohibido",
    "viral",
    "top",
    "record",
}


@dataclass
class VideoCandidate:
    title: str
    url: str
    view_count: int
    duration: Optional[int]
    channel: str
    video_id: str
    upload_date: Optional[str] = None
    views_per_day: float = 0.0
    ai_score: float = 0.0
    ai_reason: str = ""


@dataclass
class CaptionCue:
    start: float
    end: float
    text: str


@dataclass
class SegmentChoice:
    start: float
    end: float
    score: float
    hook: str


def log(message: str) -> None:
    print(f"[pipeline] {message}")


def slugify(text: str, max_len: int = 70) -> str:
    base = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    if not base:
        base = "video"
    return base[:max_len]


def parse_ts(ts: str) -> float:
    # VTT format: HH:MM:SS.mmm
    hms, ms = ts.split(".")
    h, m, s = hms.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000.0


def fmt_srt(seconds: float) -> str:
    whole = int(seconds)
    ms = int(round((seconds - whole) * 1000))
    h = whole // 3600
    m = (whole % 3600) // 60
    s = whole % 60
    return f"{h:02}:{m:02}:{s:02},{ms:03}"


def normalize_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def escape_drawtext(text: str) -> str:
    return (
        text.replace("\\", r"\\")
        .replace(":", r"\:")
        .replace("'", r"\'")
        .replace("%", r"\%")
        .replace(",", r"\,")
    )


def score_text(text: str) -> int:
    tokens = re.findall(r"[a-zA-Z0-9]+", text.lower())
    impact_hits = sum(1 for t in tokens if t in IMPACT_WORDS)
    numeric_hits = sum(1 for t in tokens if t.isdigit())
    punct_bonus = text.count("!") + text.count("?")
    return impact_hits * 4 + numeric_hits * 2 + punct_bonus


def parse_upload_date_ymd(upload_date: Optional[str]) -> Optional[date]:
    if not upload_date:
        return None
    try:
        return datetime.strptime(upload_date, "%Y%m%d").date()
    except Exception:
        return None


def ymd_to_iso(upload_date: Optional[str]) -> str:
    d = parse_upload_date_ymd(upload_date)
    return d.isoformat() if d else "N/A"


def compute_views_per_day(view_count: int, upload_date: Optional[str], today: date) -> float:
    d = parse_upload_date_ymd(upload_date)
    if not d:
        return 0.0
    age_days = max(1, (today - d).days + 1)
    return float(view_count) / float(age_days)


def is_within_last_days(upload_date: Optional[str], days: int, today: date) -> bool:
    d = parse_upload_date_ymd(upload_date)
    if not d:
        return False
    return d >= (today - timedelta(days=days))


def parse_vtt(path: Path) -> List[CaptionCue]:
    cues: List[CaptionCue] = []
    for cue in webvtt.read(str(path)):
        text = normalize_text(cue.text)
        if not text:
            continue
        cues.append(CaptionCue(start=parse_ts(cue.start), end=parse_ts(cue.end), text=text))
    return cues


def pick_hook(cues: Iterable[CaptionCue]) -> str:
    best_text = ""
    best_score = -1
    for cue in cues:
        s = score_text(cue.text)
        if s > best_score:
            best_score = s
            best_text = cue.text
    if not best_text:
        return "NO TE LO PIERDAS"

    tokens = re.findall(r"[A-Za-z0-9]+", best_text)
    short = " ".join(tokens[:6]).upper()[:34].strip()
    if len(short) < 10:
        short = "NO TE LO PIERDAS"
    return short


def choose_segment(cues: List[CaptionCue], source_duration: Optional[int], target_duration: int) -> SegmentChoice:
    if not cues:
        max_end = float(source_duration or target_duration)
        end = min(max_end, float(target_duration))
        return SegmentChoice(start=0.0, end=end, score=0.0, hook="NO TE LO PIERDAS")

    max_end = max(c.end for c in cues)
    if source_duration:
        max_end = min(max_end, float(source_duration))

    window = min(float(target_duration), max_end)
    if window <= 0:
        return SegmentChoice(start=0.0, end=float(target_duration), score=0.0, hook="NO TE LO PIERDAS")

    best = SegmentChoice(start=0.0, end=window, score=-1.0, hook="NO TE LO PIERDAS")

    max_start = max(0.0, max_end - window)
    probe_starts = [float(x) for x in range(0, int(max_start) + 1, 5)]
    if not probe_starts:
        probe_starts = [0.0]

    for start in probe_starts:
        end = start + window
        in_window: List[CaptionCue] = []
        words = 0
        impact = 0
        for cue in cues:
            if cue.end < start or cue.start > end:
                continue
            in_window.append(cue)
            words += len(re.findall(r"[a-zA-Z0-9]+", cue.text))
            impact += score_text(cue.text)

        score = words + impact
        if score > best.score:
            best = SegmentChoice(start=start, end=end, score=float(score), hook=pick_hook(in_window))

    return best


def fmt_ass(seconds: float) -> str:
    total_cs = max(0, int(round(seconds * 100)))
    h = total_cs // 360000
    rem = total_cs % 360000
    m = rem // 6000
    rem %= 6000
    s = rem // 100
    cs = rem % 100
    return f"{h}:{m:02}:{s:02}.{cs:02}"


def clean_caption_text(text: str) -> str:
    text = normalize_text(text)
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\([^)]+\)", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def wrap_caption_lines(words: List[str], max_line_chars: int = 24, max_lines: int = 1) -> str:
    if not words:
        return ""

    lines: List[str] = []
    current: List[str] = []

    for word in words:
        candidate = " ".join(current + [word]).strip()
        if not current or len(candidate) <= max_line_chars:
            current.append(word)
            continue

        lines.append(" ".join(current))
        current = [word]
        if len(lines) >= max_lines - 1:
            break

    if current and len(lines) < max_lines:
        lines.append(" ".join(current))

    if not lines:
        lines = [" ".join(words[:2])]
    return r"\N".join(lines[:max_lines])


def chunk_caption_words(text: str, cue_duration: float) -> List[str]:
    words = [w for w in re.findall(r"\w+", text, flags=re.UNICODE) if w]
    if not words:
        return []

    max_chunks_by_time = max(1, int(cue_duration / 0.75))
    chunk_count_by_words = max(1, math.ceil(len(words) / 4))
    chunk_count = min(max_chunks_by_time, chunk_count_by_words, 5, len(words))
    words_per_chunk = max(1, math.ceil(len(words) / chunk_count))

    chunks: List[str] = []
    for i in range(0, len(words), words_per_chunk):
        chunk_words = words[i : i + words_per_chunk]
        chunk_text = wrap_caption_lines(chunk_words, max_line_chars=24, max_lines=1)
        if chunk_text:
            chunks.append(chunk_text)
    return chunks


def ass_escape(text: str) -> str:
    return text.replace("\\", r"\\").replace("{", r"\{").replace("}", r"\}")


def chunks_too_similar(a: str, b: str) -> bool:
    a_words = set(re.findall(r"\w+", a.lower(), flags=re.UNICODE))
    b_words = set(re.findall(r"\w+", b.lower(), flags=re.UNICODE))
    if not a_words or not b_words:
        return False
    overlap = len(a_words & b_words) / max(len(a_words), len(b_words))
    return overlap >= 0.60


def write_segment_ass(cues: List[CaptionCue], start: float, end: float, out_path: Path) -> bool:
    events: List[str] = []
    last_clean = ""
    recent_chunks: List[str] = []

    for cue in cues:
        if cue.end <= start or cue.start >= end:
            continue

        cue_start = max(cue.start, start) - start
        cue_end = min(cue.end, end) - start
        cue_dur = cue_end - cue_start
        if cue_dur < 0.18:
            continue

        clean = clean_caption_text(cue.text)
        if not clean:
            continue
        if clean.lower() == last_clean.lower():
            continue

        chunks = chunk_caption_words(clean, cue_dur)
        if not chunks:
            continue

        part_dur = cue_dur / len(chunks)
        for idx, chunk in enumerate(chunks):
            part_start = cue_start + part_dur * idx
            part_end = cue_start + part_dur * (idx + 1)
            if part_end - part_start < 0.12:
                continue

            chunk_norm = re.sub(r"\\N", " ", chunk).lower().strip()
            if not chunk_norm:
                continue
            is_duplicate = False
            for prev in recent_chunks:
                if chunk_norm == prev or chunk_norm in prev or prev in chunk_norm:
                    is_duplicate = True
                    break
                if chunks_too_similar(chunk_norm, prev):
                    is_duplicate = True
                    break
            if is_duplicate:
                continue

            events.append(
                "Dialogue: 0,"
                f"{fmt_ass(part_start)},{fmt_ass(part_end)},Cap,,0,0,0,,{ass_escape(chunk)}"
            )
            recent_chunks.append(chunk_norm)
            recent_chunks = recent_chunks[-4:]

        last_clean = clean

    if not events:
        return False

    ass_text = "\n".join(
        [
            "[Script Info]",
            "ScriptType: v4.00+",
            "PlayResX: 1080",
            "PlayResY: 1920",
            "ScaledBorderAndShadow: yes",
            "WrapStyle: 2",
            "",
            "[V4+ Styles]",
            "Format: Name,Fontname,Fontsize,PrimaryColour,SecondaryColour,OutlineColour,BackColour,Bold,Italic,"
            "Underline,StrikeOut,ScaleX,ScaleY,Spacing,Angle,BorderStyle,Outline,Shadow,Alignment,MarginL,"
            "MarginR,MarginV,Encoding",
            "Style: Cap,Arial,50,&H00FFFFFF,&H000000FF,&H00000000,&H86000000,1,0,0,0,100,100,0,0,3,1,0,2,84,84,220,1",
            "",
            "[Events]",
            "Format: Layer,Start,End,Style,Name,MarginL,MarginR,MarginV,Effect,Text",
            *events,
            "",
        ]
    )
    out_path.write_text(ass_text, encoding="utf-8")
    return True


def yt_base_opts() -> dict:
    return {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "extract_flat": True,
        "skip_download": True,
        "noplaylist": True,
        "socket_timeout": 8,
        "retries": 1,
        "extractor_retries": 1,
    }


def enrich_candidates(candidates: List[VideoCandidate], limit: int) -> List[VideoCandidate]:
    if not candidates:
        return candidates

    opts = {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "skip_download": True,
        "noplaylist": True,
        "socket_timeout": 8,
        "retries": 1,
        "extractor_retries": 1,
    }
    out: List[VideoCandidate] = []
    deadline = time.monotonic() + 90.0
    with yt_dlp.YoutubeDL(opts) as ydl:
        for c in candidates[:limit]:
            if time.monotonic() > deadline:
                out.append(c)
                continue
            try:
                info = ydl.extract_info(c.url, download=False)
            except Exception:
                out.append(c)
                continue

            out.append(
                VideoCandidate(
                    title=info.get("title") or c.title,
                    url=info.get("webpage_url") or c.url,
                    view_count=int(info.get("view_count") or c.view_count or 0),
                    duration=info.get("duration") or c.duration,
                    channel=info.get("channel") or info.get("uploader") or c.channel,
                    video_id=info.get("id") or c.video_id,
                    upload_date=info.get("upload_date") or c.upload_date,
                    ai_score=c.ai_score,
                    ai_reason=c.ai_reason,
                )
            )

    if len(candidates) > limit:
        out.extend(candidates[limit:])

    return out


def discover_from_search(query: str, search_limit: int) -> List[VideoCandidate]:
    q = f"ytsearch{search_limit}:{query}"
    with yt_dlp.YoutubeDL(yt_base_opts()) as ydl:
        info = ydl.extract_info(q, download=False)

    entries = info.get("entries") or []
    candidates: List[VideoCandidate] = []
    for e in entries:
        if not e:
            continue
        video_id = e.get("id") or ""
        url = e.get("url") or ""
        if video_id and not url.startswith("http"):
            url = f"https://www.youtube.com/watch?v={video_id}"
        if not url:
            continue
        candidates.append(
            VideoCandidate(
                title=e.get("title") or "(sin titulo)",
                url=url,
                view_count=int(e.get("view_count") or 0),
                duration=e.get("duration"),
                channel=e.get("channel") or e.get("uploader") or "",
                video_id=video_id,
                upload_date=e.get("upload_date"),
            )
        )

    return candidates


def discover_from_channels(channels: List[str], per_channel_scan: int) -> List[VideoCandidate]:
    candidates: List[VideoCandidate] = []
    for raw_url in channels:
        channel_url = raw_url.strip()
        if not channel_url:
            continue

        log(f"Escaneando canal: {channel_url}")
        opts = dict(yt_base_opts())
        opts["playlistend"] = max(1, int(per_channel_scan))
        with yt_dlp.YoutubeDL(opts) as ydl:
            try:
                info = ydl.extract_info(channel_url, download=False)
            except Exception as exc:
                log(f"No se pudo leer el canal {channel_url}: {exc}")
                continue

        entries = info.get("entries") or []
        for e in entries[:per_channel_scan]:
            if not e:
                continue
            video_id = e.get("id") or ""
            url = e.get("url") or ""
            if video_id and not url.startswith("http"):
                url = f"https://www.youtube.com/watch?v={video_id}"
            if not url:
                continue

            candidates.append(
                VideoCandidate(
                    title=e.get("title") or "(sin titulo)",
                    url=url,
                    view_count=int(e.get("view_count") or 0),
                    duration=e.get("duration"),
                    channel=e.get("channel") or e.get("uploader") or info.get("uploader") or "",
                    video_id=video_id,
                    upload_date=e.get("upload_date"),
                )
            )

    return candidates


def read_channels_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    out: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip().lstrip("\ufeff")
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


def pick_candidates(args: argparse.Namespace) -> List[VideoCandidate]:
    if args.mode == "url":
        candidates = [
            VideoCandidate(
                title="manual-url",
                url=args.url,
                view_count=0,
                duration=None,
                channel="",
                video_id="manual",
                upload_date=None,
            )
        ]
    elif args.mode == "search":
        candidates = discover_from_search(args.query, args.search_limit)
    else:
        channels = args.channel or []
        if args.channels_file:
            channels.extend(read_channels_file(Path(args.channels_file)))
        if not channels:
            channels = list(DEFAULT_ES_CHANNELS)
        candidates = discover_from_channels(channels, args.per_channel_scan)

    candidates = enrich_candidates(candidates, limit=max(args.max_results * 4, 10))
    today = date.today()
    filtered: List[VideoCandidate] = []
    for c in candidates:
        if (c.duration or 0) < args.min_source_duration:
            continue
        c.views_per_day = compute_views_per_day(c.view_count, c.upload_date, today=today)
        filtered.append(c)
    candidates = filtered

    if args.this_week_only:
        candidates = [c for c in candidates if is_within_last_days(c.upload_date, days=7, today=today)]

    if args.sort_by == "viral":
        candidates.sort(key=lambda c: (c.views_per_day, c.view_count), reverse=True)
    else:
        candidates.sort(key=lambda c: (c.view_count, c.duration or 0), reverse=True)
    return candidates[: args.max_results]


def locate_subtitle(info: dict, job_dir: Path, preferred_lang: str) -> Optional[Path]:
    # Try yt-dlp structured fields first.
    lang_priority = [preferred_lang, "es", "es-ES", "en", "en-US"]
    candidates: List[Path] = []

    for field in ("requested_subtitles", "requested_automatic_captions"):
        bucket = info.get(field) or {}
        if not isinstance(bucket, dict):
            continue
        for lang in lang_priority:
            sub = bucket.get(lang)
            if isinstance(sub, dict) and sub.get("filepath"):
                p = Path(sub["filepath"])
                if p.exists():
                    return p

    # Fallback to scanning directory.
    for ext in ("*.vtt", "*.srt"):
        for p in job_dir.glob(ext):
            candidates.append(p)

    if not candidates:
        return None

    # Prefer files that mention language.
    for lang in lang_priority:
        for c in candidates:
            if f".{lang}." in c.name:
                return c
    return candidates[0]


def download_source_video(candidate: VideoCandidate, job_dir: Path, language: str) -> tuple[Path, Optional[Path], dict]:
    ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
    ydl_opts = {
        "format": "bv*[height<=1080]+ba/b[height<=1080]/b",
        "outtmpl": str(job_dir / "%(id)s.%(ext)s"),
        "noplaylist": True,
        "merge_output_format": "mp4",
        "writesubtitles": True,
        "writeautomaticsub": True,
        "subtitleslangs": [language],
        "subtitlesformat": "vtt",
        "restrictfilenames": True,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "ffmpeg_location": ffmpeg_bin,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(candidate.url, download=True)
    except Exception as exc:
        # Common failure: subtitle/caption endpoints return 429.
        # Retry downloading only media so pipeline can still continue.
        if "subtitle" not in str(exc).lower() and "caption" not in str(exc).lower():
            raise
        log("Fallo descargando subtitulos; reintentando sin subtitulos...")
        retry_opts = dict(ydl_opts)
        retry_opts.pop("writesubtitles", None)
        retry_opts.pop("writeautomaticsub", None)
        retry_opts.pop("subtitleslangs", None)
        retry_opts.pop("subtitlesformat", None)
        with yt_dlp.YoutubeDL(retry_opts) as ydl:
            info = ydl.extract_info(candidate.url, download=True)

    requested = info.get("requested_downloads") or []
    video_file: Optional[Path] = None
    if requested and isinstance(requested[0], dict):
        fp = requested[0].get("filepath")
        if fp:
            video_file = Path(fp)

    if not video_file or not video_file.exists():
        # Fallback: most recent media file in job dir.
        media_files = sorted(
            [p for p in job_dir.iterdir() if p.suffix.lower() in {".mp4", ".mkv", ".webm"}],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if media_files:
            video_file = media_files[0]

    if not video_file or not video_file.exists():
        raise RuntimeError("No se pudo encontrar el video descargado.")

    sub_file = locate_subtitle(info, job_dir, preferred_lang=language)
    return video_file, sub_file, info


def render_short(
    ffmpeg_bin: str,
    input_video: Path,
    output_video: Path,
    segment: SegmentChoice,
    hook_text: str,
    include_hook_overlay: bool = False,
) -> None:
    filters = [
        "scale=1080:1920:force_original_aspect_ratio=increase",
        "crop=1080:1920",
        "eq=contrast=1.06:saturation=1.14",
    ]
    if include_hook_overlay and hook_text.strip():
        filters.extend(
            [
                "drawbox=x=40:y=100:w=1000:h=170:color=black@0.38:t=fill",
                (
                    "drawtext="
                    f"font=Arial:text='{escape_drawtext(hook_text)}':"
                    "x=(w-text_w)/2:y=145:fontsize=46:fontcolor=white:borderw=2:bordercolor=black"
                ),
            ]
        )

    cmd = [
        ffmpeg_bin,
        "-y",
        "-ss",
        f"{segment.start:.3f}",
        "-i",
        str(input_video.name),
        "-t",
        f"{segment.end - segment.start:.3f}",
        "-vf",
        ",".join(filters),
        "-af",
        "loudnorm=I=-16:TP=-1.5:LRA=11",
        "-r",
        "30",
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "20",
        "-c:a",
        "aac",
        "-b:a",
        "160k",
        "-movflags",
        "+faststart",
        str(output_video.name),
    ]

    proc = subprocess.run(cmd, cwd=input_video.parent, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg fallo:\n{proc.stderr[-2000:]}")


def upload_to_tiktok_playwright(
    video_path: Path,
    caption: str,
    profile_dir: Path,
    headless: bool,
    auto_post: bool,
    manual_wait_seconds: int,
    browser_channel: str,
) -> None:
    try:
        from playwright.sync_api import TimeoutError as PwTimeout
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright no esta instalado. Ejecuta: pip install playwright && playwright install chromium"
        ) from exc

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=headless,
            channel=browser_channel,
            args=["--start-maximized"],
            viewport={"width": 1400, "height": 900},
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        log("Abriendo TikTok upload...")
        page.goto("https://www.tiktok.com/upload?lang=es", wait_until="domcontentloaded")

        try:
            page.wait_for_selector("input[type='file']", timeout=120000)
        except PwTimeout as exc:
            context.close()
            raise RuntimeError(
                "No se encontro el input de carga en TikTok. Inicia sesion en la ventana abierta y reintenta."
            ) from exc

        page.set_input_files("input[type='file']", str(video_path))
        log("Video enviado al formulario de TikTok.")

        try:
            caption_box = page.locator("div[contenteditable='true']").first
            caption_box.click(timeout=15000)
            caption_box.fill(caption[:150])
        except Exception:
            log("No se pudo autocompletar caption; continua manualmente si hace falta.")

        if auto_post:
            # Selector can change often; keep this as best effort.
            post_btn = page.get_by_role("button", name=re.compile(r"(Publicar|Post)", re.I)).first
            post_btn.click(timeout=20000)
            log("Intentando publicar automaticamente...")
            page.wait_for_timeout(15000)
        else:
            log(f"Carga lista. Tienes {manual_wait_seconds}s para revisar/publicar en la ventana del navegador.")
            page.wait_for_timeout(manual_wait_seconds * 1000)

        context.close()


def run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    work_dir = Path(args.work_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    log("Buscando candidatos...")
    candidates = pick_candidates(args)
    if not candidates:
        log("No se encontraron candidatos validos.")
        return 1

    for idx, c in enumerate(candidates, start=1):
        log(
            f"{idx}. {c.title[:90]} | views={c.view_count:,} | vpd={c.views_per_day:,.0f} | "
            f"fecha={ymd_to_iso(c.upload_date)} | dur={c.duration}s | canal={c.channel or 'N/A'}"
        )

    if args.dry_run:
        log("Dry-run finalizado. No se descargo ni renderizo nada.")
        return 0

    ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
    selected = candidates[0]
    job_slug = slugify(f"{selected.channel}-{selected.title}")
    job_dir = work_dir / job_slug
    job_dir.mkdir(parents=True, exist_ok=True)

    log(f"Descargando fuente: {selected.url}")
    source_video, subtitle_file, info = download_source_video(selected, job_dir=job_dir, language=args.language)

    cues: List[CaptionCue] = []
    if subtitle_file and subtitle_file.exists() and subtitle_file.suffix.lower() == ".vtt":
        log(f"Subtitulos detectados: {subtitle_file.name}")
        cues = parse_vtt(subtitle_file)
    else:
        log("No hay subtitulos VTT disponibles; se renderiza sin subtitulos.")

    source_duration = info.get("duration") or selected.duration
    segment = choose_segment(cues, source_duration=source_duration, target_duration=args.duration)

    file_slug = slugify(selected.title, max_len=60)
    output_file = output_dir / f"{file_slug}_tiktok.mp4"
    hook_text = segment.hook
    log(f"Renderizando short ({segment.start:.1f}s -> {segment.end:.1f}s)...")
    render_short(
        ffmpeg_bin=ffmpeg_bin,
        input_video=source_video,
        output_video=job_dir / output_file.name,
        segment=segment,
        hook_text=hook_text,
    )

    (job_dir / output_file.name).replace(output_file)

    metadata = {
        "source_url": selected.url,
        "source_title": selected.title,
        "source_channel": selected.channel,
        "source_views": selected.view_count,
        "segment_start": segment.start,
        "segment_end": segment.end,
        "hook": hook_text,
        "output_file": str(output_file),
    }
    meta_path = output_file.with_suffix(".json")
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    log(f"Short generado: {output_file}")

    if args.publish_tiktok:
        caption = args.tiktok_caption or hook_text
        upload_to_tiktok_playwright(
            video_path=output_file,
            caption=caption,
            profile_dir=Path(args.tiktok_profile_dir),
            headless=args.tiktok_headless,
            auto_post=args.tiktok_auto_post,
            manual_wait_seconds=args.tiktok_manual_wait,
            browser_channel=args.tiktok_browser_channel,
        )
        log("Paso de publicacion TikTok completado.")

    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Create TikTok-ready shorts from YouTube sources.")

    p.add_argument("--mode", choices=["channels", "search", "url"], default="channels")

    p.add_argument("--query", default="videos virales espana curiosidades")
    p.add_argument("--search-limit", type=int, default=25)

    p.add_argument("--url", default="")

    p.add_argument("--channel", action="append", default=[])
    p.add_argument("--channels-file", default="channels_es.txt")
    p.add_argument("--per-channel-scan", type=int, default=20)

    p.add_argument("--max-results", type=int, default=5)
    p.add_argument("--min-source-duration", type=int, default=95)
    p.add_argument("--duration", type=int, default=60)
    p.add_argument("--language", default="es")
    p.add_argument("--this-week-only", action="store_true")
    p.add_argument("--sort-by", choices=["views", "viral"], default="views")

    p.add_argument("--work-dir", default="work")
    p.add_argument("--output-dir", default="output")

    p.add_argument("--publish-tiktok", action="store_true")
    p.add_argument("--tiktok-profile-dir", default=".tiktok_profile")
    p.add_argument("--tiktok-caption", default="")
    p.add_argument("--tiktok-headless", action="store_true")
    p.add_argument("--tiktok-auto-post", action="store_true")
    p.add_argument("--tiktok-manual-wait", type=int, default=180)
    p.add_argument("--tiktok-browser-channel", choices=["chrome", "msedge", "chromium"], default="chrome")

    p.add_argument("--dry-run", action="store_true")
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.mode == "url" and not args.url:
        parser.error("--url es obligatorio cuando --mode=url")

    try:
        return run(args)
    except KeyboardInterrupt:
        log("Proceso interrumpido por el usuario.")
        return 130
    except Exception as exc:
        log(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())


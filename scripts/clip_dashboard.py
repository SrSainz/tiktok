#!/usr/bin/env python
"""Generate multiple TikTok clip options from one YouTube URL.

Output:
- MP4 previews for top segments
- options_manifest.json with ranking and commands
- dashboard.html to review options quickly
"""

from __future__ import annotations

import argparse
import html
import json
import math
import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable, List

from youtube_tiktok_pipeline import (
    CaptionCue,
    SegmentChoice,
    VideoCandidate,
    compute_views_per_day,
    discover_most_popular_es,
    download_source_video,
    enrich_candidates,
    is_within_last_days,
    discover_from_channels,
    discover_from_search,
    parse_vtt,
    pick_hook,
    render_short,
    score_text,
    slugify,
)

DEFAULT_CREATOR_CHANNELS = [
    "https://www.youtube.com/@TheGrefg/videos",
    "https://www.youtube.com/@AuronPlay/videos",
    "https://www.youtube.com/@Ibai/videos",
    "https://www.youtube.com/@YoSoyPlex/videos",
    "https://www.youtube.com/@elrubiusOMG/videos",
]

DISCOVERY_MODES = {"viral_es", "creators_es"}


@dataclass
class ClipOption:
    option_id: int
    start: float
    end: float
    duration: float
    score: float
    interest_score: float
    reach_score: float
    audio_score: float
    visual_score: float
    hook: str
    short_description: str
    why_it_may_work: str
    transcript_preview: str
    cue_count: int
    speech_density: float
    question_hits: int
    exclaim_hits: int
    number_hits: int
    scene_cut_count: int
    signal_tags: List[str]
    preview_file: str
    poster_file: str
    manual_upload_file: str


@dataclass
class CandidateSegment:
    segment: SegmentChoice
    interest_score: float
    reach_score: float
    audio_score: float
    visual_score: float
    short_description: str
    why_it_may_work: str
    transcript_preview: str
    cue_count: int
    speech_density: float
    question_hits: int
    exclaim_hits: int
    number_hits: int
    scene_cut_count: int
    signal_tags: List[str]
    topic_tokens: set[str]


@dataclass
class WindowAnalysis:
    score: float
    interest_score: float
    reach_score: float
    audio_score: float
    visual_score: float
    cues: List[CaptionCue]
    short_description: str
    why_it_may_work: str
    topic_tokens: set[str]
    transcript_preview: str
    cue_count: int
    speech_density: float
    question_hits: int
    exclaim_hits: int
    number_hits: int
    scene_cut_count: int
    signal_tags: List[str]


@dataclass
class DashboardConfig:
    url: str
    language: str = "es"
    duration: int = 60
    options: int = 6
    stride: int = 10
    max_pool: int = 50
    overlap_ratio: float = 0.40
    output_dir: str = "output"
    work_dir: str = "work"


@dataclass
class DashboardResult:
    dashboard_dir: str
    dashboard_html: str
    manifest_path: str
    source_title: str
    source_url: str
    options: List[ClipOption]


def log(message: str) -> None:
    print(f"[clip-dashboard] {message}")


def overlap_seconds(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))


AI_HOOK_TERMS = {
    "nunca",
    "nadie",
    "increible",
    "brutal",
    "locura",
    "error",
    "secreto",
    "reto",
    "vs",
    "top",
    "24h",
    "24",
    "historico",
    "primera",
    "primer",
}


def _safe_age_days(upload_date: str | None, today: date) -> int | None:
    if not upload_date:
        return None
    try:
        d = datetime.strptime(upload_date, "%Y%m%d").date()
        return max(0, (today - d).days)
    except Exception:
        return None


def _normalize_0_100(value: float, min_v: float, max_v: float) -> float:
    if max_v - min_v <= 1e-9:
        return 50.0
    return (value - min_v) * 100.0 / (max_v - min_v)


def score_candidate_ai(c: VideoCandidate, today: date) -> tuple[float, str]:
    title = (c.title or "").lower()
    tokens = re.findall(r"[a-z0-9]+", title)
    hook_hits = sum(1 for t in tokens if t in AI_HOOK_TERMS)
    has_question = "?" in title
    has_digits = any(t.isdigit() for t in tokens)
    views = float(c.view_count or 0)
    vpd = float(c.views_per_day or 0.0)
    age_days = _safe_age_days(c.upload_date, today)
    duration = float(c.duration or 0)

    fresh_score = 0.0
    if age_days is not None:
        # 0 days -> 100, 10+ days -> near 0
        fresh_score = max(0.0, 100.0 - age_days * 10.0)

    # Duration sweet spot for clip extraction: 8 to 25 min.
    dur_score = 30.0
    if duration > 0:
        if 8 * 60 <= duration <= 25 * 60:
            dur_score = 100.0
        elif 4 * 60 <= duration <= 40 * 60:
            dur_score = 70.0
        elif duration < 120:
            dur_score = 15.0
        else:
            dur_score = 40.0

    title_score = min(100.0, hook_hits * 16.0 + (15.0 if has_question else 0.0) + (12.0 if has_digits else 0.0))
    vpd_score = min(100.0, math.log10(vpd + 1.0) * 24.0)
    views_score = min(100.0, math.log10(views + 1.0) * 16.0)

    score = (
        vpd_score * 0.36
        + fresh_score * 0.22
        + title_score * 0.20
        + dur_score * 0.12
        + views_score * 0.10
    )

    reason_parts: List[str] = []
    if vpd_score >= 60:
        reason_parts.append("subida fuerte de vistas/dia")
    if fresh_score >= 60:
        reason_parts.append("muy reciente")
    if title_score >= 55:
        reason_parts.append("titulo con gancho")
    if dur_score >= 85:
        reason_parts.append("duracion ideal para extraer clips")
    if not reason_parts:
        reason_parts.append("equilibrio general de senales")
    return score, "; ".join(reason_parts)


def discover_creator_videos(
    channels: List[str] | None = None,
    per_channel_scan: int = 20,
    this_week_only: bool = True,
    min_source_duration: int = 90,
    max_results: int = 20,
    mode: str = "viral_es",
    log_fn: Callable[[str], None] | None = None,
) -> List[VideoCandidate]:
    def _log(message: str) -> None:
        if log_fn:
            log_fn(message)

    mode = (mode or "viral_es").strip().lower()
    if mode not in DISCOVERY_MODES:
        mode = "viral_es"

    channels = channels or list(DEFAULT_CREATOR_CHANNELS)
    candidates: List[VideoCandidate] = []
    yt_api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    yt_trend_categories = [x.strip() for x in os.getenv("YOUTUBE_TREND_CATEGORY_IDS", "").split(",") if x.strip()]
    used_charts = False

    if yt_api_key and mode == "viral_es":
        try:
            _log("Consultando charts oficiales de YouTube para ES (sin preferencia de creador)...")
            candidates = discover_most_popular_es(
                api_key=yt_api_key,
                max_results=max(30, max_results * 4),
                category_ids=None,
                region_code="ES",
            )
            used_charts = bool(candidates)
            if len(candidates) < max_results and yt_trend_categories:
                _log("Charts generales escasos; completando con categorias adicionales configuradas.")
                extra_candidates = discover_most_popular_es(
                    api_key=yt_api_key,
                    max_results=max(20, max_results * 2),
                    category_ids=yt_trend_categories,
                    region_code="ES",
                )
                extra_by_id = {c.video_id: c for c in candidates if c.video_id}
                for c in extra_candidates:
                    if c.video_id and c.video_id in extra_by_id:
                        continue
                    candidates.append(c)
        except Exception as exc:
            _log(f"Fallo YouTube Data API, usando fallback por canales: {exc}")

    if not candidates:
        _log(f"Escaneando {len(channels)} canales...")
        candidates = discover_from_channels(channels, per_channel_scan=per_channel_scan)
    if not candidates:
        _log("Sin resultados por canales. Probando fallback de busqueda...")
        search_q = "TheGrefg AuronPlay Ibai YoSoyPlex elrubius"
        candidates = discover_from_search(search_q, search_limit=max(30, max_results * 4))
    _log(f"Candidatos brutos: {len(candidates)}")
    enrich_limit = min(len(candidates), max(max_results * 2, 20))
    _log(f"Enriqueciendo metadatos de {enrich_limit} videos...")
    candidates = enrich_candidates(candidates, limit=enrich_limit)

    today = date.today()
    filtered: List[VideoCandidate] = []
    for c in candidates:
        if (c.duration or 0) < min_source_duration:
            continue
        c.views_per_day = compute_views_per_day(c.view_count, c.upload_date, today=today)
        if this_week_only and not is_within_last_days(c.upload_date, days=7, today=today):
            continue
        filtered.append(c)

    if this_week_only and filtered:
        seen_video_ids = {c.video_id for c in filtered if c.video_id}
        need_temporal_backfill = used_charts and len(filtered) < max_results
        if need_temporal_backfill:
            _log("Pocos resultados esta semana; ampliando candidatos a ultimos 30 dias.")
            for c in candidates:
                if (c.duration or 0) < min_source_duration:
                    continue
                if c.video_id and c.video_id in seen_video_ids:
                    continue
                if not is_within_last_days(c.upload_date, days=30, today=today):
                    continue
                c.views_per_day = compute_views_per_day(c.view_count, c.upload_date, today=today)
                filtered.append(c)
                if c.video_id:
                    seen_video_ids.add(c.video_id)

    if this_week_only and not filtered:
        _log("Sin resultados de esta semana. Relaxing filtro temporal (ultimos 30 dias).")
        for c in candidates:
            if (c.duration or 0) < min_source_duration:
                continue
            c.views_per_day = compute_views_per_day(c.view_count, c.upload_date, today=today)
            if not is_within_last_days(c.upload_date, days=30, today=today):
                continue
            filtered.append(c)

    if not filtered:
        _log("Sin resultados con filtro de fecha. Mostrando mejores candidatos disponibles.")
        for c in candidates:
            if (c.duration or 0) < min_source_duration:
                continue
            c.views_per_day = compute_views_per_day(c.view_count, c.upload_date, today=today)
            filtered.append(c)

    if filtered:
        raw_scores: List[float] = []
        for c in filtered:
            score, reason = score_candidate_ai(c, today=today)
            c.ai_score = score
            c.ai_reason = reason
            raw_scores.append(score)
        s_min, s_max = min(raw_scores), max(raw_scores)
        for c in filtered:
            c.ai_score = round(_normalize_0_100(c.ai_score, s_min, s_max), 1)

    if used_charts:
        filtered.sort(key=lambda x: (x.view_count, x.views_per_day, x.ai_score), reverse=True)
    else:
        filtered.sort(key=lambda x: (x.ai_score, x.views_per_day, x.view_count), reverse=True)
    _log(f"Videos validos tras filtros: {len(filtered)}")
    if used_charts:
        selected = filtered[:max_results]
        _log(f"Seleccion final por visitas ES: {len(selected)} videos")
    else:
        selected = _select_diverse_by_channel(filtered, max_results=max_results)
        channels_used = len({(c.channel or "").strip().lower() for c in selected if (c.channel or "").strip()})
        _log(f"Seleccion final: {len(selected)} videos de {max(1, channels_used)} canales")
    return selected


def _channel_key(candidate: VideoCandidate) -> str:
    key = (candidate.channel or "").strip().lower()
    if key:
        return key
    # Fallback minimal when channel metadata is missing.
    return "canal-desconocido"


def _select_diverse_by_channel(candidates: List[VideoCandidate], max_results: int) -> List[VideoCandidate]:
    if not candidates or max_results <= 0:
        return []

    groups: dict[str, List[VideoCandidate]] = {}
    for c in candidates:
        groups.setdefault(_channel_key(c), []).append(c)

    ordered_channels = sorted(
        groups.keys(),
        key=lambda ch: (groups[ch][0].ai_score, groups[ch][0].views_per_day, groups[ch][0].view_count),
        reverse=True,
    )

    selected: List[VideoCandidate] = []

    # First pass: try at least one video per channel to avoid single-channel dominance.
    for ch in ordered_channels:
        if len(selected) >= max_results:
            break
        if groups[ch]:
            selected.append(groups[ch].pop(0))

    # Then fill remaining slots round-robin by channel quality order.
    while len(selected) < max_results:
        progressed = False
        for ch in ordered_channels:
            if len(selected) >= max_results:
                break
            if groups[ch]:
                selected.append(groups[ch].pop(0))
                progressed = True
        if not progressed:
            break

    return selected


INTEREST_TERMS = {
    "increible",
    "secreto",
    "nunca",
    "siempre",
    "historia",
    "record",
    "viral",
    "shock",
    "brutal",
    "locura",
}

REACH_TERMS = {
    "mira",
    "atento",
    "ojo",
    "espera",
    "importante",
    "top",
    "mejor",
    "peor",
    "como",
    "por",
    "porque",
}

ES_STOPWORDS = {
    "de", "la", "el", "que", "y", "a", "en", "un", "una", "los", "las", "por", "para", "con", "sin",
    "del", "al", "lo", "se", "es", "me", "te", "le", "les", "mi", "tu", "su", "ya", "pero", "como",
    "porque", "pues", "si", "no", "o", "u", "muy", "mas", "menos", "esto", "esta", "este", "esa",
    "ese", "hoy", "ayer", "manana", "cuando", "donde", "quien", "que", "cual", "cuales", "todo", "toda",
    "todos", "todas", "nada", "algo", "tambien", "solo", "otra", "otro", "otras", "otros",
}


def _run_ffmpeg_stream(
    ffmpeg_bin: str,
    args: List[str],
    on_line: Callable[[str], None],
    cwd: Path | None = None,
) -> int:
    """Run ffmpeg and stream combined output line-by-line."""
    cmd = [ffmpeg_bin, *args]
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1,
    )
    if proc.stdout:
        for line in proc.stdout:
            on_line(line.rstrip("\n"))
    return proc.wait()


def analyze_scene_changes(ffmpeg_bin: str, source_video: Path, max_seconds: int = 300) -> List[float]:
    # Detect significant frame differences as scene cuts.
    times: List[float] = []

    def _parse_line(line: str) -> None:
        m = re.search(r"pts_time:([0-9]+(?:\.[0-9]+)?)", line)
        if not m:
            return
        try:
            times.append(float(m.group(1)))
        except Exception:
            return

    code = _run_ffmpeg_stream(
        ffmpeg_bin,
        [
            "-hide_banner",
            "-nostats",
            "-nostdin",
            "-i",
            str(source_video),
            "-t",
            str(max(60, int(max_seconds))),
            "-vf",
            "select='gt(scene,0.34)',showinfo",
            "-an",
            "-f",
            "null",
            "-",
        ],
        on_line=_parse_line,
    )
    if code != 0:
        return []
    return sorted(times)


def analyze_audio_energy(ffmpeg_bin: str, source_video: Path, max_seconds: int = 300) -> dict[int, float]:
    # Build per-second loudness map using astats RMS level.
    rms_by_second: dict[int, float] = {}
    current_t = 0.0

    def _parse_line(line: str) -> None:
        nonlocal current_t
        t_match = re.search(r"pts_time:([0-9]+(?:\.[0-9]+)?)", line)
        if t_match:
            try:
                current_t = float(t_match.group(1))
            except Exception:
                pass
            return
        r_match = re.search(r"RMS_level=([-+]?[0-9]+(?:\.[0-9]+)?)", line)
        if not r_match:
            return
        try:
            db = float(r_match.group(1))
        except Exception:
            return
        if db < -120 or db > 6:
            return
        sec = int(max(0.0, math.floor(current_t)))
        prev = rms_by_second.get(sec)
        if prev is None or db > prev:
            rms_by_second[sec] = db

    code = _run_ffmpeg_stream(
        ffmpeg_bin,
        [
            "-hide_banner",
            "-nostats",
            "-nostdin",
            "-i",
            str(source_video),
            "-t",
            str(max(60, int(max_seconds))),
            "-vn",
            "-af",
            "astats=metadata=1:reset=1,ametadata=print:file=-",
            "-f",
            "null",
            "-",
        ],
        on_line=_parse_line,
    )
    if code != 0:
        return {}
    return rms_by_second


def window_audio_score(rms_by_second: dict[int, float], start: float, end: float) -> float:
    if not rms_by_second:
        return 0.0
    start_s = int(math.floor(start))
    end_s = int(math.ceil(end))
    vals: List[float] = []
    for sec in range(start_s, end_s + 1):
        if sec in rms_by_second:
            vals.append(rms_by_second[sec])
    if not vals:
        return 0.0
    vals.sort(reverse=True)
    keep = max(1, int(len(vals) * 0.35))
    top = vals[:keep]
    # Normalize dB range: -45 (quiet) -> -12 (energetic)
    normalized = [max(0.0, min(1.0, (v + 45.0) / 33.0)) for v in top]
    return sum(normalized) / len(normalized) * 100.0


def window_visual_score(scene_times: List[float], start: float, end: float) -> float:
    if not scene_times:
        return 0.0
    duration = max(1.0, end - start)
    cuts = 0
    for t in scene_times:
        if t < start:
            continue
        if t > end:
            break
        cuts += 1
    cuts_per_min = cuts * 60.0 / duration
    return min(100.0, cuts_per_min * 11.0)


def window_scene_cut_count(scene_times: List[float], start: float, end: float) -> int:
    cuts = 0
    for t in scene_times:
        if t < start:
            continue
        if t > end:
            break
        cuts += 1
    return cuts


def summarize_transcript_preview(cues: List[CaptionCue], max_chars: int = 180) -> str:
    text = " ".join(c.text.strip() for c in cues if c.text.strip())
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    trimmed = text[: max_chars - 1]
    if " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0]
    return f"{trimmed}…"


def build_signal_tags(
    *,
    cue_count: int,
    speech_density: float,
    question_hits: int,
    exclaim_hits: int,
    number_hits: int,
    audio_score: float,
    visual_score: float,
    scene_cut_count: int,
) -> List[str]:
    tags: List[str] = []
    if question_hits > 0:
        tags.append("Pregunta")
    if number_hits > 0:
        tags.append("Dato")
    if exclaim_hits > 0:
        tags.append("Impacto")
    if speech_density >= 2.6 or cue_count >= 8:
        tags.append("Mucho texto")
    if audio_score >= 68.0:
        tags.append("Audio alto")
    elif audio_score >= 48.0:
        tags.append("Audio estable")
    if visual_score >= 58.0 or scene_cut_count >= 2:
        tags.append("Cambio escena")
    if not tags:
        if visual_score >= audio_score and visual_score >= 35.0:
            tags.append("Ritmo visual")
        elif audio_score >= 35.0:
            tags.append("Buen audio")
        else:
            tags.append("Momento claro")
    return tags[:5]


def extract_topic_tokens(cues: List[CaptionCue]) -> set[str]:
    blob = " ".join(c.text.lower() for c in cues)
    tokens = re.findall(r"[a-zA-Z0-9]{3,}", blob)
    return {t for t in tokens if t not in ES_STOPWORDS and not t.isdigit()}


def topic_similarity(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a.intersection(b))
    union = len(a.union(b))
    if union <= 0:
        return 0.0
    return inter / union


def _window_cues(cues: List[CaptionCue], start: float, end: float) -> List[CaptionCue]:
    in_window: List[CaptionCue] = []
    for cue in cues:
        if cue.end <= start or cue.start >= end:
            continue
        in_window.append(cue)
    return in_window


def align_window_to_cues(
    cues: List[CaptionCue],
    start: float,
    end: float,
    source_duration: float,
) -> tuple[float, float]:
    if not cues:
        return start, end

    target_duration = max(1.0, end - start)
    start_candidates = [
        cue
        for cue in cues
        if (start - 1.0) <= cue.start <= (start + min(4.5, max(2.5, target_duration * 0.18)))
    ]
    if start_candidates:
        best_start_cue = min(
            start_candidates,
            key=lambda cue: (
                abs(cue.start - start),
                -score_text(cue.text),
            ),
        )
        start = best_start_cue.start

    target_end = min(source_duration, start + target_duration)
    end_candidates = [
        cue
        for cue in cues
        if (target_end - min(4.5, max(2.5, target_duration * 0.18))) <= cue.end <= (target_end + 1.5)
        and (cue.end - start) >= max(12.0, target_duration * 0.75)
    ]
    if end_candidates:
        best_end_cue = min(end_candidates, key=lambda cue: abs(cue.end - target_end))
        end = best_end_cue.end
    else:
        end = min(source_duration, target_end)

    min_duration = max(12.0, target_duration * 0.72)
    if end - start < min_duration:
        end = min(source_duration, start + min_duration)

    return max(0.0, start), min(source_duration, end)


def _description_from_cues(in_window: List[CaptionCue]) -> tuple[str, str]:
    if not in_window:
        return "Momento destacado", "Fragmento con potencial de retencion."

    joined = " ".join(c.text for c in in_window)
    joined = html.unescape(joined)
    joined = re.sub(r"\[&nbsp;__&nbsp;\]", " ", joined, flags=re.IGNORECASE)
    tokens = re.findall(r"\w+", joined.lower(), flags=re.UNICODE)
    num_hits = sum(1 for t in tokens if t.isdigit())
    has_question = "?" in joined
    has_exclaim = "!" in joined
    interest_hits = sum(1 for t in tokens if t in INTEREST_TERMS)
    reach_hits = sum(1 for t in tokens if t in REACH_TERMS)

    if has_question:
        theme = "Curiosidad/Pregunta"
    elif num_hits > 0:
        theme = "Dato concreto"
    elif interest_hits >= 2:
        theme = "Momento impactante"
    elif reach_hits >= 2:
        theme = "Momento explicativo"
    else:
        theme = "Momento entretenido"

    sample = ""
    for cue in in_window:
        txt = html.unescape(cue.text)
        txt = re.sub(r"\[&nbsp;__&nbsp;\]", " ", txt, flags=re.IGNORECASE)
        txt = re.sub(r"\s+", " ", txt).strip()
        if len(txt) >= 22:
            sample = txt
            break
    if not sample:
        sample = re.sub(r"\s+", " ", in_window[0].text).strip()
    words = sample.split()
    if len(words) > 15:
        sample = " ".join(words[:15]) + "..."

    why = "Buena mezcla de ritmo y claridad."
    if has_exclaim or interest_hits >= 2:
        why = "Tiene carga emocional y puede enganchar rapido."
    elif has_question:
        why = "Abre un bucle de curiosidad que mejora retencion."
    elif num_hits > 0:
        why = "Incluye datos concretos que suelen retener mejor."

    return f"{theme}: {sample}", why


def window_score(
    cues: List[CaptionCue],
    start: float,
    end: float,
    rms_by_second: dict[int, float] | None = None,
    scene_times: List[float] | None = None,
) -> WindowAnalysis:
    in_window = _window_cues(cues, start, end)
    if not in_window:
        return WindowAnalysis(
            score=0.0,
            interest_score=0.0,
            reach_score=0.0,
            audio_score=0.0,
            visual_score=0.0,
            cues=[],
            short_description="Momento destacado",
            why_it_may_work="Sin subtitulos suficientes.",
            topic_tokens=set(),
            transcript_preview="",
            cue_count=0,
            speech_density=0.0,
            question_hits=0,
            exclaim_hits=0,
            number_hits=0,
            scene_cut_count=0,
            signal_tags=[],
        )

    duration = max(1.0, end - start)
    text_blob = " ".join(c.text for c in in_window)
    words = re.findall(r"\w+", text_blob.lower(), flags=re.UNICODE)
    total_words = len(words)
    diversity = len(set(c.text.strip().lower() for c in in_window))
    exclaim_hits = text_blob.count("!")
    question_hits = text_blob.count("?")
    punctuation_bonus = exclaim_hits + question_hits
    impact_raw = sum(score_text(c.text) for c in in_window)
    curiosity_hits = sum(1 for t in words if t in REACH_TERMS)
    interest_hits = sum(1 for t in words if t in INTEREST_TERMS)
    number_hits = sum(1 for t in words if t.isdigit())
    speech_density = total_words / duration
    impact_rate = impact_raw / duration
    diversity_rate = diversity / max(1.0, duration / 3.0)
    curiosity_rate = curiosity_hits / max(1.0, total_words)
    number_rate = number_hits / max(1.0, total_words)
    question_rate = question_hits / duration
    opening_cues = [cue for cue in in_window if cue.start < start + min(6.0, duration * 0.28)]
    opening_hook_score = max((score_text(cue.text) for cue in opening_cues), default=0.0)
    dead_air_start = max(0.0, min(4.0, in_window[0].start - start))
    dead_air_end = max(0.0, min(4.0, end - in_window[-1].end))

    interest_score = (
        speech_density * 11.5
        + diversity_rate * 14.0
        + impact_rate * 8.0
        + interest_hits * 1.8
        + punctuation_bonus * 0.35
        + opening_hook_score * 2.2
    )
    reach_score = (
        speech_density * 9.5
        + curiosity_rate * 90.0
        + number_rate * 120.0
        + question_rate * 120.0
        + impact_rate * 6.0
        + exclaim_hits * 0.2
        + opening_hook_score * 1.4
    )
    audio_score = window_audio_score(rms_by_second or {}, start, end)
    visual_score = window_visual_score(scene_times or [], start, end)
    scene_cut_count = window_scene_cut_count(scene_times or [], start, end)
    combined = (
        interest_score * 0.46
        + reach_score * 0.33
        + audio_score * 0.12
        + visual_score * 0.09
    )
    combined -= dead_air_start * 6.5
    combined -= dead_air_end * 3.0
    short_desc, why = _description_from_cues(in_window)
    topic_tokens = extract_topic_tokens(in_window)
    transcript_preview = summarize_transcript_preview(in_window)
    signal_tags = build_signal_tags(
        cue_count=len(in_window),
        speech_density=speech_density,
        question_hits=question_hits,
        exclaim_hits=exclaim_hits,
        number_hits=number_hits,
        audio_score=audio_score,
        visual_score=visual_score,
        scene_cut_count=scene_cut_count,
    )
    if audio_score >= 65.0:
        why = f"{why} Audio con energia alta."
    if visual_score >= 55.0:
        why = f"{why} Ritmo visual dinamico."
    if signal_tags:
        why = f"{why} Senales: {', '.join(signal_tags)}."
    return WindowAnalysis(
        score=combined,
        interest_score=interest_score,
        reach_score=reach_score,
        audio_score=audio_score,
        visual_score=visual_score,
        cues=in_window,
        short_description=short_desc,
        why_it_may_work=why,
        topic_tokens=topic_tokens,
        transcript_preview=transcript_preview,
        cue_count=len(in_window),
        speech_density=speech_density,
        question_hits=question_hits,
        exclaim_hits=exclaim_hits,
        number_hits=number_hits,
        scene_cut_count=scene_cut_count,
        signal_tags=signal_tags,
    )


def _normalize(value: float, min_v: float, max_v: float) -> float:
    if max_v - min_v < 1e-9:
        return 50.0
    return 20.0 + 80.0 * ((value - min_v) / (max_v - min_v))


def normalize_candidate_scores(pool: List[CandidateSegment]) -> None:
    if not pool:
        return
    i_vals = [p.interest_score for p in pool]
    r_vals = [p.reach_score for p in pool]
    a_vals = [p.audio_score for p in pool]
    v_vals = [p.visual_score for p in pool]
    c_vals = [p.segment.score for p in pool]

    i_min, i_max = min(i_vals), max(i_vals)
    r_min, r_max = min(r_vals), max(r_vals)
    a_min, a_max = min(a_vals), max(a_vals)
    v_min, v_max = min(v_vals), max(v_vals)
    c_min, c_max = min(c_vals), max(c_vals)

    for p in pool:
        p.interest_score = round(_normalize(p.interest_score, i_min, i_max), 1)
        p.reach_score = round(_normalize(p.reach_score, r_min, r_max), 1)
        p.audio_score = round(_normalize(p.audio_score, a_min, a_max), 1)
        p.visual_score = round(_normalize(p.visual_score, v_min, v_max), 1)
        p.segment.score = round(_normalize(p.segment.score, c_min, c_max), 1)


def build_candidate_segments(
    cues: List[CaptionCue],
    source_duration: float,
    clip_duration: int,
    stride_seconds: int,
    max_pool: int,
    rms_by_second: dict[int, float] | None = None,
    scene_times: List[float] | None = None,
) -> List[CandidateSegment]:
    if source_duration <= 0:
        return []

    window = min(float(clip_duration), float(source_duration))
    if window <= 0:
        return []

    max_start = max(0.0, source_duration - window)
    starts = [float(i) for i in range(0, int(max_start) + 1, max(1, stride_seconds))]
    if not starts:
        starts = [0.0]

    pool: List[CandidateSegment] = []
    if cues:
        seen_ranges: set[tuple[int, int]] = set()
        for start in starts:
            end = start + window
            start, end = align_window_to_cues(cues, start, end, source_duration)
            range_key = (int(round(start * 10.0)), int(round(end * 10.0)))
            if range_key in seen_ranges:
                continue
            seen_ranges.add(range_key)
            analysis = window_score(
                cues,
                start,
                end,
                rms_by_second=rms_by_second,
                scene_times=scene_times,
            )
            if analysis.score <= 0:
                continue
            hook = pick_hook(analysis.cues)
            pool.append(
                CandidateSegment(
                    segment=SegmentChoice(start=start, end=end, score=analysis.score, hook=hook),
                    interest_score=analysis.interest_score,
                    reach_score=analysis.reach_score,
                    audio_score=analysis.audio_score,
                    visual_score=analysis.visual_score,
                    short_description=analysis.short_description,
                    why_it_may_work=analysis.why_it_may_work,
                    transcript_preview=analysis.transcript_preview,
                    cue_count=analysis.cue_count,
                    speech_density=analysis.speech_density,
                    question_hits=analysis.question_hits,
                    exclaim_hits=analysis.exclaim_hits,
                    number_hits=analysis.number_hits,
                    scene_cut_count=analysis.scene_cut_count,
                    signal_tags=analysis.signal_tags,
                    topic_tokens=analysis.topic_tokens,
                )
            )
    else:
        # Fallback without subtitles: spread candidates evenly.
        parts = max(1, min(max_pool, int(math.ceil(source_duration / max(10.0, window)))))
        step = max(1.0, max_start / max(1, parts - 1)) if parts > 1 else 1.0
        for i in range(parts):
            start = min(max_start, i * step)
            end = start + window
            audio_score = window_audio_score(rms_by_second or {}, start, end)
            visual_score = window_visual_score(scene_times or [], start, end)
            combined = 30.0 + audio_score * 0.45 + visual_score * 0.55
            pool.append(
                CandidateSegment(
                    segment=SegmentChoice(start=start, end=end, score=combined, hook="NO TE LO PIERDAS"),
                    interest_score=35.0,
                    reach_score=35.0,
                    audio_score=audio_score,
                    visual_score=visual_score,
                    short_description="Momento variado del video sin subtitulos",
                    why_it_may_work="Seleccion por dinamica de audio y ritmo visual (sin texto).",
                    transcript_preview="",
                    cue_count=0,
                    speech_density=0.0,
                    question_hits=0,
                    exclaim_hits=0,
                    number_hits=0,
                    scene_cut_count=window_scene_cut_count(scene_times or [], start, end),
                    signal_tags=build_signal_tags(
                        cue_count=0,
                        speech_density=0.0,
                        question_hits=0,
                        exclaim_hits=0,
                        number_hits=0,
                        audio_score=audio_score,
                        visual_score=visual_score,
                        scene_cut_count=window_scene_cut_count(scene_times or [], start, end),
                    ),
                    topic_tokens=set(),
                )
            )

    normalize_candidate_scores(pool)
    pool.sort(key=lambda s: s.segment.score, reverse=True)
    return pool[:max_pool]


def pick_non_overlapping(pool: List[CandidateSegment], max_options: int, overlap_ratio_limit: float) -> List[CandidateSegment]:
    selected: List[CandidateSegment] = []
    remaining = list(pool)
    diversity_lambda = 0.32
    hard_similarity_limit = 0.88

    while remaining and len(selected) < max_options:
        best: CandidateSegment | None = None
        best_adjusted = -1e9
        best_idx = -1
        for idx, cand in enumerate(remaining):
            duration = max(0.1, cand.segment.end - cand.segment.start)
            overlaps = False
            for s in selected:
                ov = overlap_seconds(cand.segment.start, cand.segment.end, s.segment.start, s.segment.end)
                if (ov / duration) > overlap_ratio_limit:
                    overlaps = True
                    break
            if overlaps:
                continue

            max_sim = 0.0
            for s in selected:
                sim = topic_similarity(cand.topic_tokens, s.topic_tokens)
                if sim > max_sim:
                    max_sim = sim
            if max_sim > hard_similarity_limit:
                continue

            adjusted = cand.segment.score - (max_sim * 100.0 * diversity_lambda)
            adjusted += cand.audio_score * 0.05 + cand.visual_score * 0.04
            if adjusted > best_adjusted:
                best_adjusted = adjusted
                best = cand
                best_idx = idx

        if best is None:
            break
        selected.append(best)
        remaining.pop(best_idx)
    return selected


def extract_poster_frame(ffmpeg_bin: str, input_video: Path, output_image: Path, at_second: float = 0.8) -> None:
    cmd = [
        ffmpeg_bin,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{max(0.0, at_second):.3f}",
        "-i",
        str(input_video.name),
        "-frames:v",
        "1",
        "-q:v",
        "3",
        str(output_image.name),
    ]
    proc = subprocess.run(cmd, cwd=input_video.parent, capture_output=True, text=True)
    if proc.returncode == 0 and output_image.exists():
        return
    fallback_cmd = cmd.copy()
    fallback_cmd[6] = "0.000"
    proc_fb = subprocess.run(fallback_cmd, cwd=input_video.parent, capture_output=True, text=True)
    if proc_fb.returncode != 0 or not output_image.exists():
        raise RuntimeError((proc_fb.stderr or proc.stderr or "No se pudo generar poster.")[-1200:])


def write_dashboard_html(
    out_html: Path,
    source_title: str,
    source_url: str,
    options: List[ClipOption],
) -> None:
    cards: List[str] = []
    for opt in options:
        poster_attr = f' poster="{html.escape(opt.poster_file)}"' if opt.poster_file else ""
        tags_html = "".join(f'<span class="tag">{html.escape(tag)}</span>' for tag in opt.signal_tags)
        transcript_html = (
            f'<p class="transcript">{html.escape(opt.transcript_preview)}</p>' if opt.transcript_preview else ""
        )
        cards.append(
            f"""
            <article class="card">
              <div class="topline">
                <h2>Option {opt.option_id}</h2>
                <span class="score">Score {opt.score:.1f}</span>
              </div>
              <p class="meta">Start: {opt.start:.1f}s | End: {opt.end:.1f}s | Interes: {opt.interest_score:.1f} | Alcance: {opt.reach_score:.1f}</p>
              <video controls preload="metadata"{poster_attr} src="{html.escape(opt.preview_file)}"></video>
              <div class="signals">{tags_html}</div>
              <p class="hook">{html.escape(opt.short_description)}</p>
              <p class="why">{html.escape(opt.why_it_may_work)}</p>
              {transcript_html}
              <div class="metrics">
                <span>Audio {opt.audio_score:.1f}</span>
                <span>Visual {opt.visual_score:.1f}</span>
                <span>Cues {opt.cue_count}</span>
                <span>Escenas {opt.scene_cut_count}</span>
              </div>
              <code>Subir manualmente este archivo: {html.escape(opt.manual_upload_file)}</code>
            </article>
            """
        )

    page_html = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Clip Dashboard</title>
  <style>
    :root {{
      --bg: #0f1115;
      --card: #1b212b;
      --text: #f2f5f8;
      --muted: #90a0b3;
      --accent: #ff8a3d;
    }}
    body {{
      margin: 0;
      font-family: Segoe UI, Arial, sans-serif;
      background: radial-gradient(circle at 15% 0%, #1f2a3d 0%, var(--bg) 40%);
      color: var(--text);
    }}
    .wrap {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 22px;
    }}
    h1 {{
      margin: 0 0 6px 0;
      font-size: 28px;
    }}
    .source {{
      margin: 0 0 18px 0;
      color: var(--muted);
    }}
    .source a {{ color: var(--accent); }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 14px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid #2d3746;
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 18px 40px rgba(0,0,0,0.18);
    }}
    .topline {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }}
    .meta {{
      margin: 0 0 8px 0;
      color: var(--muted);
      font-size: 13px;
    }}
    .score {{
      font-size: 12px;
      color: #ffd9bf;
      border: 1px solid #5b4736;
      border-radius: 999px;
      padding: 4px 8px;
      background: rgba(255, 138, 61, 0.08);
    }}
    .hook {{
      margin: 0 0 10px 0;
      color: #ffd6bb;
      font-weight: 600;
    }}
    .why {{
      margin: 0 0 10px 0;
      color: #b9c9da;
      font-size: 13px;
    }}
    video {{
      width: 100%;
      border-radius: 10px;
      border: 1px solid #344158;
      background: #000;
      margin-bottom: 8px;
      aspect-ratio: 9 / 16;
    }}
    .signals {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-bottom: 8px;
    }}
    .tag {{
      display: inline-flex;
      align-items: center;
      border: 1px solid #31425b;
      background: #121b28;
      color: #dbe9ff;
      border-radius: 999px;
      padding: 4px 8px;
      font-size: 12px;
    }}
    .transcript {{
      margin: 10px 0 8px 0;
      font-size: 13px;
      color: #edf4ff;
      line-height: 1.45;
      padding: 10px;
      background: #101722;
      border: 1px solid #2b3443;
      border-radius: 10px;
    }}
    .metrics {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-bottom: 10px;
    }}
    .metrics span {{
      font-size: 12px;
      color: #b9c9da;
      border: 1px solid #2c3a4d;
      border-radius: 8px;
      padding: 4px 7px;
      background: #111722;
    }}
    code {{
      display: block;
      white-space: pre-wrap;
      font-size: 12px;
      color: #d9e2ef;
      background: #111722;
      border: 1px solid #2b3443;
      border-radius: 8px;
      padding: 8px;
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <h1>Clip Dashboard</h1>
    <p class="source">Source: <strong>{source_title}</strong><br/><a href="{source_url}" target="_blank" rel="noreferrer">{source_url}</a></p>
    <section class="grid">
      {"".join(cards)}
    </section>
  </main>
</body>
</html>"""
    out_html.write_text(page_html, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate multiple short clip options from one YouTube URL.")
    p.add_argument("--url", required=True)
    p.add_argument("--language", default="es")
    p.add_argument("--duration", type=int, default=60, help="Seconds per option clip.")
    p.add_argument("--options", type=int, default=6, help="How many options to generate.")
    p.add_argument("--stride", type=int, default=10, help="Sliding window step in seconds.")
    p.add_argument("--max-pool", type=int, default=50, help="Window pool size before overlap filter.")
    p.add_argument("--overlap-ratio", type=float, default=0.40, help="Max overlap ratio between options.")
    p.add_argument("--output-dir", default="output")
    p.add_argument("--work-dir", default="work")
    return p


def generate_dashboard(config: DashboardConfig, log_fn: Callable[[str], None] = log) -> DashboardResult:
    output_root = Path(config.output_dir)
    work_root = Path(config.work_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    work_root.mkdir(parents=True, exist_ok=True)

    candidate = VideoCandidate(
        title="manual-url",
        url=config.url,
        view_count=0,
        duration=None,
        channel="manual",
        video_id="manual",
    )
    slug = slugify(config.url, max_len=40)
    job_dir = work_root / f"dashboard_{slug}"
    job_dir.mkdir(parents=True, exist_ok=True)

    log_fn("Descargando fuente de YouTube...")
    source_video, subtitle_file, info = download_source_video(candidate, job_dir=job_dir, language=config.language)
    ffmpeg_bin = __import__("imageio_ffmpeg").get_ffmpeg_exe()

    source_title = info.get("title") or source_video.stem
    source_duration = float(info.get("duration") or 0.0)
    source_slug = slugify(source_title, max_len=55)

    cues: List[CaptionCue] = []
    if subtitle_file and subtitle_file.exists() and subtitle_file.suffix.lower() == ".vtt":
        cues = parse_vtt(subtitle_file)
        log_fn(f"Subtitulos encontrados: {subtitle_file.name}")
    else:
        log_fn("No hay subtitulos: opciones se repartiran por duracion.")

    if source_duration <= 0:
        raise RuntimeError("No se pudo detectar duracion del video.")

    # Signal analysis for stronger clip ranking.
    analysis_cap = max(60, min(300, int(os.getenv("CLIP_ANALYSIS_MAX_SECONDS", "150"))))
    analysis_seconds = int(min(max(60.0, source_duration), float(analysis_cap)))
    log_fn(f"Analizando dinamica de audio y ritmo visual (primeros {analysis_seconds}s)...")
    try:
        rms_by_second = analyze_audio_energy(ffmpeg_bin, source_video, max_seconds=analysis_seconds)
    except Exception:
        rms_by_second = {}
    try:
        scene_times = analyze_scene_changes(ffmpeg_bin, source_video, max_seconds=analysis_seconds)
    except Exception:
        scene_times = []

    pool = build_candidate_segments(
        cues=cues,
        source_duration=source_duration,
        clip_duration=config.duration,
        stride_seconds=config.stride,
        max_pool=max(config.options * 5, config.max_pool),
        rms_by_second=rms_by_second,
        scene_times=scene_times,
    )
    selected = pick_non_overlapping(pool, max_options=config.options, overlap_ratio_limit=config.overlap_ratio)
    if not selected:
        raise RuntimeError("No se pudieron generar opciones de clip.")

    dashboard_dir = output_root / f"clip_dashboard_{source_slug}"
    dashboard_dir.mkdir(parents=True, exist_ok=True)

    options: List[ClipOption] = []
    for idx, cand in enumerate(selected, start=1):
        seg = cand.segment
        preview_name = f"option_{idx:02}.mp4"
        poster_name = f"option_{idx:02}.jpg"
        out_video = dashboard_dir / preview_name
        poster_file = ""
        log_fn(f"Render option {idx}/{len(selected)} ({seg.start:.1f}s -> {seg.end:.1f}s)")
        render_short(
            ffmpeg_bin=ffmpeg_bin,
            input_video=source_video,
            output_video=job_dir / preview_name,
            segment=seg,
            hook_text=seg.hook,
            include_hook_overlay=False,
        )
        (job_dir / preview_name).replace(out_video)
        try:
            extract_poster_frame(ffmpeg_bin, out_video, dashboard_dir / poster_name)
            poster_file = poster_name
        except Exception:
            poster_file = ""
        options.append(
            ClipOption(
                option_id=idx,
                start=seg.start,
                end=seg.end,
                duration=seg.end - seg.start,
                score=seg.score,
                interest_score=cand.interest_score,
                reach_score=cand.reach_score,
                audio_score=cand.audio_score,
                visual_score=cand.visual_score,
                hook=seg.hook,
                short_description=cand.short_description,
                why_it_may_work=cand.why_it_may_work,
                transcript_preview=cand.transcript_preview,
                cue_count=cand.cue_count,
                speech_density=cand.speech_density,
                question_hits=cand.question_hits,
                exclaim_hits=cand.exclaim_hits,
                number_hits=cand.number_hits,
                scene_cut_count=cand.scene_cut_count,
                signal_tags=cand.signal_tags,
                preview_file=preview_name,
                poster_file=poster_file,
                manual_upload_file=str(out_video),
            )
        )

    manifest = {
        "source_title": source_title,
        "source_url": info.get("webpage_url") or config.url,
        "source_duration": source_duration,
        "options": [asdict(o) for o in options],
    }
    manifest_path = dashboard_dir / "options_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    html_path = dashboard_dir / "dashboard.html"
    write_dashboard_html(
        out_html=html_path,
        source_title=source_title,
        source_url=manifest["source_url"],
        options=options,
    )

    log_fn(f"Dashboard listo: {html_path}")
    log_fn(f"Manifest: {manifest_path}")
    return DashboardResult(
        dashboard_dir=str(dashboard_dir),
        dashboard_html=str(html_path),
        manifest_path=str(manifest_path),
        source_title=source_title,
        source_url=manifest["source_url"],
        options=options,
    )


def main() -> int:
    args = build_parser().parse_args()
    config = DashboardConfig(
        url=args.url,
        language=args.language,
        duration=args.duration,
        options=args.options,
        stride=args.stride,
        max_pool=args.max_pool,
        overlap_ratio=args.overlap_ratio,
        output_dir=args.output_dir,
        work_dir=args.work_dir,
    )
    generate_dashboard(config)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("[clip-dashboard] Interrumpido por usuario.")
        raise SystemExit(130)

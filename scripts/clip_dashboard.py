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
    preview_file: str
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
    topic_tokens: set[str]


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
    log_fn: Callable[[str], None] | None = None,
) -> List[VideoCandidate]:
    def _log(message: str) -> None:
        if log_fn:
            log_fn(message)

    channels = channels or list(DEFAULT_CREATOR_CHANNELS)
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

    filtered.sort(key=lambda x: (x.ai_score, x.views_per_day, x.view_count), reverse=True)
    _log(f"Videos validos tras filtros: {len(filtered)}")
    return filtered[:max_results]


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


def analyze_scene_changes(ffmpeg_bin: str, source_video: Path, max_seconds: int = 900) -> List[float]:
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


def analyze_audio_energy(ffmpeg_bin: str, source_video: Path, max_seconds: int = 900) -> dict[int, float]:
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
) -> tuple[float, float, float, float, float, List[CaptionCue], str, str, set[str]]:
    in_window = _window_cues(cues, start, end)
    if not in_window:
        return 0.0, 0.0, 0.0, 0.0, 0.0, [], "Momento destacado", "Sin subtitulos suficientes.", set()

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

    interest_score = (
        speech_density * 11.5
        + diversity_rate * 14.0
        + impact_rate * 8.0
        + interest_hits * 1.8
        + punctuation_bonus * 0.35
    )
    reach_score = (
        speech_density * 9.5
        + curiosity_rate * 90.0
        + number_rate * 120.0
        + question_rate * 120.0
        + impact_rate * 6.0
        + exclaim_hits * 0.2
    )
    audio_score = window_audio_score(rms_by_second or {}, start, end)
    visual_score = window_visual_score(scene_times or [], start, end)
    combined = (
        interest_score * 0.46
        + reach_score * 0.33
        + audio_score * 0.12
        + visual_score * 0.09
    )
    short_desc, why = _description_from_cues(in_window)
    topic_tokens = extract_topic_tokens(in_window)
    if audio_score >= 65.0:
        why = f"{why} Audio con energia alta."
    if visual_score >= 55.0:
        why = f"{why} Ritmo visual dinamico."
    return combined, interest_score, reach_score, audio_score, visual_score, in_window, short_desc, why, topic_tokens


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
        for start in starts:
            end = start + window
            (
                score,
                interest_score,
                reach_score,
                audio_score,
                visual_score,
                in_window,
                short_desc,
                why,
                topic_tokens,
            ) = window_score(
                cues,
                start,
                end,
                rms_by_second=rms_by_second,
                scene_times=scene_times,
            )
            if score <= 0:
                continue
            hook = pick_hook(in_window)
            pool.append(
                CandidateSegment(
                    segment=SegmentChoice(start=start, end=end, score=score, hook=hook),
                    interest_score=interest_score,
                    reach_score=reach_score,
                    audio_score=audio_score,
                    visual_score=visual_score,
                    short_description=short_desc,
                    why_it_may_work=why,
                    topic_tokens=topic_tokens,
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


def write_dashboard_html(
    out_html: Path,
    source_title: str,
    source_url: str,
    options: List[ClipOption],
) -> None:
    cards: List[str] = []
    for opt in options:
        cards.append(
            f"""
            <article class="card">
              <h2>Option {opt.option_id}</h2>
              <p class="meta">Start: {opt.start:.1f}s | End: {opt.end:.1f}s | Global: {opt.score:.1f}</p>
              <p class="meta">Interes: {opt.interest_score:.1f} | Alcance: {opt.reach_score:.1f} | Audio: {opt.audio_score:.1f} | Visual: {opt.visual_score:.1f}</p>
              <p class="hook">{opt.short_description}</p>
              <p class="why">{opt.why_it_may_work}</p>
              <video controls preload="metadata" src="{opt.preview_file}"></video>
              <code>Subir manualmente este archivo: {opt.manual_upload_file}</code>
            </article>
            """
        )

    html = f"""<!doctype html>
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
    }}
    .meta {{
      margin: 0 0 8px 0;
      color: var(--muted);
      font-size: 13px;
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
    out_html.write_text(html, encoding="utf-8")


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
    log_fn("Analizando dinamica de audio y ritmo visual...")
    analysis_seconds = int(min(max(180.0, source_duration), 900.0))
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
        out_video = dashboard_dir / preview_name
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
                preview_file=preview_name,
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

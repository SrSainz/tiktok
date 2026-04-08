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
from typing import Any, Callable, List
from zoneinfo import ZoneInfo

from youtube_tiktok_pipeline import (
    CaptionCue,
    SegmentChoice,
    VideoCandidate,
    clean_caption_text,
    compute_views_per_day,
    discover_most_popular_es,
    download_source_video,
    enrich_candidates,
    extract_hook_focus_text,
    is_within_last_days,
    discover_from_channels,
    discover_from_search,
    parse_vtt,
    pick_hook,
    render_short,
    score_text,
    slugify,
    write_segment_ass,
)

DEFAULT_CREATOR_CHANNELS = [
    "https://www.youtube.com/@TheGrefg/videos",
    "https://www.youtube.com/@AuronPlay/videos",
    "https://www.youtube.com/@Ibai/videos",
    "https://www.youtube.com/@YoSoyPlex/videos",
    "https://www.youtube.com/@elrubiusOMG/videos",
    "https://www.youtube.com/@Dagar/videos",
    "https://www.youtube.com/@Lyna/videos",
    "https://www.youtube.com/@illojuan/videos",
    "https://www.youtube.com/@JordiWild/videos",
    "https://www.youtube.com/@byViruZz/videos",
]

DISCOVERY_MODES = {"viral_es", "creators_es"}

CREATOR_CHANNEL_BLOCKLIST = {
    "records",
    "music",
    "pictures",
    "studios",
    "studio",
    "films",
    "film",
    "trailers",
    "trailer",
    "tv",
    "television",
    "news",
    "noticias",
    "radio",
    "label",
    "labels",
    "vevo",
    "entertainment",
    "official",
}

CREATOR_TITLE_BLOCKLIST = {
    "official trailer",
    "trailer oficial",
    "tráiler oficial",
    "teaser",
    "official mv",
    "video oficial",
    "official video",
    "lyric video",
    "lyrics",
    "letra",
    "con letra",
    "audio oficial",
    "banda sonora",
    "soundtrack",
    "episode",
    "capitulo completo",
    "full episode",
}


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
    tiktok_title: str
    tiktok_caption: str
    tiktok_hashtags: List[str]
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

TOPIC_HASHTAG_RULES = [
    (("minecraft", "creeper", "survival", "mod"), ["#minecraft", "#gaming", "#clipenespanol"]),
    (("roblox",), ["#roblox", "#gaming", "#clipenespanol"]),
    (("clash", "clash royale", "brawl", "brawl stars"), ["#gaming", "#mobilegaming", "#clipenespanol"]),
    (("fortnite",), ["#fortnite", "#gaming", "#clipenespanol"]),
    (("velada", "boxeo", "combate"), ["#boxeo", "#velada", "#clipenespanol"]),
    (("entrevista", "podcast", "charla"), ["#podcast", "#clips", "#clipenespanol"]),
    (("historia", "curiosidad", "dato"), ["#curiosidades", "#datos", "#clipenespanol"]),
]


SIGNAL_HASHTAG_RULES = {
    "Pregunta": ["#pregunta", "#debate"],
    "Dato": ["#dato", "#curiosidad"],
    "Impacto": ["#momentazo", "#impactante"],
    "Mucho texto": ["#storytime", "#clipenespanol"],
    "Audio alto": ["#reaccion", "#momentazo"],
    "Audio estable": ["#clipenespanol"],
    "Cambio escena": ["#momento", "#clipenespanol"],
    "Ritmo visual": ["#satisfying", "#clipenespanol"],
    "Buen audio": ["#audio", "#clipenespanol"],
    "Momento claro": ["#clipenespanol"],
}

BASE_TIKTOK_HASHTAGS = ["#clipenespanol"]

SOCIAL_BAD_EDGE_WORDS = {
    "a",
    "al",
    "asi",
    "bueno",
    "claro",
    "como",
    "con",
    "cuando",
    "de",
    "del",
    "el",
    "en",
    "entonces",
    "era",
    "es",
    "esta",
    "estamos",
    "esto",
    "ha",
    "hay",
    "la",
    "las",
    "lo",
    "los",
    "me",
    "mi",
    "nada",
    "no",
    "o",
    "osea",
    "para",
    "pero",
    "pues",
    "que",
    "se",
    "si",
    "sin",
    "un",
    "una",
    "vale",
    "vamos",
    "y",
    "ya",
    "yo",
}

SOCIAL_FILLER_WORDS = {
    "ah",
    "ahi",
    "bueno",
    "claro",
    "eh",
    "em",
    "esto",
    "literalmente",
    "nada",
    "osea",
    "pues",
    "rollo",
    "sabes",
    "tipo",
    "vale",
}

SOCIAL_BAD_PHRASES = (
    "estamos arropando",
    "claro nunca",
    "o sea",
    "en plan",
    "vamos a ver",
    "yo que se",
)

PLANNER_SLOTS = [
    {
        "slot_key": "lunch",
        "label": "Comida",
        "publish_time": "13:30",
        "strategy": "Clip facil de entender y con hook rapido para el primer pico del dia.",
    },
    {
        "slot_key": "afternoon",
        "label": "Tarde",
        "publish_time": "18:30",
        "strategy": "Momento con tension o dato claro para el tramo de salida del trabajo.",
    },
    {
        "slot_key": "prime",
        "label": "Prime",
        "publish_time": "21:30",
        "strategy": "La apuesta mas fuerte del dia: mayor alcance y mejor hook.",
    },
    {
        "slot_key": "late",
        "label": "Noche",
        "publish_time": "23:00",
        "strategy": "Ultimo disparo del dia: clip mas comentable o con giro.",
    },
]


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


def _norm_text(value: str) -> str:
    text = (value or "").strip().lower()
    return re.sub(r"\s+", " ", text)


def _looks_like_corporate_creator_block(candidate: VideoCandidate) -> bool:
    channel = _norm_text(candidate.channel)
    title = _norm_text(candidate.title)
    haystack = f"{channel} | {title}"
    if any(token in haystack for token in CREATOR_TITLE_BLOCKLIST):
        return True
    if any(token in channel for token in CREATOR_CHANNEL_BLOCKLIST):
        return True
    if re.search(r"\b(mv|ost|bso|trailer|teaser)\b", title):
        return True
    return False


def _clean_social_text(value: str, *, allow_punctuation: bool = True) -> str:
    text = html.unescape(str(value or ""))
    text = (
        text.replace("â€¦", "...")
        .replace("Ë‡", " ")
        .replace("Å¼", " ")
        .replace("ï¿½", " ")
    )
    text = re.sub(r"\[[^\]]+\]", " ", text)
    text = re.sub(r"\([^\)]*\)", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    if allow_punctuation:
        text = re.sub(r"[^\w\s?!.,:;/'%+\-]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n-_|:;,")
    if not allow_punctuation:
        text = re.sub(r"[^\w\s]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
    return text


def _truncate_copy(text: str, limit: int) -> str:
    clean = _clean_social_text(text)
    if len(clean) <= limit:
        return clean
    cut = clean[: limit - 1].rsplit(" ", 1)[0].strip()
    base = (cut or clean[: limit - 1].strip()).rstrip(". ")
    return base + "..."


def _dedupe_keep_order(items: List[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for item in items:
        key = item.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(item.strip())
    return ordered


def _looks_noisy_title(text: str) -> bool:
    raw = str(text or "")
    clean = _clean_social_text(raw)
    if not clean:
        return True
    if any(marker in raw for marker in ("â€¦", "ˇ", "ż", "�")):
        return True
    tokens = clean.split()
    if tokens and (sum(1 for token in tokens if len(token) == 1) / len(tokens)) > 0.18:
        return True
    letters = [char for char in clean if char.isalpha()]
    if len(clean) > 18 and letters:
        upper_ratio = sum(1 for char in letters if char.isupper()) / len(letters)
        if upper_ratio > 0.72:
            return True
    return False


def _build_topic_hashtags(*texts: str, signal_tags: List[str] | None = None) -> List[str]:
    haystack = " ".join(
        _clean_social_text(text, allow_punctuation=False).lower()
        for text in texts
        if text
    ).strip()
    hashtags: List[str] = []
    for keywords, tags in TOPIC_HASHTAG_RULES:
        if any(keyword in haystack for keyword in keywords):
            hashtags.extend(tags)
    for tag in signal_tags or []:
        hashtags.extend(SIGNAL_HASHTAG_RULES.get(tag, []))
    hashtags.extend(BASE_TIKTOK_HASHTAGS)
    clean_tags: List[str] = []
    for tag in hashtags:
        slug = "#" + slugify(tag.lstrip("#"), max_len=24).replace("-", "")
        if slug != "#":
            clean_tags.append(slug.lower())
    return _dedupe_keep_order(clean_tags)[:4]


def _strip_clip_prefix(text: str) -> str:
    clean = _clean_social_text(text)
    clean = re.sub(
        r"^(curiosidad/pregunta|dato concreto|momento impactante|momento explicativo|momento entretenido)\s*:\s*",
        "",
        clean,
        flags=re.IGNORECASE,
    )
    return clean.strip()


def _sentence_case(text: str) -> str:
    value = _strip_clip_prefix(text)
    if not value:
        return ""
    return value[0].upper() + value[1:]


def _channel_hashtag(source_channel: str) -> str:
    if not source_channel:
        return ""
    clean = _clean_social_text(source_channel, allow_punctuation=False)
    lowered = clean.lower()
    blocked = ("records", "official", "studios", "topic", "trailers", "pictures", "music")
    if any(token in lowered for token in blocked):
        return ""
    slug = "#" + slugify(clean, max_len=20).replace("-", "")
    if slug == "#" or len(slug) < 4:
        return ""
    return slug.lower()


def _build_caption_cta(signal_tags: List[str], hook: str, why_it_may_work: str) -> str:
    hook_clean = _strip_clip_prefix(hook)
    why_clean = _strip_clip_prefix(why_it_may_work)
    if "Pregunta" in signal_tags:
        return "Yo aqui no lo tenia nada claro."
    if "Dato" in signal_tags or re.search(r"\b\d+\b", hook_clean):
        return "Hay un detalle que cambia todo."
    if "Impacto" in signal_tags:
        return "El giro llega antes de lo que parece."
    if why_clean:
        why_clean = _truncate_copy(why_clean, 80)
        if why_clean:
            return why_clean[0].upper() + why_clean[1:]
    return "Tiene ese punto que te hace quedarte."


def _source_channel_line(source_channel: str, title: str) -> str:
    channel = _clean_social_text(source_channel, allow_punctuation=False).strip()
    if not channel:
        return ""
    norm_channel = _norm_text(channel)
    norm_title = _norm_text(title)
    if norm_channel and norm_channel in norm_title:
        return ""
    blocked = ("records", "official", "studios", "topic", "trailers", "pictures", "music")
    if any(token in channel.lower() for token in blocked):
        return ""
    return f"Visto en {channel}."


def _social_tokens(text: str) -> List[str]:
    return re.findall(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9]+", _clean_social_text(text), flags=re.UNICODE)


def _content_token_count(tokens: List[str]) -> int:
    return sum(
        1
        for token in tokens
        if any(ch.isdigit() for ch in token) or (len(token) > 2 and token.lower() not in SOCIAL_BAD_EDGE_WORDS)
    )


def _simple_spanish_stem(token: str) -> str:
    base = token.lower()
    for suffix in ("aciones", "acion", "mente", "amiento", "imientos", "imiento", "adora", "ador", "adoras", "adores"):
        if base.endswith(suffix) and len(base) > len(suffix) + 2:
            return base[: -len(suffix)]
    for suffix in ("ando", "iendo", "ados", "adas", "ado", "ada", "idos", "idas", "ido", "ida", "amos", "emos", "imos", "ar", "er", "ir", "es", "as", "os", "a", "o"):
        if base.endswith(suffix) and len(base) > len(suffix) + 2:
            return base[: -len(suffix)]
    return base


def _has_repeated_root_pattern(tokens: List[str]) -> bool:
    if len(tokens) < 3:
        return False
    connectors = {"no", "ni", "y", "o"}
    for idx in range(len(tokens) - 2):
        left = _simple_spanish_stem(tokens[idx])
        middle = tokens[idx + 1].lower()
        right = _simple_spanish_stem(tokens[idx + 2])
        if middle in connectors and len(left) >= 5 and left == right:
            return True
    return False


def _extract_social_snippets(text: str) -> List[str]:
    clean = _strip_clip_prefix(text)
    if not clean:
        return []

    snippets = [clean]
    tokens = _social_tokens(clean)
    if len(tokens) >= 6:
        snippets.append(" ".join(tokens[:6]))
    if len(tokens) >= 8:
        snippets.append(" ".join(tokens[:8]))
    for idx, token in enumerate(tokens):
        low = token.lower()
        if low == "o" or any(ch.isdigit() for ch in token):
            start = max(0, idx - 3)
            end = min(len(tokens), idx + 4)
            compact = " ".join(tokens[start:end]).strip()
            if len(compact) >= 8:
                snippets.append(compact)
    for chunk in re.split(r"[.!?…\n]+", clean):
        part = chunk.strip(" ,;:-")
        if len(part) >= 8:
            snippets.append(part)
    for chunk in re.split(r"\s+[—-]\s+|:\s+|;\s+|,\s+", clean):
        part = chunk.strip(" ,;:-")
        if len(part) >= 10:
            snippets.append(part)
    return _dedupe_keep_order(snippets)


def _looks_fragmentary_social_lead(text: str) -> bool:
    clean = _strip_clip_prefix(text)
    if not clean or _looks_noisy_title(clean):
        return True
    lowered = clean.lower()
    if any(phrase in lowered for phrase in SOCIAL_BAD_PHRASES):
        return True
    tokens = _social_tokens(clean)
    if len(tokens) < 2:
        return True
    if _content_token_count(tokens) < 2:
        return True
    if _has_repeated_root_pattern(tokens):
        return True
    if tokens[0].lower() in SOCIAL_BAD_EDGE_WORDS:
        return True
    if len(tokens) >= 4 and tokens[-1].lower() in SOCIAL_BAD_EDGE_WORDS:
        return True
    if len(tokens) >= 4 and (sum(1 for token in tokens if len(token) <= 2) / len(tokens)) > 0.34:
        return True
    if re.search(r"\b(?:claro|bueno|pues|osea|o sea|nada|vale|literalmente)$", lowered):
        return True
    return False


def _score_social_title_candidate(text: str, *, signal_tags: List[str], source_title: str) -> float:
    clean = _strip_clip_prefix(text)
    if not clean:
        return -999.0
    tokens = _social_tokens(clean)
    if _looks_fragmentary_social_lead(clean):
        return -200.0

    score = 0.0
    content_count = _content_token_count(tokens)
    score += min(16.0, content_count * 3.0)

    if "?" in clean:
        score += 22.0
    if re.search(r"\b\d+\b", clean):
        score += 14.0
    if re.search(r"\bo\b", clean, flags=re.IGNORECASE):
        score += 8.0
    if any(token.lower() in AI_HOOK_TERMS for token in tokens):
        score += 8.0
    if "Pregunta" in signal_tags and ("?" in clean or re.search(r"\bo\b", clean, flags=re.IGNORECASE)):
        score += 8.0
    if "Dato" in signal_tags and re.search(r"\b\d+\b", clean):
        score += 8.0
    if "Impacto" in signal_tags and any(term in clean.lower() for term in ("nunca", "brutal", "historia", "record", "reto")):
        score += 6.0

    length = len(clean)
    if 16 <= length <= 52:
        score += 12.0
    elif 10 <= length <= 68:
        score += 6.0
    else:
        score -= 8.0

    token_count = len(tokens)
    if 3 <= token_count <= 7:
        score += 10.0
    elif token_count <= 10:
        score += 4.0
    else:
        score -= 6.0

    if _norm_text(clean) == _norm_text(source_title):
        score -= 4.0
    if clean.lower().startswith(("estamos", "claro", "bueno", "nada", "pues", "vale")):
        score -= 10.0
    return score


def _select_social_title(
    *,
    source_title: str,
    hook: str,
    short_description: str,
    why_it_may_work: str,
    transcript_preview: str,
    signal_tags: List[str],
) -> str:
    focus_title = _strip_clip_prefix(extract_hook_focus_text(f"{short_description}. {transcript_preview}. {hook}"))
    raw_candidates: List[str] = []
    for text in (hook, focus_title, short_description, transcript_preview, why_it_may_work, source_title):
        raw_candidates.extend(_extract_social_snippets(text))

    scored: List[tuple[float, str]] = []
    for candidate in _dedupe_keep_order(raw_candidates):
        clean = _truncate_copy(_strip_clip_prefix(candidate), 72)
        if not clean:
            continue
        scored.append(
            (
                _score_social_title_candidate(clean, signal_tags=signal_tags, source_title=source_title),
                clean,
            )
        )

    scored.sort(key=lambda item: item[0], reverse=True)
    if scored and scored[0][0] > -50:
        return scored[0][1]

    fallback = _truncate_copy(_strip_clip_prefix(short_description or source_title or hook or "Clip viral del dia"), 72)
    if fallback and not _looks_fragmentary_social_lead(fallback):
        return fallback
    return _truncate_copy(_strip_clip_prefix(source_title or "Clip viral del dia"), 72)


def build_tiktok_copy(
    *,
    source_title: str,
    source_channel: str = "",
    hook: str,
    short_description: str,
    why_it_may_work: str,
    transcript_preview: str,
    signal_tags: List[str],
) -> tuple[str, str, List[str]]:
    title = _select_social_title(
        source_title=source_title,
        hook=hook,
        short_description=short_description,
        why_it_may_work=why_it_may_work,
        transcript_preview=transcript_preview,
        signal_tags=signal_tags,
    )
    title = _sentence_case(title).rstrip(" .")
    if title and title[-1] not in "!?" and (len(title.split()) > 3 or len(title) > 28):
        title = f"{title}..."

    caption_parts = [title] if title else []
    cta_line = _build_caption_cta(signal_tags, hook, why_it_may_work)
    if cta_line and _norm_text(cta_line) not in _norm_text(" ".join(caption_parts)):
        caption_parts.append(cta_line)
    source_line = _source_channel_line(source_channel, title)
    if source_line and _norm_text(source_line) not in _norm_text(" ".join(caption_parts)):
        caption_parts.append(source_line)
    caption = ""
    for idx, part in enumerate(part.strip() for part in caption_parts if part):
        if not caption:
            caption = part
            continue
        separator = " "
        if caption[-1] not in ".!?":
            separator = ". "
        caption = f"{caption}{separator}{part}"
    caption = _truncate_copy(caption, 170)
    hashtags = _build_topic_hashtags(
        source_title,
        source_channel,
        hook,
        short_description,
        transcript_preview,
        why_it_may_work,
        signal_tags=signal_tags,
    )
    channel_hashtag = _channel_hashtag(source_channel)
    if channel_hashtag and channel_hashtag not in hashtags:
        hashtags = [channel_hashtag, *hashtags][:3]
    else:
        hashtags = hashtags[:3]
    return title, caption, hashtags


def _creator_mode_candidates(candidates: List[VideoCandidate], log_fn: Callable[[str], None] | None = None) -> List[VideoCandidate]:
    def _log(message: str) -> None:
        if log_fn:
            log_fn(message)

    filtered: List[VideoCandidate] = []
    for c in candidates:
        duration = int(c.duration or 0)
        if duration < 6 * 60:
            continue
        if _looks_like_corporate_creator_block(c):
            continue
        filtered.append(c)
    _log(f"Filtro Creadores ES: {len(filtered)}/{len(candidates)} candidatos tras excluir labels/trailers/canales corporativos.")
    return filtered


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

    if yt_api_key and mode in {"viral_es", "creators_es"}:
        try:
            if mode == "creators_es":
                _log("Consultando charts oficiales de YouTube para ES y filtrando a videos de creadores...")
            else:
                _log("Consultando charts oficiales de YouTube para ES (sin preferencia de creador)...")
            candidates = discover_most_popular_es(
                api_key=yt_api_key,
                max_results=max(30, max_results * 4),
                category_ids=None,
                region_code="ES",
            )
            used_charts = bool(candidates)
            if yt_trend_categories:
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
            if mode == "creators_es":
                candidates = _creator_mode_candidates(candidates, log_fn=_log)
        except Exception as exc:
            raise RuntimeError(f"YouTube Data API fallo en modo {mode}: {exc}") from exc

    if mode == "creators_es" and len(candidates) < max_results:
        _log("Pool oficial de creadores escaso; completando con un pool curado de creadores españoles.")
        extra_channel_candidates = discover_from_channels(channels, per_channel_scan=max(6, min(per_channel_scan, 12)))
        seen_ids = {c.video_id for c in candidates if c.video_id}
        for c in extra_channel_candidates:
            if c.video_id and c.video_id in seen_ids:
                continue
            candidates.append(c)
            if c.video_id:
                seen_ids.add(c.video_id)

    if not candidates:
        if mode in {"viral_es", "creators_es"}:
            _log(f"Sin charts ES disponibles para {mode}. Usando fallback por canales; los resultados ya no son charts oficiales.")
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
        if mode == "creators_es":
            selected = _select_top_with_channel_cap(filtered, max_results=max_results, per_channel_cap=1)
            _log(f"Seleccion final Creadores ES por visitas: {len(selected)} videos")
        else:
            selected = filtered[:max_results]
            _log(f"Seleccion final por visitas ES: {len(selected)} videos")
    else:
        selected = _select_diverse_by_channel(filtered, max_results=max_results)
        channels_used = len({(c.channel or "").strip().lower() for c in selected if (c.channel or "").strip()})
        _log(f"Seleccion final: {len(selected)} videos de {max(1, channels_used)} canales")
    return selected


def _planner_title_traits(candidate: VideoCandidate) -> dict[str, bool]:
    title = _norm_text(candidate.title)
    return {
        "question": "?" in (candidate.title or ""),
        "digits": bool(re.search(r"\b\d+\b", title)),
        "comparison": bool(re.search(r"\bo\b|vs|versus", title)),
        "impact": any(term in title for term in ("nunca", "brutal", "historia", "record", "reto", "locura")),
        "story": any(term in title for term in ("historia", "entrevista", "charla", "podcast", "explica", "cuenta")),
    }


def _daily_plan_score(candidate: VideoCandidate, slot: dict[str, str], *, used_channels: set[str]) -> float:
    channel_key = _channel_key(candidate)
    if channel_key in used_channels:
        return -999.0

    traits = _planner_title_traits(candidate)
    score = float(candidate.ai_score or 0.0) * 0.62
    score += min(22.0, math.log10(float(candidate.views_per_day or 0.0) + 1.0) * 10.0)
    score += min(14.0, math.log10(float(candidate.view_count or 0.0) + 1.0) * 4.0)

    slot_key = slot.get("slot_key", "")
    if slot_key == "lunch":
        if traits["question"] or traits["comparison"]:
            score += 9.0
        if traits["digits"]:
            score += 5.0
    elif slot_key == "afternoon":
        if traits["impact"]:
            score += 8.0
        if traits["digits"]:
            score += 4.0
    elif slot_key == "prime":
        if traits["impact"]:
            score += 10.0
        score += min(10.0, math.log10(float(candidate.view_count or 0.0) + 1.0) * 1.2)
    elif slot_key == "late":
        if traits["story"] or traits["question"]:
            score += 8.0
        if traits["comparison"]:
            score += 4.0

    return score


def _plan_entry(candidate: VideoCandidate, *, slot: dict[str, str], plan_score: float, role: str) -> dict[str, Any]:
    views = int(candidate.view_count or 0)
    views_per_day = float(candidate.views_per_day or 0.0)
    return {
        "slot_key": slot.get("slot_key", role),
        "slot_label": slot.get("label", role.title()),
        "publish_time": slot.get("publish_time", ""),
        "strategy": slot.get("strategy", ""),
        "role": role,
        "plan_score": round(plan_score, 1),
        "reason": candidate.ai_reason or "equilibrio general de señales",
        "candidate": candidate,
        "summary": f"{candidate.channel} | {views:,} views | {views_per_day:,.0f}/dia".replace(",", "."),
    }


def _slot_minutes(slot: dict[str, str]) -> int:
    raw = str(slot.get("publish_time") or "00:00").strip()
    try:
        hour_s, minute_s = raw.split(":", 1)
        return int(hour_s) * 60 + int(minute_s)
    except Exception:
        return 0


def _pick_active_slots(posts_per_day: int) -> list[dict[str, str]]:
    posts_per_day = max(1, min(posts_per_day, len(PLANNER_SLOTS)))
    if posts_per_day >= len(PLANNER_SLOTS):
        return list(PLANNER_SLOTS)

    timezone_name = os.getenv("PLAN_TIMEZONE", "Europe/Madrid").strip() or "Europe/Madrid"
    try:
        now_local = datetime.now(ZoneInfo(timezone_name))
    except Exception:
        now_local = datetime.now()
    now_minutes = now_local.hour * 60 + now_local.minute

    ranked = sorted(
        enumerate(PLANNER_SLOTS),
        key=lambda pair: abs(_slot_minutes(pair[1]) - now_minutes),
    )
    selected_indexes = sorted(idx for idx, _slot in ranked[:posts_per_day])
    return [PLANNER_SLOTS[idx] for idx in selected_indexes]


def _build_slot_alternatives(
    candidates: List[VideoCandidate],
    *,
    slot: dict[str, str],
    primary_candidate: VideoCandidate,
    slot_option_count: int,
    blocked_channels: set[str],
) -> list[dict[str, Any]]:
    if slot_option_count <= 1:
        return []

    selected_channels = {_channel_key(primary_candidate)}
    alternatives: list[dict[str, Any]] = []
    ranked = sorted(
        candidates,
        key=lambda candidate: _daily_plan_score(candidate, slot, used_channels=set()),
        reverse=True,
    )

    for candidate in ranked:
        channel_key = _channel_key(candidate)
        if channel_key in selected_channels:
            continue
        if channel_key in blocked_channels:
            continue
        score = _daily_plan_score(candidate, slot, used_channels=set())
        alternatives.append(_plan_entry(candidate, slot=slot, plan_score=score, role="alternative"))
        selected_channels.add(channel_key)
        if len(alternatives) >= slot_option_count - 1:
            return alternatives

    for candidate in ranked:
        channel_key = _channel_key(candidate)
        if channel_key in selected_channels:
            continue
        score = _daily_plan_score(candidate, slot, used_channels=set())
        alternatives.append(_plan_entry(candidate, slot=slot, plan_score=score, role="alternative"))
        selected_channels.add(channel_key)
        if len(alternatives) >= slot_option_count - 1:
            break

    return alternatives


def build_daily_post_plan(
    *,
    channels: List[str] | None = None,
    per_channel_scan: int = 12,
    this_week_only: bool = True,
    min_source_duration: int = 90,
    max_results: int = 18,
    posts_per_day: int = 4,
    reserve_count: int = 2,
    slot_option_count: int = 3,
    mode: str = "creators_es",
    log_fn: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    posts_per_day = max(1, min(posts_per_day, len(PLANNER_SLOTS)))
    reserve_count = max(0, min(reserve_count, 4))
    slot_option_count = max(1, min(slot_option_count, 3))

    candidates = discover_creator_videos(
        channels=channels,
        per_channel_scan=per_channel_scan,
        this_week_only=this_week_only,
        min_source_duration=min_source_duration,
        max_results=max(max_results, posts_per_day + reserve_count + 4),
        mode=mode,
        log_fn=log_fn,
    )

    if not candidates:
        return {
            "date": date.today().isoformat(),
            "timezone": os.getenv("PLAN_TIMEZONE", "Europe/Madrid"),
            "mode": mode,
            "slots": [],
            "reserves": [],
            "notes": "Sin candidatos suficientes para construir el plan de hoy.",
        }

    used_channels: set[str] = set()
    planned: List[dict[str, Any]] = []
    remaining = list(candidates)

    active_slots = _pick_active_slots(posts_per_day)
    for slot in active_slots:
        best = max(remaining, key=lambda candidate: _daily_plan_score(candidate, slot, used_channels=used_channels))
        best_score = _daily_plan_score(best, slot, used_channels=used_channels)
        planned.append(_plan_entry(best, slot=slot, plan_score=best_score, role="planned"))
        used_channels.add(_channel_key(best))
        remaining = [candidate for candidate in remaining if _channel_key(candidate) != _channel_key(best)]
        if not remaining:
            break

    planned_channel_keys = {
        _channel_key(entry["candidate"])
        for entry in planned
        if entry.get("candidate") is not None
    }
    for entry in planned:
        candidate = entry.get("candidate")
        if candidate is None:
            entry["alternatives"] = []
            continue
        blocked = {key for key in planned_channel_keys if key != _channel_key(candidate)}
        entry["alternatives"] = _build_slot_alternatives(
            candidates,
            slot={
                "slot_key": str(entry.get("slot_key") or ""),
                "label": str(entry.get("slot_label") or ""),
                "publish_time": str(entry.get("publish_time") or ""),
                "strategy": str(entry.get("strategy") or ""),
            },
            primary_candidate=candidate,
            slot_option_count=slot_option_count,
            blocked_channels=blocked,
        )

    reserves: List[dict[str, Any]] = []
    for candidate in remaining:
        reserve_slot = {
            "slot_key": "reserve",
            "label": "Reserva",
            "publish_time": "",
            "strategy": "Guardado por si uno de los clips planeados no convence o toca sustituir creador.",
        }
        reserves.append(
            _plan_entry(
                candidate,
                slot=reserve_slot,
                plan_score=float(candidate.ai_score or 0.0),
                role="reserve",
            )
        )
        if len(reserves) >= reserve_count:
            break

    notes = "Ventanas sugeridas para probar hoy en España. La idea es comparar rendimiento real en @pixelboom8 y luego afinar por histórico."
    if len(planned) < posts_per_day:
        notes += f" Hoy solo salieron {len(planned)} creadores distintos con señal suficiente; mejor eso que forzar repetidos."
    return {
        "date": date.today().isoformat(),
        "timezone": os.getenv("PLAN_TIMEZONE", "Europe/Madrid"),
        "mode": mode,
        "posts_per_day": posts_per_day,
        "reserve_count": reserve_count,
        "slot_option_count": slot_option_count,
        "slots": planned,
        "reserves": reserves,
        "notes": notes,
    }


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


def _select_top_with_channel_cap(
    candidates: List[VideoCandidate],
    max_results: int,
    per_channel_cap: int = 1,
) -> List[VideoCandidate]:
    if not candidates or max_results <= 0:
        return []

    selected: List[VideoCandidate] = []
    leftovers: List[VideoCandidate] = []
    counts: dict[str, int] = {}

    for c in candidates:
        key = _channel_key(c)
        current = counts.get(key, 0)
        if current < per_channel_cap:
            selected.append(c)
            counts[key] = current + 1
            if len(selected) >= max_results:
                return selected
        else:
            leftovers.append(c)

    for c in leftovers:
        selected.append(c)
        if len(selected) >= max_results:
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
    cleaned_parts = [clean_caption_text(c.text) for c in cues]
    text = " ".join(part for part in cleaned_parts if part)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    trimmed = text[: max_chars - 1]
    if " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0]
    return f"{trimmed}..."


def build_signal_tags(
    *,
    cue_count: int,
    speech_density: float,
    question_hits: int,
    exclaim_hits: int,
    number_hits: int,
    opening_hook_score: float,
    audio_score: float,
    visual_score: float,
    scene_cut_count: int,
) -> List[str]:
    tags: List[str] = []
    if opening_hook_score >= 7.5:
        tags.append("Hook fuerte")
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

    cleaned_cues = [clean_caption_text(c.text) for c in in_window]
    cleaned_cues = [text for text in cleaned_cues if text]
    if not cleaned_cues:
        return "Momento destacado", "Fragmento con potencial de retencion."

    joined = " ".join(cleaned_cues)
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
    for txt in cleaned_cues:
        if len(txt) >= 22:
            sample = txt
            break
    if not sample:
        sample = cleaned_cues[0]
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
    opening_blob = " ".join(cue.text for cue in opening_cues[:4])
    opening_focus_text = extract_hook_focus_text(opening_blob)
    opening_focus_bonus = 0.0
    if opening_focus_text:
        focus_tokens = re.findall(r"\w+", opening_focus_text, flags=re.UNICODE)
        opening_focus_bonus += min(14.0, len(focus_tokens) * 3.2)
        if any(token.isdigit() for token in focus_tokens):
            opening_focus_bonus += 4.0
        if " o " in f" {opening_focus_text.lower()} ":
            opening_focus_bonus += 4.0
    opening_word_count = len(re.findall(r"\w+", opening_blob, flags=re.UNICODE))
    opening_density = opening_word_count / max(1.0, min(4.0, duration))
    dead_air_start = max(0.0, min(4.0, in_window[0].start - start))
    dead_air_end = max(0.0, min(4.0, end - in_window[-1].end))

    interest_score = (
        speech_density * 11.5
        + diversity_rate * 14.0
        + impact_rate * 8.0
        + interest_hits * 1.8
        + punctuation_bonus * 0.35
        + opening_hook_score * 2.2
        + opening_focus_bonus * 1.5
        + opening_density * 3.5
    )
    reach_score = (
        speech_density * 9.5
        + curiosity_rate * 90.0
        + number_rate * 120.0
        + question_rate * 120.0
        + impact_rate * 6.0
        + exclaim_hits * 0.2
        + opening_hook_score * 1.4
        + opening_focus_bonus * 1.2
        + opening_density * 2.8
    )
    audio_score = window_audio_score(rms_by_second or {}, start, end)
    visual_score = window_visual_score(scene_times or [], start, end)
    scene_cut_count = window_scene_cut_count(scene_times or [], start, end)
    combined = (
        interest_score * 0.44
        + reach_score * 0.31
        + audio_score * 0.12
        + visual_score * 0.09
        + opening_hook_score * 1.3
        + opening_focus_bonus * 1.1
    )
    combined -= dead_air_start * 8.5
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
        opening_hook_score=opening_hook_score,
        audio_score=audio_score,
        visual_score=visual_score,
        scene_cut_count=scene_cut_count,
    )
    if audio_score >= 65.0:
        why = f"{why} Audio con energia alta."
    if visual_score >= 55.0:
        why = f"{why} Ritmo visual dinamico."
    if opening_focus_bonus >= 9.0 or opening_hook_score >= 8.0:
        why = f"{why} Entra con hook claro en los primeros segundos."
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
                        opening_hook_score=0.0,
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
        hashtags_html = "".join(f'<span class="tag social">{html.escape(tag)}</span>' for tag in opt.tiktok_hashtags)
        transcript_html = (
            f'<p class="transcript">{html.escape(opt.transcript_preview)}</p>' if opt.transcript_preview else ""
        )
        social_html = f"""
              <div class="social-pack">
                <p class="social-title">{html.escape(opt.tiktok_title)}</p>
                <p class="social-caption">{html.escape(opt.tiktok_caption)}</p>
                <div class="signals social-tags">{hashtags_html}</div>
              </div>
        """
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
              {social_html}
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
    .social-pack {{
      margin: 10px 0 8px 0;
      padding: 10px;
      background: #111722;
      border: 1px solid #2b3443;
      border-radius: 10px;
    }}
    .social-title {{
      margin: 0 0 6px 0;
      color: #fff0e2;
      font-size: 14px;
      font-weight: 700;
    }}
    .social-caption {{
      margin: 0;
      color: #dbe7f5;
      font-size: 13px;
      line-height: 1.45;
    }}
    .social-tags {{
      margin-top: 8px;
      margin-bottom: 0;
    }}
    .tag.social {{
      background: #192133;
      color: #ffd5b4;
      border-color: #523d30;
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
        tiktok_title, tiktok_caption, tiktok_hashtags = build_tiktok_copy(
            source_title=source_title,
            source_channel=str(info.get("channel") or info.get("uploader") or ""),
            hook=seg.hook,
            short_description=cand.short_description,
            why_it_may_work=cand.why_it_may_work,
            transcript_preview=cand.transcript_preview,
            signal_tags=cand.signal_tags,
        )
        overlay_hook_text = (
            _strip_clip_prefix(tiktok_title).rstrip(".!?… ").strip()
            or _strip_clip_prefix(extract_hook_focus_text(seg.hook)).rstrip(".!?… ").strip()
            or _strip_clip_prefix(cand.short_description).rstrip(".!?… ").strip()
            or _strip_clip_prefix(cand.transcript_preview).rstrip(".!?… ").strip()
        )
        log_fn(f"Render option {idx}/{len(selected)} ({seg.start:.1f}s -> {seg.end:.1f}s)")
        subtitle_ass = job_dir / f"option_{idx:02}.ass"
        if cues:
            try:
                if not write_segment_ass(cues, seg.start, seg.end, subtitle_ass, hook_text=overlay_hook_text):
                    subtitle_ass = None
            except Exception:
                subtitle_ass = None
        else:
            subtitle_ass = None
        render_short(
            ffmpeg_bin=ffmpeg_bin,
            input_video=source_video,
            output_video=job_dir / preview_name,
            segment=seg,
            hook_text=overlay_hook_text,
            subtitle_ass=subtitle_ass,
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
                tiktok_title=tiktok_title,
                tiktok_caption=tiktok_caption,
                tiktok_hashtags=tiktok_hashtags,
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





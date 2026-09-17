import asyncio
import hashlib
import ipaddress
import json
import mimetypes
import os
import re
import secrets
import shutil
import socket
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from http.cookies import SimpleCookie

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

APP_VERSION = "6.5.1"
SESSION_TTL = int(os.getenv("SESSION_TTL_SECONDS", "1800"))
JOB_TTL = int(os.getenv("JOB_TTL_SECONDS", "1800"))
API_KEY = os.getenv("ADAPTER_API_KEY", "").strip()
YTDLP = os.getenv("YTDLP_BIN", "/opt/venv/bin/yt-dlp")
POT_URL = os.getenv("POT_PROVIDER_URL", "http://127.0.0.1:4416").rstrip("/")
MAX_ANALYZE_SECONDS = int(os.getenv("MAX_ANALYZE_SECONDS", "90"))
MAX_PREPARE_SECONDS = int(os.getenv("MAX_PREPARE_SECONDS", "900"))
MAX_PREVIEW_SECONDS = int(os.getenv("MAX_PREVIEW_SECONDS", "240"))
PREVIEW_CACHE_ROOT = Path(os.getenv("PREVIEW_CACHE_ROOT", "/tmp/anything-downloader-preview"))
PREVIEW_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
FFMPEG = os.getenv("FFMPEG_BIN", shutil.which("ffmpeg") or "ffmpeg")

app = FastAPI(title="Anything Downloader Acquisition Adapter", version=APP_VERSION)
SESSIONS: dict[str, dict[str, Any]] = {}
JOBS: dict[str, dict[str, Any]] = {}
ACTIVE_TASKS: set[asyncio.Task] = set()
PREVIEW_LOCKS: dict[str, asyncio.Lock] = {}


class AnalyzeRequest(BaseModel):
    url: str


class PrepareRequest(BaseModel):
    session: str
    choiceId: str
    start: float | None = None
    end: float | None = None


def require_auth(authorization: str | None) -> None:
    if API_KEY and authorization != f"Bearer {API_KEY}":
        raise HTTPException(401, "Unauthorized")


def safe_name(value: str, fallback: str = "media") -> str:
    keep = "".join(c if c not in '<>:"/\\|?*\x00\r\n\t' else "_" for c in str(value or ""))
    keep = " ".join(keep.split()).strip(" .")[:140]
    return keep or fallback


def cleanup() -> None:
    now = time.time()
    for key in list(SESSIONS):
        if SESSIONS[key]["expires"] < now:
            rec = SESSIONS.pop(key, None) or {}
            preview_dir = rec.get("preview_dir")
            if preview_dir:
                shutil.rmtree(preview_dir, ignore_errors=True)
            for lock_key in [k for k in PREVIEW_LOCKS if k.startswith(f"{key}:")]:
                PREVIEW_LOCKS.pop(lock_key, None)
    for key in list(JOBS):
        rec = JOBS[key]
        if rec.get("expires", 0) < now:
            path = rec.get("path")
            directory = rec.get("directory")
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass
            if directory:
                shutil.rmtree(directory, ignore_errors=True)
            JOBS.pop(key, None)


async def validate_public_https(raw: str) -> str:
    raw = raw.strip()
    try:
        u = urlparse(raw)
    except Exception:
        raise HTTPException(400, "Invalid URL")
    if u.scheme != "https" or not u.hostname:
        raise HTTPException(400, "Only HTTPS URLs are accepted")
    host = u.hostname.lower()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        raise HTTPException(400, "Local/private hosts are not accepted")
    try:
        ip = ipaddress.ip_address(host)
        if not ip.is_global:
            raise HTTPException(400, "Local/private IP addresses are not accepted")
    except ValueError:
        try:
            infos = await asyncio.get_running_loop().run_in_executor(
                None, lambda: socket.getaddrinfo(host, u.port or 443, type=socket.SOCK_STREAM)
            )
            for info in infos:
                addr = info[4][0].split("%", 1)[0]
                ip = ipaddress.ip_address(addr)
                if not ip.is_global:
                    raise HTTPException(400, "Source hostname resolves to a local/private address")
        except HTTPException:
            raise
        except Exception:
            # Let yt-dlp produce the useful network/DNS error later.
            pass
    return raw






def cookie_header_for_url(cookie_dump: Any, media_url: str) -> str | None:
    """Return only cookies whose Domain/Path match the resolved media URL.

    yt-dlp may attach short-lived cookies to extracted formats. Replaying the
    resolved URL without those cookies can produce 403 responses on some CDNs.
    Do not forward unrelated cookies to third-party media hosts.
    """
    if not isinstance(cookie_dump, str) or not cookie_dump.strip():
        return None
    try:
        parsed = urlparse(media_url)
        host = (parsed.hostname or "").lower()
        path = parsed.path or "/"
        jar = SimpleCookie()
        jar.load(cookie_dump)
    except Exception:
        return None

    pairs: list[str] = []
    for name, morsel in jar.items():
        domain = str(morsel["domain"] or "").strip().lower().lstrip(".")
        if domain and not (host == domain or host.endswith("." + domain)):
            continue
        cookie_path = str(morsel["path"] or "/").strip() or "/"
        if not path.startswith(cookie_path):
            continue
        secure = bool(morsel["secure"])
        if secure and parsed.scheme != "https":
            continue
        pairs.append(f"{name}={morsel.coded_value}")
    return "; ".join(pairs) or None


def merged_media_headers(info: dict[str, Any], f: dict[str, Any]) -> dict[str, str]:
    """Merge page-level and format-level request context for resolved media."""
    headers: dict[str, str] = {}
    for source in (info.get("http_headers"), f.get("http_headers")):
        if not isinstance(source, dict):
            continue
        for k, v in source.items():
            if k and v is not None:
                headers[str(k)] = str(v)

    media_url = f.get("url")
    if isinstance(media_url, str) and not any(k.lower() == "cookie" for k in headers):
        cookie_dump = f.get("cookies") or info.get("cookies")
        cookie_header = cookie_header_for_url(cookie_dump, media_url)
        if cookie_header:
            headers["Cookie"] = cookie_header
    return headers

def infer_ext_from_url(url: str) -> str | None:
    try:
        suffix = Path(urlparse(url).path).suffix.lower().lstrip(".")
    except Exception:
        return None
    return suffix if suffix in {"mp4", "webm", "mov", "m4v", "flv", "ts", "m3u8", "mp3", "m4a", "aac", "opus", "ogg"} else None


def quality_dimension(f: dict[str, Any]) -> int | None:
    """Return the conventional video quality dimension.

    yt-dlp's `res` sort key is based on the smaller frame dimension. That is
    what users expect from labels such as 360p/720p/1080p, and it also avoids
    mislabeling portrait 1080x1920 video as 1920p.
    """
    def posint(v):
        try:
            n = int(float(v))
            return n if n > 0 else None
        except (TypeError, ValueError):
            return None

    w, h = posint(f.get("width")), posint(f.get("height"))
    if w and h:
        return min(w, h)

    resolution = str(f.get("resolution") or "")
    m = re.search(r"(\d{2,5})\s*[xX×]\s*(\d{2,5})", resolution)
    if m:
        return min(int(m.group(1)), int(m.group(2)))

    # Prefer an explicit 720p/1080p-style note over a lone width/height field.
    # Sparse extractors occasionally populate only one physical dimension.
    note = " ".join(str(f.get(k) or "") for k in ("format_note", "format", "quality"))
    m = re.search(r"(?<!\d)(\d{3,4})p(?!\d)", note, flags=re.I)
    if m:
        return int(m.group(1))

    return h or w


def normalize_sparse_formats(info: dict[str, Any], formats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fill only metadata needed by our delivery layer when an extractor is sparse.

    Some extractors (notably Kick Clips) may legally return a format containing
    little more than a direct URL. yt-dlp can still download it, so absence of
    format_id/height/protocol must not make us discard the media.
    """
    out: list[dict[str, Any]] = []
    used_ids: set[str] = set()
    for idx, original in enumerate(formats, 1):
        f = dict(original)
        url = f.get("url")
        if isinstance(url, str):
            guessed_ext = infer_ext_from_url(url)
            if not f.get("ext"):
                f["ext"] = guessed_ext or info.get("ext")
            if not f.get("protocol"):
                if str(f.get("ext") or guessed_ext or "").lower() == "m3u8":
                    f["protocol"] = "m3u8_native"
                elif url.startswith("https://"):
                    f["protocol"] = "https"
                elif url.startswith("http://"):
                    f["protocol"] = "http"

        # Only inherit page-level dimensions when this is genuinely one source
        # format. With multiple renditions (Facebook is a common example), the
        # top-level dimensions describe the overall media and are NOT evidence
        # that every sparse SD/HD URL has that resolution. Copying them onto each
        # format can label a 360p file as 1080p/1920p/4K.
        if len(formats) == 1:
            if not f.get("height") and info.get("height"):
                f["height"] = info.get("height")
            if not f.get("width") and info.get("width"):
                f["width"] = info.get("width")

        # yt-dlp often stores required media request context (for example a
        # Referer) at the info-dict level rather than repeating it on every
        # format. Resolve that inheritance now so relay/preview/FFmpeg requests
        # faithfully replay the extractor's request context.
        f["http_headers"] = merged_media_headers(info, f)

        fid = str(f.get("format_id") or "").strip()
        if not fid:
            fid = "source" if len(formats) == 1 else f"source-{idx}"
        base = fid
        n = 2
        while fid in used_ids:
            fid = f"{base}-{n}"
            n += 1
        used_ids.add(fid)
        f["format_id"] = fid
        out.append(f)
    return out

def protocol_name(f: dict[str, Any]) -> str:
    return str(f.get("protocol") or "").lower()


def has_usable_url(f: dict[str, Any]) -> bool:
    url = f.get("url")
    return isinstance(url, str) and url.startswith("https://") and not f.get("has_drm")


def is_progressive_http_format(f: dict[str, Any]) -> bool:
    if not has_usable_url(f):
        return False
    p = protocol_name(f)
    return p in {"https", "http", "https_native", "http_native"}


def is_fragmented_format(f: dict[str, Any]) -> bool:
    if not has_usable_url(f):
        return False
    p = protocol_name(f)
    return p in {
        "m3u8", "m3u8_native", "http_dash_segments", "dash", "f4m", "ism", "mss",
    } or "dash" in p or "m3u8" in p


def public_format(f: dict[str, Any]) -> dict[str, Any]:
    delivery = "browser" if is_progressive_http_format(f) else "server" if is_fragmented_format(f) else "detected"
    return {
        "id": str(f.get("format_id") or ""),
        "label": f.get("format_note") or f.get("resolution") or f.get("format") or str(f.get("format_id") or "format"),
        "qualityLabel": (f"{quality_dimension(f)}p" if quality_dimension(f) else (f.get("resolution") if f.get("resolution") not in {None, "audio only"} else None)),
        "ext": f.get("ext"),
        "protocol": f.get("protocol"),
        "width": f.get("width"),
        "height": f.get("height"),
        "fps": f.get("fps"),
        "vcodec": f.get("vcodec"),
        "acodec": f.get("acodec"),
        "tbr": f.get("tbr"),
        "abr": f.get("abr"),
        "filesize": f.get("filesize") or f.get("filesize_approx"),
        "hasVideo": is_video_format(f),
        "hasAudio": (str(f.get("acodec") or "").lower() != "none" and (f.get("acodec") not in {None, ""} or is_progressive_http_format(f))),
        "delivery": delivery,
        "direct": delivery == "browser",
        "hasDrm": bool(f.get("has_drm")),
    }


def codec_score(f: dict[str, Any]) -> float:
    ext = str(f.get("ext") or "").lower()
    vcodec = str(f.get("vcodec") or "").lower()
    score = float(f.get("tbr") or f.get("vbr") or 0)
    if is_progressive_http_format(f):
        score += 250000
    if ext in {"mp4", "m4v"}:
        score += 50000
    if vcodec.startswith("avc1") or vcodec.startswith("h264"):
        score += 30000
    elif "vp9" in vcodec or vcodec.startswith("vp0"):
        score += 20000
    elif vcodec.startswith("av01"):
        score += 10000
    return score


def audio_score(f: dict[str, Any], preferred_family: str) -> float:
    ext = str(f.get("ext") or "").lower()
    acodec = str(f.get("acodec") or "").lower()
    score = float(f.get("abr") or f.get("tbr") or 0)
    if is_progressive_http_format(f):
        score += 250000
    if preferred_family == "mp4" and (ext in {"m4a", "mp4"} or "mp4a" in acodec or "aac" in acodec):
        score += 100000
    if preferred_family == "webm" and (ext == "webm" or "opus" in acodec or "vorbis" in acodec):
        score += 100000
    return score


def is_video_format(f: dict[str, Any]) -> bool:
    """Treat missing codec metadata as unknown, not audio-only.

    Several yt-dlp extractors (notably Twitch Clips) return direct video
    qualities with URL/height/FPS but omit vcodec/acodec. Only an explicit
    vcodec == "none" is reliable evidence that a format is audio-only.
    """
    vcodec = f.get("vcodec")
    if isinstance(vcodec, str) and vcodec.lower() == "none":
        return False
    if vcodec not in {None, ""}:
        return True
    if f.get("height") or f.get("width"):
        return True
    ext = str(f.get("ext") or "").lower()
    return ext in {"mp4", "webm", "mov", "m4v", "flv", "ts"}


def is_audio_only_format(f: dict[str, Any]) -> bool:
    vcodec = f.get("vcodec")
    acodec = f.get("acodec")
    return isinstance(vcodec, str) and vcodec.lower() == "none" and acodec not in {None, "", "none"}


def video_codec_family(f: dict[str, Any]) -> str:
    v = str(f.get("vcodec") or "").lower()
    if any(x in v for x in ("h265", "hevc", "hvc1", "hev1", "bytevc1")):
        return "h265"
    if v.startswith("avc1") or "h264" in v or v.startswith("avc"):
        return "h264"
    if "av01" in v:
        return "av1"
    if "vp9" in v or v.startswith("vp0"):
        return "vp9"
    return "other"


def codec_label(family: str, raw: Any = None) -> str:
    return {
        "h264": "H.264",
        "h265": "H.265",
        "av1": "AV1",
        "vp9": "VP9",
    }.get(family, str(raw or "Source codec"))


def choose_audio_donor(
    chosen: dict[str, Any],
    audio_only: list[dict[str, Any]],
    combined_audio_donors: list[dict[str, Any]],
) -> dict[str, Any] | None:
    family = "webm" if str(chosen.get("ext") or "").lower() == "webm" else "mp4"
    if audio_only:
        return max(audio_only, key=lambda f: audio_score(f, family))
    if not combined_audio_donors:
        return None

    def donor_score(f):
        ext = str(f.get("ext") or "").lower()
        acodec = str(f.get("acodec") or "").lower()
        family_match = (
            family == "mp4" and (ext in {"mp4", "m4a"} or "aac" in acodec or "mp4a" in acodec)
        ) or (
            family == "webm" and (ext == "webm" or "opus" in acodec or "vorbis" in acodec)
        )
        size = int(f.get("filesize") or f.get("filesize_approx") or 2**62)
        height = int(f.get("height") or 0)
        return (0 if family_match else 1, size, height)

    return min(combined_audio_donors, key=donor_score)


def make_choices(formats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    usable = [f for f in formats if has_usable_url(f)]
    video = [f for f in usable if is_video_format(f) and quality_dimension(f)]
    unranked_video = [f for f in usable if is_video_format(f) and not quality_dimension(f)]
    audio = [f for f in usable if is_audio_only_format(f)]
    combined_audio_donors = [
        f for f in usable
        if is_video_format(f) and str(f.get("acodec") or "").lower() not in {"", "none"}
    ]
    by_height: dict[int, list[dict[str, Any]]] = {}
    for f in video:
        q = quality_dimension(f)
        if q:
            by_height.setdefault(int(q), []).append(f)

    choices: list[dict[str, Any]] = []
    for height in sorted(by_height, reverse=True):
        candidates = sorted(by_height[height], key=codec_score, reverse=True)
        families: dict[str, list[dict[str, Any]]] = {}
        for f in candidates:
            families.setdefault(video_codec_family(f), []).append(f)

        # If H.264 and H.265 are both present at the same resolution, expose both.
        # Otherwise keep the best available codec so existing site behavior stays compact.
        ordered_families = [x for x in ("h264", "h265") if x in families]
        if not ordered_families:
            ordered_families = [video_codec_family(candidates[0])]

        for codec_family in ordered_families:
            group = families[codec_family]
            combined = [
                f for f in group
                if f.get("acodec") not in {None, "none"}
                or (is_progressive_http_format(f) and f.get("acodec") is None)
            ]
            chosen = combined[0] if combined else group[0]
            family = "webm" if str(chosen.get("ext") or "").lower() == "webm" else "mp4"
            audio_f = None
            if str(chosen.get("acodec") or "").lower() == "none":
                audio_f = choose_audio_donor(chosen, audio, combined_audio_donors)

            browser_delivery = is_progressive_http_format(chosen) and (audio_f is None or is_progressive_http_format(audio_f))
            delivery = "browser" if browser_delivery else "server"
            ext = "webm" if family == "webm" else "mp4"
            size = (chosen.get("filesize") or chosen.get("filesize_approx") or 0) + ((audio_f or {}).get("filesize") or (audio_f or {}).get("filesize_approx") or 0)
            video_id = str(chosen.get("format_id"))
            audio_id = str(audio_f.get("format_id")) if audio_f else None
            selector = f"{video_id}+{audio_id}" if audio_id else video_id
            cid = f"{height}-{codec_family}-{video_id}" + (f"-{audio_id}" if audio_id else "")
            choices.append({
                "id": cid,
                "label": f"{height}p" + (" (4K)" if height >= 2160 else ""),
                "height": height,
                "fps": chosen.get("fps"),
                "container": ext,
                "videoCodec": chosen.get("vcodec"),
                "codecFamily": codec_family,
                "codecLabel": codec_label(codec_family, chosen.get("vcodec")),
                "audioCodec": chosen.get("acodec") if not audio_f else audio_f.get("acodec"),
                "videoFormatId": video_id,
                "audioFormatId": audio_id,
                "videoExt": chosen.get("ext") or ext,
                "audioExt": audio_f.get("ext") if audio_f else None,
                "combined": audio_f is None and (chosen.get("acodec") not in {"none"}),
                "filesize": size or None,
                "delivery": delivery,
                "formatSelector": selector,
            })

    if not choices and unranked_video:
        chosen = max(unranked_video, key=codec_score)
        family = "webm" if str(chosen.get("ext") or "").lower() == "webm" else "mp4"
        video_id = str(chosen.get("format_id") or "source")
        browser_delivery = is_progressive_http_format(chosen)
        cf = video_codec_family(chosen)
        choices.append({
            "id": f"source-{video_id}",
            "label": chosen.get("format_note") or chosen.get("resolution") or "Source",
            "height": None,
            "fps": chosen.get("fps"),
            "container": family,
            "videoCodec": chosen.get("vcodec"),
            "codecFamily": cf,
            "codecLabel": codec_label(cf, chosen.get("vcodec")),
            "audioCodec": chosen.get("acodec"),
            "videoFormatId": video_id,
            "audioFormatId": None,
            "videoExt": chosen.get("ext") or family,
            "audioExt": None,
            "combined": True,
            "filesize": chosen.get("filesize") or chosen.get("filesize_approx"),
            "delivery": "browser" if browser_delivery else "server",
            "formatSelector": video_id,
        })
    return choices


def make_audio_choices(formats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    usable = [f for f in formats if has_usable_url(f)]
    audio_only = [f for f in usable if is_audio_only_format(f)]
    combined = [
        f for f in usable
        if is_video_format(f) and str(f.get("acodec") or "").lower() not in {"", "none"}
    ]
    if not audio_only and not combined:
        return []

    if audio_only:
        # A true audio-only rendition is ideal: choose the best native audio.
        source = max(audio_only, key=lambda f: audio_score(f, "mp4"))
    else:
        # Some extractors only expose audio inside combined A/V renditions.
        # Audio is frequently identical across every video resolution; pulling
        # the highest-resolution video just to discard it makes audio export
        # needlessly slow. If explicit ABR exists, preserve the highest known
        # audio bitrate, then choose the lightest rendition carrying it. If ABR
        # is unknown, choose the smallest/lowest rendition with audio.
        known_abr = [float(f.get("abr") or 0) for f in combined if float(f.get("abr") or 0) > 0]
        candidates = combined
        if known_abr:
            best_abr = max(known_abr)
            candidates = [f for f in combined if float(f.get("abr") or 0) >= best_abr * 0.98]

        def lightest_audio_donor(f: dict[str, Any]):
            size = int(f.get("filesize") or f.get("filesize_approx") or 2**62)
            height = int(quality_dimension(f) or 10**9)
            tbr = float(f.get("tbr") or 10**9)
            return (size, height, tbr)

        source = min(candidates, key=lightest_audio_donor)

    source_id = str(source.get("format_id") or "")
    if not source_id:
        return []
    acodec = str(source.get("acodec") or "audio")
    abr = int(round(float(source.get("abr") or 0))) or None
    ext = str(source.get("ext") or "").lower()
    if "opus" in acodec.lower() or ext in {"opus", "webm", "ogg"}:
        original_ext = "opus"
    elif "mp3" in acodec.lower() or ext == "mp3":
        original_ext = "mp3"
    else:
        original_ext = "m4a"

    out = [{
        "id": f"audio-original-{source_id}",
        "label": "Original audio",
        "detail": f"{acodec.upper()}" + (f" · ~{abr} kbps" if abr else ""),
        "audioOnly": True,
        "sourceFormatId": source_id,
        "output": "original",
        "outputExt": original_ext,
        "bitrate": abr,
        "audioCodec": acodec,
        "delivery": "server",
    }]
    for kbps in (128, 192, 256, 320):
        out.append({
            "id": f"audio-mp3-{kbps}-{source_id}",
            "label": f"MP3 {kbps} kbps",
            "detail": "MP3",
            "audioOnly": True,
            "sourceFormatId": source_id,
            "output": "mp3",
            "outputExt": "mp3",
            "bitrate": kbps,
            "audioCodec": "mp3",
            "delivery": "server",
        })
    return out


def youtube_like(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    except Exception:
        return False
    return host == "youtu.be" or host == "youtube.com" or host.endswith(".youtube.com")


def is_tiktok_info(info: dict[str, Any]) -> bool:
    extractor = str(info.get("extractor_key") or info.get("extractor") or "").lower()
    return extractor == "tiktok" or extractor.startswith("tiktok:")


async def ytdlp_download_format(source_url: str, format_id: str, directory: str, stem: str) -> Path:
    """Let yt-dlp perform the media transfer for sites whose signed CDN URLs
    cannot be replayed reliably by a generic HTTP client.

    The extractor is re-run by yt-dlp and its own downloader receives the exact
    request context it expects. No cookies/account data are supplied by us.
    """
    Path(directory).mkdir(parents=True, exist_ok=True)
    template = str(Path(directory) / f"{stem}.%(ext)s")
    cmd = [
        YTDLP,
        "--no-playlist",
        "--no-warnings",
        "--socket-timeout", "20",
        "--retries", "3",
        "--fragment-retries", "3",
        "--force-ipv4",
        "--js-runtimes", "node",
        "--no-part",
        "--no-continue",
        "-f", str(format_id),
        "-o", template,
        source_url,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=MAX_PREPARE_SECONDS)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise RuntimeError("yt-dlp media transfer timed out")
    if proc.returncode != 0:
        detail = stderr.decode("utf-8", "replace").strip()[-3000:] or stdout.decode("utf-8", "replace").strip()[-1500:] or "yt-dlp media transfer failed"
        raise RuntimeError(detail)

    candidates = [
        x for x in Path(directory).glob(f"{stem}.*")
        if x.is_file() and not x.name.endswith((".part", ".ytdl"))
    ]
    if not candidates:
        raise RuntimeError("yt-dlp finished but did not produce a media file")
    return max(candidates, key=lambda x: x.stat().st_mtime)


async def run_ytdlp(url: str) -> dict[str, Any]:
    base_cmd = [
        YTDLP,
        "--dump-single-json",
        "--skip-download",
        "--no-playlist",
        "--no-warnings",
        "--socket-timeout", "20",
        "--retries", "2",
        "--extractor-retries", "2",
        "--js-runtimes", "node",
    ]
    if youtube_like(url):
        base_cmd += [
            "--extractor-args", "youtube:player_client=default,mweb",
            "--extractor-args", f"youtubepot-bgutilhttp:base_url={POT_URL}",
        ]

    async def attempt(extra: list[str] | None = None):
        cmd = [*base_cmd, *(extra or []), url]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=MAX_ANALYZE_SECONDS)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            raise HTTPException(504, "Media analysis timed out")
        return proc.returncode, stdout, stderr.decode("utf-8", "replace").strip()

    code, stdout, detail = await attempt()

    # yt-dlp's generic extractor intentionally does not impersonate by default.
    # When it specifically reports a Cloudflare/browser-fingerprint challenge,
    # retry once with generic impersonation now that curl_cffi is installed.
    low = detail.lower()
    if code != 0 and not youtube_like(url) and (
        "generic:impersonate" in low
        or "cloudflare anti-bot challenge" in low
        or ("impersonat" in low and "unavailable" not in low)
    ):
        code, stdout, detail2 = await attempt(["--extractor-args", "generic:impersonate"])
        if detail2:
            detail = detail2
        low = detail.lower()

    if code != 0:
        detail = detail[-3000:] or "yt-dlp failed"
        status = 429 if "confirm you’re not a bot" in low or "confirm you're not a bot" in low else 502
        raise HTTPException(status, detail)
    try:
        return json.loads(stdout)
    except Exception:
        raise HTTPException(502, "yt-dlp returned invalid JSON")


def ffmpeg_input_header_args(headers: dict[str, Any]) -> list[str]:
    """Convert yt-dlp request headers into safe per-input FFmpeg HTTP options."""
    clean: list[str] = []
    user_agent = None
    for raw_k, raw_v in (headers or {}).items():
        if not raw_k or raw_v is None:
            continue
        k = str(raw_k).strip()
        v = str(raw_v).replace("\r", "").replace("\n", " ").strip()
        if not k or not v:
            continue
        lk = k.lower()
        # FFmpeg manages these itself; forwarding them can break HLS/DASH reads.
        if lk in {"host", "connection", "content-length", "range", "accept-encoding"}:
            continue
        if lk == "user-agent":
            user_agent = v
            continue
        clean.append(f"{k}: {v}")
    out: list[str] = []
    if user_agent:
        out += ["-user_agent", user_agent]
    if clean:
        out += ["-headers", "\r\n".join(clean) + "\r\n"]
    return out


def build_resolved_ffmpeg_command(
    video: dict[str, Any],
    audio: dict[str, Any] | None,
    output_path: str,
    container: str,
    start: float | None,
    end: float | None,
) -> list[str]:
    """Build a command that consumes the exact streams resolved during Analyze."""
    cmd = [FFMPEG, "-hide_banner", "-loglevel", "error", "-nostdin", "-y"]
    clip_len = None
    if start is not None and end is not None:
        clip_len = max(0.0, end - start)

    if start is not None:
        cmd += ["-ss", f"{start:.3f}"]
    cmd += ffmpeg_input_header_args(video.get("headers") or {})
    cmd += ["-i", video["url"]]

    if audio:
        if start is not None:
            cmd += ["-ss", f"{start:.3f}"]
        cmd += ffmpeg_input_header_args(audio.get("headers") or {})
        cmd += ["-i", audio["url"]]

    if clip_len is not None:
        cmd += ["-t", f"{clip_len:.3f}"]

    if audio:
        cmd += ["-map", "0:v:0", "-map", "1:a:0"]
    else:
        # Combined HLS/progressive streams normally expose both tracks on input 0.
        cmd += ["-map", "0:v:0?", "-map", "0:a:0?"]

    # Stream-copy first. This is cheap on a small host and preserves source quality.
    cmd += ["-c", "copy"]
    if container == "mp4":
        cmd += ["-movflags", "+faststart"]
    cmd += [output_path]
    return cmd


async def run_native_transfer_prepare(job_id: str, session_id: str, choice_id: str, start: float | None, end: float | None) -> None:
    """Prepare a choice by letting yt-dlp itself fetch the selected formats.

    This is intentionally used only for extractors such as TikTok where the
    resolved CDN URL may return 403 when replayed by an unrelated HTTP client.
    FFmpeg only touches local files after yt-dlp has completed the transfer.
    """
    rec = JOBS[job_id]
    session = SESSIONS.get(session_id)
    if not session:
        rec.update(status="error", error="Media session expired before preparation started")
        return
    choice = session.get("choices", {}).get(choice_id)
    if not choice:
        rec.update(status="error", error="Unknown quality choice")
        return

    directory = f"/tmp/anything-downloader-{job_id}"
    os.makedirs(directory, exist_ok=True)
    rec["directory"] = directory
    rec["status"] = "preparing"
    rec["strategy"] = "yt-dlp-native-transfer"

    try:
        video_path = await ytdlp_download_format(
            session["source_url"], str(choice.get("videoFormatId") or ""), directory, "video"
        )
        audio_path = None
        if choice.get("audioFormatId"):
            audio_path = await ytdlp_download_format(
                session["source_url"], str(choice["audioFormatId"]), directory, "audio"
            )
    except Exception as e:
        rec.update(status="error", error=f"yt-dlp media transfer failed: {e}")
        return

    container = str(choice.get("container") or "mp4").lower()
    if container not in {"mp4", "webm", "mkv", "mov", "m4v"}:
        container = "mp4"
    output_path = os.path.join(directory, f"media.{container}")
    cmd = [FFMPEG, "-hide_banner", "-loglevel", "error", "-nostdin", "-y"]
    clip_len = None
    if start is not None and end is not None:
        clip_len = max(0.0, end - start)
        cmd += ["-ss", f"{start:.3f}"]
    cmd += ["-i", str(video_path)]
    if audio_path:
        if start is not None:
            cmd += ["-ss", f"{start:.3f}"]
        cmd += ["-i", str(audio_path)]
    if clip_len is not None:
        cmd += ["-t", f"{clip_len:.3f}"]
    if audio_path:
        cmd += ["-map", "0:v:0", "-map", "1:a:0", "-shortest"]
    else:
        cmd += ["-map", "0:v:0?", "-map", "0:a:0?"]
    cmd += ["-c", "copy"]
    if container == "mp4":
        cmd += ["-movflags", "+faststart"]
    cmd += [output_path]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=MAX_PREPARE_SECONDS)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        rec.update(status="error", error="Local mux/clip step timed out")
        return
    if proc.returncode != 0:
        detail = stderr.decode("utf-8", "replace").strip()[-2500:] or stdout.decode("utf-8", "replace").strip()[-1000:] or "FFmpeg local mux failed"
        rec.update(status="error", error=f"Local mux/clip failed: {detail}")
        return

    path = Path(output_path)
    if not path.is_file() or path.stat().st_size <= 0:
        rec.update(status="error", error="Preparation finished but no output file was produced")
        return
    base = safe_name(session.get("title") or "media")
    if start is not None and end is not None:
        base += f"_{int(start)}-{int(end)}"
    filename = f"{base}.{container}"
    rec.update(
        status="ready", path=str(path), filename=filename, size=path.stat().st_size,
        content_type=mimetypes.guess_type(filename)[0] or "application/octet-stream",
        expires=time.time() + JOB_TTL,
    )


async def run_audio_prepare(job_id: str, session_id: str, choice_id: str, start: float | None, end: float | None) -> None:
    rec = JOBS[job_id]
    session = SESSIONS.get(session_id)
    if not session:
        rec.update(status="error", error="Media session expired before audio preparation started")
        return
    choice = session.get("audio_choices", {}).get(choice_id)
    if not choice:
        rec.update(status="error", error="Unknown audio choice")
        return

    directory = f"/tmp/anything-downloader-{job_id}"
    os.makedirs(directory, exist_ok=True)
    rec.update(directory=directory, status="preparing", strategy="audio-extract", phase="Resolving audio source", progress=2)

    source_id = str(choice.get("sourceFormatId") or "")
    stream = session.get("streams", {}).get(source_id)
    native_transfer = bool(session.get("native_transfer"))
    source_path: Path | None = None

    # Only sites whose signed CDN URLs cannot be replayed (currently TikTok)
    # should force a fresh yt-dlp media transfer. Everywhere else, consume the
    # exact URL and headers captured during Analyze. This avoids transient
    # format-ID failures and avoids downloading a whole video to disk before
    # audio extraction can even begin.
    if native_transfer or not stream or not stream.get("url"):
        rec.update(phase="Fetching source with yt-dlp", progress=5)
        try:
            source_path = await ytdlp_download_format(
                session["source_url"], source_id, directory, "audio-source"
            )
        except Exception as e:
            rec.update(status="error", error=f"yt-dlp audio transfer failed: {e}")
            return
    else:
        try:
            await validate_public_https(stream["url"])
        except Exception as e:
            rec.update(status="error", error=f"Resolved audio source is no longer usable: {e}")
            return

    output_mode = str(choice.get("output") or "original")
    out_ext = str(choice.get("outputExt") or "m4a").lower()
    if out_ext not in {"m4a", "mp3", "opus", "ogg"}:
        out_ext = "m4a"
    output_path = Path(directory) / f"audio.{out_ext}"

    cmd = [FFMPEG, "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-progress", "pipe:1", "-nostats"]
    if start is not None:
        cmd += ["-ss", f"{start:.3f}"]
    if source_path is not None:
        cmd += ["-i", str(source_path)]
    else:
        cmd += ffmpeg_input_header_args((stream or {}).get("headers") or {})
        cmd += ["-i", str(stream["url"])]
    if start is not None and end is not None:
        cmd += ["-t", f"{max(0.0, end - start):.3f}"]
    cmd += ["-vn", "-map", "0:a:0"]
    if output_mode == "mp3":
        kbps = int(choice.get("bitrate") or 192)
        kbps = min(320, max(64, kbps))
        cmd += ["-c:a", "libmp3lame", "-b:a", f"{kbps}k"]
        phase = f"Encoding MP3 {kbps} kbps"
    else:
        # Original means no lossy re-encode. AAC/mp4a stream-copies cleanly to
        # M4A, Opus to .opus, and MP3 to .mp3.
        cmd += ["-c:a", "copy"]
        phase = "Extracting original audio"
    cmd += [str(output_path)]

    rec.update(phase=phase, progress=8)
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )

    total_duration = None
    if start is not None and end is not None:
        total_duration = max(0.001, end - start)
    elif session.get("duration"):
        try:
            total_duration = max(0.001, float(session["duration"]))
        except Exception:
            total_duration = None

    stderr_task = asyncio.create_task(proc.stderr.read())
    try:
        async def watch_progress():
            while True:
                line = await proc.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", "replace").strip()
                if text.startswith("out_time_ms=") and total_duration:
                    try:
                        # FFmpeg's out_time_ms value is expressed in microseconds.
                        seconds = int(text.split("=", 1)[1]) / 1_000_000.0
                        pct = max(0.0, min(1.0, seconds / total_duration))
                        rec["progress"] = max(int(rec.get("progress") or 8), min(94, 8 + int(pct * 86)))
                    except Exception:
                        pass
        await asyncio.wait_for(watch_progress(), timeout=MAX_PREPARE_SECONDS)
        returncode = await asyncio.wait_for(proc.wait(), timeout=30)
        stderr = await stderr_task
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        if not stderr_task.done():
            stderr_task.cancel()
        rec.update(status="error", error="Audio preparation timed out")
        return

    if returncode != 0:
        detail = stderr.decode("utf-8", "replace").strip()[-2500:] or "FFmpeg audio preparation failed"
        rec.update(status="error", error=f"Audio preparation failed: {detail}")
        return
    if not output_path.is_file() or output_path.stat().st_size <= 0:
        rec.update(status="error", error="Audio preparation finished but no output file was produced")
        return

    base = safe_name(session.get("title") or "audio")
    if start is not None and end is not None:
        base += f"_{int(start)}-{int(end)}"
    filename = f"{base}.{out_ext}"
    rec.update(
        status="ready", phase="Ready", progress=100,
        path=str(output_path), filename=filename, size=output_path.stat().st_size,
        content_type=mimetypes.guess_type(filename)[0] or "audio/mpeg",
        expires=time.time() + JOB_TTL,
    )


async def run_prepare(job_id: str, session_id: str, choice_id: str, start: float | None, end: float | None) -> None:
    rec = JOBS[job_id]
    session = SESSIONS.get(session_id)
    if not session:
        rec.update(status="error", error="Media session expired before preparation started")
        return
    audio_choice = session.get("audio_choices", {}).get(choice_id)
    if audio_choice:
        await run_audio_prepare(job_id, session_id, choice_id, start, end)
        return

    choice = session.get("choices", {}).get(choice_id)
    if not choice:
        rec.update(status="error", error="Unknown quality choice")
        return

    if session.get("native_transfer"):
        await run_native_transfer_prepare(job_id, session_id, choice_id, start, end)
        return

    streams = session.get("streams", {})
    video = streams.get(str(choice.get("videoFormatId") or ""))
    audio_id = choice.get("audioFormatId")
    audio = streams.get(str(audio_id)) if audio_id else None
    if not video or not video.get("url"):
        rec.update(status="error", error="The resolved video stream is no longer available; analyze the URL again")
        return
    if audio_id and (not audio or not audio.get("url")):
        rec.update(status="error", error="The resolved audio stream is no longer available; analyze the URL again")
        return

    # Re-check resolved media hosts before the server fetches them.
    try:
        await validate_public_https(video["url"])
        if audio:
            await validate_public_https(audio["url"])
    except HTTPException as e:
        rec.update(status="error", error=f"Resolved media URL was rejected: {e.detail}")
        return

    directory = f"/tmp/anything-downloader-{job_id}"
    os.makedirs(directory, exist_ok=True)
    rec["directory"] = directory
    container = str(choice.get("container") or video.get("ext") or "mp4").lower()
    if container not in {"mp4", "webm", "mkv", "mov", "m4v"}:
        container = "mp4"
    output_path = os.path.join(directory, f"media.{container}")
    cmd = build_resolved_ffmpeg_command(video, audio, output_path, container, start, end)

    rec["status"] = "preparing"
    rec["strategy"] = "resolved-stream-ffmpeg"
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=MAX_PREPARE_SECONDS)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        rec.update(status="error", error="Server-side preparation timed out")
        return

    if proc.returncode != 0:
        detail = stderr.decode("utf-8", "replace").strip()[-3500:] or stdout.decode("utf-8", "replace").strip()[-1500:] or "FFmpeg preparation failed"
        rec.update(status="error", error=f"Resolved-stream preparation failed: {detail}")
        return

    path = Path(output_path)
    if not path.is_file() or path.stat().st_size <= 0:
        rec.update(status="error", error="Preparation finished but no output file was produced")
        return
    ext = path.suffix.lstrip(".") or container
    base = safe_name(session.get("title") or "media")
    if start is not None and end is not None:
        base += f"_{int(start)}-{int(end)}"
    filename = f"{base}.{ext}"
    rec.update(
        status="ready",
        path=str(path),
        filename=filename,
        size=path.stat().st_size,
        content_type=mimetypes.guess_type(filename)[0] or "application/octet-stream",
        expires=time.time() + JOB_TTL,
    )


@app.get("/health")
async def health():
    return {
        "ok": True,
        "engine": "yt-dlp-multisite+resolved-stream-prep+ejs+bgutil-pot",
        "version": APP_VERSION,
        "potProvider": POT_URL,
        "serverPrepare": True,
    }


@app.post("/analyze")
async def analyze(req: AnalyzeRequest, authorization: str | None = Header(default=None)):
    require_auth(authorization)
    cleanup()
    source_url = await validate_public_https(req.url)
    info = await run_ytdlp(source_url)
    raw_formats = [f for f in info.get("formats", []) if isinstance(f, dict)]
    # Some yt-dlp extractors return a single top-level media format instead of formats[].
    # Normalize that shape so supported sites are not rejected merely because they expose
    # one playable stream.
    if not raw_formats and isinstance(info.get("url"), str):
        raw_formats = [info]
    # A few generic/embed extractors can return a one-entry result wrapper.
    if not raw_formats and isinstance(info.get("entries"), list):
        for entry in info.get("entries") or []:
            if not isinstance(entry, dict):
                continue
            nested = [f for f in entry.get("formats", []) if isinstance(f, dict)]
            if nested:
                info = {**entry, "webpage_url": entry.get("webpage_url") or source_url}
                raw_formats = nested
                break
            if isinstance(entry.get("url"), str):
                info = {**entry, "webpage_url": entry.get("webpage_url") or source_url}
                raw_formats = [entry]
                break
    raw_formats = normalize_sparse_formats(info, raw_formats)
    choices = make_choices(raw_formats)
    audio_choices = make_audio_choices(raw_formats)
    if not choices:
        raise HTTPException(422, "The source was identified, but no non-DRM video formats were available")

    native_transfer = "yt-dlp" if is_tiktok_info(info) else None
    if native_transfer:
        # TikTok's signed CDN URLs may reject a generic replay even though
        # extraction succeeded. Keep all selected qualities on the backend so
        # yt-dlp itself performs the media transfer.
        for choice in choices:
            choice["delivery"] = "server"

    sid = secrets.token_urlsafe(24)
    streams: dict[str, dict[str, Any]] = {}
    for f in raw_formats:
        if not has_usable_url(f):
            continue
        fid = str(f.get("format_id") or "")
        if not fid:
            continue
        streams[fid] = {
            "url": f["url"],
            "headers": f.get("http_headers") or {},
            "mime": f.get("mime_type") or ("video/webm" if f.get("ext") == "webm" else "video/mp4"),
            "ext": f.get("ext") or "bin",
            "protocol": f.get("protocol"),
            "hasVideo": is_video_format(f),
            "hasAudio": (str(f.get("acodec") or "").lower() != "none" and (f.get("acodec") not in {None, ""} or is_progressive_http_format(f))),
        }
    # Prefer a lightweight progressive HTTP video as the browser preview source,
    # even when yt-dlp did not populate codec metadata for that direct format.
    # This keeps preview/seek traffic in the browser while HLS/DASH stays on the
    # resolved-stream server-prep path for the actual selected download quality.
    preview_candidates = []
    for f in raw_formats:
        if not has_usable_url(f) or not is_progressive_http_format(f):
            continue
        try:
            h = int(quality_dimension(f) or 0)
        except Exception:
            h = 0
        ext = str(f.get("ext") or "").lower()
        if ext not in {"mp4", "webm", "m4v", "mov"}:
            continue
        preview_candidates.append(f)

    preview_format = None
    if preview_candidates:
        # Audio matters more than hitting exactly 480p. Some sites (notably
        # Instagram Reels) expose a convenient 480p video-only rendition next
        # to a slightly larger combined rendition. Picking by resolution first
        # makes the browser preview look fine but leaves its audio control greyed
        # out. Prefer known audio-bearing progressive files, then unknown-audio
        # direct files, and only then explicitly silent video-only files.
        def preview_score(f):
            h = int(quality_dimension(f) or 0)
            acodec = str(f.get("acodec") or "").lower()
            if acodec and acodec != "none":
                audio_rank = 0
            elif f.get("acodec") in {None, ""}:
                audio_rank = 1  # unknown; many direct MP4 extractors omit codec metadata
            else:
                audio_rank = 2  # explicitly video-only
            metadata_penalty = 0 if f.get("vcodec") not in {None, "none"} else 1
            return (audio_rank, abs(h - 480), metadata_penalty, h)
        preview_format = min(preview_candidates, key=preview_score)
    preview_source = preview_format or choose_preview_source(raw_formats)

    choice_map = {c["id"]: c for c in choices}

    # TikTok direct CDN URLs are intentionally not replayed by the browser relay.
    # Prefer the smallest combined AVC/AAC choice as the native yt-dlp preview.
    native_preview_choice = None
    if native_transfer:
        combined_choices = [c for c in choices if c.get("combined") and not c.get("audioFormatId")]
        if combined_choices:
            native_preview_choice = min(
                combined_choices,
                key=lambda c: (int(c.get("height") or 10**9), int(c.get("filesize") or 10**18)),
            )

    # If the best preview-quality video has a separate audio stream, the browser
    # cannot play those two URLs as one <video>. Reuse the existing resolved
    # choice and let /preview temporarily mux video+audio into a fast-start MP4.
    audio_preview_choices = [c for c in choices if c.get("audioFormatId")]
    preview_choice = None
    if audio_preview_choices:
        def preview_choice_score(c):
            h = int(c.get("height") or 0)
            return (abs(h - 480), h)
        preview_choice = min(audio_preview_choices, key=preview_choice_score)
    if native_preview_choice:
        preview_choice = native_preview_choice

    SESSIONS[sid] = {
        "expires": time.time() + SESSION_TTL,
        "formats": {k: v for k, v in streams.items() if str(v.get("protocol") or "").lower() in {"https", "http", "https_native", "http_native"}},
        "streams": streams,
        "choices": choice_map,
        "audio_choices": {c["id"]: c for c in audio_choices},
        "source_url": source_url,
        "title": info.get("title") or "media",
        "duration": info.get("duration"),
        "extractor": info.get("extractor_key") or info.get("extractor"),
        "native_transfer": native_transfer,
        "preview_dir": None,
    }

    return {
        "ok": True,
        "engine": "yt-dlp-multisite",
        "version": APP_VERSION,
        "id": info.get("id"),
        "extractor": info.get("extractor_key") or info.get("extractor"),
        "extractorId": info.get("extractor"),
        "title": info.get("title"),
        "duration": info.get("duration"),
        "thumbnail": info.get("thumbnail"),
        "webpageUrl": info.get("webpage_url") or source_url,
        "session": sid,
        "expiresIn": SESSION_TTL,
        "formats": [
            ({**public_format(f), "delivery": "server", "direct": False} if native_transfer else public_format(f))
            for f in raw_formats
        ],
        "choices": choices,
        "audioChoices": audio_choices,
        "previewFormatId": str(preview_format.get("format_id")) if preview_format else None,
        "previewSourceFormatId": str(preview_source.get("format_id")) if preview_source else None,
        "previewChoiceId": preview_choice.get("id") if preview_choice else None,
        # Force the muxed preview only when the direct preview is explicitly
        # video-only (or absent). If the direct file already has audio, keep the
        # faster browser-relay path and retain previewChoiceId only as fallback.
        "previewUsePrepared": bool(
            preview_choice and (
                native_transfer
                or not preview_format
                or str(preview_format.get("acodec") or "").lower() == "none"
            )
        ),
        "previewHeight": int(quality_dimension(preview_format) or 0) if preview_format else None,
        "previewExt": preview_format.get("ext") if preview_format else None,
        "browserChoiceCount": sum(1 for c in choices if c["delivery"] == "browser"),
        "serverChoiceCount": sum(1 for c in choices if c["delivery"] == "server"),
    }


def choose_preview_source(raw_formats: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pick a resolved video stream FFmpeg can normalize into a seekable preview."""
    candidates: list[dict[str, Any]] = []
    for f in raw_formats:
        if not has_usable_url(f) or not is_video_format(f):
            continue
        candidates.append(f)
    if not candidates:
        return None

    def score(f: dict[str, Any]):
        try:
            h = int(quality_dimension(f) or 0)
        except Exception:
            h = 0
        if h:
            return (0, abs(h - 480), h)
        return (1, 0, 0)

    return min(candidates, key=score)


async def ensure_seekable_preview(session_id: str, format_id: str) -> Path:
    cleanup()
    session = SESSIONS.get(session_id)
    if not session:
        raise HTTPException(410, "Media session expired; analyze the URL again")

    duration = float(session.get("duration") or 0)
    if duration <= 0:
        raise HTTPException(409, "Preview duration is unavailable")
    if duration > MAX_PREVIEW_SECONDS:
        raise HTTPException(409, f"Prepared preview is limited to {MAX_PREVIEW_SECONDS} seconds")

    streams = session.get("streams", {})
    choice = session.get("choices", {}).get(format_id)
    if choice:
        video_id = str(choice.get("videoFormatId") or "")
        audio_id = str(choice.get("audioFormatId") or "")
        item = streams.get(video_id)
        audio_item = streams.get(audio_id) if audio_id else None
    else:
        item = streams.get(format_id)
        audio_item = None

    if not item or not item.get("url"):
        raise HTTPException(404, "Preview source was not found")
    if choice and choice.get("audioFormatId") and (not audio_item or not audio_item.get("url")):
        raise HTTPException(404, "Preview audio source was not found")

    native_transfer = bool(session.get("native_transfer"))
    if not native_transfer:
        await validate_public_https(item["url"])
        if audio_item:
            await validate_public_https(audio_item["url"])

    cache_dir = PREVIEW_CACHE_ROOT / session_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    session["preview_dir"] = str(cache_dir)
    digest = hashlib.sha256(format_id.encode("utf-8")).hexdigest()[:20]
    output_path = cache_dir / f"{digest}.mp4"
    if output_path.exists() and output_path.stat().st_size > 0:
        return output_path

    lock_key = f"{session_id}:{format_id}"
    lock = PREVIEW_LOCKS.setdefault(lock_key, asyncio.Lock())
    async with lock:
        if output_path.exists() and output_path.stat().st_size > 0:
            return output_path

        tmp_path = output_path.with_suffix(".tmp.mp4")
        tmp_path.unlink(missing_ok=True)
        common = [FFMPEG, "-hide_banner", "-loglevel", "error", "-nostdin", "-y"]

        local_video_path: Path | None = None
        local_audio_path: Path | None = None
        if native_transfer:
            try:
                native_dir = str(cache_dir / f"native-{digest}")
                local_video_path = await ytdlp_download_format(
                    session["source_url"],
                    str(choice.get("videoFormatId") if choice else format_id),
                    native_dir,
                    "video",
                )
                if choice and choice.get("audioFormatId"):
                    local_audio_path = await ytdlp_download_format(
                        session["source_url"], str(choice["audioFormatId"]), native_dir, "audio"
                    )
            except Exception as e:
                raise HTTPException(502, f"yt-dlp preview transfer failed: {e}")

        async def run(cmd: list[str]):
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=min(MAX_PREPARE_SECONDS, 180))
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                return 124, b"", b"Preview preparation timed out"
            return proc.returncode, stdout, stderr

        copy_cmd = [*common]
        if native_transfer and local_video_path:
            copy_cmd += ["-i", str(local_video_path)]
            if local_audio_path:
                copy_cmd += ["-i", str(local_audio_path)]
                copy_cmd += ["-map", "0:v:0", "-map", "1:a:0", "-shortest"]
            else:
                copy_cmd += ["-map", "0:v:0", "-map", "0:a:0?"]
        else:
            copy_cmd += ffmpeg_input_header_args(item.get("headers") or {})
            copy_cmd += ["-i", item["url"]]
            if audio_item:
                copy_cmd += ffmpeg_input_header_args(audio_item.get("headers") or {})
                copy_cmd += ["-i", audio_item["url"]]
                copy_cmd += ["-map", "0:v:0", "-map", "1:a:0", "-shortest"]
            else:
                copy_cmd += ["-map", "0:v:0", "-map", "0:a:0?"]
        copy_cmd += ["-c", "copy", "-movflags", "+faststart", str(tmp_path)]
        code, _, stderr = await run(copy_cmd)

        if code != 0 or not tmp_path.exists() or tmp_path.stat().st_size <= 0:
            tmp_path.unlink(missing_ok=True)
            transcode_cmd = [*common]
            if native_transfer and local_video_path:
                transcode_cmd += ["-i", str(local_video_path)]
                if local_audio_path:
                    transcode_cmd += ["-i", str(local_audio_path)]
                    transcode_cmd += ["-map", "0:v:0", "-map", "1:a:0", "-shortest"]
                else:
                    transcode_cmd += ["-map", "0:v:0", "-map", "0:a:0?"]
            else:
                transcode_cmd += ffmpeg_input_header_args(item.get("headers") or {})
                transcode_cmd += ["-i", item["url"]]
                if audio_item:
                    transcode_cmd += ffmpeg_input_header_args(audio_item.get("headers") or {})
                    transcode_cmd += ["-i", audio_item["url"]]
                    transcode_cmd += ["-map", "0:v:0", "-map", "1:a:0", "-shortest"]
                else:
                    transcode_cmd += ["-map", "0:v:0", "-map", "0:a:0?"]
            transcode_cmd += [
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "27",
                "-c:a", "aac", "-b:a", "128k",
                "-movflags", "+faststart", str(tmp_path),
            ]
            code, _, stderr = await run(transcode_cmd)

        if code != 0 or not tmp_path.exists() or tmp_path.stat().st_size <= 0:
            tmp_path.unlink(missing_ok=True)
            detail = stderr.decode("utf-8", "replace")[-1200:] if stderr else "FFmpeg could not build the preview"
            raise HTTPException(502, detail or "FFmpeg could not build the preview")

        tmp_path.replace(output_path)
        return output_path


@app.get("/preview/{session}/{format_id}")
async def preview(session: str, format_id: str, authorization: str | None = Header(default=None)):
    require_auth(authorization)
    path = await ensure_seekable_preview(session, format_id)
    return FileResponse(
        path,
        media_type="video/mp4",
        headers={"Cache-Control": "private, no-store", "Accept-Ranges": "bytes"},
    )


@app.get("/media/{session}/{format_id}")
async def media(session: str, format_id: str, request: Request, authorization: str | None = Header(default=None)):
    require_auth(authorization)
    cleanup()
    s = SESSIONS.get(session)
    if not s:
        raise HTTPException(410, "Media session expired; analyze the URL again")
    item = s["formats"].get(format_id)
    if not item:
        raise HTTPException(404, "This format is not a progressive browser-relay format")
    await validate_public_https(item["url"])

    headers = {str(k): str(v) for k, v in (item.get("headers") or {}).items() if k and v}
    if request.headers.get("range"):
        headers["Range"] = request.headers["range"]

    client = httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(30.0, read=90.0))
    req = client.build_request("GET", item["url"], headers=headers)
    upstream = await client.send(req, stream=True)
    if upstream.status_code >= 400:
        body = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(upstream.status_code, body.decode("utf-8", "replace")[:1200] or "Upstream media request failed")

    async def body_iter():
        try:
            async for chunk in upstream.aiter_bytes():
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    out_headers = {}
    for key in ["content-type", "content-length", "content-range", "accept-ranges", "etag", "last-modified"]:
        val = upstream.headers.get(key)
        if val:
            out_headers[key] = val
    out_headers["cache-control"] = "private, no-store"
    return StreamingResponse(body_iter(), status_code=upstream.status_code, headers=out_headers, media_type=upstream.headers.get("content-type"))


@app.post("/prepare")
async def prepare(req: PrepareRequest, authorization: str | None = Header(default=None)):
    require_auth(authorization)
    cleanup()
    session = SESSIONS.get(req.session)
    if not session:
        raise HTTPException(410, "Media session expired; analyze the URL again")
    choice = session.get("choices", {}).get(req.choiceId)
    audio_choice = session.get("audio_choices", {}).get(req.choiceId)
    if not choice and not audio_choice:
        raise HTTPException(404, "Unknown quality choice")
    if choice and choice.get("delivery") != "server":
        raise HTTPException(400, "This quality does not require server-side preparation")

    start = req.start
    end = req.end
    if (start is None) != (end is None):
        raise HTTPException(400, "Provide both start and end, or neither")
    if start is not None and end is not None:
        start = max(0.0, float(start))
        end = float(end)
        duration = float(session.get("duration") or 0)
        if duration:
            end = min(end, duration)
        if end <= start:
            raise HTTPException(400, "End must be after start")

    job_id = secrets.token_urlsafe(24)
    JOBS[job_id] = {
        "created": time.time(),
        "expires": time.time() + JOB_TTL,
        "status": "queued",
        "session": req.session,
        "choiceId": req.choiceId,
    }
    task = asyncio.create_task(run_prepare(job_id, req.session, req.choiceId, start, end))
    ACTIVE_TASKS.add(task)
    task.add_done_callback(ACTIVE_TASKS.discard)
    return {"ok": True, "job": job_id, "status": "queued", "expiresIn": JOB_TTL}


@app.get("/prepare/{job_id}")
async def prepare_status(job_id: str, authorization: str | None = Header(default=None)):
    require_auth(authorization)
    cleanup()
    rec = JOBS.get(job_id)
    if not rec:
        raise HTTPException(404, "Preparation job not found or expired")
    out = {"ok": rec.get("status") != "error", "job": job_id, "status": rec.get("status")}
    if rec.get("phase"):
        out["phase"] = rec.get("phase")
    if rec.get("progress") is not None:
        out["progress"] = rec.get("progress")
    if rec.get("status") == "ready":
        out.update(filename=rec.get("filename"), size=rec.get("size"), contentType=rec.get("content_type"))
    if rec.get("error"):
        out["error"] = rec["error"]
    return out


@app.get("/prepared/{job_id}")
async def prepared(job_id: str, authorization: str | None = Header(default=None)):
    require_auth(authorization)
    cleanup()
    rec = JOBS.get(job_id)
    if not rec:
        raise HTTPException(404, "Preparation job not found or expired")
    if rec.get("status") != "ready" or not rec.get("path"):
        raise HTTPException(409, f"Preparation is {rec.get('status', 'not ready')}")
    return FileResponse(
        rec["path"],
        filename=rec.get("filename") or "media.bin",
        media_type=rec.get("content_type") or "application/octet-stream",
        headers={"Cache-Control": "private, no-store"},
    )

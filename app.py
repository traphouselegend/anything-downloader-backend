import asyncio
import hashlib
import ipaddress
import json
import mimetypes
import os
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

APP_VERSION = "6.4.10"
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
        "qualityLabel": f.get("resolution") if f.get("resolution") not in {None, "audio only"} else (f"{f.get('height')}p" if f.get("height") else None),
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


def make_choices(formats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    usable = [f for f in formats if has_usable_url(f)]
    video = [f for f in usable if is_video_format(f) and f.get("height")]
    unranked_video = [f for f in usable if is_video_format(f) and not f.get("height")]
    audio = [f for f in usable if is_audio_only_format(f)]
    # Some sites expose their highest resolutions as video-only but also return
    # a lower-resolution combined file containing a perfectly usable audio track.
    # That combined file can safely act as an audio donor when no audio-only
    # format exists; FFmpeg maps only its audio stream.
    combined_audio_donors = [
        f for f in usable
        if is_video_format(f) and str(f.get("acodec") or "").lower() not in {"", "none"}
    ]
    by_height: dict[int, list[dict[str, Any]]] = {}
    for f in video:
        by_height.setdefault(int(f["height"]), []).append(f)

    choices: list[dict[str, Any]] = []
    for height in sorted(by_height, reverse=True):
        candidates = sorted(by_height[height], key=codec_score, reverse=True)
        combined = [
            f for f in candidates
            if f.get("acodec") not in {None, "none"}
            or (is_progressive_http_format(f) and f.get("acodec") is None)
        ]
        chosen = combined[0] if combined else candidates[0]
        family = "webm" if str(chosen.get("ext") or "").lower() == "webm" else "mp4"
        audio_f = None
        # Missing acodec on a progressive video is "unknown", not proof that
        # audio is absent. Twitch Clip MP4s are a common example. Only pair a
        # separate audio stream when the chosen format explicitly says acodec=none.
        if str(chosen.get("acodec") or "").lower() == "none":
            if audio:
                audio_f = max(audio, key=lambda f: audio_score(f, family))
            elif combined_audio_donors:
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
                    # Prefer compatible containers/codecs, then the smallest donor
                    # because only its audio stream is needed.
                    return (0 if family_match else 1, size, height)
                audio_f = min(combined_audio_donors, key=donor_score)

        browser_delivery = is_progressive_http_format(chosen) and (audio_f is None or is_progressive_http_format(audio_f))
        delivery = "browser" if browser_delivery else "server"
        ext = "webm" if family == "webm" else "mp4"
        size = (chosen.get("filesize") or chosen.get("filesize_approx") or 0) + ((audio_f or {}).get("filesize") or (audio_f or {}).get("filesize_approx") or 0)
        video_id = str(chosen.get("format_id"))
        audio_id = str(audio_f.get("format_id")) if audio_f else None
        selector = f"{video_id}+{audio_id}" if audio_id else video_id
        cid = f"{height}-{video_id}" + (f"-{audio_id}" if audio_id else "")
        choices.append({
            "id": cid,
            "label": f"{height}p" + (" (4K)" if height >= 2160 else ""),
            "height": height,
            "fps": chosen.get("fps"),
            "container": ext,
            "videoCodec": chosen.get("vcodec"),
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

    # Some extractors expose one direct video file without any resolution metadata.
    # A missing height is not a reason to make that otherwise-valid source unusable.
    if not choices and unranked_video:
        chosen = max(unranked_video, key=codec_score)
        family = "webm" if str(chosen.get("ext") or "").lower() == "webm" else "mp4"
        video_id = str(chosen.get("format_id") or "source")
        browser_delivery = is_progressive_http_format(chosen)
        choices.append({
            "id": f"source-{video_id}",
            "label": chosen.get("format_note") or chosen.get("resolution") or "Source",
            "height": None,
            "fps": chosen.get("fps"),
            "container": family,
            "videoCodec": chosen.get("vcodec"),
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


def youtube_like(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    except Exception:
        return False
    return host == "youtu.be" or host == "youtube.com" or host.endswith(".youtube.com")


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


async def run_prepare(job_id: str, session_id: str, choice_id: str, start: float | None, end: float | None) -> None:
    rec = JOBS[job_id]
    session = SESSIONS.get(session_id)
    if not session:
        rec.update(status="error", error="Media session expired before preparation started")
        return
    choice = session.get("choices", {}).get(choice_id)
    if not choice:
        rec.update(status="error", error="Unknown quality choice")
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
    if not choices:
        raise HTTPException(422, "The source was identified, but no non-DRM video formats were available")

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
            h = int(f.get("height") or 0)
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
            h = int(f.get("height") or 0)
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
    SESSIONS[sid] = {
        "expires": time.time() + SESSION_TTL,
        "formats": {k: v for k, v in streams.items() if str(v.get("protocol") or "").lower() in {"https", "http", "https_native", "http_native"}},
        "streams": streams,
        "choices": choice_map,
        "source_url": source_url,
        "title": info.get("title") or "media",
        "duration": info.get("duration"),
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
        "formats": [public_format(f) for f in raw_formats],
        "choices": choices,
        "previewFormatId": str(preview_format.get("format_id")) if preview_format else None,
        "previewSourceFormatId": str(preview_source.get("format_id")) if preview_source else None,
        "previewChoiceId": preview_choice.get("id") if preview_choice else None,
        # Force the muxed preview only when the direct preview is explicitly
        # video-only (or absent). If the direct file already has audio, keep the
        # faster browser-relay path and retain previewChoiceId only as fallback.
        "previewUsePrepared": bool(
            preview_choice and (
                not preview_format
                or str(preview_format.get("acodec") or "").lower() == "none"
            )
        ),
        "previewHeight": int(preview_format.get("height") or 0) if preview_format else None,
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
            h = int(f.get("height") or 0)
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
    if not choice:
        raise HTTPException(404, "Unknown quality choice")
    if choice.get("delivery") != "server":
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

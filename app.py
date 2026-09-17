import asyncio
import ipaddress
import json
import os
import secrets
import time
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

APP_VERSION = "6.1"
SESSION_TTL = int(os.getenv("SESSION_TTL_SECONDS", "1200"))
API_KEY = os.getenv("ADAPTER_API_KEY", "").strip()
YTDLP = os.getenv("YTDLP_BIN", "/opt/venv/bin/yt-dlp")
POT_URL = os.getenv("POT_PROVIDER_URL", "http://127.0.0.1:4416").rstrip("/")
MAX_ANALYZE_SECONDS = int(os.getenv("MAX_ANALYZE_SECONDS", "75"))

app = FastAPI(title="Anything Downloader Acquisition Adapter", version=APP_VERSION)
SESSIONS: dict[str, dict[str, Any]] = {}


class AnalyzeRequest(BaseModel):
    url: str


def require_auth(authorization: str | None) -> None:
    if API_KEY and authorization != f"Bearer {API_KEY}":
        raise HTTPException(401, "Unauthorized")


def cleanup() -> None:
    now = time.time()
    for key in list(SESSIONS):
        if SESSIONS[key]["expires"] < now:
            SESSIONS.pop(key, None)


def validate_public_https(raw: str) -> str:
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
        pass
    return raw


def is_direct_http_format(f: dict[str, Any]) -> bool:
    url = f.get("url")
    if not isinstance(url, str) or not url.startswith("https://"):
        return False
    if f.get("has_drm"):
        return False
    protocol = str(f.get("protocol") or "").lower()
    # Keep the relay predictable. Manifest-only streams can be added later.
    return protocol in {"https", "http", "https_native", "http_dash_segments"} or protocol.startswith("https")


def public_format(f: dict[str, Any]) -> dict[str, Any]:
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
        "hasVideo": f.get("vcodec") not in {None, "none"},
        "hasAudio": f.get("acodec") not in {None, "none"},
        "direct": is_direct_http_format(f),
    }


def codec_score(f: dict[str, Any]) -> float:
    ext = str(f.get("ext") or "").lower()
    vcodec = str(f.get("vcodec") or "").lower()
    score = float(f.get("tbr") or f.get("vbr") or 0)
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
    if preferred_family == "mp4" and (ext in {"m4a", "mp4"} or "mp4a" in acodec or "aac" in acodec):
        score += 100000
    if preferred_family == "webm" and (ext == "webm" or "opus" in acodec or "vorbis" in acodec):
        score += 100000
    return score


def make_choices(formats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    direct = [f for f in formats if is_direct_http_format(f)]
    video = [f for f in direct if f.get("vcodec") not in {None, "none"} and f.get("height")]
    audio = [f for f in direct if f.get("vcodec") in {None, "none"} and f.get("acodec") not in {None, "none"}]
    by_height: dict[int, list[dict[str, Any]]] = {}
    for f in video:
        by_height.setdefault(int(f["height"]), []).append(f)

    choices: list[dict[str, Any]] = []
    for height in sorted(by_height, reverse=True):
        candidates = sorted(by_height[height], key=codec_score, reverse=True)
        combined = [f for f in candidates if f.get("acodec") not in {None, "none"}]
        chosen = combined[0] if combined else candidates[0]
        family = "webm" if str(chosen.get("ext") or "").lower() == "webm" else "mp4"
        audio_f = None
        if chosen.get("acodec") in {None, "none"} and audio:
            audio_f = max(audio, key=lambda f: audio_score(f, family))
        ext = "webm" if family == "webm" else "mp4"
        size = (chosen.get("filesize") or chosen.get("filesize_approx") or 0) + ((audio_f or {}).get("filesize") or (audio_f or {}).get("filesize_approx") or 0)
        choices.append({
            "id": f"{height}-{chosen.get('format_id')}",
            "label": f"{height}p" + (" (4K)" if height >= 2160 else ""),
            "height": height,
            "fps": chosen.get("fps"),
            "container": ext,
            "videoCodec": chosen.get("vcodec"),
            "audioCodec": chosen.get("acodec") if not audio_f else audio_f.get("acodec"),
            "videoFormatId": str(chosen.get("format_id")),
            "audioFormatId": str(audio_f.get("format_id")) if audio_f else None,
            "videoExt": chosen.get("ext") or ext,
            "audioExt": (audio_f.get("ext") if audio_f else None),
            "combined": audio_f is None and chosen.get("acodec") not in {None, "none"},
            "filesize": size or None,
        })
    return choices


async def run_ytdlp(url: str) -> dict[str, Any]:
    cmd = [
        YTDLP,
        "--dump-single-json",
        "--skip-download",
        "--no-playlist",
        "--no-warnings",
        "--socket-timeout", "20",
        "--retries", "2",
        "--extractor-retries", "2",
        "--js-runtimes", "node",
        "--extractor-args", "youtube:player_client=default,mweb",
        "--extractor-args", f"youtubepot-bgutilhttp:base_url={POT_URL}",
        url,
    ]
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
    if proc.returncode != 0:
        detail = stderr.decode("utf-8", "replace").strip()[-2500:] or "yt-dlp failed"
        status = 429 if "confirm you’re not a bot" in detail.lower() or "confirm you're not a bot" in detail.lower() else 502
        raise HTTPException(status, detail)
    try:
        return json.loads(stdout)
    except Exception:
        raise HTTPException(502, "yt-dlp returned invalid JSON")


@app.get("/health")
async def health():
    return {
        "ok": True,
        "engine": "yt-dlp+ejs+bgutil-pot",
        "version": APP_VERSION,
        "potProvider": POT_URL,
    }


@app.post("/analyze")
async def analyze(req: AnalyzeRequest, authorization: str | None = Header(default=None)):
    require_auth(authorization)
    cleanup()
    source_url = validate_public_https(req.url)
    info = await run_ytdlp(source_url)
    raw_formats = [f for f in info.get("formats", []) if isinstance(f, dict)]
    usable = [f for f in raw_formats if is_direct_http_format(f)]
    choices = make_choices(raw_formats)
    if not choices:
        raise HTTPException(422, "The source was identified, but no ordinary HTTPS media streams were available")

    sid = secrets.token_urlsafe(24)
    private: dict[str, dict[str, Any]] = {}
    for f in usable:
        fid = str(f.get("format_id") or "")
        if not fid:
            continue
        private[fid] = {
            "url": f["url"],
            "headers": f.get("http_headers") or {},
            "mime": f.get("mime_type") or ("video/webm" if f.get("ext") == "webm" else "video/mp4"),
            "ext": f.get("ext") or "bin",
        }
    SESSIONS[sid] = {"expires": time.time() + SESSION_TTL, "formats": private}

    thumb = info.get("thumbnail")
    return {
        "ok": True,
        "engine": "yt-dlp+ejs+bgutil-pot",
        "version": APP_VERSION,
        "id": info.get("id"),
        "extractor": info.get("extractor_key") or info.get("extractor"),
        "title": info.get("title"),
        "duration": info.get("duration"),
        "thumbnail": thumb,
        "webpageUrl": info.get("webpage_url") or source_url,
        "session": sid,
        "expiresIn": SESSION_TTL,
        "formats": [public_format(f) for f in raw_formats],
        "choices": choices,
    }


@app.get("/media/{session}/{format_id}")
async def media(
    session: str,
    format_id: str,
    request: Request,
    authorization: str | None = Header(default=None),
):
    require_auth(authorization)
    cleanup()
    s = SESSIONS.get(session)
    if not s:
        raise HTTPException(410, "Media session expired; analyze the URL again")
    item = s["formats"].get(format_id)
    if not item:
        raise HTTPException(404, "Unknown media format")

    headers = {str(k): str(v) for k, v in (item.get("headers") or {}).items() if k and v}
    if request.headers.get("range"):
        headers["Range"] = request.headers["range"]

    client = httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(30.0, read=60.0))
    req = client.build_request("GET", item["url"], headers=headers)
    upstream = await client.send(req, stream=True)
    if upstream.status_code >= 400:
        body = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(upstream.status_code, body.decode("utf-8", "replace")[:1000] or "Upstream media request failed")

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

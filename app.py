import asyncio
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

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

APP_VERSION = "6.4"
SESSION_TTL = int(os.getenv("SESSION_TTL_SECONDS", "1800"))
JOB_TTL = int(os.getenv("JOB_TTL_SECONDS", "1800"))
API_KEY = os.getenv("ADAPTER_API_KEY", "").strip()
YTDLP = os.getenv("YTDLP_BIN", "/opt/venv/bin/yt-dlp")
POT_URL = os.getenv("POT_PROVIDER_URL", "http://127.0.0.1:4416").rstrip("/")
MAX_ANALYZE_SECONDS = int(os.getenv("MAX_ANALYZE_SECONDS", "90"))
MAX_PREPARE_SECONDS = int(os.getenv("MAX_PREPARE_SECONDS", "900"))

app = FastAPI(title="Anything Downloader Acquisition Adapter", version=APP_VERSION)
SESSIONS: dict[str, dict[str, Any]] = {}
JOBS: dict[str, dict[str, Any]] = {}
ACTIVE_TASKS: set[asyncio.Task] = set()


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
            SESSIONS.pop(key, None)
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
        "hasVideo": f.get("vcodec") not in {None, "none"},
        "hasAudio": f.get("acodec") not in {None, "none"},
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


def make_choices(formats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    usable = [f for f in formats if has_usable_url(f)]
    video = [f for f in usable if f.get("vcodec") not in {None, "none"} and f.get("height")]
    audio = [f for f in usable if f.get("vcodec") in {None, "none"} and f.get("acodec") not in {None, "none"}]
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
            "combined": audio_f is None and chosen.get("acodec") not in {None, "none"},
            "filesize": size or None,
            "delivery": delivery,
            "formatSelector": selector,
        })
    return choices


def youtube_like(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    except Exception:
        return False
    return host == "youtu.be" or host == "youtube.com" or host.endswith(".youtube.com")


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
    ]
    if youtube_like(url):
        cmd += [
            "--extractor-args", "youtube:player_client=default,mweb",
            "--extractor-args", f"youtubepot-bgutilhttp:base_url={POT_URL}",
        ]
    cmd.append(url)
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
        detail = stderr.decode("utf-8", "replace").strip()[-3000:] or "yt-dlp failed"
        low = detail.lower()
        status = 429 if "confirm you’re not a bot" in low or "confirm you're not a bot" in low else 502
        raise HTTPException(status, detail)
    try:
        return json.loads(stdout)
    except Exception:
        raise HTTPException(502, "yt-dlp returned invalid JSON")


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

    directory = f"/tmp/anything-downloader-{job_id}"
    os.makedirs(directory, exist_ok=True)
    rec["directory"] = directory
    outtmpl = os.path.join(directory, "media.%(ext)s")
    cmd = [
        YTDLP,
        "--no-playlist",
        "--no-warnings",
        "--socket-timeout", "30",
        "--retries", "3",
        "--fragment-retries", "3",
        "--concurrent-fragments", "4",
        "--js-runtimes", "node",
        "--format", choice["formatSelector"],
        "--output", outtmpl,
    ]
    if youtube_like(session["source_url"]):
        cmd += [
            "--extractor-args", "youtube:player_client=default,mweb",
            "--extractor-args", f"youtubepot-bgutilhttp:base_url={POT_URL}",
        ]
    if start is not None and end is not None:
        cmd += ["--download-sections", f"*{start:.3f}-{end:.3f}"]
    # If yt-dlp merges separate A/V streams, prefer MP4 when the selected family is MP4.
    if choice.get("container") == "mp4":
        cmd += ["--merge-output-format", "mp4"]
    cmd.append(session["source_url"])

    rec["status"] = "preparing"
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
        detail = stderr.decode("utf-8", "replace").strip()[-3500:] or stdout.decode("utf-8", "replace").strip()[-1500:] or "yt-dlp preparation failed"
        rec.update(status="error", error=detail)
        return

    files = [p for p in Path(directory).glob("media.*") if p.is_file() and not p.name.endswith((".part", ".ytdl"))]
    if not files:
        rec.update(status="error", error="Preparation finished but no output file was produced")
        return
    path = max(files, key=lambda p: p.stat().st_size)
    ext = path.suffix.lstrip(".") or choice.get("container") or "mp4"
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
        "engine": "yt-dlp-multisite+ejs+bgutil-pot",
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
    choices = make_choices(raw_formats)
    if not choices:
        raise HTTPException(422, "The source was identified, but no non-DRM video formats were available")

    sid = secrets.token_urlsafe(24)
    private: dict[str, dict[str, Any]] = {}
    for f in raw_formats:
        if not is_progressive_http_format(f):
            continue
        fid = str(f.get("format_id") or "")
        if not fid:
            continue
        private[fid] = {
            "url": f["url"],
            "headers": f.get("http_headers") or {},
            "mime": f.get("mime_type") or ("video/webm" if f.get("ext") == "webm" else "video/mp4"),
            "ext": f.get("ext") or "bin",
        }
    choice_map = {c["id"]: c for c in choices}
    SESSIONS[sid] = {
        "expires": time.time() + SESSION_TTL,
        "formats": private,
        "choices": choice_map,
        "source_url": source_url,
        "title": info.get("title") or "media",
        "duration": info.get("duration"),
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
        "browserChoiceCount": sum(1 for c in choices if c["delivery"] == "browser"),
        "serverChoiceCount": sum(1 for c in choices if c["delivery"] == "server"),
    }


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

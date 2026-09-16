import os, secrets, time, asyncio
from typing import Dict
import httpx, yt_dlp
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel

app=FastAPI(title="Anything Downloader Extractor + Relay")
origins=[x.strip() for x in os.getenv("ALLOWED_ORIGINS","*").split(",") if x.strip()]
app.add_middleware(CORSMiddleware,allow_origins=origins,allow_credentials=False,allow_methods=["*"],allow_headers=["*"],expose_headers=["Content-Length","Content-Range","Accept-Ranges","Content-Type"])
class AnalyzeRequest(BaseModel): url:str
SESSIONS:Dict[str,dict]={}; SESSION_TTL=3600
def purge():
    now=time.time()
    for t in list(SESSIONS):
        if now-SESSIONS[t]["created"]>SESSION_TTL: SESSIONS.pop(t,None)
def pub(f):
    if not f.get("url") or f.get("protocol") not in ("https","http"): return None
    return {"format_id":str(f.get("format_id")),"ext":f.get("ext"),"protocol":f.get("protocol"),"width":f.get("width"),"height":f.get("height"),"fps":f.get("fps"),"vcodec":f.get("vcodec"),"acodec":f.get("acodec"),"tbr":f.get("tbr"),"filesize":f.get("filesize") or f.get("filesize_approx")}

@app.get("/ffmpeg/ffmpeg-core.wasm")
def ffmpeg_wasm():
    return FileResponse(
        "/app/backend-assets/ffmpeg-core.wasm",
        media_type="application/wasm",
        headers={"Cache-Control":"public, max-age=31536000, immutable"}
    )

@app.get("/health")
def health(): return {"ok":True,"relay":True}
@app.post("/analyze")
async def analyze(req:AnalyzeRequest):
    purge(); opts={
        "quiet":False,
        "verbose":True,
        "no_warnings":False,
        "skip_download":True,
        "noplaylist":True,
        "js_runtimes":{"deno":{}},
        "remote_components":{"ejs:npm"},
        "extractor_args":{
            "youtube":{"player_client":["mweb","web_embedded"]},
            "youtubepot-bgutilscript":{"server_home":["/opt/bgutil-ytdlp-pot-provider/server"]}
        }
    }
    def run_extract():
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(req.url, download=False)

    try:
        # yt-dlp is synchronous. Run it off the FastAPI event loop and prevent
        # an upstream YouTube/provider stall from leaving the UI waiting forever.
        info = await asyncio.wait_for(asyncio.to_thread(run_extract), timeout=45)
    except asyncio.TimeoutError:
        raise HTTPException(
            504,
            "Media analysis timed out after 45 seconds while contacting YouTube. Please try again later."
        )
    except Exception as e:
        message = str(e)
        lower = message.lower()
        if "429" in lower or "too many requests" in lower:
            raise HTTPException(503, "YouTube temporarily rejected the cloud analysis server (HTTP 429). Please try again later.")
        if "not a bot" in lower or "sign in to confirm" in lower:
            raise HTTPException(503, "YouTube temporarily challenged the cloud analysis server. Please try again later.")
        raise HTTPException(400, f"Could not analyze this URL: {message}")
    formats=[]; private={}
    for f in info.get("formats",[]):
        p=pub(f)
        if p:
            formats.append(p); private[p["format_id"]]={"url":f["url"],"headers":f.get("http_headers") or {}}
    token=secrets.token_urlsafe(24); SESSIONS[token]={"created":time.time(),"formats":private}
    return {"id":info.get("id"),"title":info.get("title"),"duration":info.get("duration"),"thumbnail":info.get("thumbnail"),"webpage_url":info.get("webpage_url"),"session":token,"formats":formats}
@app.get("/media/{token}/{format_id}")
async def media(request:Request,token:str,format_id:str):
    purge(); s=SESSIONS.get(token)
    if not s: raise HTTPException(404,"Media session expired. Analyze again.")
    item=s["formats"].get(format_id)
    if not item: raise HTTPException(404,"Unknown media format.")
    headers=dict(item["headers"])
    if request.headers.get("range"): headers["Range"]=request.headers["range"]
    client=httpx.AsyncClient(follow_redirects=True,timeout=httpx.Timeout(30,read=None))
    try: up=await client.send(client.build_request("GET",item["url"],headers=headers),stream=True)
    except Exception as e:
        await client.aclose(); raise HTTPException(502,f"Could not open media stream: {e}")
    if up.status_code>=400:
        await up.aclose(); await client.aclose(); raise HTTPException(up.status_code,f"Upstream media request failed ({up.status_code}).")
    rh={k:up.headers[k] for k in ("content-length","content-range","accept-ranges","content-type") if k in up.headers}; rh["cache-control"]="private, no-store"
    async def chunks():
        try:
            async for c in up.aiter_bytes(262144): yield c
        finally:
            await up.aclose(); await client.aclose()
    return StreamingResponse(chunks(),status_code=up.status_code,headers=rh,media_type=up.headers.get("content-type"))

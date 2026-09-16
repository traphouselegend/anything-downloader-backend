# Anything Downloader Render Backend V2.1

Diagnostic/fallback build for authorized, non-DRM media.

Changes:
- Keeps the bgutil PO-token provider and Deno.
- Uses mweb first and web_embedded as an additional YouTube client.
- Enables yt-dlp verbose diagnostics so Render logs show provider/client details.
- Uses the provider's current source checkout rather than an assumed branch tag.
- Preserves /health, /analyze, /media and /ffmpeg/ffmpeg-core.wasm.

After deploying, run Analyze once and inspect the Render application logs.

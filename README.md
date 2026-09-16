# Anything Downloader Render Backend V2.3

Changes from V2.2:
- Adds a hard 45-second timeout around yt-dlp analysis.
- Runs synchronous yt-dlp extraction in a worker thread so FastAPI's event loop is not blocked.
- Returns HTTP 504 with a clear message if YouTube/provider analysis stalls.
- Preserves V2.2's explicit handling for upstream 429/challenge responses.
- Does not attempt to bypass an upstream access restriction.

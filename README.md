# Anything Downloader Render Backend V2

For media you own, created, or are authorized to download.

Changes from V1:
- Installs current yt-dlp plus bgutil-ytdlp-pot-provider.
- Installs BgUtils provider 2.0.0 in script mode using Deno.
- Requests the yt-dlp YouTube mweb client and automatic PO-token provider.
- Preserves /health, /analyze, /media, and /ffmpeg/ffmpeg-core.wasm.

Replace the four backend files in the GitHub repo with this package and let Render rebuild.

Note: PO tokens do not guarantee that YouTube will accept a cloud/datacenter IP.

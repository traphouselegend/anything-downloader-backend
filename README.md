# Anything Downloader Backend

Cloud backend for the Anything Downloader prototype.

## Endpoints
- `GET /health`
- `POST /analyze`
- `GET /media/{token}/{format_id}`
- `GET /ffmpeg/ffmpeg-core.wasm`

The large FFmpeg WASM file is downloaded during the Docker image build and is
not stored in this repository.

Use only with media you own or are authorized to download.

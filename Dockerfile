FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deno.land/install.sh | sh
ENV PATH="/root/.deno/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m yt_dlp --version
RUN deno --version

# BgUtils PO-token provider, script mode. It remains internal to this Render container.
RUN git clone --depth 1 --branch 2.0.0 \
    https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git \
    /opt/bgutil-ytdlp-pot-provider \
    && cd /opt/bgutil-ytdlp-pot-provider/server \
    && deno install --allow-scripts=npm:canvas --frozen

COPY extractor.py .

# Keep the large FFmpeg WASM inside the cloud container, not GitHub/Cloudflare.
RUN mkdir -p /app/backend-assets \
    && curl -fL --retry 3 \
       -o /app/backend-assets/ffmpeg-core.wasm \
       https://cdn.jsdelivr.net/npm/@ffmpeg/core@0.12.10/dist/esm/ffmpeg-core.wasm

ENV TOKEN_TTL=6
ENV PORT=10000
CMD ["sh", "-c", "uvicorn extractor:app --host 0.0.0.0 --port ${PORT}"]

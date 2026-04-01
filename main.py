from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import subprocess
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_audio_url(query: str):
    ydl_opts = {
        "format": "bestaudio/best",
        "quiet": True,
        "noplaylist": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"scsearch1:{query}", download=False)
        if not info or not info.get("entries"):
            return None
        entry = info["entries"][0]
        formats = entry.get("formats", [])
        audio_formats = [f for f in formats if f.get("acodec") != "none" and f.get("url")]
        if not audio_formats:
            return None
        best = sorted(audio_formats, key=lambda f: f.get("abr") or 0, reverse=True)[0]
        return {"url": best["url"], "title": entry.get("title", query)}


@app.get("/stream")
async def stream_audio(artist: str = Query(...), name: str = Query(...)):
    queries = [f"{artist} {name}", f"{artist} - {name}", f"{name} {artist}"]

    info = None
    for q in queries:
        try:
            info = get_audio_url(q)
            if info:
                break
        except Exception as e:
            print(f"Query failed '{q}': {e}")

    if not info:
        return JSONResponse({"error": "Трек не найден"}, status_code=404)

    try:
        # Конвертируем в mp3 через ffmpeg прямо в стрим
        cmd = [
            "ffmpeg", "-i", info["url"],
            "-vn",                    # без видео
            "-acodec", "libmp3lame", # mp3
            "-ab", "192k",           # битрейт
            "-ar", "44100",          # частота
            "-f", "mp3",             # формат
            "-loglevel", "quiet",
            "pipe:1"                 # вывод в stdout
        ]

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        def generate():
            try:
                while True:
                    chunk = process.stdout.read(8192)
                    if not chunk:
                        break
                    yield chunk
            finally:
                process.kill()

        return StreamingResponse(
            generate(),
            media_type="audio/mpeg",
            headers={"Cache-Control": "no-cache", "Accept-Ranges": "none"}
        )

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/health")
async def health():
    return {"status": "ok"}

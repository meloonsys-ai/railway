from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import httpx
import io
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_audio_url(query: str) -> dict:
    ydl_opts = {
        "format": "bestaudio/best",
        "quiet": True,
        "noplaylist": True,
        "extract_flat": False,
        "cookiefile": "cookies.txt",
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"ytsearch1:{query}", download=False)
        if not info or not info.get("entries"):
            return None
        entry = info["entries"][0]
        # Берём прямую ссылку на аудио
        formats = entry.get("formats", [])
        audio_formats = [f for f in formats if f.get("acodec") != "none" and f.get("vcodec") == "none"]
        if not audio_formats:
            audio_formats = formats
        best = sorted(audio_formats, key=lambda f: f.get("abr") or 0, reverse=True)[0]
        return {
            "url": best["url"],
            "title": entry.get("title"),
            "duration": entry.get("duration"),
            "ext": best.get("ext", "webm"),
        }

@app.get("/stream")
async def stream_audio(artist: str = Query(...), name: str = Query(...)):
    query = f"{artist} {name} audio"
    try:
        info = get_audio_url(query)
        if not info:
            return JSONResponse({"error": "Не найдено"}, status_code=404)

        # Стримим аудио с YouTube напрямую клиенту
        async def audio_generator():
            async with httpx.AsyncClient(timeout=60) as client:
                async with client.stream("GET", info["url"]) as r:
                    async for chunk in r.aiter_bytes(chunk_size=8192):
                        yield chunk

        media_type = "audio/webm" if info["ext"] == "webm" else "audio/mpeg"
        return StreamingResponse(
            audio_generator(),
            media_type=media_type,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Disposition": f'inline; filename="{info["title"]}.{info["ext"]}"',
            }
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/health")
async def health():
    return {"status": "ok"}

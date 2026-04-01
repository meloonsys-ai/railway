from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import httpx
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
        "format": "bestaudio[ext=webm]/bestaudio/best",
        "quiet": True,
        "noplaylist": True,
        "extract_flat": False,
        "cookiefile": "cookies.txt" if os.path.exists("cookies.txt") else None,
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        },
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"ytsearch1:{query}", download=False)
        if not info or not info.get("entries"):
            return None
        entry = info["entries"][0]
        formats = entry.get("formats", [])

        audio_formats = [
            f for f in formats
            if f.get("acodec") != "none"
            and f.get("vcodec") in ("none", None)
            and f.get("url")
        ]

        if not audio_formats:
            audio_formats = [f for f in formats if f.get("acodec") != "none" and f.get("url")]

        if not audio_formats:
            return None

        best = sorted(audio_formats, key=lambda f: f.get("abr") or 0, reverse=True)[0]
        return {
            "url": best["url"],
            "ext": best.get("ext", "webm"),
            "title": entry.get("title", query),
        }


@app.get("/stream")
async def stream_audio(artist: str = Query(...), name: str = Query(...)):
    queries = [
        f"{artist} {name} audio",
        f"{artist} {name}",
        f"{name} {artist}",
    ]

    info = None
    for q in queries:
        try:
            info = get_audio_url(q)
            if info:
                break
        except Exception as e:
            print(f"Query failed '{q}': {e}")
            continue

    if not info:
        return JSONResponse({"error": "Трек не найден"}, status_code=404)

    try:
        async def audio_generator():
            async with httpx.AsyncClient(
                timeout=60,
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.youtube.com/"}
            ) as client:
                async with client.stream("GET", info["url"]) as r:
                    async for chunk in r.aiter_bytes(chunk_size=16384):
                        yield chunk

        content_type = "audio/webm" if info["ext"] == "webm" else "audio/mpeg"
        return StreamingResponse(
            audio_generator(),
            media_type=content_type,
            headers={"Accept-Ranges": "bytes", "Cache-Control": "no-cache"}
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/health")
async def health():
    return {"status": "ok"}

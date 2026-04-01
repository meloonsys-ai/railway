from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import httpx

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_audio_info(query: str):
    ydl_opts = {
        "format": "bestaudio[ext=mp3]/bestaudio[acodec=mp3]/bestaudio/best",
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
        # Предпочитаем mp3
        mp3 = [f for f in audio_formats if f.get("ext") == "mp3"]
        best = sorted(mp3 or audio_formats, key=lambda f: f.get("abr") or 0, reverse=True)[0]
        return {
            "url": best["url"],
            "ext": best.get("ext", "mp3"),
            "title": entry.get("title", query),
        }


@app.get("/stream")
async def stream_audio(artist: str = Query(...), name: str = Query(...)):
    queries = [f"{artist} {name}", f"{artist} - {name}", f"{name} {artist}"]
    info = None
    for q in queries:
        try:
            info = get_audio_info(q)
            if info:
                break
        except Exception as e:
            print(f"Failed '{q}': {e}")

    if not info:
        return JSONResponse({"error": "Не найдено"}, status_code=404)

    try:
        async def generate():
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                async with client.stream("GET", info["url"]) as r:
                    async for chunk in r.aiter_bytes(8192):
                        yield chunk

        mime = "audio/mpeg" if info["ext"] == "mp3" else "audio/ogg" if info["ext"] == "ogg" else "audio/mpeg"
        print(f"Sending format: ext={info['ext']}, url={info['url'][:50]}")
        return StreamingResponse(
            generate(),
            media_type=mime,
            headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"}
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/health")
async def health():
    return {"status": "ok"}

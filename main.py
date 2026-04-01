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
        "format": "bestaudio[protocol!*=m3u8][protocol!*=hls][ext!=m3u8]/bestaudio[protocol=https]/bestaudio",
        "quiet": True,
        "noplaylist": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"scsearch1:{query}", download=False)
        if not info or not info.get("entries"):
            return None
        entry = info["entries"][0]
        formats = entry.get("formats", [])

        # Фильтруем — только прямые ссылки без HLS
        good_formats = [
            f for f in formats
            if f.get("acodec") != "none"
            and f.get("url")
            and "m3u8" not in f.get("url", "")
            and "m3u8" not in (f.get("ext") or "")
            and "hls" not in (f.get("protocol") or "")
            and f.get("protocol") in ("https", "http", None)
        ]

        print(f"Total formats: {len(formats)}, Good formats: {len(good_formats)}")
        for f in good_formats:
            print(f"  ext={f.get('ext')} abr={f.get('abr')} protocol={f.get('protocol')} url={f.get('url','')[:60]}")

        if not good_formats:
            return None

        best = sorted(good_formats, key=lambda f: f.get("abr") or 0, reverse=True)[0]
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
        print(f"Streaming: ext={info['ext']}, url={info['url'][:80]}")

        async def generate():
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                async with client.stream("GET", info["url"]) as r:
                    async for chunk in r.aiter_bytes(8192):
                        yield chunk

        mime = "audio/mpeg" if info["ext"] in ("mp3", "m4a") else "audio/ogg" if info["ext"] == "ogg" else "audio/mpeg"
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

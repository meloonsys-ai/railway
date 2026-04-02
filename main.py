from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import httpx
import boto3
from botocore.config import Config
from supabase import create_client
import os
import io

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── R2 ──
R2_ENDPOINT = "https://9af077c35ecf6b5d9ab31b5567543b20.r2.cloudflarestorage.com"
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY", "e03630dca937d64b56052be79f738cab")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY", "4f1cd960d9e8e0811d2b5a3f06129444350d6b25fe9518349ecbc6a6c7eb09cc")
R2_BUCKET = "libaud-tracks"
R2_PUBLIC_URL = "https://pub-ed15d27eabe345ad970c236044a84977.r2.dev"

r2 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="auto",
)

# ── SUPABASE ──
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://foevsbhnbqrcqjnqqxoz.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZvZXZzYmhuYnFyY3FqbnFxeG96Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDM1MjcyNTIsImV4cCI6MjA1OTEwMzI1Mn0.H1YpBHGSIsInJlZiI6ImZvZXZzYmhuYnFyY3FqbnFxeG96Iiwicm9sZSI6ImFub24ifQ")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_audio_info(query: str):
    ydl_opts = {
        "format": "bestaudio[protocol!*=m3u8][protocol!*=hls]/bestaudio",
        "quiet": True,
        "noplaylist": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"scsearch1:{query}", download=False)
        if not info or not info.get("entries"):
            return None
        entry = info["entries"][0]
        formats = entry.get("formats", [])
        good = [f for f in formats if f.get("acodec") != "none" and f.get("url")
                and "m3u8" not in f.get("url", "") and f.get("protocol") in ("https", "http", None)]
        if not good:
            return None
        best = sorted(good, key=lambda f: f.get("abr") or 0, reverse=True)[0]
        return {"url": best["url"], "ext": best.get("ext", "mp3")}


# ── STREAM (для воспроизведения) ──
@app.get("/stream")
async def stream_audio(artist: str = Query(...), name: str = Query(...)):
    queries = [f"{artist} {name}", f"{artist} - {name}", f"{name} {artist}"]
    info = None
    for q in queries:
        try:
            info = get_audio_info(q)
            if info: break
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

        mime = "audio/mpeg" if info["ext"] == "mp3" else "audio/mpeg"
        return StreamingResponse(generate(), media_type=mime,
                                 headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── DOWNLOAD (скачать в R2 + сохранить в Supabase) ──
@app.post("/download")
async def download_track(
    user_id: int = Query(...),
    track_id: str = Query(...),
    artist: str = Query(...),
    name: str = Query(...),
    cover: str = Query(default=""),
    album: str = Query(default=""),
    duration: str = Query(default=""),
):
    # Проверяем — может уже скачан другим пользователем
    r2_key = f"{track_id}.mp3"
    file_url = f"{R2_PUBLIC_URL}/{r2_key}"

    try:
        r2.head_object(Bucket=R2_BUCKET, Key=r2_key)
        # Файл уже есть в R2 — просто записываем в Supabase
        print(f"File already in R2: {r2_key}")
    except Exception:
        # Файла нет — скачиваем с SoundCloud и загружаем в R2
        queries = [f"{artist} {name}", f"{artist} - {name}"]
        info = None
        for q in queries:
            try:
                info = get_audio_info(q)
                if info: break
            except: pass

        if not info:
            return JSONResponse({"error": "Трек не найден"}, status_code=404)

        try:
            async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
                res = await client.get(info["url"])
                audio_data = res.content

            r2.put_object(
                Bucket=R2_BUCKET,
                Key=r2_key,
                Body=audio_data,
                ContentType="audio/mpeg",
            )
            print(f"Uploaded to R2: {r2_key}")
        except Exception as e:
            return JSONResponse({"error": f"Ошибка загрузки: {e}"}, status_code=500)

    # Записываем в Supabase
    try:
        supabase.table("downloads").upsert({
            "user_id": user_id,
            "track_id": track_id,
            "file_url": file_url,
            "name": name,
            "artist": artist,
            "album": album,
            "cover_url": cover,
            "duration": duration,
        }).execute()
    except Exception as e:
        print(f"Supabase error: {e}")

    return JSONResponse({"success": True, "file_url": file_url})


# ── GET DOWNLOADS (получить список скачанных для пользователя) ──
@app.get("/downloads")
async def get_downloads(user_id: int = Query(...)):
    try:
        res = supabase.table("downloads").select("*").eq("user_id", user_id).order("created_at", desc=True).execute()
        return JSONResponse({"downloads": res.data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── DELETE DOWNLOAD ──
@app.delete("/download")
async def delete_download(user_id: int = Query(...), track_id: str = Query(...)):
    try:
        supabase.table("downloads").delete().eq("user_id", user_id).eq("track_id", track_id).execute()
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/health")
async def health():
    return {"status": "ok"}

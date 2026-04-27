from fastapi import FastAPI, Query, Header, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import httpx
import boto3
from botocore.config import Config
from supabase import create_client
import os
import io
import hmac
import hashlib
import json
import uuid
from urllib.parse import parse_qs, unquote
from datetime import datetime, timedelta, timezone

app = FastAPI()

# Thread pool побольше для параллельных yt-dlp поисков
import asyncio
from concurrent.futures import ThreadPoolExecutor

@app.on_event("startup")
async def setup_thread_pool():
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=10))

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

# ── TELEGRAM BOT TOKEN (для проверки initData) ──
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")


# ══════════════════════════════════════════
# ── TELEGRAM AUTH ──
# ══════════════════════════════════════════

def validate_init_data(init_data: str) -> dict | None:
    """Проверяет подпись initData через HMAC-SHA256 с bot_token"""
    if not init_data or not BOT_TOKEN:
        return None
    try:
        parsed = dict(parse_qs(init_data, keep_blank_values=True))
        # parse_qs возвращает списки, берём первые элементы
        params = {k: v[0] for k, v in parsed.items()}

        received_hash = params.pop("hash", "")
        data_check_string = "\n".join(
            f"{k}={v}" for k, v in sorted(params.items())
        )

        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if calculated_hash != received_hash:
            return None

        user_data = json.loads(params.get("user", "{}"))
        return user_data
    except Exception as e:
        print(f"Auth error: {e}")
        return None


def get_user_from_header(x_telegram_init_data: str | None) -> dict | None:
    """Извлекает юзера из заголовка"""
    if not x_telegram_init_data:
        return None
    return validate_init_data(x_telegram_init_data)


async def check_friendship(user_id: str, target_id: str) -> bool:
    """Проверяет, являются ли юзеры друзьями"""
    try:
        res = supabase.table("friendships").select("id").or_(
            f"and(sender_id.eq.{user_id},receiver_id.eq.{target_id},status.eq.accepted),"
            f"and(sender_id.eq.{target_id},receiver_id.eq.{user_id},status.eq.accepted)"
        ).execute()
        return len(res.data) > 0
    except:
        return False


async def check_privacy(viewer_id: str, target_id: str, field: str) -> bool:
    """Проверяет настройки приватности"""
    if viewer_id == target_id:
        return True
    try:
        res = supabase.table("social_users").select(field).eq("id", target_id).execute()
        if not res.data:
            return False
        level = res.data[0].get(field, "friends")
        if level == "all":
            return True
        if level == "nobody":
            return False
        return await check_friendship(viewer_id, target_id)
    except:
        return False


# ══════════════════════════════════════
# ── SOCIAL: Auth & Register ──
# ══════════════════════════════════════

@app.post("/api/social/auth")
async def social_auth(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user.get("id"))
    try:
        supabase.table("social_users").upsert({
            "id": user_id,
            "username": user.get("username"),
            "first_name": user.get("first_name"),
            "photo_url": user.get("photo_url"),
            "last_seen": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="id").execute()

        res = supabase.table("social_users").select("*").eq("id", user_id).execute()
        return JSONResponse({"user": res.data[0] if res.data else None})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ══════════════════════════════════════
# ── SOCIAL: Search Users ──
# ══════════════════════════════════════

@app.get("/api/social/search")
async def social_search(request: Request, q: str = Query("")):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    if len(q.strip()) < 2:
        return JSONResponse({"users": []})

    search_q = q.lstrip("@")
    user_id = str(user["id"])
    try:
        res = supabase.table("social_users").select(
            "id, username, first_name, photo_url"
        ).ilike("username", f"%{search_q}%").neq("id", user_id).limit(10).execute()
        return JSONResponse({"users": res.data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ══════════════════════════════════════
# ── SOCIAL: Friends ──
# ══════════════════════════════════════

@app.get("/api/social/friends")
async def get_friends(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    try:
        # Получаем все accepted дружбы
        res = supabase.table("friendships").select("sender_id, receiver_id").eq(
            "status", "accepted"
        ).or_(f"sender_id.eq.{user_id},receiver_id.eq.{user_id}").execute()

        friend_ids = []
        for f in res.data:
            fid = f["receiver_id"] if f["sender_id"] == user_id else f["sender_id"]
            friend_ids.append(fid)

        if not friend_ids:
            return JSONResponse({"friends": []})

        # Получаем инфо о друзьях
        users_res = supabase.table("social_users").select(
            "id, username, first_name, photo_url"
        ).in_("id", friend_ids).execute()

        # Пре-фетч now_playing для всех друзей
        np_res = supabase.table("now_playing").select("*").in_(
            "user_id", friend_ids
        ).execute()
        np_map = {np["user_id"]: np for np in np_res.data}

        # Фильтруем expired now_playing (>90 сек без обновления)
        now = datetime.now(timezone.utc)
        friends = []
        for u in users_res.data:
            np = np_map.get(u["id"])
            now_playing = None
            if np and np.get("is_playing"):
                updated = datetime.fromisoformat(np["updated_at"].replace("Z", "+00:00"))
                if (now - updated).total_seconds() < 90:
                    now_playing = {
                        "track_name": np["track_name"],
                        "artist": np["artist"],
                        "cover_url": np["cover_url"],
                    }
            friends.append({**u, "now_playing": now_playing})

        return JSONResponse({"friends": friends})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/social/friends/requests")
async def get_friend_requests(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    try:
        res = supabase.table("friendships").select("sender_id, created_at").eq(
            "receiver_id", user_id
        ).eq("status", "requested").order("created_at", desc=True).execute()

        if not res.data:
            return JSONResponse({"requests": []})

        sender_ids = [r["sender_id"] for r in res.data]
        users_res = supabase.table("social_users").select(
            "id, username, first_name, photo_url"
        ).in_("id", sender_ids).execute()

        user_map = {u["id"]: u for u in users_res.data}
        requests = []
        for r in res.data:
            u = user_map.get(r["sender_id"], {})
            requests.append({**u, "created_at": r["created_at"]})

        return JSONResponse({"requests": requests})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/social/friends/request")
async def send_friend_request(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    body = await request.json()
    target_id = body.get("target_id")
    user_id = str(user["id"])

    if not target_id or target_id == user_id:
        return JSONResponse({"error": "Invalid target"}, status_code=400)

    try:
        # Проверяем существующую связь
        existing = supabase.table("friendships").select("*").or_(
            f"and(sender_id.eq.{user_id},receiver_id.eq.{target_id}),"
            f"and(sender_id.eq.{target_id},receiver_id.eq.{user_id})"
        ).execute()

        if existing.data:
            f = existing.data[0]
            if f["status"] == "accepted":
                return JSONResponse({"status": "already_friends"})
            if f["status"] == "blocked":
                return JSONResponse({"error": "Blocked"}, status_code=403)
            # Встречный запрос — автопринятие
            if f["sender_id"] == target_id and f["status"] == "requested":
                supabase.table("friendships").update({
                    "status": "accepted",
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }).eq("id", f["id"]).execute()
                return JSONResponse({"status": "accepted"})
            return JSONResponse({"status": "already_requested"})

        supabase.table("friendships").insert({
            "sender_id": user_id,
            "receiver_id": target_id,
            "status": "requested"
        }).execute()
        return JSONResponse({"status": "requested"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/social/friends/accept")
async def accept_friend(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    body = await request.json()
    sender_id = body.get("sender_id")
    user_id = str(user["id"])

    supabase.table("friendships").update({
        "status": "accepted",
        "updated_at": datetime.now(timezone.utc).isoformat()
    }).eq("sender_id", sender_id).eq("receiver_id", user_id).eq("status", "requested").execute()

    return JSONResponse({"status": "accepted"})


@app.delete("/api/social/friends/{target_id}")
async def remove_friend(target_id: str, request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    supabase.table("friendships").delete().or_(
        f"and(sender_id.eq.{user_id},receiver_id.eq.{target_id}),"
        f"and(sender_id.eq.{target_id},receiver_id.eq.{user_id})"
    ).execute()

    return JSONResponse({"status": "removed"})


# ══════════════════════════════════════
# ── SOCIAL: Profile ──
# ══════════════════════════════════════

@app.get("/api/social/profile/{target_id}")
async def get_profile(target_id: str, request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    viewer_id = str(user["id"])

    try:
        # Базовая инфа
        user_res = supabase.table("social_users").select(
            "id, username, first_name, photo_url"
        ).eq("id", target_id).execute()
        if not user_res.data:
            return JSONResponse({"error": "Not found"}, status_code=404)

        profile = {
            "user": user_res.data[0],
            "is_friend": await check_friendship(viewer_id, target_id),
            "top_weekly": [],
            "recently_added": [],
            "now_playing": None,
        }

        # Top Weekly
        if await check_privacy(viewer_id, target_id, "vis_history"):
            week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            log_res = supabase.table("listening_log").select(
                "track_id, track_name, artist, cover_url"
            ).eq("user_id", target_id).gte("listened_at", week_ago).execute()

            # Агрегируем вручную (Supabase не поддерживает GROUP BY через API)
            counts = {}
            for r in log_res.data:
                key = r["track_id"]
                if key not in counts:
                    counts[key] = {**r, "play_count": 0}
                counts[key]["play_count"] += 1

            profile["top_weekly"] = sorted(
                counts.values(), key=lambda x: x["play_count"], reverse=True
            )[:10]

        # Recently Added
        if await check_privacy(viewer_id, target_id, "vis_history"):
            recent_res = supabase.table("listening_log").select(
                "track_id, track_name, artist, cover_url, listened_at"
            ).eq("user_id", target_id).order("listened_at", desc=True).limit(10).execute()

            # Deduplicate by track_id
            seen = set()
            recent = []
            for r in recent_res.data:
                if r["track_id"] not in seen:
                    seen.add(r["track_id"])
                    recent.append(r)
            profile["recently_added"] = recent

        # Now Playing
        if await check_privacy(viewer_id, target_id, "vis_status"):
            np_res = supabase.table("now_playing").select("*").eq("user_id", target_id).execute()
            if np_res.data:
                np = np_res.data[0]
                updated = datetime.fromisoformat(np["updated_at"].replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - updated).total_seconds() < 90:
                    profile["now_playing"] = {
                        "track_name": np["track_name"],
                        "artist": np["artist"],
                        "cover_url": np["cover_url"],
                    }

        return JSONResponse(profile)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ══════════════════════════════════════
# ── SOCIAL: Now Playing ──
# ══════════════════════════════════════

@app.post("/api/social/now-playing")
async def update_now_playing(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    body = await request.json()

    try:
        if body.get("is_playing") and body.get("track_id"):
            supabase.table("now_playing").upsert({
                "user_id": user_id,
                "track_id": body["track_id"],
                "track_name": body.get("track_name", ""),
                "artist": body.get("artist", ""),
                "cover_url": body.get("cover_url", ""),
                "position_ms": body.get("position_ms", 0),
                "is_playing": True,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }, on_conflict="user_id").execute()
        else:
            supabase.table("now_playing").update({
                "is_playing": False,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("user_id", user_id).execute()

        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/social/listen-log")
async def log_listen(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    body = await request.json()

    if not body.get("track_id"):
        return JSONResponse({"error": "No track"}, status_code=400)

    try:
        supabase.table("listening_log").insert({
            "user_id": user_id,
            "track_id": body["track_id"],
            "track_name": body.get("track_name", ""),
            "artist": body.get("artist", ""),
            "cover_url": body.get("cover_url", ""),
            "duration_ms": body.get("duration_ms", 0),
        }).execute()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ══════════════════════════════════════
# ── SOCIAL: Drops ──
# ══════════════════════════════════════

@app.post("/api/social/drop")
async def send_drop(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    body = await request.json()
    receiver_id = body.get("receiver_id")

    if not receiver_id or not body.get("track_id"):
        return JSONResponse({"error": "Missing data"}, status_code=400)

    # Проверяем приватность
    if not await check_privacy(user_id, receiver_id, "vis_drops"):
        return JSONResponse({"error": "User disabled drops"}, status_code=403)

    try:
        supabase.table("inbox_drops").insert({
            "sender_id": user_id,
            "receiver_id": receiver_id,
            "track_id": body["track_id"],
            "track_name": body.get("track_name", ""),
            "artist": body.get("artist", ""),
            "cover_url": body.get("cover_url", ""),
            "album": body.get("album", ""),
        }).execute()
        return JSONResponse({"status": "dropped"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/social/drops")
async def get_drops(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    try:
        res = supabase.table("inbox_drops").select("*").eq(
            "receiver_id", user_id
        ).order("created_at", desc=True).limit(50).execute()

        # Получаем инфо об отправителях
        sender_ids = list(set(d["sender_id"] for d in res.data))
        if sender_ids:
            senders_res = supabase.table("social_users").select(
                "id, username, first_name, photo_url"
            ).in_("id", sender_ids).execute()
            sender_map = {s["id"]: s for s in senders_res.data}
        else:
            sender_map = {}

        drops = []
        for d in res.data:
            sender = sender_map.get(d["sender_id"], {})
            drops.append({
                **d,
                "sender_name": sender.get("first_name", ""),
                "sender_username": sender.get("username", ""),
                "sender_photo": sender.get("photo_url", ""),
            })

        # Помечаем как прочитанные
        supabase.table("inbox_drops").update({"is_read": True}).eq(
            "receiver_id", user_id
        ).eq("is_read", False).execute()

        return JSONResponse({"drops": drops})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/social/drops/unread")
async def get_unread_drops(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    try:
        res = supabase.table("inbox_drops").select(
            "id", count="exact"
        ).eq("receiver_id", user_id).eq("is_read", False).execute()
        return JSONResponse({"unread": res.count or 0})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ══════════════════════════════════════
# ── SOCIAL: Privacy ──
# ══════════════════════════════════════

@app.get("/api/social/privacy")
async def get_privacy(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    res = supabase.table("social_users").select(
        "vis_status, vis_history, vis_drops"
    ).eq("id", user_id).execute()
    return JSONResponse(res.data[0] if res.data else {})


@app.post("/api/social/privacy")
async def set_privacy(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    body = await request.json()
    valid = {"all", "friends", "nobody"}

    updates = {}
    for field in ["vis_status", "vis_history", "vis_drops"]:
        if body.get(field) in valid:
            updates[field] = body[field]

    if updates:
        supabase.table("social_users").update(updates).eq("id", user_id).execute()

    return JSONResponse({"ok": True})


# ══════════════════════════════════════
# ── SOCIAL: Listen Together (HTTP polling) ──
# ══════════════════════════════════════

@app.post("/api/social/session/create")
async def create_session(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    session_id = str(uuid.uuid4())[:8]

    try:
        supabase.table("listen_sessions").insert({
            "id": session_id,
            "host_id": user_id,
        }).execute()
        return JSONResponse({"session_id": session_id})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/social/session/join")
async def join_session(request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    body = await request.json()
    session_id = body.get("session_id")
    user_id = str(user["id"])

    try:
        # Проверяем сессию
        res = supabase.table("listen_sessions").select("*").eq("id", session_id).execute()
        if not res.data:
            return JSONResponse({"error": "Session not found"}, status_code=404)

        supabase.table("session_guests").upsert({
            "session_id": session_id,
            "user_id": user_id,
        }, on_conflict="session_id,user_id").execute()

        session = res.data[0]
        return JSONResponse({
            "status": "joined",
            "track_id": session.get("track_id"),
            "track_name": session.get("track_name"),
            "artist": session.get("artist"),
            "cover_url": session.get("cover_url"),
            "position_ms": session.get("position_ms", 0),
            "is_playing": session.get("is_playing", False),
            "server_time": int(datetime.now(timezone.utc).timestamp() * 1000),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/social/session/update")
async def update_session(request: Request):
    """Хост обновляет состояние сессии"""
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    body = await request.json()
    session_id = body.get("session_id")

    try:
        supabase.table("listen_sessions").update({
            "track_id": body.get("track_id"),
            "track_name": body.get("track_name"),
            "artist": body.get("artist"),
            "cover_url": body.get("cover_url"),
            "position_ms": body.get("position_ms", 0),
            "is_playing": body.get("is_playing", False),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", session_id).eq("host_id", user_id).execute()

        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/social/session/sync/{session_id}")
async def sync_session(session_id: str, request: Request):
    """Гость запрашивает текущее состояние"""
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    try:
        res = supabase.table("listen_sessions").select("*").eq("id", session_id).execute()
        if not res.data:
            return JSONResponse({"error": "Session ended"}, status_code=404)

        session = res.data[0]
        return JSONResponse({
            "track_id": session.get("track_id"),
            "track_name": session.get("track_name"),
            "artist": session.get("artist"),
            "cover_url": session.get("cover_url"),
            "position_ms": session.get("position_ms", 0),
            "is_playing": session.get("is_playing", False),
            "server_time": int(datetime.now(timezone.utc).timestamp() * 1000),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/social/session/{session_id}")
async def close_session(session_id: str, request: Request):
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    supabase.table("session_guests").delete().eq("session_id", session_id).execute()
    supabase.table("listen_sessions").delete().eq("id", session_id).eq("host_id", user_id).execute()
    return JSONResponse({"status": "closed"})


# ══════════════════════════════════════════
# ── EXISTING: Audio streaming & downloads ──
# ══════════════════════════════════════════


# ── ГЛОБАЛЬНЫЙ HTTP КЛИЕНТ ──
http_client = httpx.AsyncClient(
    timeout=httpx.Timeout(connect=5, read=60, write=30, pool=5),
    follow_redirects=True,
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
)

# ── ПОИСК НА SOUNDCLOUD (scsearch5 + матчинг по длительности) ──
def get_audio_info(artist: str, name: str, duration_ms: int = 0):
    GARBAGE_WORDS = {"remix", "bass boosted", "cover", "karaoke", "nightcore",
                     "slowed", "reverb", "sped up", "speed up", "instrumental"}

    def name_is_clean(entry_title: str) -> bool:
        t = entry_title.lower()
        orig_has_remix = "remix" in name.lower()
        for word in GARBAGE_WORDS:
            if word in t and (word != "remix" or not orig_has_remix):
                return False
        return True

    def dur_diff(entry_dur: int) -> int:
        if not duration_ms or not entry_dur:
            return 999
        return abs(entry_dur - duration_ms // 1000)

    query = f"{artist} {name}"
    ydl_opts = {"format": "bestaudio[protocol!*=m3u8][protocol!*=hls]/bestaudio",
                "quiet": True, "noplaylist": True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"scsearch5:{query}", download=False)
            if not info or not info.get("entries"):
                return None

            candidates = []
            for entry in (info["entries"] or []):
                if not entry: continue
                formats = [f for f in entry.get("formats", []) if
                           f.get("acodec") != "none" and f.get("url") and
                           "m3u8" not in f.get("url", "") and
                           f.get("protocol") in ("https", "http", None)]
                if not formats: continue
                best_fmt = sorted(formats, key=lambda f: f.get("abr") or 0, reverse=True)[0]
                candidates.append({
                    "url": best_fmt["url"], "ext": best_fmt.get("ext", "mp3"),
                    "duration": entry.get("duration", 0),
                    "title": entry.get("title", ""),
                    "clean": name_is_clean(entry.get("title", "")),
                    "diff": dur_diff(entry.get("duration", 0)),
                })

            if not candidates: return None
            # Сначала чистые по длительности, потом всё остальное
            clean = [c for c in candidates if c["clean"]]
            pool = sorted(clean or candidates, key=lambda c: c["diff"])
            best = pool[0]
            print(f"[yt-dlp] '{best['title']}' diff={best['diff']}s clean={best['clean']}")
            return {"url": best["url"], "ext": best["ext"], "dur_diff": best["diff"], "sc_duration": best["duration"]}
    except Exception as e:
        print(f"[yt-dlp] Error: {e}")
        return None


async def get_cached_stream_url(track_id: str) -> str | None:
    try:
        res = supabase.table("track_cache").select(
            "sc_url, expires_at, r2_path"
        ).eq("spotify_id", track_id).execute()
        if not res.data: return None
        row = res.data[0]
        if row.get("r2_path"):
            return f"{R2_PUBLIC_URL}/{row['r2_path']}"
        if not row.get("sc_url"): return None
        expires = datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00"))
        if expires > datetime.now(timezone.utc) + timedelta(minutes=5):
            return row["sc_url"]
        return None
    except: return None


async def save_stream_url(track_id: str, url: str, artist: str = "",
                          name: str = "", duration_ms: int = 0,
                          sc_duration: int = 0, dur_diff: int = 0):
    try:
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        supabase.table("track_cache").upsert({
            "spotify_id": track_id,
            "artist": artist[:200] if artist else "",
            "name": name[:200] if name else "",
            "sc_url": url,
            "sc_duration": sc_duration,
            "duration_ms": duration_ms,
            "dur_diff_sec": min(dur_diff, 32767),
            "is_verified": dur_diff < 5,
            "expires_at": expires_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="spotify_id").execute()
    except Exception as e:
        print(f"[cache] {e}")


async def mark_r2_in_cache(track_id: str, r2_path: str):
    try:
        supabase.table("track_cache").upsert({
            "spotify_id": track_id,
            "r2_path": r2_path,
            "expires_at": (datetime.now(timezone.utc) + timedelta(days=365)).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="spotify_id").execute()
    except Exception as e:
        print(f"[cache r2] {e}")


@app.get("/stream")
async def stream_audio(
    artist: str = Query(...),
    name: str = Query(...),
    track_id: str = Query(default=""),
    duration_ms: int = Query(default=0),
    request: Request = None,
):
    # 1. R2 (скачан ранее)
    if track_id:
        try:
            r2.head_object(Bucket=R2_BUCKET, Key=f"{track_id}.mp3")
            from fastapi.responses import RedirectResponse
            return RedirectResponse(url=f"{R2_PUBLIC_URL}/{track_id}.mp3", status_code=302)
        except: pass

    # 2. Link Cache
    stream_url = None
    if track_id:
        stream_url = await get_cached_stream_url(track_id)
        if stream_url: print(f"[cache] HIT {track_id}")

    # 3. yt-dlp поиск (в отдельном thread pool чтобы не блокировать event loop)
    if not stream_url:
        import asyncio
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(None, get_audio_info, artist, name, duration_ms)
        if not info:
            return JSONResponse({"error": "Не найдено"}, status_code=404)
        stream_url = info["url"]
        if track_id:
            asyncio.create_task(save_stream_url(
                    track_id, stream_url,
                    artist=artist, name=name,
                    duration_ms=duration_ms,
                    sc_duration=info.get("sc_duration", 0),
                    dur_diff=info.get("dur_diff", 0)
                ))

    # 4. Стрим 32KB чанками
    range_header = request.headers.get("range") if request else None

    async def generate():
        req_headers = {"Range": range_header} if range_header else {}
        async with http_client.stream("GET", stream_url, headers=req_headers) as r:
            async for chunk in r.aiter_bytes(32768):
                yield chunk

    response_headers = {
        "Access-Control-Allow-Origin": "*",
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
    }
    status_code = 200
    if range_header:
        try:
            async with http_client.stream("GET", stream_url, headers={"Range": range_header}) as probe:
                if probe.status_code == 206:
                    response_headers["Content-Range"] = probe.headers.get("content-range", "")
                    cl = probe.headers.get("content-length", "")
                    if cl: response_headers["Content-Length"] = cl
                    status_code = 206
        except: pass

    return StreamingResponse(generate(), media_type="audio/mpeg",
                             headers=response_headers, status_code=status_code)


@app.post("/prefetch")
async def prefetch_tracks(request: Request):
    body = await request.json()
    tracks = body.get("tracks", [])[:5]  # До 5 треков
    import asyncio

    async def cache_one(t):
        tid = t.get("id", "")
        if not tid: return
        try:
            r2.head_object(Bucket=R2_BUCKET, Key=f"{tid}.mp3")
            return
        except: pass
        if await get_cached_stream_url(tid): return
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(
            None, get_audio_info,
            t.get("artist",""), t.get("name",""), t.get("duration_ms",0)
        )
        if info:
            await save_stream_url(
                tid, info["url"],
                artist=t.get("artist",""), name=t.get("name",""),
                duration_ms=t.get("duration_ms",0),
                sc_duration=info.get("sc_duration", 0),
                dur_diff=info.get("dur_diff", 0)
            )

    # Fire-and-forget — клиент не ждёт завершения
    for t in tracks:
        asyncio.create_task(cache_one(t))

    return JSONResponse({"ok": True, "queued": len(tracks)})

@app.post("/download")
async def download_track(
    user_id: int = Query(...),
    track_id: str = Query(...),
    artist: str = Query(...),
    name: str = Query(...),
    cover: str = Query(default=""),
    album: str = Query(default=""),
    duration: str = Query(default=""),
    duration_ms: int = Query(default=0),
):
    r2_key = f"{track_id}.mp3"
    file_url = f"{R2_PUBLIC_URL}/{r2_key}"

    try:
        r2.head_object(Bucket=R2_BUCKET, Key=r2_key)
    except Exception:
        stream_url = await get_cached_stream_url(track_id)
        if not stream_url:
            info = get_audio_info(artist, name, duration_ms)
            if not info:
                return JSONResponse({"error": "Трек не найден"}, status_code=404)
            stream_url = info["url"]

        try:
            res = await http_client.get(stream_url)
            r2.put_object(Bucket=R2_BUCKET, Key=r2_key,
                          Body=res.content, ContentType="audio/mpeg")
            import asyncio
            asyncio.create_task(mark_r2_in_cache(track_id, r2_key))
        except Exception as e:
            return JSONResponse({"error": f"Ошибка загрузки: {e}"}, status_code=500)

    try:
        supabase.table("downloads").upsert({
            "user_id": user_id, "track_id": track_id,
            "file_url": file_url, "name": name, "artist": artist,
            "album": album, "cover_url": cover, "duration": duration,
        }, on_conflict="user_id,track_id").execute()
    except Exception as e:
        print(f"Supabase error: {e}")

    return JSONResponse({"success": True, "file_url": file_url})

@app.get("/downloads")
async def get_downloads(user_id: int = Query(...)):
    try:
        res = supabase.table("downloads").select("*").eq("user_id", user_id).order("created_at", desc=True).execute()
        return JSONResponse({"downloads": res.data})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/download")
async def delete_download(user_id: int = Query(...), track_id: str = Query(...)):
    try:
        supabase.table("downloads").delete().eq("user_id", user_id).eq("track_id", track_id).execute()
        return JSONResponse({"success": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ══════════════════════════════════════════════════════
# ── ОНБОРДИНГ: Динамическое расширение (снежный ком) ──
# ══════════════════════════════════════════════════════

# In-memory кеш related artists (24 часа)
_related_cache: dict = {}  # artist_id -> {artists: [...], expires: timestamp}

async def get_spotify_related(artist_id: str) -> list:
    """Получаем похожих артистов из Spotify с кешем на 24 часа"""
    import time

    # Проверяем in-memory кеш
    cached = _related_cache.get(artist_id)
    if cached and cached["expires"] > time.time():
        return cached["artists"]

    # Проверяем artist_cache в Supabase
    try:
        res = supabase.table("artist_cache").select(
            "related_ids, related_at, name"
        ).eq("spotify_id", artist_id).execute()
        if res.data and res.data[0].get("related_ids"):
            row = res.data[0]
            related_at = row.get("related_at")
            if related_at:
                ra = datetime.fromisoformat(related_at.replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - ra).total_seconds() < 86400:
                    # Возвращаем из Supabase кеша
                    ids = row["related_ids"]
                    result = [{"id": i} for i in ids]
                    _related_cache[artist_id] = {"artists": result, "expires": time.time() + 86400}
                    return result
    except: pass

    # Запрашиваем Spotify
    try:
        token_r = await http_client.post(
            "https://accounts.spotify.com/api/token",
            headers={
                "Authorization": "Basic " + __import__("base64").b64encode(
                    f"{os.environ.get('SPOTIFY_CLIENT_ID')}:{os.environ.get('SPOTIFY_CLIENT_SECRET')}".encode()
                ).decode()
            },
            data={"grant_type": "client_credentials"}
        )
        token = token_r.json().get("access_token")
        if not token:
            return []

        rel_r = await http_client.get(
            f"https://api.spotify.com/v1/artists/{artist_id}/related-artists",
            headers={"Authorization": f"Bearer {token}"}
        )
        data = rel_r.json()
        artists = data.get("artists", [])[:10]

        result = [
            {
                "id": a["id"],
                "name": a["name"],
                "cover": a["images"][0]["url"] if a.get("images") else None,
                "genres": a.get("genres", [])[:2],
                "popularity": a.get("popularity", 0),
            }
            for a in artists
        ]

        # Сохраняем в in-memory
        _related_cache[artist_id] = {"artists": result, "expires": time.time() + 86400}

        # Сохраняем related_ids в artist_cache
        try:
            supabase.table("artist_cache").upsert({
                "spotify_id": artist_id,
                "related_ids": [a["id"] for a in result],
                "related_at": datetime.now(timezone.utc).isoformat(),
            }, on_conflict="spotify_id").execute()
        except: pass

        return result
    except Exception as e:
        print(f"[onboarding/expand] {e}")
        return []


@app.get("/api/onboarding/expand")
async def onboarding_expand(artist_id: str = Query(...)):
    """Снежный ком: возвращает похожих артистов для выбранного"""
    if not artist_id:
        return JSONResponse({"error": "artist_id required"}, status_code=400)

    related = await get_spotify_related(artist_id)
    return JSONResponse({"artists": related})


@app.get("/api/onboarding/seeds")
async def onboarding_seeds():
    """Начальный набор артистов для онбординга — разные жанры"""
    # Статичный список — 20 топовых артистов разных жанров
    SEED_ARTISTS = [
        {"id": "3TVXtAsR1Inumwj472S9r4", "name": "Drake", "genre": "hip-hop"},
        {"id": "1Xyo4u8uXC1ZmMpatF05PJ", "name": "The Weeknd", "genre": "r&b"},
        {"id": "2YZyLoL8N0Wb9xBt1NhZWg", "name": "Kendrick Lamar", "genre": "rap"},
        {"id": "06HL4z0CvFAxyc27GXpf02", "name": "Taylor Swift", "genre": "pop"},
        {"id": "246dkjvS1zLTtiykXe5h60", "name": "Post Malone", "genre": "hip-hop"},
        {"id": "6qqNVTkY8uBg9cP3Jd7DAH", "name": "Billie Eilish", "genre": "alt-pop"},
        {"id": "0Y5tJX1MQlPlqiwlOH1tJY", "name": "Travis Scott", "genre": "trap"},
        {"id": "4q3ewBCX7sLwd24euuV69X", "name": "Bad Bunny", "genre": "reggaeton"},
        {"id": "5LHRHt1k9lMyONurDHEdrp", "name": "Eminem", "genre": "rap"},
        {"id": "4oLeXFyACqeem2VImYeBFe", "name": "Kanye West", "genre": "rap"},
        {"id": "7dGJo4pcD2V6oG8kP0tJRR", "name": "Eminem", "genre": "rap"},
        # Русские артисты
        {"id": "2A7Ch1dIhGMz3EWyxbNWBo", "name": "Скриптонит", "genre": "рэп"},
        {"id": "3JRMkSBcnAXlmGMBnYsV3c", "name": "Miyagi & Эндшпиль", "genre": "рэп"},
        {"id": "4lGnEkKKONfFpJfcJDWV3w", "name": "Oxxxymiron", "genre": "рэп"},
        {"id": "0HiLKNOkpGYkH6Mwe9YZEM", "name": "PHARAOH", "genre": "рэп"},
        {"id": "1SqNqMmwGJXr9EkAHjifqD", "name": "MORGENSHTERN", "genre": "рэп"},
        # Другие жанры
        {"id": "53XhwfbYqKCa1cC15pYq2q", "name": "Imagine Dragons", "genre": "rock"},
        {"id": "1vCWHaC5f2uS3yhpwWbIA6", "name": "Avicii", "genre": "electronic"},
        {"id": "7n2Ycct7Beij7Dj7meI4X0", "name": "Rammstein", "genre": "metal"},
        {"id": "0C8ZW7ezQVs4URX5aX7Kqx", "name": "Coldplay", "genre": "indie"},
    ]

    # Обогащаем данными из Spotify если есть в кеше
    result = []
    for a in SEED_ARTISTS:
        try:
            cached = supabase.table("artist_cache").select(
                "name, cover_url, genres"
            ).eq("spotify_id", a["id"]).execute()
            if cached.data and cached.data[0].get("cover_url"):
                row = cached.data[0]
                result.append({
                    "id": a["id"],
                    "name": row["name"] or a["name"],
                    "cover": row["cover_url"],
                    "genres": row.get("genres") or [],
                })
                continue
        except: pass
        result.append({"id": a["id"], "name": a["name"], "cover": None, "genres": []})

    return JSONResponse({"artists": result})


@app.get("/api/onboarding/check")
async def onboarding_check(request: Request):
    """Проверяем прошёл ли пользователь онбординг (для восстановления при удалении localStorage)"""
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"completed": False, "artist_ids": []})

    user_id = str(user["id"])
    try:
        res = supabase.table("user_profiles").select("favorite_artist_ids").eq("user_id", user_id).execute()
        if res.data and res.data[0].get("favorite_artist_ids"):
            ids = res.data[0]["favorite_artist_ids"]
            return JSONResponse({"completed": len(ids) >= 3, "artist_ids": ids})
    except: pass
    return JSONResponse({"completed": False, "artist_ids": []})


@app.post("/api/onboarding/reset")
async def onboarding_reset(request: Request):
    """Удаляем профиль пользователя — онбординг покажется заново"""
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    user_id = str(user["id"])
    try:
        supabase.table("user_profiles").delete().eq("user_id", user_id).execute()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/onboarding/complete")
async def onboarding_complete(request: Request):
    """Сохраняем выбранные артисты и запускаем пре-фетч волны"""
    init_data = request.headers.get("x-telegram-init-data", "")
    user = validate_init_data(init_data)
    if not user:
        return JSONResponse({"error": "Invalid auth"}, status_code=401)

    body = await request.json()
    artist_ids = body.get("artist_ids", [])
    if len(artist_ids) < 3:
        return JSONResponse({"error": "Нужно минимум 3 артиста"}, status_code=400)

    user_id = str(user["id"])

    try:
        # Сохраняем профиль
        supabase.table("user_profiles").upsert({
            "user_id": user_id,
            "favorite_artist_ids": artist_ids,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="user_id").execute()

        # Также сохраняем в social_users
        supabase.table("social_users").upsert({
            "id": user_id,
            "last_seen": datetime.now(timezone.utc).isoformat(),
        }, on_conflict="id").execute()

        return JSONResponse({"ok": True, "saved": len(artist_ids)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/health")
async def health():
    return {"status": "ok", "social": True}

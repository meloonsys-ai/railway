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

        return StreamingResponse(generate(), media_type="audio/mpeg",
                                 headers={"Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


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
    r2_key = f"{track_id}.mp3"
    file_url = f"{R2_PUBLIC_URL}/{r2_key}"

    try:
        r2.head_object(Bucket=R2_BUCKET, Key=r2_key)
    except Exception:
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
                Bucket=R2_BUCKET, Key=r2_key,
                Body=audio_data, ContentType="audio/mpeg",
            )
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


@app.get("/health")
async def health():
    return {"status": "ok", "social": True}

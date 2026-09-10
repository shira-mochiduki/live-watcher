import asyncio
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs, urljoin
from html.parser import HTMLParser
import xml.etree.ElementTree as ET

import httpx
from fastapi import FastAPI, Request, Response, HTTPException

# ----------------------------
# Configuration
# ----------------------------

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
CHANNELS_JSON = os.getenv("CHANNELS_JSON", "[]")
PORT = int(os.getenv("PORT", "8000"))

APP_VERSION = "0.1.2"

# Independent-project federation output.
CENTRAL_NODE_URL = os.getenv("CENTRAL_NODE_URL", "").rstrip("/")
CENTRAL_NODE_TOKEN = os.getenv("CENTRAL_NODE_TOKEN", "").strip()
CENTRAL_NODE_TIMEOUT_SECONDS = float(os.getenv("CENTRAL_NODE_TIMEOUT_SECONDS", "5"))
LIVE_WATCHER_INSTANCE = os.getenv("LIVE_WATCHER_INSTANCE", "default").strip() or "default"

# 開始直前に入った待機枠だけ、この間隔で videos.list を確認する
UPCOMING_POLL_SECONDS = int(os.getenv("UPCOMING_POLL_SECONDS", "60"))

# scheduledStartTime の何分前から API 監視を開始するか
UPCOMING_PRESTART_MINUTES = int(os.getenv("UPCOMING_PRESTART_MINUTES", "10"))

# 待機枠が予定時刻を過ぎても upcoming のままの場合の段階バックオフ
UPCOMING_AFTER_15M_SECONDS = int(os.getenv("UPCOMING_AFTER_15M_SECONDS", "300"))    # 5分
UPCOMING_AFTER_1H_SECONDS = int(os.getenv("UPCOMING_AFTER_1H_SECONDS", "900"))      # 15分
UPCOMING_AFTER_6H_SECONDS = int(os.getenv("UPCOMING_AFTER_6H_SECONDS", "3600"))     # 60分

JST = ZoneInfo("Asia/Tokyo")

# WebSub購読更新。Hubから返された lease_seconds を優先し、
# その一定割合まで更新しない。leaseが得られない場合だけ固定値を使う。
RESUBSCRIBE_SECONDS = int(os.getenv("RESUBSCRIBE_SECONDS", "43200"))  # fallback: 12h
WEBSUB_RENEW_RATIO = float(os.getenv("WEBSUB_RENEW_RATIO", "0.80"))
WEBSUB_VERIFY_WAIT_SECONDS = int(os.getenv("WEBSUB_VERIFY_WAIT_SECONDS", "120"))
WEBSUB_LOOP_SECONDS = int(os.getenv("WEBSUB_LOOP_SECONDS", "30"))

# WebSubが未確認/障害中だけRSSを巡回する間隔
RSS_FALLBACK_SECONDS = int(os.getenv("RSS_FALLBACK_SECONDS", "60"))  # 60秒
RSS_DEGRADED_RECHECK_LIMIT = max(1, int(os.getenv("RSS_DEGRADED_RECHECK_LIMIT", "5")))

# WebSub障害中の再購読間隔
WEBSUB_RECOVERY_SECONDS = int(os.getenv("WEBSUB_RECOVERY_SECONDS", "300"))  # 5分

# YouTubeチャンネル /live を使った現在LIVE確認。HTTP確認自体はData API quotaを消費しない。
LIVE_PROBE_SECONDS = int(os.getenv("LIVE_PROBE_SECONDS", "60"))

HUB_URL = "https://pubsubhubbub.appspot.com/subscribe"
YOUTUBE_API = "https://www.googleapis.com/youtube/v3/videos"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("live-watcher")

try:
    CHANNELS: List[dict] = json.loads(CHANNELS_JSON)
except json.JSONDecodeError as e:
    raise RuntimeError(f"CHANNELS_JSON is invalid JSON: {e}")

CHANNEL_MAP: Dict[str, dict] = {
    item["channel_id"]: item
    for item in CHANNELS
    if item.get("channel_id")
}

if not YOUTUBE_API_KEY:
    log.warning("YOUTUBE_API_KEY is not set")
if not PUBLIC_BASE_URL:
    log.warning("PUBLIC_BASE_URL is not set")
if not CENTRAL_NODE_URL:
    log.warning("CENTRAL_NODE_URL is not set; federation events will not be delivered")
if not CHANNEL_MAP:
    log.warning("No valid channels are configured")

def _resolve_db_path() -> str:
    explicit = os.getenv("STATE_DB", "").strip()
    if explicit:
        path = explicit
    elif os.path.isdir("/var/data"):
        path = "/var/data/state.db"
    else:
        path = "state.db"

    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    return path


DB_PATH = _resolve_db_path()

WEBSUB_HEALTH: Dict[str, bool] = {channel_id: False for channel_id in CHANNEL_MAP}
WEBSUB_STATE_LOCK = asyncio.Lock()

# Runtime WebSub timing state.
# A successful VERIFY records the actual lease and next renewal time.
WEBSUB_LEASE_EXPIRES_AT: Dict[str, datetime] = {}
WEBSUB_RENEW_AT: Dict[str, datetime] = {}
WEBSUB_VERIFY_PENDING_UNTIL: Dict[str, datetime] = {}
WEBSUB_LAST_ATTEMPT_AT: Dict[str, datetime] = {}


async def set_websub_health(channel_id: str, healthy: bool, reason: str):
    async with WEBSUB_STATE_LOCK:
        old = WEBSUB_HEALTH.get(channel_id)
        WEBSUB_HEALTH[channel_id] = healthy

    if old != healthy:
        if healthy:
            log.info("WEBSUB HEALTHY channel=%s reason=%s -> RSS FALLBACK OFF", channel_id, reason)
        else:
            log.warning("WEBSUB DEGRADED channel=%s reason=%s -> RSS FALLBACK ON", channel_id, reason)


def unhealthy_channels() -> List[str]:
    return [cid for cid, healthy in WEBSUB_HEALTH.items() if not healthy]


def _websub_now() -> datetime:
    return datetime.now(timezone.utc)


def _lease_still_valid(channel_id: str, now: Optional[datetime] = None) -> bool:
    now = now or _websub_now()
    expires = WEBSUB_LEASE_EXPIRES_AT.get(channel_id)
    return bool(expires and expires > now)


def _record_verified_lease(channel_id: str, lease_seconds_raw: Optional[str]):
    now = _websub_now()
    try:
        lease_seconds = int(lease_seconds_raw or 0)
    except (TypeError, ValueError):
        lease_seconds = 0

    if lease_seconds <= 0:
        lease_seconds = RESUBSCRIBE_SECONDS

    # Keep the ratio sane even if an environment variable is mistyped.
    ratio = min(max(WEBSUB_RENEW_RATIO, 0.50), 0.95)
    renew_after = max(300, int(lease_seconds * ratio))

    WEBSUB_LEASE_EXPIRES_AT[channel_id] = now + timedelta(seconds=lease_seconds)
    WEBSUB_RENEW_AT[channel_id] = now + timedelta(seconds=renew_after)
    WEBSUB_VERIFY_PENDING_UNTIL.pop(channel_id, None)

    log.info(
        "WEBSUB LEASE channel=%s lease=%ss renew_in=%ss renew_ratio=%.2f expires_at=%s",
        channel_id,
        lease_seconds,
        renew_after,
        ratio,
        WEBSUB_LEASE_EXPIRES_AT[channel_id].isoformat(),
    )


def _mark_subscribe_accepted(channel_id: str):
    now = _websub_now()
    WEBSUB_LAST_ATTEMPT_AT[channel_id] = now
    WEBSUB_VERIFY_PENDING_UNTIL[channel_id] = now + timedelta(
        seconds=WEBSUB_VERIFY_WAIT_SECONDS
    )


def _recovery_due(channel_id: str, now: datetime) -> bool:
    pending_until = WEBSUB_VERIFY_PENDING_UNTIL.get(channel_id)
    if pending_until and pending_until > now:
        return False

    last = WEBSUB_LAST_ATTEMPT_AT.get(channel_id)
    if last and (now - last).total_seconds() < WEBSUB_RECOVERY_SECONDS:
        return False

    return True


# ----------------------------
# SQLite state
# ----------------------------

def db_connect():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def _column_exists(con: sqlite3.Connection, table: str, column: str) -> bool:
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)


def init_db():
    with db_connect() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            title TEXT,
            last_state TEXT,
            scheduled_start_time TEXT,
            upcoming_last_checked_at TEXT,
            upcoming_event_emitted INTEGER NOT NULL DEFAULT 0,
            live_event_emitted INTEGER NOT NULL DEFAULT 0,
            ended_event_emitted INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """)

        # 既存DBを壊さず自動マイグレーション
        if not _column_exists(con, "videos", "scheduled_start_time"):
            con.execute("ALTER TABLE videos ADD COLUMN scheduled_start_time TEXT")
            log.info("DB MIGRATION added videos.scheduled_start_time")

        if not _column_exists(con, "videos", "upcoming_last_checked_at"):
            con.execute("ALTER TABLE videos ADD COLUMN upcoming_last_checked_at TEXT")
            log.info("DB MIGRATION added videos.upcoming_last_checked_at")

        for event_column in (
            "upcoming_event_emitted",
            "live_event_emitted",
            "ended_event_emitted",
        ):
            if not _column_exists(con, "videos", event_column):
                con.execute(
                    f"ALTER TABLE videos ADD COLUMN {event_column} INTEGER NOT NULL DEFAULT 0"
                )
                log.info("DB MIGRATION added videos.%s", event_column)

        con.execute("""
        CREATE TABLE IF NOT EXISTS rss_checked (
            video_id TEXT PRIMARY KEY,
            checked_at TEXT NOT NULL,
            result TEXT
        )
        """)
        con.commit()


def upsert_video(
    video_id: str,
    channel_id: str,
    title: str,
    state: str,
    scheduled_start_time: Optional[str] = None,
):
    now = datetime.now(timezone.utc).isoformat()
    with db_connect() as con:
        con.execute("""
        INSERT INTO videos(
            video_id, channel_id, title, last_state,
            scheduled_start_time, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            channel_id=excluded.channel_id,
            title=excluded.title,
            last_state=excluded.last_state,
            scheduled_start_time=
                CASE
                    WHEN excluded.scheduled_start_time IS NOT NULL
                    THEN excluded.scheduled_start_time
                    ELSE videos.scheduled_start_time
                END,
            updated_at=excluded.updated_at
        """, (
            video_id,
            channel_id,
            title,
            state,
            scheduled_start_time,
            now,
        ))
        con.commit()


def get_video_state(video_id: str):
    with db_connect() as con:
        return con.execute(
            "SELECT * FROM videos WHERE video_id=?",
            (video_id,)
        ).fetchone()


def set_event_emitted(video_id: str, state: str):
    column_map = {
        "upcoming": "upcoming_event_emitted",
        "live": "live_event_emitted",
        "ended": "ended_event_emitted",
    }
    column = column_map[state]

    with db_connect() as con:
        con.execute(
            f"UPDATE videos SET {column}=1, updated_at=? WHERE video_id=?",
            (datetime.now(timezone.utc).isoformat(), video_id),
        )
        con.commit()


def mark_video_missing(video_id: str):
    """
    videos.list が200 OKでも対象IDを返さなかった場合、
    削除・非公開化などで追跡不能になった動画として upcoming 監視から外す。
    """
    with db_connect() as con:
        con.execute(
            """
            UPDATE videos
            SET last_state='missing',
                updated_at=?
            WHERE video_id=?
            """,
            (datetime.now(timezone.utc).isoformat(), video_id),
        )
        con.commit()


def _parse_youtube_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None



def _format_jst_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return "-"
    return dt.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S JST")


def _upcoming_poll_interval_seconds(now: datetime, scheduled: Optional[datetime]) -> int:
    """
    scheduledStartTime からの経過時間に応じてポーリング間隔を決める。

    開始10分前〜開始15分後: 60秒
    開始15分後〜1時間後:   5分
    開始1時間後〜6時間後:  15分
    開始6時間後以降:        60分

    scheduledStartTime が取れない場合は従来どおり60秒。
    """
    if scheduled is None:
        return UPCOMING_POLL_SECONDS

    delta = now - scheduled

    if delta <= timedelta(minutes=15):
        return UPCOMING_POLL_SECONDS
    if delta <= timedelta(hours=1):
        return UPCOMING_AFTER_15M_SECONDS
    if delta <= timedelta(hours=6):
        return UPCOMING_AFTER_1H_SECONDS
    return UPCOMING_AFTER_6H_SECONDS


def mark_upcoming_checked(video_ids: List[str]):
    if not video_ids:
        return
    now = datetime.now(timezone.utc).isoformat()
    with db_connect() as con:
        con.executemany(
            """
            UPDATE videos
            SET upcoming_last_checked_at=?
            WHERE video_id=?
            """,
            [(now, video_id) for video_id in video_ids],
        )
        con.commit()


def get_due_upcoming_video_ids() -> List[str]:
    """
    upcoming のうち、scheduledStartTime と段階バックオフを考慮して
    今回 API 確認すべき動画だけ返す。
    """
    now = datetime.now(timezone.utc)
    prestart = timedelta(minutes=UPCOMING_PRESTART_MINUTES)

    with db_connect() as con:
        rows = con.execute("""
            SELECT video_id, scheduled_start_time, upcoming_last_checked_at
            FROM videos
            WHERE last_state='upcoming' AND live_event_emitted=0
        """).fetchall()

    due = []

    for row in rows:
        scheduled = _parse_youtube_time(row["scheduled_start_time"])
        last_checked = _parse_youtube_time(row["upcoming_last_checked_at"])

        # scheduledStartTime がない待機枠は安全側で従来どおり監視
        if scheduled is None:
            interval = UPCOMING_POLL_SECONDS
        else:
            # 開始10分前までは完全に寝かせる
            if now < scheduled - prestart:
                continue
            interval = _upcoming_poll_interval_seconds(now, scheduled)

        if last_checked is None or (now - last_checked).total_seconds() >= interval:
            due.append(row["video_id"])

    return due


def get_sleeping_upcoming_count() -> int:
    now = datetime.now(timezone.utc)
    prestart = timedelta(minutes=UPCOMING_PRESTART_MINUTES)

    with db_connect() as con:
        rows = con.execute("""
            SELECT scheduled_start_time
            FROM videos
            WHERE last_state='upcoming' AND live_event_emitted=0
        """).fetchall()

    count = 0
    for row in rows:
        scheduled = _parse_youtube_time(row["scheduled_start_time"])
        if scheduled is not None and now < scheduled - prestart:
            count += 1

    return count



def get_rss_checked_ids(video_ids: List[str]) -> set:
    if not video_ids:
        return set()

    placeholders = ",".join("?" for _ in video_ids)
    with db_connect() as con:
        rows = con.execute(
            f"SELECT video_id FROM rss_checked WHERE video_id IN ({placeholders})",
            video_ids,
        ).fetchall()

    return {r["video_id"] for r in rows}


def mark_rss_checked(video_id: str, result: str):
    with db_connect() as con:
        con.execute("""
            INSERT INTO rss_checked(video_id, checked_at, result)
            VALUES (?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                checked_at=excluded.checked_at,
                result=excluded.result
        """, (
            video_id,
            datetime.now(timezone.utc).isoformat(),
            result,
        ))
        con.commit()


# ----------------------------
# YouTube / Central Node
# ----------------------------

async def youtube_videos(video_ids: List[str]) -> Dict[str, dict]:
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY is not set")

    unique_ids = list(dict.fromkeys(video_ids))
    results: Dict[str, dict] = {}

    if not unique_ids:
        return results

    async with httpx.AsyncClient(timeout=20) as client:
        for offset in range(0, len(unique_ids), 50):
            batch = unique_ids[offset:offset + 50]

            params = {
                "part": "snippet,status,liveStreamingDetails",
                "id": ",".join(batch),
                "key": YOUTUBE_API_KEY,
            }

            log.info(
                "API BATCH videos.list requested=%d batch=%d",
                len(unique_ids),
                len(batch),
            )

            r = await client.get(YOUTUBE_API, params=params)
            r.raise_for_status()

            for item in r.json().get("items", []):
                if item.get("id"):
                    results[item["id"]] = item

    return results


async def youtube_video(video_id: str) -> Optional[dict]:
    items = await youtube_videos([video_id])
    return items.get(video_id)


def classify_live(video: dict) -> str:
    live = video.get("liveStreamingDetails")
    if not live:
        return "not-live"

    if live.get("actualEndTime"):
        return "ended"
    if live.get("actualStartTime"):
        return "live"
    if live.get("scheduledStartTime"):
        return "upcoming"

    return "live-resource"


def _build_live_event_payload(video: dict, detected_via: str) -> dict:
    snippet = video.get("snippet", {})
    status_data = video.get("status", {})
    live = video.get("liveStreamingDetails", {})
    video_id = video.get("id", "")

    return {
        "channel_id": snippet.get("channelId", ""),
        "video_id": video_id,
        "title": snippet.get("title", ""),
        "youtube_url": f"https://www.youtube.com/watch?v={video_id}" if video_id else None,
        "privacy_status": status_data.get("privacyStatus", "unknown"),
        "scheduled_start_time": live.get("scheduledStartTime"),
        "actual_start_time": live.get("actualStartTime"),
        "actual_end_time": live.get("actualEndTime"),
        "detected_via": detected_via,
    }


async def emit_central_node_event(name: str, video: dict, detected_via: str) -> bool:
    """
    Send an observed fact to Central Node.

    Failure does not stop monitoring. The caller only marks the event as
    emitted after a successful HTTP response, allowing later observations
    to retry delivery.
    """
    if not CENTRAL_NODE_URL:
        log.warning(
            "EVENT NOT DELIVERED name=%s reason=central-node-url-not-configured",
            name,
        )
        return False

    message = {
        "kind": "event",
        "name": name,
        "source": "live-watcher",
        "payload": _build_live_event_payload(video, detected_via),
        "metadata": {
            "watcher_version": APP_VERSION,
            "watcher_instance": LIVE_WATCHER_INSTANCE,
        },
    }

    headers = {"Content-Type": "application/json"}
    if CENTRAL_NODE_TOKEN:
        headers["Authorization"] = f"Bearer {CENTRAL_NODE_TOKEN}"

    try:
        timeout = httpx.Timeout(CENTRAL_NODE_TIMEOUT_SECONDS)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                f"{CENTRAL_NODE_URL}/messages",
                json=message,
                headers=headers,
            )
            response.raise_for_status()

        log.info(
            "EVENT DELIVERED name=%s id=%s channel=%s via=%s",
            name,
            video.get("id", ""),
            video.get("snippet", {}).get("channelId", ""),
            detected_via,
        )
        return True

    except Exception as exc:
        log.warning(
            "EVENT DELIVERY FAILED name=%s id=%s error=%s",
            name,
            video.get("id", ""),
            type(exc).__name__,
        )
        return False


async def process_video_data(video: dict, source: str):
    video_id = video.get("id", "")
    if not video_id:
        return "missing-id"

    snippet = video.get("snippet", {})
    status = video.get("status", {})
    live = video.get("liveStreamingDetails", {})
    channel_id = snippet.get("channelId", "")
    title = snippet.get("title", "")
    privacy = status.get("privacyStatus", "unknown")
    live_state = classify_live(video)
    scheduled_start_time = live.get("scheduledStartTime")

    log.info(
        "VIDEO source=%s id=%s channel=%s privacy=%s live=%s scheduled=%s title=%r",
        source,
        video_id,
        channel_id,
        privacy,
        live_state,
        scheduled_start_time or "-",
        title,
    )

    cfg = CHANNEL_MAP.get(channel_id)
    if not cfg:
        log.info("SKIP id=%s reason=channel-not-configured", video_id)
        return "channel-not-configured"

    if privacy != "public":
        log.info("BLOCK id=%s reason=privacy:%s", video_id, privacy)
        return f"privacy:{privacy}"

    if live_state == "not-live":
        log.info("SKIP id=%s reason=not-live", video_id)
        return "not-live"

    upsert_video(
        video_id,
        channel_id,
        title,
        live_state,
        scheduled_start_time=scheduled_start_time,
    )
    saved = get_video_state(video_id)

    if live_state == "upcoming":
        if scheduled_start_time:
            scheduled = _parse_youtube_time(scheduled_start_time)
            if scheduled:
                wake_at = scheduled - timedelta(minutes=UPCOMING_PRESTART_MINUTES)
                log.info(
                    "UPCOMING SCHEDULE id=%s scheduled_jst=%s api-monitor-from_jst=%s scheduled_utc=%s",
                    video_id,
                    _format_jst_dt(scheduled),
                    _format_jst_dt(wake_at),
                    scheduled.isoformat(),
                )

        if saved and not saved["upcoming_event_emitted"]:
            if await emit_central_node_event("live.upcoming.detected", video, source):
                set_event_emitted(video_id, "upcoming")
        else:
            log.info("SKIP id=%s reason=upcoming-event-already-emitted", video_id)
        return "upcoming"

    elif live_state == "live":
        if saved and not saved["live_event_emitted"]:
            if await emit_central_node_event("live.started", video, source):
                set_event_emitted(video_id, "live")
        else:
            log.info("SKIP id=%s reason=live-event-already-emitted", video_id)
        return "live"

    elif live_state == "ended":
        if saved and not saved["ended_event_emitted"]:
            if await emit_central_node_event("live.ended", video, source):
                set_event_emitted(video_id, "ended")
        else:
            log.info("SKIP id=%s reason=ended-event-already-emitted", video_id)
        return "ended"

    return live_state


async def process_video(video_id: str, source: str):
    video = await youtube_video(video_id)
    if not video:
        log.warning("MISSING id=%s source=%s not-returned-by-api", video_id, source)
        return "missing"

    return await process_video_data(video, source)


# ----------------------------
# WebSub / RSS
# ----------------------------

def topic_url(channel_id: str) -> str:
    return f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"


async def subscribe_channel(channel_id: str):
    if not PUBLIC_BASE_URL:
        raise RuntimeError("PUBLIC_BASE_URL is not set")

    callback = f"{PUBLIC_BASE_URL}/youtube/websub"
    form = {
        "hub.mode": "subscribe",
        "hub.topic": topic_url(channel_id),
        "hub.callback": callback,
        "hub.verify": "async",
    }

    timeout = httpx.Timeout(
        connect=20.0,
        read=90.0,
        write=20.0,
        pool=20.0,
    )

    retry_delays = [0, 10, 30]
    last_error = None

    for attempt, delay in enumerate(retry_delays, start=1):
        if delay:
            log.info(
                "SUBSCRIBE retry-wait channel=%s attempt=%d wait=%ss",
                channel_id,
                attempt,
                delay,
            )
            await asyncio.sleep(delay)

        log.info(
            "SUBSCRIBE attempt channel=%s attempt=%d callback=%s",
            channel_id,
            attempt,
            callback,
        )

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.post(HUB_URL, data=form)

            if r.status_code in (202, 204):
                log.info(
                    "SUBSCRIBE accepted channel=%s status=%s attempt=%d",
                    channel_id,
                    r.status_code,
                    attempt,
                )
                _mark_subscribe_accepted(channel_id)
                return

            last_error = RuntimeError(
                f"Hub rejected request status={r.status_code} "
                f"body={r.text[:300]!r}"
            )

            log.warning(
                "SUBSCRIBE rejected channel=%s status=%s attempt=%d body=%r",
                channel_id,
                r.status_code,
                attempt,
                r.text[:300],
            )

        except httpx.ReadTimeout as e:
            last_error = e
            log.warning("SUBSCRIBE read-timeout channel=%s attempt=%d", channel_id, attempt)
        except httpx.ConnectTimeout as e:
            last_error = e
            log.warning("SUBSCRIBE connect-timeout channel=%s attempt=%d", channel_id, attempt)
        except httpx.HTTPError as e:
            last_error = e
            log.warning(
                "SUBSCRIBE http-error channel=%s attempt=%d error=%r",
                channel_id, attempt, e
            )

    raise RuntimeError(
        f"WebSub subscribe failed after {len(retry_delays)} attempts "
        f"channel={channel_id}: {last_error!r}"
    )


async def subscribe_all(reason: str = "recovery"):
    all_ok = True

    for channel_id in CHANNEL_MAP:
        WEBSUB_LAST_ATTEMPT_AT[channel_id] = _websub_now()
        try:
            await subscribe_channel(channel_id)
            log.info(
                "SUBSCRIBE request accepted; waiting VERIFY channel=%s reason=%s",
                channel_id,
                reason,
            )
        except Exception as e:
            all_ok = False

            # If this was merely a renewal attempt and the already-verified
            # lease is still valid, keep WebSub healthy. A failed renewal does
            # not cancel the existing subscription.
            if _lease_still_valid(channel_id):
                log.warning(
                    "SUBSCRIBE renewal failed but current lease is still valid "
                    "channel=%s reason=%s -> KEEP HEALTHY",
                    channel_id,
                    type(e).__name__,
                )
            else:
                await set_websub_health(
                    channel_id,
                    False,
                    f"subscribe-failed:{type(e).__name__}",
                )

            log.exception("SUBSCRIBE ERROR channel=%s", channel_id)

    return all_ok


def parse_atom(body: bytes):
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
    }

    root = ET.fromstring(body)
    events = []

    for entry in root.findall("atom:entry", ns):
        video_id = entry.findtext("yt:videoId", default="", namespaces=ns)
        channel_id = entry.findtext("yt:channelId", default="", namespaces=ns)
        title = entry.findtext("atom:title", default="", namespaces=ns)
        if video_id:
            events.append({
                "video_id": video_id,
                "channel_id": channel_id,
                "title": title,
            })

    return events


async def fetch_channel_rss(channel_id: str) -> List[str]:
    url = topic_url(channel_id)
    timeout = httpx.Timeout(connect=15.0, read=30.0, write=15.0, pool=15.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(url)
        r.raise_for_status()
        body = r.content

    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        raise RuntimeError(f"RSS XML parse failed channel={channel_id}: {e}") from e

    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "yt": "http://www.youtube.com/xml/schemas/2015",
    }

    ids = []
    for entry in root.findall("atom:entry", ns):
        video_id = entry.findtext("yt:videoId", default="", namespaces=ns).strip()
        if video_id and video_id not in ids:
            ids.append(video_id)

    return ids


async def rss_fallback_monitor():
    """
    RSS safety net.

    WebSub healthy:
      - check only previously unseen RSS video IDs.

    WebSub degraded:
      - re-check the newest RSS entries even when already known, because
        a single video can transition upcoming -> live -> ended.
      - batch candidates across all configured channels into videos.list.
    """
    while True:
        channels = list(CHANNEL_MAP.keys())

        log.info(
            "RSS SAFETY CHECK channels=%d interval=%ss websub_healthy=%d/%d degraded_recheck_limit=%d",
            len(channels),
            RSS_FALLBACK_SECONDS,
            sum(1 for healthy in WEBSUB_HEALTH.values() if healthy),
            len(WEBSUB_HEALTH),
            RSS_DEGRADED_RECHECK_LIMIT,
        )

        channel_cycle: Dict[str, dict] = {}
        all_candidate_ids: List[str] = []

        for channel_id in channels:
            try:
                ids = await fetch_channel_rss(channel_id)

                checked_ids = get_rss_checked_ids(ids)
                new_ids = [
                    video_id
                    for video_id in ids
                    if video_id not in checked_ids
                ]

                websub_healthy = bool(WEBSUB_HEALTH.get(channel_id))

                if websub_healthy:
                    candidate_ids = list(new_ids)
                    mode = "new-only"
                else:
                    recent_ids = ids[:RSS_DEGRADED_RECHECK_LIMIT]
                    candidate_ids = list(dict.fromkeys(new_ids + recent_ids))
                    mode = "degraded-recheck"

                channel_cycle[channel_id] = {
                    "candidate_ids": candidate_ids,
                    "websub_healthy": websub_healthy,
                }

                for video_id in candidate_ids:
                    if video_id not in all_candidate_ids:
                        all_candidate_ids.append(video_id)

                log.info(
                    "RSS SAFETY channel=%s entries=%d checked=%d new=%d candidates=%d websub=%s mode=%s",
                    channel_id,
                    len(ids),
                    len(checked_ids),
                    len(new_ids),
                    len(candidate_ids),
                    "healthy" if websub_healthy else "degraded",
                    mode,
                )

            except Exception:
                log.exception("RSS SAFETY FETCH ERROR channel=%s", channel_id)

        if not all_candidate_ids:
            log.info("RSS SAFETY no-api-candidates -> API SKIP")
            await asyncio.sleep(RSS_FALLBACK_SECONDS)
            continue

        log.info(
            "RSS SAFETY API BATCH candidates=%d ids=%s",
            len(all_candidate_ids),
            ",".join(all_candidate_ids),
        )

        try:
            videos = await youtube_videos(all_candidate_ids)
        except Exception:
            log.exception(
                "RSS SAFETY API ERROR candidates=%d",
                len(all_candidate_ids),
            )
            await asyncio.sleep(RSS_FALLBACK_SECONDS)
            continue

        for channel_id, cycle in channel_cycle.items():
            source = (
                "rss-safety"
                if cycle["websub_healthy"]
                else "rss-degraded-recheck"
            )

            for video_id in cycle["candidate_ids"]:
                video = videos.get(video_id)

                if not video:
                    log.warning(
                        "MISSING id=%s source=%s not-returned-by-api",
                        video_id,
                        source,
                    )
                    mark_rss_checked(video_id, "missing")
                    continue

                try:
                    result = await process_video_data(video, source)
                    mark_rss_checked(video_id, result or "processed")
                except Exception:
                    log.exception(
                        "RSS VIDEO ERROR channel=%s id=%s source=%s",
                        channel_id,
                        video_id,
                        source,
                    )

        await asyncio.sleep(RSS_FALLBACK_SECONDS)


# ----------------------------
# Current LIVE probe
# ----------------------------

def extract_watch_video_id(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)

        if parsed.netloc not in (
            "www.youtube.com",
            "youtube.com",
            "m.youtube.com",
        ):
            return None

        if parsed.path != "/watch":
            return None

        video_id = (parse_qs(parsed.query).get("v") or [""])[0].strip()
        return video_id or None

    except Exception:
        return None


class CanonicalLinkParser(HTMLParser):
    """Extract the first <link rel="canonical" href="..."> value."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.canonical_href: Optional[str] = None

    def handle_starttag(self, tag: str, attrs):
        if self.canonical_href is not None or tag.lower() != "link":
            return

        attr_map = {
            str(key).lower(): value
            for key, value in attrs
            if key
        }

        rel_value = str(attr_map.get("rel") or "")
        rel_tokens = {
            token.strip().lower()
            for token in rel_value.split()
            if token.strip()
        }

        href = attr_map.get("href")
        if "canonical" in rel_tokens and href:
            self.canonical_href = str(href).strip()


def extract_canonical_watch_video_id(html: str, base_url: str) -> Optional[str]:
    """
    Extract a YouTube watch video ID from the HTML canonical link.

    YouTube may return /channel/<id>/live as the final URL even while a
    livestream is active, with the real /watch?v=... URL exposed only as
    <link rel="canonical"> inside the HTML.
    """
    if not html:
        return None

    parser = CanonicalLinkParser()

    try:
        parser.feed(html)
        parser.close()
    except Exception:
        return None

    if not parser.canonical_href:
        return None

    canonical_url = urljoin(base_url, parser.canonical_href)
    return extract_watch_video_id(canonical_url)


async def probe_channel_live(channel_id: str) -> Optional[str]:
    url = f"https://www.youtube.com/channel/{channel_id}/live"
    timeout = httpx.Timeout(
        connect=15.0,
        read=30.0,
        write=15.0,
        pool=15.0,
    )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/142.0 Safari/537.36"
        )
    }

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers=headers,
    ) as client:
        r = await client.get(url)
        r.raise_for_status()
        final_url = str(r.url)
        response_html = r.text

    # 1) Traditional behavior: /live redirects directly to /watch?v=...
    video_id = extract_watch_video_id(final_url)

    if video_id:
        log.info(
            "LIVE PROBE channel=%s detected-video-id=%s method=redirect final_url=%s",
            channel_id,
            video_id,
            final_url,
        )
        return video_id

    # 2) Current fallback: final URL may remain /live while canonical points
    #    at the real /watch?v=... page.
    video_id = extract_canonical_watch_video_id(response_html, final_url)

    if video_id:
        log.info(
            "LIVE PROBE channel=%s detected-video-id=%s method=canonical final_url=%s",
            channel_id,
            video_id,
            final_url,
        )
        return video_id

    log.info(
        "LIVE PROBE channel=%s no-current-live methods=redirect,canonical final_url=%s",
        channel_id,
        final_url,
    )
    return None


async def live_probe_monitor():
    """
    WebSubやRSSで配信開始を拾えない場合の第三の安全網。
    /live は毎分確認するが、Video IDが既にlive.started発行済みならData APIは呼ばない。
    """
    last_seen: Dict[str, Optional[str]] = {
        channel_id: None
        for channel_id in CHANNEL_MAP
    }

    while True:
        for channel_id in CHANNEL_MAP:
            try:
                video_id = await probe_channel_live(channel_id)

                if not video_id:
                    last_seen[channel_id] = None
                    continue

                saved = get_video_state(video_id)

                if saved and saved["live_event_emitted"]:
                    if last_seen.get(channel_id) != video_id:
                        log.info(
                            "LIVE PROBE channel=%s id=%s already-live-event-emitted -> API SKIP",
                            channel_id,
                            video_id,
                        )

                    last_seen[channel_id] = video_id
                    continue

                log.info(
                    "LIVE PROBE channel=%s id=%s -> API CHECK",
                    channel_id,
                    video_id,
                )

                await process_video(video_id, "live-probe")
                last_seen[channel_id] = video_id

            except Exception:
                log.exception("LIVE PROBE ERROR channel=%s", channel_id)

        await asyncio.sleep(LIVE_PROBE_SECONDS)


# ----------------------------
# Background workers
# ----------------------------

async def upcoming_monitor():
    """
    毎分ループ自体は維持するが、APIを叩くのは
    scheduledStartTime の UPCOMING_PRESTART_MINUTES 分前以降だけ。
    """
    while True:
        try:
            due_ids = get_due_upcoming_video_ids()
            sleeping = get_sleeping_upcoming_count()

            if due_ids or sleeping:
                log.info(
                    "UPCOMING-POLL due=%d sleeping=%d prestart=%dmin backoff=60s/5m/15m/60m",
                    len(due_ids),
                    sleeping,
                    UPCOMING_PRESTART_MINUTES,
                )

            if due_ids:
                # 複数待機枠があっても最大50件を1 APIリクエストにまとめる
                videos = await youtube_videos(due_ids)
                mark_upcoming_checked(due_ids)

                for video_id in due_ids:
                    video = videos.get(video_id)
                    if not video:
                        log.warning(
                            "MISSING id=%s source=upcoming-poll not-returned-by-api",
                            video_id,
                        )
                        mark_video_missing(video_id)
                        log.info(
                            "UPCOMING REMOVE id=%s reason=missing-from-api -> monitoring-stopped",
                            video_id,
                        )
                        continue

                    try:
                        await process_video_data(video, "upcoming-poll")
                    except Exception:
                        log.exception("UPCOMING ERROR id=%s", video_id)

        except Exception:
            log.exception("UPCOMING MONITOR ERROR")

        await asyncio.sleep(UPCOMING_POLL_SECONDS)


async def resubscribe_loop():
    # Initial subscription attempt. After a 202/204 we wait for VERIFY instead
    # of scheduling another recovery immediately.
    await subscribe_all(reason="startup")

    while True:
        await asyncio.sleep(WEBSUB_LOOP_SECONDS)
        now = _websub_now()

        for channel_id in CHANNEL_MAP:
            healthy = WEBSUB_HEALTH.get(channel_id, False)
            pending_until = WEBSUB_VERIFY_PENDING_UNTIL.get(channel_id)

            if pending_until and pending_until > now:
                remaining = int((pending_until - now).total_seconds())
                log.info(
                    "WEBSUB VERIFY PENDING channel=%s timeout_in=%ss",
                    channel_id,
                    max(0, remaining),
                )
                continue

            if healthy:
                renew_at = WEBSUB_RENEW_AT.get(channel_id)
                expires_at = WEBSUB_LEASE_EXPIRES_AT.get(channel_id)

                # No lease info should be rare. Use the configured fallback.
                if renew_at is None:
                    last = WEBSUB_LAST_ATTEMPT_AT.get(channel_id, now)
                    renew_at = last + timedelta(seconds=RESUBSCRIBE_SECONDS)
                    WEBSUB_RENEW_AT[channel_id] = renew_at

                if now < renew_at:
                    continue

                log.info(
                    "RESUBSCRIBE renewal starting channel=%s lease_expires_at=%s",
                    channel_id,
                    expires_at.isoformat() if expires_at else "unknown",
                )
                WEBSUB_LAST_ATTEMPT_AT[channel_id] = now
                try:
                    await subscribe_channel(channel_id)
                except Exception as e:
                    if _lease_still_valid(channel_id, now):
                        # Retry later, but never flip a valid subscription to degraded.
                        WEBSUB_RENEW_AT[channel_id] = now + timedelta(
                            seconds=WEBSUB_RECOVERY_SECONDS
                        )
                        log.warning(
                            "RESUBSCRIBE failed channel=%s error=%s current-lease-valid=1 "
                            "retry_in=%ss -> KEEP HEALTHY",
                            channel_id,
                            type(e).__name__,
                            WEBSUB_RECOVERY_SECONDS,
                        )
                    else:
                        await set_websub_health(
                            channel_id,
                            False,
                            f"renewal-failed:{type(e).__name__}",
                        )
                        log.exception("RESUBSCRIBE ERROR channel=%s", channel_id)
                continue

            # Degraded/unverified state. Retry only when the pending VERIFY
            # window has elapsed and the recovery interval has passed.
            if not _recovery_due(channel_id, now):
                continue

            log.info("WEBSUB RECOVERY attempt starting channel=%s", channel_id)
            WEBSUB_LAST_ATTEMPT_AT[channel_id] = now
            try:
                await subscribe_channel(channel_id)
                log.info(
                    "SUBSCRIBE request accepted; waiting VERIFY channel=%s reason=recovery",
                    channel_id,
                )
            except Exception as e:
                await set_websub_health(
                    channel_id,
                    False,
                    f"subscribe-failed:{type(e).__name__}",
                )
                log.exception("SUBSCRIBE ERROR channel=%s", channel_id)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    log.info(
        "STATE DB path=%s persistent_hint=%s",
        os.path.abspath(DB_PATH),
        "yes" if os.path.abspath(DB_PATH).startswith("/var/data/") else "check-mount",
    )

    tasks = [
        asyncio.create_task(upcoming_monitor()),
        asyncio.create_task(resubscribe_loop()),
        asyncio.create_task(rss_fallback_monitor()),
        asyncio.create_task(live_probe_monitor()),
    ]

    log.info(
        "START channels=%d upcoming_poll=%ss upcoming_prestart=%dmin "
        "upcoming_backoff=60s/5m/15m/60m rss_safety=%ss live_probe=%ss "
        "websub_recovery=%ss websub_renew_ratio=%.2f verify_wait=%ss public_base=%s central_node=%s",
        len(CHANNEL_MAP),
        UPCOMING_POLL_SECONDS,
        UPCOMING_PRESTART_MINUTES,
        RSS_FALLBACK_SECONDS,
        LIVE_PROBE_SECONDS,
        WEBSUB_RECOVERY_SECONDS,
        WEBSUB_RENEW_RATIO,
        WEBSUB_VERIFY_WAIT_SECONDS,
        PUBLIC_BASE_URL or "(unset)",
        CENTRAL_NODE_URL or "(unset)",
    )

    yield

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(title="Live Watcher", version=APP_VERSION, lifespan=lifespan)


# ----------------------------
# HTTP endpoints
# ----------------------------

@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "live-watcher",
        "version": APP_VERSION,
        "channels": len(CHANNEL_MAP),
        "upcoming_poll_seconds": UPCOMING_POLL_SECONDS,
        "upcoming_prestart_minutes": UPCOMING_PRESTART_MINUTES,
        "rss_safety_seconds": RSS_FALLBACK_SECONDS,
        "rss_degraded_recheck_limit": RSS_DEGRADED_RECHECK_LIMIT,
        "rss_safety_always_on": True,
        "live_probe_seconds": LIVE_PROBE_SECONDS,
        "live_probe_always_on": True,
        "websub_recovery_seconds": WEBSUB_RECOVERY_SECONDS,
        "websub_health": WEBSUB_HEALTH,
        "rss_checked_cache": True,
        "videos_list_batching": True,
        "scheduled_start_aware_polling": True,
        "upcoming_backoff": {
            "prestart_to_plus15m_seconds": UPCOMING_POLL_SECONDS,
            "plus15m_to_plus1h_seconds": UPCOMING_AFTER_15M_SECONDS,
            "plus1h_to_plus6h_seconds": UPCOMING_AFTER_1H_SECONDS,
            "after_plus6h_seconds": UPCOMING_AFTER_6H_SECONDS,
        },
        "display_timezone": "Asia/Tokyo",
        "central_node_configured": bool(CENTRAL_NODE_URL),
        "federation_events": [
            "live.upcoming.detected",
            "live.started",
            "live.ended",
        ],
    }


@app.get("/health")
async def health():
    return {
        "ok": True,
        "websub_health": WEBSUB_HEALTH,
        "rss_safety_always_on": True,
        "rss_safety_seconds": RSS_FALLBACK_SECONDS,
        "rss_degraded_recheck_limit": RSS_DEGRADED_RECHECK_LIMIT,
        "live_probe_seconds": LIVE_PROBE_SECONDS,
        "scheduled_start_aware_polling": True,
        "central_node_configured": bool(CENTRAL_NODE_URL),
        "version": APP_VERSION,
    }


@app.get("/youtube/websub")
async def websub_verify(request: Request):
    q = request.query_params
    mode = q.get("hub.mode")
    topic = q.get("hub.topic")
    challenge = q.get("hub.challenge")

    log.info(
        "VERIFY mode=%s topic=%s lease=%s",
        mode, topic, q.get("hub.lease_seconds")
    )

    if mode not in ("subscribe", "unsubscribe") or not challenge:
        raise HTTPException(status_code=400, detail="invalid verification request")

    allowed_topics = {topic_url(cid) for cid in CHANNEL_MAP}
    if topic and topic not in allowed_topics:
        log.warning("VERIFY rejected unknown topic=%s", topic)
        raise HTTPException(status_code=404, detail="unknown topic")

    if mode == "subscribe" and topic:
        verified_channel = next(
            (cid for cid in CHANNEL_MAP if topic_url(cid) == topic),
            None,
        )
        if verified_channel:
            _record_verified_lease(
                verified_channel,
                q.get("hub.lease_seconds"),
            )
            await set_websub_health(verified_channel, True, "hub-verify")

    elif mode == "unsubscribe" and topic:
        unsubscribed_channel = next(
            (cid for cid in CHANNEL_MAP if topic_url(cid) == topic),
            None,
        )
        if unsubscribed_channel:
            await set_websub_health(unsubscribed_channel, False, "hub-unsubscribe")

    return Response(content=challenge, media_type="text/plain")


@app.post("/youtube/websub")
async def websub_notification(request: Request):
    body = await request.body()

    try:
        events = parse_atom(body)
    except ET.ParseError:
        log.exception("PUSH invalid XML")
        return Response(status_code=204)

    log.info("PUSH entries=%d", len(events))

    for event in events:
        video_id = event["video_id"]
        channel_id = event["channel_id"]

        log.info(
            "PUSH EVENT id=%s channel=%s title=%r",
            video_id, channel_id, event["title"]
        )

        if channel_id in CHANNEL_MAP:
            await set_websub_health(channel_id, True, "push-received")

        asyncio.create_task(process_video(video_id, "websub"))

    return Response(status_code=204)


@app.post("/admin/subscribe")
async def admin_subscribe():
    await subscribe_all()
    return {"ok": True, "channels": list(CHANNEL_MAP.keys())}

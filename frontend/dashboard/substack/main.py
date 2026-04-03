"""
main.py — FastAPI backend for privacy-first analytics engine.
Bot filtering, geo lookup, SSE live stream, pre-aggregated queries.
"""

import asyncio
import datetime as _dt
import hashlib
import json
import os
import re
import time
from collections import defaultdict
from typing import Optional

import httpx
from fastapi import FastAPI, Request, Response, Query
from fastapi.responses import (
    HTMLResponse, FileResponse, StreamingResponse, JSONResponse
)
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from database import (
    init_db, get_db, insert_event,
    get_hourly_stats, get_top_referrers, get_funnel,
    get_heatmap, get_recent_events, get_geo_data,
    get_dashboard_summary,
)

# ── App Setup ─────────────────────────────────────────────────────────────────

app = FastAPI(title="WebAnalytics", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static directory for assets
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".")), name="static")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# SSE subscribers
_sse_subscribers: list[asyncio.Queue] = []

# Rate limiter: ip -> list of timestamps
_rate_map: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT_WINDOW = 60     # seconds
RATE_LIMIT_MAX    = 50     # max events per window

# Timing check: session_id -> last event timestamp
_timing_map: dict[str, float] = {}
MIN_EVENT_GAP_MS = 100     # minimum ms between events

# ── Bot Detection ─────────────────────────────────────────────────────────────

BOT_UA_PATTERNS = re.compile(
    r"(bot|crawl|spider|slurp|mediapartners|headless|phantom|selenium|puppeteer"
    r"|lighthouse|pagespeed|gtmetrix|pingdom|uptimerobot|monitor|check|scan"
    r"|wget|curl|python-requests|httpx|go-http|java/|nutch|scrapy"
    r"|baiduspider|yandex|sogou|exabot|facebot|ia_archiver|archive\.org"
    r"|semrush|ahrefs|mj12bot|dotbot|petalbot|bytespider"
    r"|googlebot|bingbot|duckduckbot|yahoo)",
    re.IGNORECASE,
)

SUSPICIOUS_UA_MIN_LEN = 20  # very short UAs are suspicious


def _is_bot_ua(ua: str) -> bool:
    if not ua or len(ua) < SUSPICIOUS_UA_MIN_LEN:
        return True
    return bool(BOT_UA_PATTERNS.search(ua))


def _check_rate_limit(ip: str) -> bool:
    """Returns True if rate-limited (too many events)."""
    now = time.time()
    timestamps = _rate_map[ip]
    # Prune old entries
    _rate_map[ip] = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_map[ip]) >= RATE_LIMIT_MAX:
        return True
    _rate_map[ip].append(now)
    return False


def _check_timing(session_id: str) -> bool:
    """Returns True if timing is suspiciously fast (bot-like)."""
    now = time.time() * 1000  # ms
    last = _timing_map.get(session_id)
    _timing_map[session_id] = now
    if last is not None and (now - last) < MIN_EVENT_GAP_MS:
        return True
    return False


def _anonymize_ip(ip: str) -> str:
    """Hash the IP for privacy — we still use original for geo & rate limiting."""
    return hashlib.sha256(ip.encode()).hexdigest()[:16]


def _detect_bot(ua: str, ip: str, session_id: str, accept_lang: str | None) -> tuple[bool, str]:
    """
    Multi-layer bot detection. Returns (is_bot, reason).
    """
    if _is_bot_ua(ua):
        return True, "bot_useragent"
    if _check_rate_limit(ip):
        return True, "rate_limited"
    if _check_timing(session_id):
        return True, "timing_too_fast"
    if not accept_lang:
        return True, "missing_accept_language"
    return False, ""


# ── Geo Lookup ────────────────────────────────────────────────────────────────

_geo_cache: dict[str, dict] = {}

async def _geo_lookup(ip: str) -> dict:
    """Lookup geo data from ip-api.com with caching."""
    if ip in ("127.0.0.1", "::1", "testclient", "localhost"):
        return {"lat": None, "lng": None, "country": "", "city": ""}
    if ip in _geo_cache:
        return _geo_cache[ip]
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"http://ip-api.com/json/{ip}?fields=lat,lon,country,city,status")
            if resp.status_code == 200:
                d = resp.json()
                if d.get("status") == "success":
                    result = {
                        "lat": d.get("lat"),
                        "lng": d.get("lon"),
                        "country": d.get("country", ""),
                        "city": d.get("city", ""),
                    }
                    _geo_cache[ip] = result
                    return result
    except Exception:
        pass
    return {"lat": None, "lng": None, "country": "", "city": ""}


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def on_startup():
    init_db()


# ── Static Serving ────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return FileResponse(os.path.join(BASE_DIR, "dashboard.html"))


@app.get("/tracker.js")
async def serve_tracker():
    return FileResponse(
        os.path.join(BASE_DIR, "tracker.js"),
        media_type="application/javascript",
    )


@app.get("/penguin.png")
async def serve_penguin():
    return FileResponse(
        os.path.join(BASE_DIR, "penguin.png"),
        media_type="image/png",
    )


# ── POST /collect ─────────────────────────────────────────────────────────────

@app.post("/collect")
async def collect(request: Request):
    """Receive analytics events, run bot filter, write to DB."""
    try:
        body = await request.body()
        data = json.loads(body)
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)

    # Extract client info
    ip = request.client.host if request.client else "unknown"
    ua = data.get("user_agent", request.headers.get("user-agent", ""))
    session_id = data.get("session_id", "")
    accept_lang = request.headers.get("accept-language")

    # Bot detection
    is_bot, reason = _detect_bot(ua, ip, session_id, accept_lang)

    # Geo lookup (skip for bots)
    geo = {"lat": None, "lng": None, "country": "", "city": ""}
    if not is_bot:
        geo = await _geo_lookup(ip)

    # Build event record
    event_data = {
        "session_id": session_id,
        "event_type": data.get("event_type", "pageview"),
        "page_url": data.get("page_url", "/"),
        "referrer": data.get("referrer", ""),
        "timestamp": data.get("timestamp"),
        "click_x": data.get("click_x"),
        "click_y": data.get("click_y"),
        "vp_width": data.get("vp_width"),
        "vp_height": data.get("vp_height"),
        "user_agent": ua,
        "ip_address": _anonymize_ip(ip),
        "latitude": geo["lat"],
        "longitude": geo["lng"],
        "country": geo["country"],
        "city": geo["city"],
        "is_bot": is_bot,
    }

    with get_db() as db:
        event = insert_event(db, event_data)

    # Notify SSE subscribers (non-bot events only)
    if not is_bot:
        sse_payload = {
            "id": event.id,
            "event_type": event.event_type,
            "page_url": event.page_url,
            "timestamp": event.timestamp.isoformat() if event.timestamp else "",
            "latitude": event.latitude,
            "longitude": event.longitude,
            "country": event.country,
            "city": event.city,
        }
        dead = []
        for i, q in enumerate(_sse_subscribers):
            try:
                q.put_nowait(sse_payload)
            except asyncio.QueueFull:
                dead.append(i)
        for i in reversed(dead):
            _sse_subscribers.pop(i)

    return JSONResponse({"ok": True, "bot": is_bot, "reason": reason})


# ── GET /stats ────────────────────────────────────────────────────────────────

@app.get("/stats")
async def stats(hours: int = Query(24, ge=1, le=168)):
    with get_db() as db:
        data = get_hourly_stats(db, hours)
    return JSONResponse(data)


# ── GET /referrers ────────────────────────────────────────────────────────────

@app.get("/referrers")
async def referrers(limit: int = Query(10, ge=1, le=50)):
    with get_db() as db:
        data = get_top_referrers(db, limit)
    return JSONResponse(data)


# ── GET /funnel ───────────────────────────────────────────────────────────────

@app.get("/funnel")
async def funnel(steps: str = Query(..., description="Comma-separated step URLs")):
    step_list = [s.strip() for s in steps.split(",") if s.strip()]
    if not step_list:
        return JSONResponse({"error": "no_steps"}, status_code=400)
    with get_db() as db:
        data = get_funnel(db, step_list)
    return JSONResponse(data)


# ── GET /heatmap ──────────────────────────────────────────────────────────────

@app.get("/heatmap")
async def heatmap(page: str = Query("/", description="Page URL to get heatmap for")):
    with get_db() as db:
        data = get_heatmap(db, page)
    return JSONResponse(data)


# ── GET /live (SSE) ───────────────────────────────────────────────────────────

@app.get("/live")
async def live_stream(request: Request):
    """Server-Sent Events stream for real-time analytics."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    _sse_subscribers.append(queue)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield f": keepalive\n\n"
        finally:
            if queue in _sse_subscribers:
                _sse_subscribers.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ── GET /geo ──────────────────────────────────────────────────────────────────

@app.get("/geo")
async def geo(limit: int = Query(100, ge=1, le=500)):
    with get_db() as db:
        data = get_geo_data(db, limit)
    return JSONResponse(data)


# ── GET /summary ──────────────────────────────────────────────────────────────

@app.get("/summary")
async def summary():
    with get_db() as db:
        data = get_dashboard_summary(db)
    return JSONResponse(data)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "time": _dt.datetime.utcnow().isoformat()}

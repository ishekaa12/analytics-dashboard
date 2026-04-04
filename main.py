"""
main.py — Unified FastAPI backend for Luma + Substack analytics.
Every table and endpoint is namespaced by site_id ("luma" | "substack").
"""

import asyncio
import hashlib
import json
import re
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional

import httpx
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, Text,
    Index, func, select, distinct,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", "./analytics.db")
DATABASE_URL = f"sqlite+aiosqlite:///{DB_PATH}"
VALID_SITES = ("luma", "substack")

BOT_UA_PATTERNS = re.compile(
    r"(bot|crawl|spider|slurp|mediapartners|headless|phantom|selenium|puppeteer"
    r"|lighthouse|pagespeed|gtmetrix|pingdom|uptimerobot|monitor|check|scan"
    r"|wget|curl|python-requests|httpx|go-http|java/|nutch|scrapy"
    r"|baiduspider|yandex|sogou|exabot|facebot|ia_archiver|archive\.org"
    r"|semrush|ahrefs|mj12bot|dotbot|petalbot|bytespider"
    r"|googlebot|bingbot|duckduckbot|yahoo"
    r"|twitterbot|linkedinbot|applebot|facebookexternalhit)",
    re.IGNORECASE,
)
SUSPICIOUS_UA_MIN_LEN = 20
RATE_LIMIT = 100
RATE_LIMIT_WINDOW = 60
MIN_EVENT_GAP_MS = 100

Base = declarative_base()

# ── Models (all include site_id) ──────────────────────────────────────────────

class Event(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(String(16), default="luma", index=True)
    session_id = Column(String(64), nullable=True, index=True)
    event_type = Column(String(16), default="pageview", index=True)
    page_url = Column(String(2048))
    referrer = Column(String(2048))
    user_agent = Column(String(512))
    ip_address = Column(String(45))
    ip_hash = Column(String(64))
    screen_width = Column(Integer)
    screen_height = Column(Integer)
    vp_width = Column(Integer, nullable=True)
    vp_height = Column(Integer, nullable=True)
    click_x = Column(Integer)
    click_y = Column(Integer)
    timestamp = Column(DateTime)
    is_bot = Column(Boolean, default=False)
    country = Column(String(64), nullable=True)
    city = Column(String(128), nullable=True)
    region = Column(String(128), nullable=True)
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)

class HourlySummary(Base):
    __tablename__ = "hourly_summary"
    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(String(16), default="luma", index=True)
    hour = Column(DateTime, index=True)
    page_url = Column(String(2048))
    pageviews = Column(Integer, default=0)
    unique_visitors = Column(Integer, default=0)
    clicks = Column(Integer, default=0)

class ReferrerSummary(Base):
    __tablename__ = "referrer_summary"
    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(String(16), default="luma", index=True)
    hour = Column(DateTime, index=True)
    referrer = Column(String(2048))
    count = Column(Integer, default=0)

class ClicksHourly(Base):
    __tablename__ = "clicks_hourly"
    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(String(16), default="luma", index=True)
    hour = Column(DateTime, index=True)
    page_url = Column(String(2048))
    x = Column(Integer)
    y = Column(Integer)
    count = Column(Integer, default=0)

class GeoSummary(Base):
    __tablename__ = "geo_summary"
    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(String(16), default="luma", index=True)
    hour = Column(DateTime, index=True)
    lat = Column(Float)
    lng = Column(Float)
    city = Column(String(128), nullable=True)
    region = Column(String(128), nullable=True)
    country = Column(String(64), nullable=True)
    count = Column(Integer, default=0)

# ── App Setup ─────────────────────────────────────────────────────────────────

app = FastAPI(title="Analytics Engine — Luma + Substack")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession)

live_subscribers: set[asyncio.Queue] = set()
ip_request_counts: dict = defaultdict(lambda: {"count": 0, "window_start": datetime.utcnow()})
_timing_map: dict[str, float] = {}
_geo_cache: dict[str, dict] = {}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _validate_site(site: str) -> str:
    if site not in VALID_SITES:
        raise HTTPException(status_code=400, detail=f"Invalid site: {site}. Must be one of {VALID_SITES}")
    return site

def is_bot_user_agent(ua: str) -> bool:
    if not ua or len(ua) < SUSPICIOUS_UA_MIN_LEN:
        return True
    return bool(BOT_UA_PATTERNS.search(ua))

def is_rate_limited(ip_address: str) -> bool:
    now = datetime.utcnow()
    ip_data = ip_request_counts[ip_address]
    if (now - ip_data["window_start"]).total_seconds() > RATE_LIMIT_WINDOW:
        ip_data["count"] = 0
        ip_data["window_start"] = now
    ip_data["count"] += 1
    return ip_data["count"] > RATE_LIMIT

def check_timing(session_id: str) -> bool:
    if not session_id:
        return False
    now = time.time() * 1000
    last = _timing_map.get(session_id)
    _timing_map[session_id] = now
    if last is not None and (now - last) < MIN_EVENT_GAP_MS:
        return True
    return False

def detect_bot(ua, ip, session_id, accept_lang):
    if is_bot_user_agent(ua):
        return True, "bot_useragent"
    if is_rate_limited(ip):
        return True, "rate_limited"
    if check_timing(session_id or ""):
        return True, "timing_too_fast"
    if not accept_lang:
        return True, "missing_accept_language"
    return False, ""

def hash_ip(ip_address: str) -> str:
    return hashlib.sha256(ip_address.encode()).hexdigest()

def get_hour_bucket(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0)

async def lookup_geo(ip_address: str) -> tuple:
    if ip_address in ("127.0.0.1", "::1", "testclient", "localhost"):
        return None, None, None, None, None
    if ip_address in _geo_cache:
        c = _geo_cache[ip_address]
        return c.get("lat"), c.get("lng"), c.get("country"), c.get("city"), c.get("region")
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"http://ip-api.com/json/{ip_address}?fields=lat,lon,countryCode,city,regionName,status")
            if resp.status_code == 200:
                d = resp.json()
                if d.get("status") == "success":
                    r = {"lat": d.get("lat"), "lng": d.get("lon"), "country": d.get("countryCode"), "city": d.get("city"), "region": d.get("regionName")}
                    _geo_cache[ip_address] = r
                    return r["lat"], r["lng"], r["country"], r["city"], r["region"]
    except Exception:
        pass
    return None, None, None, None, None

# ── Aggregation ───────────────────────────────────────────────────────────────

async def aggregate_hourly(session: AsyncSession, hour: datetime, site_id: str = None):
    sites = [site_id] if site_id else list(VALID_SITES)
    for sid in sites:
        base_filter = [Event.site_id == sid, Event.timestamp >= hour, Event.timestamp < hour + timedelta(hours=1), Event.is_bot == False]
        # Pageview aggregation
        stmt = select(Event.page_url, func.count(Event.id).label("pageviews"),
            func.count(distinct(Event.ip_hash)).label("unique_visitors"),
            func.sum(func.cast(Event.click_x != None, Integer)).label("clicks")
        ).where(*base_filter).group_by(Event.page_url)
        for row in (await session.execute(stmt)).all():
            ex = await session.execute(select(HourlySummary).where(HourlySummary.site_id == sid, HourlySummary.hour == hour, HourlySummary.page_url == row.page_url))
            existing = ex.scalar_one_or_none()
            if existing:
                existing.pageviews = row.pageviews
                existing.unique_visitors = row.unique_visitors
                existing.clicks = row.clicks or 0
            else:
                session.add(HourlySummary(site_id=sid, hour=hour, page_url=row.page_url, pageviews=row.pageviews, unique_visitors=row.unique_visitors, clicks=row.clicks or 0))
        # Referrer aggregation
        ref_stmt = select(Event.referrer, func.count(Event.id).label("count")).where(*base_filter, Event.referrer != "").group_by(Event.referrer)
        for row in (await session.execute(ref_stmt)).all():
            ex = await session.execute(select(ReferrerSummary).where(ReferrerSummary.site_id == sid, ReferrerSummary.hour == hour, ReferrerSummary.referrer == row.referrer))
            existing = ex.scalar_one_or_none()
            if existing:
                existing.count = row.count
            else:
                session.add(ReferrerSummary(site_id=sid, hour=hour, referrer=row.referrer, count=row.count))
        # Click aggregation
        click_stmt = select(Event.page_url, Event.click_x, Event.click_y, func.count(Event.id).label("count")).where(*base_filter, Event.click_x != None, Event.click_y != None).group_by(Event.page_url, Event.click_x, Event.click_y)
        for row in (await session.execute(click_stmt)).all():
            ex = await session.execute(select(ClicksHourly).where(ClicksHourly.site_id == sid, ClicksHourly.hour == hour, ClicksHourly.page_url == row.page_url, ClicksHourly.x == row.click_x, ClicksHourly.y == row.click_y))
            existing = ex.scalar_one_or_none()
            if existing:
                existing.count = row.count
            else:
                session.add(ClicksHourly(site_id=sid, hour=hour, page_url=row.page_url, x=row.click_x, y=row.click_y, count=row.count))
        # Geo aggregation
        geo_stmt = select(Event.lat, Event.lng, Event.city, Event.region, Event.country, func.count(Event.id).label("count")).where(*base_filter, Event.lat != None, Event.lng != None).group_by(Event.city, Event.region, Event.country)
        for row in (await session.execute(geo_stmt)).all():
            ex = await session.execute(select(GeoSummary).where(GeoSummary.site_id == sid, GeoSummary.hour == hour, GeoSummary.city == row.city, GeoSummary.country == row.country))
            existing = ex.scalar_one_or_none()
            if existing:
                existing.count = row.count
            else:
                session.add(GeoSummary(site_id=sid, hour=hour, lat=row.lat, lng=row.lng, city=row.city, region=row.region, country=row.country, count=row.count))
    await session.commit()

async def periodic_aggregation():
    while True:
        await asyncio.sleep(60)
        now = datetime.utcnow()
        hour = get_hour_bucket(now) - timedelta(hours=1)
        async with async_session() as session:
            try:
                await aggregate_hourly(session, hour)
            except Exception:
                pass

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    asyncio.create_task(periodic_aggregation())

# ── POST /collect ─────────────────────────────────────────────────────────────

@app.post("/collect")
async def collect_event(request: Request, site: str = Query("luma")):
    site = _validate_site(site)
    client_ip = request.client.host if request.client else "127.0.0.1"
    try:
        body = await request.body()
        data = json.loads(body)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    ua = data.get("user_agent", request.headers.get("user-agent", ""))
    session_id = data.get("session_id", "")
    accept_lang = request.headers.get("accept-language")
    is_bot, bot_reason = detect_bot(ua, client_ip, session_id, accept_lang)
    ts = data.get("timestamp")
    if isinstance(ts, str):
        try:
            ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            ts = datetime.utcnow()
    elif ts is None:
        ts = datetime.utcnow()
    now = datetime.utcnow()
    if ts > now + timedelta(minutes=5):
        raise HTTPException(status_code=400, detail="Invalid timestamp: future")
    if ts < now - timedelta(hours=24):
        raise HTTPException(status_code=400, detail="Invalid timestamp: too old")
    lat, lng, country, city, region = None, None, None, None, None
    if not is_bot:
        lat, lng, country, city, region = await lookup_geo(client_ip)
    new_event = Event(
        site_id=site, session_id=session_id, event_type=data.get("event_type", "pageview"),
        page_url=data.get("page_url", "/"), referrer=data.get("referrer", ""),
        user_agent=ua, ip_address=client_ip, ip_hash=hash_ip(client_ip),
        screen_width=data.get("screen_width"), screen_height=data.get("screen_height"),
        vp_width=data.get("vp_width"), vp_height=data.get("vp_height"),
        click_x=data.get("click_x"), click_y=data.get("click_y"),
        timestamp=ts, is_bot=is_bot, country=country, city=city, region=region, lat=lat, lng=lng
    )
    async with async_session() as session:
        session.add(new_event)
        await session.flush()
        event_data = {"site_id": site, "page_url": new_event.page_url, "referrer": new_event.referrer or "",
            "timestamp": ts.isoformat(), "is_bot": is_bot, "city": city, "region": region,
            "country": country, "lat": lat, "lng": lng, "event_type": new_event.event_type}
        await session.commit()
        for q in live_subscribers:
            q.put_nowait(event_data)
    return JSONResponse({"status": "accepted", "bot": is_bot, "reason": bot_reason}, status_code=202)

# ── GET /stats ────────────────────────────────────────────────────────────────

@app.get("/stats")
async def get_stats(hours: int = Query(24, ge=1, le=168), site: str = Query("luma")):
    site = _validate_site(site)
    start_time = datetime.utcnow() - timedelta(hours=hours)
    async with async_session() as session:
        stmt = select(HourlySummary.hour, func.sum(HourlySummary.pageviews).label("pageviews"),
            func.sum(HourlySummary.unique_visitors).label("unique_visitors"),
            func.sum(HourlySummary.clicks).label("clicks")
        ).where(HourlySummary.site_id == site, HourlySummary.hour >= start_time
        ).group_by(HourlySummary.hour).order_by(HourlySummary.hour)
        result = await session.execute(stmt)
        stats = [{"hour": row.hour.strftime("%Y-%m-%dT%H:%M"), "pageviews": row.pageviews,
            "unique_visitors": row.unique_visitors, "clicks": row.clicks or 0} for row in result.all()]
    return JSONResponse(stats)

# ── GET /summary ──────────────────────────────────────────────────────────────

@app.get("/summary")
async def get_stats_summary(site: str = Query("luma")):
    site = _validate_site(site)
    now = datetime.utcnow()
    start_24h = now - timedelta(hours=24)
    start_1h = now - timedelta(hours=1)
    async with async_session() as session:
        total_pv = (await session.execute(select(func.sum(HourlySummary.pageviews)).where(HourlySummary.site_id == site, HourlySummary.hour >= start_24h))).scalar() or 0
        total_uv = (await session.execute(select(func.sum(HourlySummary.unique_visitors)).where(HourlySummary.site_id == site, HourlySummary.hour >= start_24h))).scalar() or 0
        total_cl = (await session.execute(select(func.sum(HourlySummary.clicks)).where(HourlySummary.site_id == site, HourlySummary.hour >= start_24h))).scalar() or 0
        live_uv = (await session.execute(select(func.sum(HourlySummary.unique_visitors)).where(HourlySummary.site_id == site, HourlySummary.hour >= start_1h))).scalar() or 0
        top_city_stmt = select(GeoSummary.city, GeoSummary.country, func.sum(GeoSummary.count).label("total")).where(
            GeoSummary.site_id == site, GeoSummary.hour >= start_24h, GeoSummary.city != None
        ).group_by(GeoSummary.city, GeoSummary.country).order_by(func.sum(GeoSummary.count).desc()).limit(1)
        top_city_row = (await session.execute(top_city_stmt)).first()
        top_city = {"city": top_city_row.city, "country": top_city_row.country, "count": top_city_row.total} if top_city_row else None
        bounce_count = (await session.execute(select(func.count(HourlySummary.id)).where(HourlySummary.site_id == site, HourlySummary.hour >= start_24h, HourlySummary.pageviews == 1))).scalar() or 0
        total_sessions = (await session.execute(select(func.count(HourlySummary.id)).where(HourlySummary.site_id == site, HourlySummary.hour >= start_24h))).scalar() or 1
        bounce_rate = round((bounce_count / total_sessions) * 100, 1)
    return JSONResponse({"total_pageviews": total_pv, "unique_visitors": total_uv, "total_clicks": total_cl,
        "live_visitors": live_uv, "bounce_rate": bounce_rate, "top_city": top_city})

@app.get("/stats/summary")
async def stats_summary_alias(site: str = Query("luma")):
    return await get_stats_summary(site)

# ── GET /referrers ────────────────────────────────────────────────────────────

@app.get("/referrers")
async def get_referrers(site: str = Query("luma")):
    site = _validate_site(site)
    start_time = datetime.utcnow() - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(ReferrerSummary.referrer, func.sum(ReferrerSummary.count).label("total")).where(
            ReferrerSummary.site_id == site, ReferrerSummary.hour >= start_time
        ).group_by(ReferrerSummary.referrer).order_by(func.sum(ReferrerSummary.count).desc()).limit(10)
        result = await session.execute(stmt)
        referrers = [{"referrer": row.referrer, "count": row.total} for row in result.all()]
    return JSONResponse(referrers)

# ── GET /funnel ───────────────────────────────────────────────────────────────

@app.get("/funnel")
async def get_funnel(steps: str = Query(..., description="Comma-separated URL paths or slugs"), site: str = Query("luma")):
    site = _validate_site(site)
    step_urls = [s.strip() for s in steps.split(",")]
    start_time = datetime.utcnow() - timedelta(hours=24)
    async with async_session() as session:
        results = []
        for step_url in step_urls:
            if step_url.startswith("http"):
                stmt = select(func.sum(HourlySummary.unique_visitors)).where(HourlySummary.site_id == site, HourlySummary.hour >= start_time, HourlySummary.page_url == step_url)
            else:
                stmt = select(func.sum(HourlySummary.unique_visitors)).where(HourlySummary.site_id == site, HourlySummary.hour >= start_time, HourlySummary.page_url.like(f"%{step_url}%"))
            visitors = (await session.execute(stmt)).scalar() or 0
            results.append({"step": step_url, "visitors": visitors})
    return JSONResponse(results)

@app.get("/funnel/demo")
async def get_funnel_demo(site: str = Query("luma")):
    site = _validate_site(site)
    start_time = datetime.utcnow() - timedelta(hours=24)
    stages = [{"label": "🎤 SF AI Meetup", "slug": "sf-ai-meetup"}, {"label": "🍽️ Founder Dinner NYC", "slug": "founder-dinner-nyc"},
        {"label": "🎨 Design Week London", "slug": "design-week-london"}, {"label": "💻 Hack Night Austin", "slug": "hack-night-austin"},
        {"label": "🚀 Product Launch Tokyo", "slug": "product-launch-tokyo"}]
    async with async_session() as session:
        results = []
        for stage in stages:
            stmt = select(func.sum(HourlySummary.unique_visitors)).where(HourlySummary.site_id == site, HourlySummary.hour >= start_time, HourlySummary.page_url.like(f"%{stage['slug']}%"))
            visitors = (await session.execute(stmt)).scalar() or 0
            results.append({"step": stage["label"], "visitors": visitors})
    return JSONResponse(results)

# ── GET /heatmap ──────────────────────────────────────────────────────────────

@app.get("/heatmap")
async def get_heatmap(page: str = Query(..., description="Page URL"), site: str = Query("luma")):
    site = _validate_site(site)
    start_time = datetime.utcnow() - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(ClicksHourly.x, ClicksHourly.y, func.sum(ClicksHourly.count).label("count")).where(
            ClicksHourly.site_id == site, ClicksHourly.hour >= start_time, ClicksHourly.page_url == page
        ).group_by(ClicksHourly.x, ClicksHourly.y)
        result = await session.execute(stmt)
        clicks = [{"x": row.x, "y": row.y, "count": row.count} for row in result.all()]
    return JSONResponse(clicks)

# ── GET /live (SSE) ───────────────────────────────────────────────────────────

@app.get("/live")
async def live_events(request: Request, site: str = Query("luma")):
    site = _validate_site(site)
    async def event_generator() -> AsyncGenerator[str, None]:
        queue = asyncio.Queue()
        live_subscribers.add(queue)
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30)
                    if event.get("site_id") == site:
                        yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield f"data: null\n\n"
        finally:
            live_subscribers.discard(queue)
            
    return StreamingResponse(event_generator(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})

# ── GET /geo ──────────────────────────────────────────────────────────────────

@app.get("/geo")
async def get_geo(site: str = Query("luma")):
    site = _validate_site(site)
    start_time = datetime.utcnow() - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(GeoSummary.lat, GeoSummary.lng, GeoSummary.city, GeoSummary.region,
            GeoSummary.country, func.sum(GeoSummary.count).label("count")).where(
            GeoSummary.site_id == site, GeoSummary.hour >= start_time
        ).group_by(GeoSummary.city, GeoSummary.region, GeoSummary.country)
        result = await session.execute(stmt)
        geo_data = [{"lat": row.lat, "lng": row.lng, "city": row.city or "Unknown",
            "region": row.region or "", "country": row.country or "", "count": row.count} for row in result.all()]
    return JSONResponse(geo_data)

@app.get("/geo/cities")
async def get_geo_cities(site: str = Query("luma")):
    site = _validate_site(site)
    start_time = datetime.utcnow() - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(GeoSummary.city, GeoSummary.region, GeoSummary.country, GeoSummary.lat, GeoSummary.lng,
            func.sum(GeoSummary.count).label("total")).where(
            GeoSummary.site_id == site, GeoSummary.hour >= start_time, GeoSummary.city != None
        ).group_by(GeoSummary.city, GeoSummary.country).order_by(func.sum(GeoSummary.count).desc()).limit(20)
        result = await session.execute(stmt)
        cities = [{"city": row.city, "region": row.region, "country": row.country,
            "lat": row.lat, "lng": row.lng, "visitors": row.total} for row in result.all()]
    return JSONResponse(cities)

@app.get("/geo/live")
async def get_geo_live(site: str = Query("luma")):
    site = _validate_site(site)
    start_time = datetime.utcnow() - timedelta(minutes=5)
    async with async_session() as session:
        stmt = select(Event.city, Event.region, Event.country, Event.lat, Event.lng, Event.page_url, Event.timestamp).where(
            Event.site_id == site, Event.timestamp >= start_time, Event.is_bot == False, Event.city != None
        ).order_by(Event.timestamp.desc()).limit(20)
        result = await session.execute(stmt)
        events = [{"city": row.city, "region": row.region, "country": row.country,
            "lat": row.lat, "lng": row.lng, "page": row.page_url, "timestamp": row.timestamp.isoformat()} for row in result.all()]
    return JSONResponse(events)

# ── GET /pages ────────────────────────────────────────────────────────────────

@app.get("/pages")
async def get_top_pages(site: str = Query("luma")):
    site = _validate_site(site)
    start_time = datetime.utcnow() - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(HourlySummary.page_url, func.sum(HourlySummary.pageviews).label("views"),
            func.sum(HourlySummary.unique_visitors).label("visitors")).where(
            HourlySummary.site_id == site, HourlySummary.hour >= start_time
        ).group_by(HourlySummary.page_url).order_by(func.sum(HourlySummary.pageviews).desc()).limit(10)
        result = await session.execute(stmt)
        pages = [{"page": row.page_url, "views": row.views, "visitors": row.visitors} for row in result.all()]
    return JSONResponse(pages)

# ── GET /bots ─────────────────────────────────────────────────────────────────

@app.get("/bots")
async def get_bot_stats(site: str = Query("luma")):
    site = _validate_site(site)
    start_time = datetime.utcnow() - timedelta(hours=24)
    async with async_session() as session:
        total = (await session.execute(select(func.count(Event.id)).where(Event.site_id == site, Event.timestamp >= start_time))).scalar() or 0
        bots = (await session.execute(select(func.count(Event.id)).where(Event.site_id == site, Event.timestamp >= start_time, Event.is_bot == True))).scalar() or 0
        human = total - bots
        hourly_stmt = select(func.strftime('%H:00', Event.timestamp).label('hour'),
            func.sum(func.cast(Event.is_bot == True, Integer)).label('bots'),
            func.sum(func.cast(Event.is_bot == False, Integer)).label('humans')
        ).where(Event.site_id == site, Event.timestamp >= start_time
        ).group_by(func.strftime('%H:00', Event.timestamp)).order_by(func.strftime('%H:00', Event.timestamp))
        hourly = [{"hour": r.hour, "bots": r.bots or 0, "humans": r.humans or 0} for r in (await session.execute(hourly_stmt)).all()]
        recent_stmt = select(Event.user_agent, Event.ip_address, Event.page_url, Event.timestamp).where(
            Event.site_id == site, Event.timestamp >= start_time, Event.is_bot == True
        ).order_by(Event.timestamp.desc()).limit(20)
        recent = [{"user_agent": r.user_agent[:80], "ip": r.ip_address, "page": r.page_url,
            "timestamp": r.timestamp.isoformat()} for r in (await session.execute(recent_stmt)).all()]
        ua_stmt = select(Event.user_agent, func.count(Event.id).label('count')).where(
            Event.site_id == site, Event.timestamp >= start_time, Event.is_bot == True
        ).group_by(Event.user_agent).order_by(func.count(Event.id).desc()).limit(10)
        ua_breakdown = [{"user_agent": r.user_agent[:60], "count": r.count} for r in (await session.execute(ua_stmt)).all()]
    return JSONResponse({"total_requests": total, "bot_requests": bots, "human_requests": human,
        "bot_percentage": round((bots / max(total, 1)) * 100, 1), "hourly": hourly,
        "recent_bots": recent, "ua_breakdown": ua_breakdown, "filters_active": len(BOT_UA_PATTERNS.pattern.split("|"))})

# ── GET /health ───────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# ── Dashboard / Static Routes ────────────────────────────────────────────────

@app.get("/dashboard.html")
@app.get("/luma")
async def dashboard():
    return FileResponse(os.path.join(BASE_DIR, "frontend/dashboard/luma/dashboard.html"))

@app.get("/substack")
async def substack():
    return FileResponse(os.path.join(BASE_DIR, "frontend/dashboard/substack/dashboard.html"))

@app.get("/")
async def home():
    with open("frontend/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/heatmap-bg.png")
async def heatmap_bg():
    return FileResponse(os.path.join(BASE_DIR, "static/heatmap-bg.png"), media_type="image/png")

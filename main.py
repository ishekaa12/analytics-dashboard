import asyncio
import hashlib
import json
import re
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
from collections import defaultdict
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional

import aiohttp
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse, HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, Index, func, select, distinct
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sse_starlette.sse import EventSourceResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

DATABASE_URL = "sqlite+aiosqlite:///./analytics.db"
BOT_USER_AGENTS = [
    "bot", "crawler", "spider", "scrapy", "curl", "wget", "python", "requests",
    "go-http-client", "java", "ruby", "php", "perl", "node", "dotnet",
    "googlebot", "bingbot", "yandex", "baidu", "duckduckbot", "facebookexternalhit",
    "twitterbot", "linkedinbot", "slurp", "applebot", "yahoo! slurp"
]
RATE_LIMIT = 100
RATE_LIMIT_WINDOW = 60

Base = declarative_base()


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    page_url = Column(String(2048))
    referrer = Column(String(2048))
    user_agent = Column(String(512))
    ip_address = Column(String(45))
    ip_hash = Column(String(64))
    screen_width = Column(Integer)
    screen_height = Column(Integer)
    click_x = Column(Integer)
    click_y = Column(Integer)
    timestamp = Column(DateTime)
    is_bot = Column(Boolean, default=False)
    country = Column(String(2), nullable=True)
    city = Column(String(128), nullable=True)
    region = Column(String(128), nullable=True)
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)


class HourlySummary(Base):
    __tablename__ = "hourly_summary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hour = Column(DateTime, index=True)
    page_url = Column(String(2048))
    pageviews = Column(Integer, default=0)
    unique_visitors = Column(Integer, default=0)
    clicks = Column(Integer, default=0)


class ReferrerSummary(Base):
    __tablename__ = "referrer_summary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hour = Column(DateTime, index=True)
    referrer = Column(String(2048))
    count = Column(Integer, default=0)


class ClicksHourly(Base):
    __tablename__ = "clicks_hourly"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hour = Column(DateTime, index=True)
    page_url = Column(String(2048))
    x = Column(Integer)
    y = Column(Integer)
    count = Column(Integer, default=0)


class GeoSummary(Base):
    __tablename__ = "geo_summary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hour = Column(DateTime, index=True)
    lat = Column(Float)
    lng = Column(Float)
    city = Column(String(128), nullable=True)
    region = Column(String(128), nullable=True)
    country = Column(String(2), nullable=True)
    count = Column(Integer, default=0)


class EventCreate(BaseModel):
    page_url: str
    referrer: Optional[str] = ""
    user_agent: str = ""
    screen_width: Optional[int] = None
    screen_height: Optional[int] = None
    click_x: Optional[int] = None
    click_y: Optional[int] = None
    timestamp: Optional[datetime] = None


class EventResponse(BaseModel):
    id: int
    page_url: str
    referrer: str
    user_agent: str
    ip_address: str
    screen_width: Optional[int]
    screen_height: Optional[int]
    click_x: Optional[int]
    click_y: Optional[int]
    timestamp: datetime
    is_bot: bool
    country: Optional[str]
    city: Optional[str]
    region: Optional[str]
    lat: Optional[float]
    lng: Optional[float]


app = FastAPI(title="Luma Analytics — Real-Time Event Intelligence")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, class_=AsyncSession)

event_queue: asyncio.Queue = asyncio.Queue()
live_subscribers: list = []
ip_request_counts: dict = defaultdict(lambda: {"count": 0, "window_start": datetime.utcnow()})


def is_bot_user_agent(user_agent: str) -> bool:
    ua_lower = user_agent.lower()
    for bot_pattern in BOT_USER_AGENTS:
        if bot_pattern in ua_lower:
            return True
    return False


def is_rate_limited(ip_address: str) -> bool:
    now = datetime.utcnow()
    ip_data = ip_request_counts[ip_address]
    if (now - ip_data["window_start"]).total_seconds() > RATE_LIMIT_WINDOW:
        ip_data["count"] = 0
        ip_data["window_start"] = now
    ip_data["count"] += 1
    return ip_data["count"] > RATE_LIMIT


def hash_ip(ip_address: str) -> str:
    return hashlib.sha256(ip_address.encode()).hexdigest()


def get_hour_bucket(timestamp: datetime) -> datetime:
    return timestamp.replace(minute=0, second=0, microsecond=0)


async def lookup_geo(ip_address: str) -> tuple:
    """Enhanced geo lookup returning lat, lng, country, city, region."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"http://ip-api.com/json/{ip_address}?fields=lat,lon,countryCode,city,regionName"
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return (
                        data.get("lat"),
                        data.get("lon"),
                        data.get("countryCode"),
                        data.get("city"),
                        data.get("regionName"),
                    )
    except Exception:
        pass
    return None, None, None, None, None


async def aggregate_hourly(session: AsyncSession, hour: datetime):
    # Pageview aggregation
    stmt = select(
        Event.page_url,
        func.count(Event.id).label("pageviews"),
        func.count(distinct(Event.ip_hash)).label("unique_visitors"),
        func.sum(func.cast(Event.click_x != None, Integer)).label("clicks")
    ).where(
        Event.timestamp >= hour,
        Event.timestamp < hour + timedelta(hours=1),
        Event.is_bot == False
    ).group_by(Event.page_url)

    result = await session.execute(stmt)
    page_data = result.all()

    for row in page_data:
        existing = await session.execute(
            select(HourlySummary).where(
                HourlySummary.hour == hour,
                HourlySummary.page_url == row.page_url
            )
        )
        existing_summary = existing.scalar_one_or_none()
        if existing_summary:
            existing_summary.pageviews = row.pageviews
            existing_summary.unique_visitors = row.unique_visitors
            existing_summary.clicks = row.clicks or 0
        else:
            summary = HourlySummary(
                hour=hour, page_url=row.page_url,
                pageviews=row.pageviews, unique_visitors=row.unique_visitors,
                clicks=row.clicks or 0
            )
            session.add(summary)

    # Referrer aggregation
    ref_stmt = select(
        Event.referrer, func.count(Event.id).label("count")
    ).where(
        Event.timestamp >= hour, Event.timestamp < hour + timedelta(hours=1),
        Event.is_bot == False, Event.referrer != ""
    ).group_by(Event.referrer)

    ref_result = await session.execute(ref_stmt)
    for row in ref_result.all():
        existing = await session.execute(
            select(ReferrerSummary).where(
                ReferrerSummary.hour == hour, ReferrerSummary.referrer == row.referrer
            )
        )
        existing_ref = existing.scalar_one_or_none()
        if existing_ref:
            existing_ref.count = row.count
        else:
            session.add(ReferrerSummary(hour=hour, referrer=row.referrer, count=row.count))

    # Click aggregation
    click_stmt = select(
        Event.page_url, Event.click_x, Event.click_y, func.count(Event.id).label("count")
    ).where(
        Event.timestamp >= hour, Event.timestamp < hour + timedelta(hours=1),
        Event.is_bot == False, Event.click_x != None, Event.click_y != None
    ).group_by(Event.page_url, Event.click_x, Event.click_y)

    for row in (await session.execute(click_stmt)).all():
        existing = await session.execute(
            select(ClicksHourly).where(
                ClicksHourly.hour == hour, ClicksHourly.page_url == row.page_url,
                ClicksHourly.x == row.click_x, ClicksHourly.y == row.click_y
            )
        )
        existing_click = existing.scalar_one_or_none()
        if existing_click:
            existing_click.count = row.count
        else:
            session.add(ClicksHourly(hour=hour, page_url=row.page_url, x=row.click_x, y=row.click_y, count=row.count))

    # Geo aggregation — city-level
    geo_stmt = select(
        Event.lat, Event.lng, Event.city, Event.region, Event.country,
        func.count(Event.id).label("count")
    ).where(
        Event.timestamp >= hour, Event.timestamp < hour + timedelta(hours=1),
        Event.is_bot == False, Event.lat != None, Event.lng != None
    ).group_by(Event.city, Event.region, Event.country)

    for row in (await session.execute(geo_stmt)).all():
        existing = await session.execute(
            select(GeoSummary).where(
                GeoSummary.hour == hour,
                GeoSummary.city == row.city,
                GeoSummary.country == row.country
            )
        )
        existing_geo = existing.scalar_one_or_none()
        if existing_geo:
            existing_geo.count = row.count
        else:
            session.add(GeoSummary(
                hour=hour, lat=row.lat, lng=row.lng,
                city=row.city, region=row.region, country=row.country, count=row.count
            ))

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


@app.post("/collect")
async def collect_event(event: EventCreate, request: Request):
    client_ip = request.client.host if request.client else "127.0.0.1"
    if is_rate_limited(client_ip):
        raise HTTPException(status_code=429, detail="Too many requests")

    timestamp = event.timestamp or datetime.utcnow()
    now = datetime.utcnow()
    if timestamp > now + timedelta(minutes=5):
        raise HTTPException(status_code=400, detail="Invalid timestamp: future")
    if timestamp < now - timedelta(hours=24):
        raise HTTPException(status_code=400, detail="Invalid timestamp: too old")

    is_bot = is_bot_user_agent(event.user_agent)

    lat, lng, country, city, region = None, None, None, None, None
    if not is_bot:
        lat, lng, country, city, region = await lookup_geo(client_ip)

    ip_hash = hash_ip(client_ip)

    new_event = Event(
        page_url=event.page_url, referrer=event.referrer or "",
        user_agent=event.user_agent, ip_address=client_ip, ip_hash=ip_hash,
        screen_width=event.screen_width, screen_height=event.screen_height,
        click_x=event.click_x, click_y=event.click_y,
        timestamp=timestamp, is_bot=is_bot,
        country=country, city=city, region=region, lat=lat, lng=lng
    )

    async with async_session() as session:
        session.add(new_event)
        await session.flush()
        event_data = {
            "page_url": event.page_url,
            "referrer": event.referrer or "",
            "timestamp": timestamp.isoformat(),
            "is_bot": is_bot,
            "ip": client_ip,
            "city": city,
            "region": region,
            "country": country,
            "lat": lat,
            "lng": lng
        }
        await session.commit()
        await event_queue.put(event_data)

    return JSONResponse({"status": "accepted"}, status_code=202)


@app.get("/stats")
async def get_stats(hours: int = Query(24, ge=1, le=168)):
    now = datetime.utcnow()
    start_time = now - timedelta(hours=hours)
    async with async_session() as session:
        stmt = select(
            HourlySummary.hour,
            func.sum(HourlySummary.pageviews).label("pageviews"),
            func.sum(HourlySummary.unique_visitors).label("unique_visitors"),
            func.sum(HourlySummary.clicks).label("clicks")
        ).where(HourlySummary.hour >= start_time).group_by(HourlySummary.hour).order_by(HourlySummary.hour)

        result = await session.execute(stmt)
        stats = [
            {
                "hour": row.hour.strftime("%Y-%m-%dT%H:%M"),
                "pageviews": row.pageviews,
                "unique_visitors": row.unique_visitors,
                "clicks": row.clicks or 0
            }
            for row in result.all()
        ]
    return JSONResponse(stats)


@app.get("/summary")
async def get_stats_summary():
    """Returns aggregate summary numbers for the dashboard header."""
    now = datetime.utcnow()
    start_24h = now - timedelta(hours=24)
    start_1h = now - timedelta(hours=1)

    async with async_session() as session:
        # Total pageviews 24h
        pv_stmt = select(func.sum(HourlySummary.pageviews)).where(HourlySummary.hour >= start_24h)
        total_pv = (await session.execute(pv_stmt)).scalar() or 0

        # Total unique visitors 24h
        uv_stmt = select(func.sum(HourlySummary.unique_visitors)).where(HourlySummary.hour >= start_24h)
        total_uv = (await session.execute(uv_stmt)).scalar() or 0

        # Total clicks 24h
        cl_stmt = select(func.sum(HourlySummary.clicks)).where(HourlySummary.hour >= start_24h)
        total_cl = (await session.execute(cl_stmt)).scalar() or 0

        # Live visitors (last hour)
        live_stmt = select(func.sum(HourlySummary.unique_visitors)).where(HourlySummary.hour >= start_1h)
        live_uv = (await session.execute(live_stmt)).scalar() or 0

        # Top city
        top_city_stmt = select(
            GeoSummary.city, GeoSummary.country,
            func.sum(GeoSummary.count).label("total")
        ).where(
            GeoSummary.hour >= start_24h, GeoSummary.city != None
        ).group_by(GeoSummary.city, GeoSummary.country).order_by(
            func.sum(GeoSummary.count).desc()
        ).limit(1)
        top_city_row = (await session.execute(top_city_stmt)).first()
        top_city = {"city": top_city_row.city, "country": top_city_row.country, "count": top_city_row.total} if top_city_row else None

        # Bounce rate approximation (pages with only 1 pageview / total)
        bounce_stmt = select(func.count(HourlySummary.id)).where(
            HourlySummary.hour >= start_24h, HourlySummary.pageviews == 1
        )
        bounce_count = (await session.execute(bounce_stmt)).scalar() or 0
        total_sessions_stmt = select(func.count(HourlySummary.id)).where(HourlySummary.hour >= start_24h)
        total_sessions = (await session.execute(total_sessions_stmt)).scalar() or 1
        bounce_rate = round((bounce_count / total_sessions) * 100, 1)

    return JSONResponse({
        "total_pageviews": total_pv,
        "unique_visitors": total_uv,
        "total_clicks": total_cl,
        "live_visitors": live_uv,
        "bounce_rate": bounce_rate,
        "top_city": top_city
    })

@app.get("/stats/summary")
async def stats_summary_alias():
    return await get_stats_summary()    


@app.get("/referrers")
async def get_referrers():
    now = datetime.utcnow()
    start_time = now - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(
            ReferrerSummary.referrer,
            func.sum(ReferrerSummary.count).label("total")
        ).where(ReferrerSummary.hour >= start_time).group_by(
            ReferrerSummary.referrer
        ).order_by(func.sum(ReferrerSummary.count).desc()).limit(10)

        result = await session.execute(stmt)
        referrers = [{"referrer": row.referrer, "count": row.total} for row in result.all()]
    return JSONResponse(referrers)


@app.get("/funnel")
async def get_funnel(steps: str = Query(..., description="Comma-separated URL paths or slugs")):
    step_urls = [s.strip() for s in steps.split(",")]
    now = datetime.utcnow()
    start_time = now - timedelta(hours=24)

    async with async_session() as session:
        results = []
        for step_url in step_urls:
            # Support both full URLs and partial slugs (LIKE matching)
            if step_url.startswith("http"):
                stmt = select(func.sum(HourlySummary.unique_visitors)).where(
                    HourlySummary.hour >= start_time, HourlySummary.page_url == step_url
                )
            else:
                stmt = select(func.sum(HourlySummary.unique_visitors)).where(
                    HourlySummary.hour >= start_time, HourlySummary.page_url.like(f"%{step_url}%")
                )
            visitors = (await session.execute(stmt)).scalar() or 0
            results.append({"step": step_url, "visitors": visitors})
    return JSONResponse(results)


@app.get("/funnel/demo")
async def get_funnel_demo():
    """Pre-built demo funnel showing event discovery → registration flow."""
    now = datetime.utcnow()
    start_time = now - timedelta(hours=24)

    # Demo funnel: popular event → niche event (shows natural drop-off)
    funnel_stages = [
        {"label": "🎤 SF AI Meetup", "slug": "sf-ai-meetup"},
        {"label": "🍽️ Founder Dinner NYC", "slug": "founder-dinner-nyc"},
        {"label": "🎨 Design Week London", "slug": "design-week-london"},
        {"label": "💻 Hack Night Austin", "slug": "hack-night-austin"},
        {"label": "🚀 Product Launch Tokyo", "slug": "product-launch-tokyo"},
    ]

    async with async_session() as session:
        results = []
        for stage in funnel_stages:
            stmt = select(func.sum(HourlySummary.unique_visitors)).where(
                HourlySummary.hour >= start_time,
                HourlySummary.page_url.like(f"%{stage['slug']}%")
            )
            visitors = (await session.execute(stmt)).scalar() or 0
            results.append({"step": stage["label"], "visitors": visitors})

    return JSONResponse(results)


@app.get("/heatmap")
async def get_heatmap(page: str = Query(..., description="Page URL")):
    now = datetime.utcnow()
    start_time = now - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(
            ClicksHourly.x, ClicksHourly.y,
            func.sum(ClicksHourly.count).label("count")
        ).where(
            ClicksHourly.hour >= start_time, ClicksHourly.page_url == page
        ).group_by(ClicksHourly.x, ClicksHourly.y)

        result = await session.execute(stmt)
        clicks = [{"x": row.x, "y": row.y, "count": row.count} for row in result.all()]
    return JSONResponse(clicks)


@app.get("/live")
async def live_events(request: Request):
    async def event_generator() -> AsyncGenerator[str, None]:
        while True:
            try:
                event = await asyncio.wait_for(event_queue.get(), timeout=30)
                yield f"data: {json.dumps(event)}\n\n"
            except asyncio.TimeoutError:
                yield f"data: null\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/geo")
async def get_geo():
    now = datetime.utcnow()
    start_time = now - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(
            GeoSummary.lat, GeoSummary.lng, GeoSummary.city, GeoSummary.region,
            GeoSummary.country, func.sum(GeoSummary.count).label("count")
        ).where(GeoSummary.hour >= start_time).group_by(
            GeoSummary.city, GeoSummary.region, GeoSummary.country
        )

        result = await session.execute(stmt)
        geo_data = [
            {
                "lat": row.lat, "lng": row.lng,
                "city": row.city or "Unknown",
                "region": row.region or "",
                "country": row.country or "",
                "count": row.count
            }
            for row in result.all()
        ]
    return JSONResponse(geo_data)


@app.get("/geo/cities")
async def get_geo_cities():
    """Top cities by visitor count — Luma-style."""
    now = datetime.utcnow()
    start_time = now - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(
            GeoSummary.city, GeoSummary.region, GeoSummary.country,
            GeoSummary.lat, GeoSummary.lng,
            func.sum(GeoSummary.count).label("total")
        ).where(
            GeoSummary.hour >= start_time, GeoSummary.city != None
        ).group_by(
            GeoSummary.city, GeoSummary.country
        ).order_by(func.sum(GeoSummary.count).desc()).limit(20)

        result = await session.execute(stmt)
        cities = [
            {
                "city": row.city, "region": row.region, "country": row.country,
                "lat": row.lat, "lng": row.lng, "visitors": row.total
            }
            for row in result.all()
        ]
    return JSONResponse(cities)


@app.get("/geo/live")
async def get_geo_live():
    """Recent geo events from the last 5 minutes for the live ticker."""
    now = datetime.utcnow()
    start_time = now - timedelta(minutes=5)
    async with async_session() as session:
        stmt = select(
            Event.city, Event.region, Event.country, Event.lat, Event.lng,
            Event.page_url, Event.timestamp
        ).where(
            Event.timestamp >= start_time, Event.is_bot == False,
            Event.city != None
        ).order_by(Event.timestamp.desc()).limit(20)

        result = await session.execute(stmt)
        events = [
            {
                "city": row.city, "region": row.region, "country": row.country,
                "lat": row.lat, "lng": row.lng,
                "page": row.page_url, "timestamp": row.timestamp.isoformat()
            }
            for row in result.all()
        ]
    return JSONResponse(events)


@app.get("/pages")
async def get_top_pages():
    """Top pages by pageviews in last 24h."""
    now = datetime.utcnow()
    start_time = now - timedelta(hours=24)
    async with async_session() as session:
        stmt = select(
            HourlySummary.page_url,
            func.sum(HourlySummary.pageviews).label("views"),
            func.sum(HourlySummary.unique_visitors).label("visitors")
        ).where(HourlySummary.hour >= start_time).group_by(
            HourlySummary.page_url
        ).order_by(func.sum(HourlySummary.pageviews).desc()).limit(10)

        result = await session.execute(stmt)
        pages = [
            {"page": row.page_url, "views": row.views, "visitors": row.visitors}
            for row in result.all()
        ]
    return JSONResponse(pages)


@app.get("/bots")
async def get_bot_stats():
    """Bot detection analytics."""
    now = datetime.utcnow()
    start_time = now - timedelta(hours=24)

    async with async_session() as session:
        # Total requests vs bots
        total_stmt = select(func.count(Event.id)).where(Event.timestamp >= start_time)
        total = (await session.execute(total_stmt)).scalar() or 0

        bot_stmt = select(func.count(Event.id)).where(
            Event.timestamp >= start_time, Event.is_bot == True
        )
        bots = (await session.execute(bot_stmt)).scalar() or 0

        human = total - bots

        # Bot requests by hour
        hourly_stmt = select(
            func.strftime('%H:00', Event.timestamp).label('hour'),
            func.sum(func.cast(Event.is_bot == True, Integer)).label('bots'),
            func.sum(func.cast(Event.is_bot == False, Integer)).label('humans')
        ).where(
            Event.timestamp >= start_time
        ).group_by(func.strftime('%H:00', Event.timestamp)).order_by(
            func.strftime('%H:00', Event.timestamp)
        )
        hourly_result = await session.execute(hourly_stmt)
        hourly = [
            {"hour": row.hour, "bots": row.bots or 0, "humans": row.humans or 0}
            for row in hourly_result.all()
        ]

        # Recent bot hits (last 20)
        recent_stmt = select(
            Event.user_agent, Event.ip_address, Event.page_url, Event.timestamp
        ).where(
            Event.timestamp >= start_time, Event.is_bot == True
        ).order_by(Event.timestamp.desc()).limit(20)
        recent_result = await session.execute(recent_stmt)
        recent = [
            {
                "user_agent": row.user_agent[:80],
                "ip": row.ip_address,
                "page": row.page_url,
                "timestamp": row.timestamp.isoformat()
            }
            for row in recent_result.all()
        ]

        # Bot user-agent breakdown
        ua_stmt = select(
            Event.user_agent,
            func.count(Event.id).label('count')
        ).where(
            Event.timestamp >= start_time, Event.is_bot == True
        ).group_by(Event.user_agent).order_by(func.count(Event.id).desc()).limit(10)
        ua_result = await session.execute(ua_stmt)
        ua_breakdown = [
            {"user_agent": row.user_agent[:60], "count": row.count}
            for row in ua_result.all()
        ]

    return JSONResponse({
        "total_requests": total,
        "bot_requests": bots,
        "human_requests": human,
        "bot_percentage": round((bots / max(total, 1)) * 100, 1),
        "hourly": hourly,
        "recent_bots": recent,
        "ua_breakdown": ua_breakdown,
        "filters_active": len(BOT_USER_AGENTS)
    })


@app.get("/dashboard.html")
@app.get("/luma")
async def dashboard():
    return FileResponse(os.path.join(BASE_DIR, "frontend/dashboard/luma/dashboard.html"))


@app.get("/substack")
async def substack():
    return FileResponse(os.path.join(BASE_DIR, "frontend/dashboard/substack/dashboard.html"))


@app.get("/")
async def home():
    with open("frontend/index.html", "r") as f:
        return HTMLResponse(f.read())


@app.get("/heatmap-bg.png")
async def heatmap_bg():
    return FileResponse(os.path.join(BASE_DIR, "static/heatmap-bg.png"), media_type="image/png")

"""
database.py — SQLAlchemy + aiosqlite data layer for the analytics engine.
Tables: raw_events, hourly_pageviews, hourly_referrers
Pre-aggregation on insert for fast dashboard queries.
"""

import os
import datetime as _dt
from sqlalchemy import (
    Column, Integer, Float, String, DateTime, Text, Index,
    create_engine, func, distinct
)
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analytics.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False}, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


# ── Models ────────────────────────────────────────────────────────────────────

class RawEvent(Base):
    __tablename__ = "raw_events"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(64), index=True)
    event_type = Column(String(16), index=True)          # pageview | click
    page_url   = Column(Text, index=True)
    referrer   = Column(Text, default="")
    timestamp  = Column(DateTime, default=_dt.datetime.utcnow, index=True)
    click_x    = Column(Float, nullable=True)
    click_y    = Column(Float, nullable=True)
    vp_width   = Column(Integer, nullable=True)
    vp_height  = Column(Integer, nullable=True)
    user_agent = Column(Text, default="")
    ip_address = Column(String(45), default="")
    latitude   = Column(Float, nullable=True)
    longitude  = Column(Float, nullable=True)
    country    = Column(String(64), default="")
    city       = Column(String(128), default="")
    is_bot     = Column(Integer, default=0)


class HourlyPageview(Base):
    __tablename__ = "hourly_pageviews"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    hour_bucket = Column(DateTime, index=True)            # truncated to hour
    page_url  = Column(Text, default="/")
    count     = Column(Integer, default=0)
    __table_args__ = (Index("ix_hp_bucket_page", "hour_bucket", "page_url"),)


class HourlyReferrer(Base):
    __tablename__ = "hourly_referrers"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    hour_bucket = Column(DateTime, index=True)
    referrer  = Column(Text, default="")
    count     = Column(Integer, default=0)
    __table_args__ = (Index("ix_hr_bucket_ref", "hour_bucket", "referrer"),)


# ── Init ──────────────────────────────────────────────────────────────────────

def init_db():
    Base.metadata.create_all(bind=engine)


@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _truncate_hour(dt: _dt.datetime) -> _dt.datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


# ── Write ─────────────────────────────────────────────────────────────────────

def insert_event(db: Session, data: dict) -> RawEvent:
    """Insert a raw event and update pre-aggregated summary tables."""
    ts = data.get("timestamp")
    if isinstance(ts, str):
        try:
            ts = _dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            ts = _dt.datetime.utcnow()
    elif ts is None:
        ts = _dt.datetime.utcnow()

    event = RawEvent(
        session_id = data.get("session_id", ""),
        event_type = data.get("event_type", "pageview"),
        page_url   = data.get("page_url", "/"),
        referrer   = data.get("referrer", ""),
        timestamp  = ts,
        click_x    = data.get("click_x"),
        click_y    = data.get("click_y"),
        vp_width   = data.get("vp_width"),
        vp_height  = data.get("vp_height"),
        user_agent = data.get("user_agent", ""),
        ip_address = data.get("ip_address", ""),
        latitude   = data.get("latitude"),
        longitude  = data.get("longitude"),
        country    = data.get("country", ""),
        city       = data.get("city", ""),
        is_bot     = 1 if data.get("is_bot") else 0,
    )
    db.add(event)

    # ── Pre-aggregate: hourly pageviews ──
    if event.event_type == "pageview" and not event.is_bot:
        bucket = _truncate_hour(ts)
        row = db.query(HourlyPageview).filter_by(
            hour_bucket=bucket, page_url=event.page_url
        ).first()
        if row:
            row.count += 1
        else:
            db.add(HourlyPageview(hour_bucket=bucket, page_url=event.page_url, count=1))

    # ── Pre-aggregate: hourly referrers ──
    ref = event.referrer.strip()
    if ref and event.event_type == "pageview" and not event.is_bot:
        bucket = _truncate_hour(ts)
        row = db.query(HourlyReferrer).filter_by(
            hour_bucket=bucket, referrer=ref
        ).first()
        if row:
            row.count += 1
        else:
            db.add(HourlyReferrer(hour_bucket=bucket, referrer=ref, count=1))

    db.commit()
    db.refresh(event)
    return event


# ── Read ──────────────────────────────────────────────────────────────────────

def get_hourly_stats(db: Session, hours: int = 24) -> list[dict]:
    """Pageviews aggregated by hour for the last N hours — from summary table."""
    cutoff = _dt.datetime.utcnow() - _dt.timedelta(hours=hours)
    rows = (
        db.query(
            HourlyPageview.hour_bucket,
            func.sum(HourlyPageview.count).label("total")
        )
        .filter(HourlyPageview.hour_bucket >= cutoff)
        .group_by(HourlyPageview.hour_bucket)
        .order_by(HourlyPageview.hour_bucket)
        .all()
    )
    return [{"hour": r.hour_bucket.isoformat(), "count": r.total} for r in rows]


def get_top_referrers(db: Session, limit: int = 10) -> list[dict]:
    """Top N referrers from summary table."""
    cutoff = _dt.datetime.utcnow() - _dt.timedelta(hours=24)
    rows = (
        db.query(
            HourlyReferrer.referrer,
            func.sum(HourlyReferrer.count).label("total")
        )
        .filter(HourlyReferrer.hour_bucket >= cutoff)
        .group_by(HourlyReferrer.referrer)
        .order_by(func.sum(HourlyReferrer.count).desc())
        .limit(limit)
        .all()
    )
    return [{"referrer": r.referrer, "count": r.total} for r in rows]


def get_funnel(db: Session, steps: list[str]) -> list[dict]:
    """
    Funnel analysis: for each step URL, count distinct sessions that visited.
    Returns drop-off counts & percentages.
    """
    if not steps:
        return []

    results = []
    prev_sessions = None
    for i, url in enumerate(steps):
        q = db.query(distinct(RawEvent.session_id)).filter(
            RawEvent.page_url == url,
            RawEvent.event_type == "pageview",
            RawEvent.is_bot == 0,
        )
        if prev_sessions is not None:
            q = q.filter(RawEvent.session_id.in_(prev_sessions))
        sessions = {r[0] for r in q.all()}
        count = len(sessions)
        results.append({
            "step": url,
            "visitors": count,
            "dropoff": (results[-1]["visitors"] - count) if i > 0 else 0,
            "rate": round(count / results[0]["visitors"] * 100, 1) if i > 0 and results[0]["visitors"] else 100.0,
        })
        prev_sessions = sessions
    return results


def get_heatmap(db: Session, page_url: str) -> list[dict]:
    """Return click x/y coordinates for a specific page."""
    rows = (
        db.query(RawEvent.click_x, RawEvent.click_y, RawEvent.vp_width, RawEvent.vp_height)
        .filter(
            RawEvent.page_url == page_url,
            RawEvent.event_type == "click",
            RawEvent.click_x.isnot(None),
            RawEvent.click_y.isnot(None),
            RawEvent.is_bot == 0,
        )
        .all()
    )
    return [{"x": r.click_x, "y": r.click_y, "vw": r.vp_width, "vh": r.vp_height} for r in rows]


def get_recent_events(db: Session, limit: int = 50) -> list[dict]:
    """Most recent events for the live feed."""
    rows = (
        db.query(RawEvent)
        .filter(RawEvent.is_bot == 0)
        .order_by(RawEvent.timestamp.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": r.id,
            "event_type": r.event_type,
            "page_url": r.page_url,
            "timestamp": r.timestamp.isoformat() if r.timestamp else "",
            "latitude": r.latitude,
            "longitude": r.longitude,
            "country": r.country,
            "city": r.city,
        }
        for r in rows
    ]


def get_geo_data(db: Session, limit: int = 100) -> list[dict]:
    """Lat/lng of recent visitors."""
    rows = (
        db.query(
            RawEvent.latitude, RawEvent.longitude,
            RawEvent.country, RawEvent.city, RawEvent.timestamp
        )
        .filter(
            RawEvent.latitude.isnot(None),
            RawEvent.longitude.isnot(None),
            RawEvent.is_bot == 0,
        )
        .order_by(RawEvent.timestamp.desc())
        .limit(limit)
        .all()
    )
    return [
        {"lat": r.latitude, "lng": r.longitude, "country": r.country,
         "city": r.city, "time": r.timestamp.isoformat() if r.timestamp else ""}
        for r in rows
    ]


def get_dashboard_summary(db: Session) -> dict:
    """Quick summary stats for the dashboard header cards."""
    cutoff_24h = _dt.datetime.utcnow() - _dt.timedelta(hours=24)

    total_pv = (
        db.query(func.sum(HourlyPageview.count))
        .filter(HourlyPageview.hour_bucket >= cutoff_24h)
        .scalar() or 0
    )
    unique_visitors = (
        db.query(func.count(distinct(RawEvent.session_id)))
        .filter(RawEvent.timestamp >= cutoff_24h, RawEvent.is_bot == 0)
        .scalar() or 0
    )
    total_clicks = (
        db.query(func.count(RawEvent.id))
        .filter(
            RawEvent.timestamp >= cutoff_24h,
            RawEvent.event_type == "click",
            RawEvent.is_bot == 0
        )
        .scalar() or 0
    )
    return {
        "total_pageviews": int(total_pv),
        "unique_visitors": int(unique_visitors),
        "total_clicks": int(total_clicks),
    }

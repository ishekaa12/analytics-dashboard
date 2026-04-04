"""
populate_all.py — Seed realistic analytics data for both Luma and Substack
into the unified analytics.db. Generates ~2000 events per site with
realistic geographic distribution, funnel drop-off, click heatmaps,
and referrer patterns.
"""

import sqlite3
import random
import datetime as _dt
import os
import hashlib

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "analytics.db")

# ── Shared Constants ──────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0 Edg/125.0",
    "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 Chrome/125.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 Chrome/125.0 Mobile Safari/537.36",
]

GEO_LOCATIONS = [
    {"lat": 37.7749, "lng": -122.4194, "country": "US", "city": "San Francisco", "region": "California", "weight": 18},
    {"lat": 40.7128, "lng": -74.0060, "country": "US", "city": "New York", "region": "New York", "weight": 15},
    {"lat": 34.0522, "lng": -118.2437, "country": "US", "city": "Los Angeles", "region": "California", "weight": 10},
    {"lat": 51.5074, "lng": -0.1278, "country": "GB", "city": "London", "region": "England", "weight": 12},
    {"lat": 48.8566, "lng": 2.3522, "country": "FR", "city": "Paris", "region": "Ile-de-France", "weight": 7},
    {"lat": 52.5200, "lng": 13.4050, "country": "DE", "city": "Berlin", "region": "Berlin", "weight": 8},
    {"lat": 35.6762, "lng": 139.6503, "country": "JP", "city": "Tokyo", "region": "Kanto", "weight": 6},
    {"lat": 19.0760, "lng": 72.8777, "country": "IN", "city": "Mumbai", "region": "Maharashtra", "weight": 9},
    {"lat": 28.6139, "lng": 77.2090, "country": "IN", "city": "New Delhi", "region": "Delhi", "weight": 7},
    {"lat": 12.9716, "lng": 77.5946, "country": "IN", "city": "Bangalore", "region": "Karnataka", "weight": 8},
    {"lat": 43.6532, "lng": -79.3832, "country": "CA", "city": "Toronto", "region": "Ontario", "weight": 6},
    {"lat": -33.8688, "lng": 151.2093, "country": "AU", "city": "Sydney", "region": "NSW", "weight": 5},
    {"lat": 1.3521, "lng": 103.8198, "country": "SG", "city": "Singapore", "region": "Central", "weight": 5},
    {"lat": -23.5505, "lng": -46.6333, "country": "BR", "city": "Sao Paulo", "region": "SP", "weight": 4},
    {"lat": 37.5665, "lng": 126.9780, "country": "KR", "city": "Seoul", "region": "Seoul", "weight": 4},
    {"lat": 25.2048, "lng": 55.2708, "country": "AE", "city": "Dubai", "region": "Dubai", "weight": 3},
    {"lat": 41.0082, "lng": 28.9784, "country": "TR", "city": "Istanbul", "region": "Istanbul", "weight": 3},
    {"lat": 59.3293, "lng": 18.0686, "country": "SE", "city": "Stockholm", "region": "Stockholm", "weight": 3},
    {"lat": 47.3769, "lng": 8.5417, "country": "CH", "city": "Zurich", "region": "Zurich", "weight": 3},
    {"lat": -1.2921, "lng": 36.8219, "country": "KE", "city": "Nairobi", "region": "Nairobi", "weight": 2},
    {"lat": 6.5244, "lng": 3.3792, "country": "NG", "city": "Lagos", "region": "Lagos", "weight": 2},
    {"lat": 13.7563, "lng": 100.5018, "country": "TH", "city": "Bangkok", "region": "Bangkok", "weight": 2},
    {"lat": 47.6062, "lng": -122.3321, "country": "US", "city": "Seattle", "region": "Washington", "weight": 7},
    {"lat": 30.2672, "lng": -97.7431, "country": "US", "city": "Austin", "region": "Texas", "weight": 6},
    {"lat": 42.3601, "lng": -71.0589, "country": "US", "city": "Boston", "region": "Massachusetts", "weight": 5},
]

VIEWPORTS = [
    (1920, 1080), (1440, 900), (1366, 768), (1536, 864),
    (2560, 1440), (375, 812), (414, 896), (390, 844),
    (768, 1024), (1024, 768),
]

# ── Luma Configuration ────────────────────────────────────────────────────────

LUMA_PAGES = [
    "https://lu.ma/sf-ai-meetup",
    "https://lu.ma/founder-dinner-nyc",
    "https://lu.ma/hack-night-austin",
    "https://lu.ma/design-week-london",
    "https://lu.ma/startup-social-berlin",
    "https://lu.ma/web3-summit-sg",
    "https://lu.ma/ml-workshop-toronto",
    "https://lu.ma/devcon-tokyo",
    "https://lu.ma/growth-meetup-bangalore",
    "https://lu.ma/data-talks-seattle",
]

LUMA_REFERRERS = [
    ("https://google.com", 28),
    ("https://twitter.com", 20),
    ("", 14),
    ("https://linkedin.com", 12),
    ("https://facebook.com", 8),
    ("https://news.ycombinator.com", 6),
    ("https://reddit.com", 5),
    ("https://lu.ma", 4),
    ("https://producthunt.com", 2),
    ("https://substack.com", 1),
]

# Click zones for Luma event pages (normalized x, y, spread_x, spread_y)
LUMA_CLICK_ZONES = [
    (0.5, 0.32, 0.12, 0.03),   # Register Now button
    (0.35, 0.03, 0.08, 0.015),  # Home nav
    (0.45, 0.03, 0.08, 0.015),  # Schedule nav
    (0.55, 0.03, 0.08, 0.015),  # Speakers nav
    (0.65, 0.03, 0.08, 0.015),  # Community nav
    (0.88, 0.03, 0.05, 0.015),  # Log In button
    (0.3, 0.55, 0.15, 0.06),    # About section
    (0.7, 0.55, 0.12, 0.06),    # Event Details sidebar
    (0.2, 0.78, 0.08, 0.04),    # Speaker 1
    (0.35, 0.78, 0.08, 0.04),   # Speaker 2
    (0.5, 0.78, 0.08, 0.04),    # Speaker 3
    (0.65, 0.78, 0.08, 0.04),   # Speaker 4
    (0.15, 0.95, 0.06, 0.015),  # Footer Home
    (0.25, 0.95, 0.06, 0.015),  # Footer Schedule
]

# ── Substack Configuration ───────────────────────────────────────────────────

SUBSTACK_PAGES = ["/", "/subscribe", "/activity", "/chat"]
SUBSTACK_FUNNEL_PROBS = [1.0, 0.55, 0.32, 0.15]

SUBSTACK_REFERRERS = [
    ("https://www.google.com", 28),
    ("https://twitter.com", 18),
    ("", 14),
    ("https://www.facebook.com", 9),
    ("https://www.reddit.com", 8),
    ("https://news.ycombinator.com", 6),
    ("https://t.co", 5),
    ("https://www.linkedin.com", 4),
    ("https://substack.com", 5),
    ("https://duckduckgo.com", 3),
]

# Click zones for Substack pages (mapped to heatmap bg screenshot)
SUBSTACK_CLICK_ZONES = {
    "/": [
        (0.5, 0.08, 0.15, 0.02),   # Navigation bar
        (0.5, 0.30, 0.20, 0.04),   # Hero title / main CTA
        (0.5, 0.42, 0.10, 0.02),   # Subscribe button
        (0.5, 0.58, 0.25, 0.06),   # Post list area
        (0.5, 0.75, 0.25, 0.06),   # More posts
        (0.85, 0.08, 0.06, 0.02),  # Sign in
    ],
    "/subscribe": [
        (0.5, 0.35, 0.18, 0.03),   # Email input
        (0.5, 0.45, 0.12, 0.03),   # Subscribe button
        (0.35, 0.65, 0.12, 0.05),  # Free plan
        (0.65, 0.65, 0.12, 0.05),  # Paid plan
    ],
    "/activity": [
        (0.5, 0.25, 0.30, 0.06),   # Activity item 1
        (0.5, 0.45, 0.30, 0.06),   # Activity item 2
        (0.5, 0.65, 0.30, 0.06),   # Activity item 3
    ],
    "/chat": [
        (0.5, 0.85, 0.25, 0.03),   # Message input
        (0.88, 0.85, 0.04, 0.03),  # Send button
        (0.15, 0.40, 0.08, 0.15),  # Chat list
    ],
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _weighted_choice(items_with_weights):
    items, weights = zip(*items_with_weights)
    return random.choices(items, weights=weights, k=1)[0]


def _random_ts(now, hours_back=24):
    hours_ago = min(random.expovariate(0.15), hours_back)
    return now - _dt.timedelta(hours=hours_ago, minutes=random.randint(0, 59), seconds=random.randint(0, 59))


def _random_geo():
    geo = random.choices(GEO_LOCATIONS, weights=[g["weight"] for g in GEO_LOCATIONS], k=1)[0]
    return {
        "lat": geo["lat"] + random.gauss(0, 0.03),
        "lng": geo["lng"] + random.gauss(0, 0.03),
        "country": geo["country"],
        "city": geo["city"],
        "region": geo["region"],
    }


def _generate_click(zones, vw, vh):
    cx, cy, sx, sy = random.choice(zones)
    x = max(0, min(vw, int((cx + random.gauss(0, sx)) * vw)))
    y = max(0, min(vh, int((cy + random.gauss(0, sy)) * vh)))
    return x, y


def _fake_ip():
    return f"{random.randint(10,200)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


# ── Seeding Functions ─────────────────────────────────────────────────────────

def seed_luma(conn, now, num_sessions=800):
    """Seed Luma event analytics data."""
    print("  Seeding Luma data...")
    cursor = conn.cursor()
    event_count = 0

    for _ in range(num_sessions):
        session_id = hashlib.md5(f"luma_{random.randint(0,999999)}".encode()).hexdigest()[:16]
        ua = random.choice(USER_AGENTS)
        geo = _random_geo()
        vw, vh = random.choice(VIEWPORTS)
        referrer = _weighted_choice(LUMA_REFERRERS)
        ip = _fake_ip()

        # Each session visits 1-3 event pages
        num_pages = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
        pages = random.sample(LUMA_PAGES, min(num_pages, len(LUMA_PAGES)))
        ts_base = _random_ts(now)

        for pi, page in enumerate(pages):
            ts = ts_base + _dt.timedelta(seconds=pi * random.randint(20, 300))

            ip_hash = hashlib.sha256(ip.encode()).hexdigest()
            cursor.execute("""INSERT INTO events
                (site_id, page_url, referrer, user_agent, ip_address, ip_hash, timestamp,
                 is_bot, lat, lng, country, city, region, session_id, event_type,
                 vp_width, vp_height)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 'pageview', ?, ?)""",
                ("luma", page, referrer if pi == 0 else "", ua, ip, ip_hash, ts.isoformat(),
                 geo["lat"], geo["lng"], geo["country"], geo["city"], geo["region"],
                 session_id, vw, vh))
            event_count += 1

            # Generate clicks on this page
            num_clicks = random.choices([0, 1, 2, 3, 4], weights=[20, 30, 25, 15, 10])[0]
            for c in range(num_clicks):
                click_ts = ts + _dt.timedelta(seconds=random.randint(3, 45) + c * 5)
                cx, cy = _generate_click(LUMA_CLICK_ZONES, vw, vh)
                cursor.execute("""INSERT INTO events
                    (site_id, page_url, referrer, user_agent, ip_address, ip_hash, timestamp,
                     is_bot, lat, lng, country, city, region, session_id, event_type,
                     click_x, click_y, vp_width, vp_height)
                    VALUES (?, ?, '', ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 'click', ?, ?, ?, ?)""",
                    ("luma", page, ua, ip, ip_hash, click_ts.isoformat(),
                     geo["lat"], geo["lng"], geo["country"], geo["city"], geo["region"],
                     session_id, cx, cy, vw, vh))
                event_count += 1

    conn.commit()
    print(f"    {event_count} Luma events created")
    return event_count


def seed_substack(conn, now, num_sessions=800):
    """Seed Substack newsletter analytics data with funnel drop-off."""
    print("  Seeding Substack data...")
    cursor = conn.cursor()
    event_count = 0

    for _ in range(num_sessions):
        session_id = hashlib.md5(f"sub_{random.randint(0,999999)}".encode()).hexdigest()[:16]
        ua = random.choice(USER_AGENTS)
        geo = _random_geo()
        vw, vh = random.choice(VIEWPORTS)
        referrer = _weighted_choice(SUBSTACK_REFERRERS)
        ip = _fake_ip()
        ts_base = _random_ts(now)

        # Walk through funnel with drop-off
        for step_idx, page in enumerate(SUBSTACK_PAGES):
            if random.random() > SUBSTACK_FUNNEL_PROBS[step_idx]:
                break

            ts = ts_base + _dt.timedelta(seconds=step_idx * random.randint(15, 180))

            ip_hash = hashlib.sha256(ip.encode()).hexdigest()
            cursor.execute("""INSERT INTO events
                (site_id, page_url, referrer, user_agent, ip_address, ip_hash, timestamp,
                 is_bot, lat, lng, country, city, region, session_id, event_type,
                 vp_width, vp_height)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 'pageview', ?, ?)""",
                ("substack", page, referrer if step_idx == 0 else "", ua, ip, ip_hash, ts.isoformat(),
                 geo["lat"], geo["lng"], geo["country"], geo["city"], geo["region"],
                 session_id, vw, vh))
            event_count += 1

            # Generate clicks
            zones = SUBSTACK_CLICK_ZONES.get(page, [(0.5, 0.5, 0.2, 0.2)])
            num_clicks = random.choices([0, 1, 2, 3, 4], weights=[15, 30, 30, 15, 10])[0]
            for c in range(num_clicks):
                click_ts = ts + _dt.timedelta(seconds=random.randint(2, 30) + c * 3)
                cx, cy = _generate_click(zones, vw, vh)
                cursor.execute("""INSERT INTO events
                    (site_id, page_url, referrer, user_agent, ip_address, ip_hash, timestamp,
                     is_bot, lat, lng, country, city, region, session_id, event_type,
                     click_x, click_y, vp_width, vp_height)
                    VALUES (?, ?, '', ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 'click', ?, ?, ?, ?)""",
                    ("substack", page, ua, ip, ip_hash, click_ts.isoformat(),
                     geo["lat"], geo["lng"], geo["country"], geo["city"], geo["region"],
                     session_id, cx, cy, vw, vh))
                event_count += 1

    conn.commit()
    print(f"    {event_count} Substack events created")
    return event_count


def aggregate(conn):
    """Pre-aggregate hourly summaries, referrer summaries, and geo summaries."""
    print("  Running aggregation...")
    cursor = conn.cursor()

    for site in ["luma", "substack"]:
        # Clear existing summaries for this site
        cursor.execute("DELETE FROM hourly_summary WHERE site_id = ?", (site,))
        cursor.execute("DELETE FROM referrer_summary WHERE site_id = ?", (site,))
        cursor.execute("DELETE FROM clicks_hourly WHERE site_id = ?", (site,))
        cursor.execute("DELETE FROM geo_summary WHERE site_id = ?", (site,))

        # Hourly summary
        cursor.execute("""
            INSERT INTO hourly_summary (site_id, hour, page_url, pageviews, unique_visitors, clicks)
            SELECT site_id,
                   strftime('%Y-%m-%dT%H:00:00', timestamp) as hour,
                   page_url,
                   SUM(CASE WHEN event_type='pageview' THEN 1 ELSE 0 END),
                   COUNT(DISTINCT ip_hash),
                   SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END)
            FROM events
            WHERE site_id = ? AND is_bot = 0
            GROUP BY site_id, hour, page_url
        """, (site,))

        # Referrer summary
        cursor.execute("""
            INSERT INTO referrer_summary (site_id, hour, referrer, count)
            SELECT site_id,
                   strftime('%Y-%m-%dT%H:00:00', timestamp) as hour,
                   CASE WHEN referrer = '' THEN 'direct' ELSE referrer END,
                   COUNT(*)
            FROM events
            WHERE site_id = ? AND is_bot = 0 AND event_type = 'pageview' AND referrer != ''
            GROUP BY site_id, hour, referrer
        """, (site,))

        # Clicks hourly
        cursor.execute("""
            INSERT INTO clicks_hourly (site_id, hour, page_url, x, y, count)
            SELECT site_id,
                   strftime('%Y-%m-%dT%H:00:00', timestamp) as hour,
                   page_url, click_x, click_y, COUNT(*)
            FROM events
            WHERE site_id = ? AND is_bot = 0 AND event_type = 'click'
            GROUP BY site_id, hour, page_url, click_x, click_y
        """, (site,))

        # Geo summary
        cursor.execute("""
            INSERT INTO geo_summary (site_id, hour, country, city, region, lat, lng, count)
            SELECT site_id,
                   strftime('%Y-%m-%dT%H:00:00', timestamp) as hour,
                   country, city, region,
                   AVG(lat), AVG(lng), COUNT(*)
            FROM events
            WHERE site_id = ? AND is_bot = 0 AND event_type = 'pageview' AND lat IS NOT NULL
            GROUP BY site_id, hour, country, city
        """, (site,))

    conn.commit()
    print("    Aggregation complete")


def main():
    print("=" * 60)
    print("  Analytics Data Seeder")
    print("=" * 60)

    if not os.path.exists(DB_PATH):
        print(f"  ERROR: {DB_PATH} not found. Start the server first to create it.")
        return

    conn = sqlite3.connect(DB_PATH)
    now = _dt.datetime.utcnow()

    # Clear old seeded data
    print("\n  Clearing existing data...")
    conn.execute("DELETE FROM events")
    conn.execute("DELETE FROM hourly_summary")
    conn.execute("DELETE FROM referrer_summary")
    conn.execute("DELETE FROM clicks_hourly")
    conn.execute("DELETE FROM geo_summary")
    conn.commit()

    print()
    luma_count = seed_luma(conn, now, num_sessions=800)
    sub_count = seed_substack(conn, now, num_sessions=800)

    print()
    aggregate(conn)

    # Final counts
    print("\n  Final database summary:")
    for table in ["events", "hourly_summary", "referrer_summary", "clicks_hourly", "geo_summary"]:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        luma_row = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE site_id='luma'").fetchone()
        sub_row = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE site_id='substack'").fetchone()
        print(f"    {table}: {row[0]} total (luma={luma_row[0]}, substack={sub_row[0]})")

    conn.close()

    print("\n" + "=" * 60)
    print("  Done! Start server: python -m uvicorn main:app --reload")
    print("=" * 60)


if __name__ == "__main__":
    main()

"""
seed_data.py — Populate the analytics DB with realistic Substack-style demo data.
Generates ~800 sessions across 24h with realistic funnel drop-off,
diverse referrers, geo data, and click heatmap coordinates.
"""

import random
import datetime as _dt
from database import init_db, get_db, insert_event

# ── Configuration ─────────────────────────────────────────────────────────────

NUM_SESSIONS = 600
HOURS_BACK = 24

# Actual Substack-like routes
PAGES = ["/", "/subscribe", "/activity", "/chat"]

# Funnel probabilities — probability of visiting each step given previous
# home -> subscribe -> activity -> chat
FUNNEL_PROBS = [1.0, 0.55, 0.32, 0.15]

REFERRERS = [
    ("https://www.google.com", 30),
    ("https://twitter.com", 18),
    ("", 15),  # direct
    ("https://www.facebook.com", 10),
    ("https://www.reddit.com", 8),
    ("https://news.ycombinator.com", 6),
    ("https://t.co", 5),
    ("https://www.linkedin.com", 4),
    ("https://substack.com", 3),
    ("https://duckduckgo.com", 1),
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
    "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
]

GEO_LOCATIONS = [
    {"lat": 40.7128, "lng": -74.0060, "country": "United States", "city": "New York"},
    {"lat": 34.0522, "lng": -118.2437, "country": "United States", "city": "Los Angeles"},
    {"lat": 51.5074, "lng": -0.1278, "country": "United Kingdom", "city": "London"},
    {"lat": 48.8566, "lng": 2.3522, "country": "France", "city": "Paris"},
    {"lat": 35.6762, "lng": 139.6503, "country": "Japan", "city": "Tokyo"},
    {"lat": 52.5200, "lng": 13.4050, "country": "Germany", "city": "Berlin"},
    {"lat": 19.0760, "lng": 72.8777, "country": "India", "city": "Mumbai"},
    {"lat": 28.6139, "lng": 77.2090, "country": "India", "city": "New Delhi"},
    {"lat": 12.9716, "lng": 77.5946, "country": "India", "city": "Bangalore"},
    {"lat": 37.7749, "lng": -122.4194, "country": "United States", "city": "San Francisco"},
    {"lat": 43.6532, "lng": -79.3832, "country": "Canada", "city": "Toronto"},
    {"lat": -33.8688, "lng": 151.2093, "country": "Australia", "city": "Sydney"},
    {"lat": 55.7558, "lng": 37.6173, "country": "Russia", "city": "Moscow"},
    {"lat": 1.3521, "lng": 103.8198, "country": "Singapore", "city": "Singapore"},
    {"lat": -23.5505, "lng": -46.6333, "country": "Brazil", "city": "São Paulo"},
    {"lat": 37.5665, "lng": 126.9780, "country": "South Korea", "city": "Seoul"},
    {"lat": 39.9042, "lng": 116.4074, "country": "China", "city": "Beijing"},
    {"lat": 25.2048, "lng": 55.2708, "country": "UAE", "city": "Dubai"},
    {"lat": 41.0082, "lng": 28.9784, "country": "Turkey", "city": "Istanbul"},
    {"lat": 59.3293, "lng": 18.0686, "country": "Sweden", "city": "Stockholm"},
    {"lat": 47.3769, "lng": 8.5417, "country": "Switzerland", "city": "Zurich"},
    {"lat": -1.2921, "lng": 36.8219, "country": "Kenya", "city": "Nairobi"},
    {"lat": 6.5244, "lng": 3.3792, "country": "Nigeria", "city": "Lagos"},
    {"lat": 13.7563, "lng": 100.5018, "country": "Thailand", "city": "Bangkok"},
]

VIEWPORTS = [
    (1920, 1080), (1440, 900), (1366, 768), (1536, 864),
    (2560, 1440), (375, 812), (414, 896), (390, 844),
    (768, 1024), (1024, 768),
]

# Click hotspot zones (normalized 0-1 for a page)
CLICK_ZONES = {
    "/": [
        (0.5, 0.15, 0.15, 0.05),   # header/logo area
        (0.5, 0.4, 0.3, 0.1),      # main CTA button
        (0.5, 0.65, 0.25, 0.08),   # secondary content
        (0.8, 0.05, 0.1, 0.03),    # nav items
        (0.2, 0.05, 0.1, 0.03),    # nav items
    ],
    "/subscribe": [
        (0.5, 0.35, 0.2, 0.05),    # email input
        (0.5, 0.45, 0.15, 0.04),   # subscribe button
        (0.5, 0.6, 0.2, 0.06),     # plan selection
        (0.3, 0.75, 0.1, 0.04),    # free plan
        (0.7, 0.75, 0.1, 0.04),    # paid plan
    ],
    "/activity": [
        (0.5, 0.2, 0.35, 0.08),    # activity list top
        (0.5, 0.4, 0.35, 0.08),    # activity list mid
        (0.5, 0.6, 0.35, 0.08),    # activity list bottom
        (0.9, 0.3, 0.05, 0.05),    # like buttons
        (0.9, 0.5, 0.05, 0.05),    # like buttons
    ],
    "/chat": [
        (0.5, 0.85, 0.3, 0.04),    # message input
        (0.85, 0.85, 0.05, 0.04),   # send button
        (0.15, 0.3, 0.1, 0.2),     # chat list sidebar
        (0.5, 0.5, 0.3, 0.15),     # message area
    ],
}


def _weighted_choice(items_with_weights):
    total = sum(w for _, w in items_with_weights)
    r = random.uniform(0, total)
    cumulative = 0
    for item, weight in items_with_weights:
        cumulative += weight
        if r <= cumulative:
            return item
    return items_with_weights[-1][0]


def _random_ts(now: _dt.datetime):
    """Random timestamp within the last HOURS_BACK hours, weighted toward recent."""
    # Weight more events toward recent hours
    hours_ago = random.expovariate(0.15)
    hours_ago = min(hours_ago, HOURS_BACK)
    return now - _dt.timedelta(hours=hours_ago, minutes=random.randint(0, 59), seconds=random.randint(0, 59))


def _generate_click(page_url: str, vw: int, vh: int):
    """Generate a click event at a realistic position based on page zones."""
    zones = CLICK_ZONES.get(page_url, [(0.5, 0.5, 0.3, 0.3)])
    cx, cy, sx, sy = random.choice(zones)
    x = max(0, min(vw, int((cx + random.gauss(0, sx)) * vw)))
    y = max(0, min(vh, int((cy + random.gauss(0, sy)) * vh)))
    return x, y


def seed():
    init_db()
    now = _dt.datetime.utcnow()
    event_count = 0

    with get_db() as db:
        for _ in range(NUM_SESSIONS):
            session_id = f"seed_{random.randint(100000, 999999)}_{random.randint(1000, 9999)}"
            ua = random.choice(USER_AGENTS)
            geo = random.choice(GEO_LOCATIONS)
            vw, vh = random.choice(VIEWPORTS)
            referrer = _weighted_choice(REFERRERS)
            ts_base = _random_ts(now)

            # Walk through funnel
            for step_idx, page in enumerate(PAGES):
                if random.random() > FUNNEL_PROBS[step_idx]:
                    break

                ts = ts_base + _dt.timedelta(seconds=step_idx * random.randint(15, 180))

                # Pageview
                insert_event(db, {
                    "session_id": session_id,
                    "event_type": "pageview",
                    "page_url": page,
                    "referrer": referrer if step_idx == 0 else "",
                    "timestamp": ts.isoformat(),
                    "vp_width": vw,
                    "vp_height": vh,
                    "user_agent": ua,
                    "ip_address": f"seed_{session_id[:8]}",
                    "latitude": geo["lat"] + random.gauss(0, 0.05),
                    "longitude": geo["lng"] + random.gauss(0, 0.05),
                    "country": geo["country"],
                    "city": geo["city"],
                    "is_bot": False,
                })
                event_count += 1

                # Random clicks on this page
                num_clicks = random.choices([0, 1, 2, 3, 4], weights=[15, 30, 30, 15, 10])[0]
                for c in range(num_clicks):
                    click_ts = ts + _dt.timedelta(seconds=random.randint(2, 30) + c * 3)
                    cx, cy = _generate_click(page, vw, vh)
                    insert_event(db, {
                        "session_id": session_id,
                        "event_type": "click",
                        "page_url": page,
                        "referrer": "",
                        "timestamp": click_ts.isoformat(),
                        "click_x": cx,
                        "click_y": cy,
                        "vp_width": vw,
                        "vp_height": vh,
                        "user_agent": ua,
                        "ip_address": f"seed_{session_id[:8]}",
                        "latitude": geo["lat"] + random.gauss(0, 0.05),
                        "longitude": geo["lng"] + random.gauss(0, 0.05),
                        "country": geo["country"],
                        "city": geo["city"],
                        "is_bot": False,
                    })
                    event_count += 1

                # Only use referrer on landing page
                referrer = ""

    print(f"[OK] Seeded {event_count} events across {NUM_SESSIONS} sessions")


if __name__ == "__main__":
    seed()
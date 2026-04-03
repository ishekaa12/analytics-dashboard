import asyncio
import hashlib
import random
from datetime import datetime, timedelta
from main import Base, Event, engine, async_session

# ─── Realistic city data for Luma-style events ───
CITIES = [
    # city, region, country, lat, lng, weight
    ("San Francisco", "California", "US", 37.7749, -122.4194, 18),
    ("New York", "New York", "US", 40.7128, -74.0060, 16),
    ("Los Angeles", "California", "US", 34.0522, -118.2437, 12),
    ("London", "England", "GB", 51.5074, -0.1278, 14),
    ("Berlin", "Berlin", "DE", 52.5200, 13.4050, 8),
    ("Mumbai", "Maharashtra", "IN", 19.0760, 72.8777, 10),
    ("Bangalore", "Karnataka", "IN", 12.9716, 77.5946, 9),
    ("Delhi", "Delhi", "IN", 28.6139, 77.2090, 7),
    ("Tokyo", "Tokyo", "JP", 35.6762, 139.6503, 8),
    ("Toronto", "Ontario", "CA", 43.6532, -79.3832, 7),
    ("Paris", "Île-de-France", "FR", 48.8566, 2.3522, 9),
    ("Sydney", "New South Wales", "AU", -33.8688, 151.2093, 6),
    ("Austin", "Texas", "US", 30.2672, -97.7431, 8),
    ("Seattle", "Washington", "US", 47.6062, -122.3321, 7),
    ("Chicago", "Illinois", "US", 41.8781, -87.6298, 6),
    ("Singapore", "Singapore", "SG", 1.3521, 103.8198, 5),
    ("Amsterdam", "North Holland", "NL", 52.3676, 4.9041, 4),
    ("Dubai", "Dubai", "AE", 25.2048, 55.2708, 4),
    ("São Paulo", "São Paulo", "BR", -23.5505, -46.6333, 5),
    ("Boston", "Massachusetts", "US", 42.3601, -71.0589, 5),
    ("Miami", "Florida", "US", 25.7617, -80.1918, 4),
    ("Denver", "Colorado", "US", 39.7392, -104.9903, 3),
    ("Portland", "Oregon", "US", 45.5152, -122.6784, 3),
    ("Stockholm", "Stockholm", "SE", 59.3293, 18.0686, 3),
    ("Lisbon", "Lisbon", "PT", 38.7223, -9.1393, 3),
    ("Melbourne", "Victoria", "AU", -37.8136, 144.9631, 3),
    ("Seoul", "Seoul", "KR", 37.5665, 126.9780, 4),
    ("Tel Aviv", "Tel Aviv", "IL", 32.0853, 34.7818, 4),
    ("Nairobi", "Nairobi", "KE", -1.2921, 36.8219, 2),
    ("Lagos", "Lagos", "NG", 6.5244, 3.3792, 2),
]

LUMA_PAGES = [
    "https://lu.ma/sf-ai-meetup",
    "https://lu.ma/founder-dinner-nyc",
    "https://lu.ma/design-week-london",
    "https://lu.ma/hack-night-austin",
    "https://lu.ma/startup-pitch-sf",
    "https://lu.ma/web3-summit-miami",
    "https://lu.ma/devcon-berlin",
    "https://lu.ma/ux-workshop-toronto",
    "https://lu.ma/ml-conference-bangalore",
    "https://lu.ma/product-launch-tokyo",
]

REFERRERS = [
    "https://google.com",
    "https://twitter.com",
    "https://linkedin.com",
    "https://facebook.com",
    "https://reddit.com",
    "",  # direct
    "https://news.ycombinator.com",
    "https://producthunt.com",
    "https://lu.ma",
    "https://substack.com",
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Weighted city selection
CITY_WEIGHTS = [c[5] for c in CITIES]


def pick_city():
    return random.choices(CITIES, weights=CITY_WEIGHTS, k=1)[0]


def jitter(val, spread=0.04):
    """Add slight randomness to lat/lng for natural clustering."""
    return val + random.uniform(-spread, spread)


async def populate_sample_data():
    """Seed 2000 realistic events spread across 24 hours."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as session:
        now = datetime.utcnow()
        events_count = 2000

        for i in range(events_count):
            city_name, region, country, base_lat, base_lng, _ = pick_city()

            # Distribute events across 24h with slight bias toward recent hours
            hours_ago = random.choices(
                range(24), weights=[24 - h for h in range(24)], k=1
            )[0]
            timestamp = now - timedelta(
                hours=hours_ago,
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )

            page_url = random.choice(LUMA_PAGES)
            referrer = random.choices(
                REFERRERS,
                weights=[20, 15, 12, 10, 8, 15, 8, 5, 5, 2],
                k=1
            )[0]
            user_agent = random.choice(USER_AGENTS)

            ip = f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            ip_hash = hashlib.sha256(ip.encode()).hexdigest()

            is_click = random.random() < 0.3
            click_x = random.randint(50, 1400) if is_click else None
            click_y = random.randint(50, 900) if is_click else None

            screen_width = random.choice([1920, 1440, 1536, 1366, 390, 414, 375])
            screen_height = random.choice([1080, 900, 864, 768, 844, 896, 812])

            is_bot = random.random() < 0.03

            event = Event(
                page_url=page_url,
                referrer=referrer,
                user_agent=user_agent,
                ip_address=ip,
                ip_hash=ip_hash,
                screen_width=screen_width,
                screen_height=screen_height,
                click_x=click_x,
                click_y=click_y,
                timestamp=timestamp,
                is_bot=is_bot,
                country=country,
                city=city_name,
                region=region,
                lat=jitter(base_lat),
                lng=jitter(base_lng),
            )
            session.add(event)

            if (i + 1) % 500 == 0:
                await session.flush()
                print(f"  → {i+1}/{events_count} events created...")

        await session.commit()
        print(f"\n✅ {events_count} realistic Luma events populated successfully!")
        print("   Run `python aggregate.py` next to build hourly summaries.")


if __name__ == "__main__":
    asyncio.run(populate_sample_data())
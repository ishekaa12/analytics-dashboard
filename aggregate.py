import asyncio
from datetime import datetime, timedelta
from main import aggregate_hourly, async_session

async def run_aggregation():
    now = datetime.utcnow()
    print("🔄 Running aggregation for the last 24 hours...")
    for i in range(24):
        hour = (now - timedelta(hours=i)).replace(minute=0, second=0, microsecond=0)
        async with async_session() as session:
            await aggregate_hourly(session, hour)
        print(f"  ✓ Hour {hour.strftime('%H:%M')} aggregated")
    print("✅ Aggregation complete!")

if __name__ == "__main__":
    asyncio.run(run_aggregation())
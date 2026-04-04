import sqlite3
conn = sqlite3.connect("analytics.db")
cursor = conn.cursor()
for table in ["events", "hourly_summary", "referrer_summary", "clicks_hourly", "geo_summary"]:
    total = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    luma = cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE site_id='luma'").fetchone()[0]
    sub = cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE site_id='substack'").fetchone()[0]
    print(f"{table}: total={total}, luma={luma}, substack={sub}")

# Check a sample hourly_summary row
print("\nSample hourly_summary:")
for row in cursor.execute("SELECT * FROM hourly_summary LIMIT 3").fetchall():
    print(row)

print("\nSample clicks_hourly:")
for row in cursor.execute("SELECT * FROM clicks_hourly LIMIT 3").fetchall():
    print(row)

print("\nSample events (click):")
for row in cursor.execute("SELECT id, site_id, event_type, click_x, click_y, ip_hash FROM events WHERE event_type='click' LIMIT 3").fetchall():
    print(row)

conn.close()

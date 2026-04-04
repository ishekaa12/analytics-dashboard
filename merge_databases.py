"""
merge_databases.py — Merge two SQLite databases (Luma + Substack) into one unified analytics.db.
Adds a site_id column ("luma" or "substack") to tag which database each row came from.
Handles different schemas gracefully by mapping columns between the two backends.

Usage:
    python merge_databases.py

This will:
1. Back up the existing analytics.db (if any)
2. Read from ./analytics.db (Luma data) and ./frontend/dashboard/substack/analytics.db (Substack data)
3. Create a new unified ./analytics.db with site_id on every table
"""

import os
import sys
import shutil
import sqlite3
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LUMA_DB = os.path.join(BASE_DIR, "analytics.db")
SUBSTACK_DB = os.path.join(BASE_DIR, "frontend", "dashboard", "substack", "analytics.db")
OUTPUT_DB = os.path.join(BASE_DIR, "analytics_merged.db")
BACKUP_DIR = os.path.join(BASE_DIR, "_db_backups")


# ── Unified schema ───────────────────────────────────────────────────────────

SCHEMA = {
    "events": """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT DEFAULT 'luma',
            session_id TEXT,
            event_type TEXT DEFAULT 'pageview',
            page_url TEXT,
            referrer TEXT,
            user_agent TEXT,
            ip_address TEXT,
            ip_hash TEXT,
            screen_width INTEGER,
            screen_height INTEGER,
            vp_width INTEGER,
            vp_height INTEGER,
            click_x INTEGER,
            click_y INTEGER,
            timestamp DATETIME,
            is_bot BOOLEAN DEFAULT 0,
            country TEXT,
            city TEXT,
            region TEXT,
            lat REAL,
            lng REAL
        )
    """,
    "hourly_summary": """
        CREATE TABLE IF NOT EXISTS hourly_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT DEFAULT 'luma',
            hour DATETIME,
            page_url TEXT,
            pageviews INTEGER DEFAULT 0,
            unique_visitors INTEGER DEFAULT 0,
            clicks INTEGER DEFAULT 0
        )
    """,
    "referrer_summary": """
        CREATE TABLE IF NOT EXISTS referrer_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT DEFAULT 'luma',
            hour DATETIME,
            referrer TEXT,
            count INTEGER DEFAULT 0
        )
    """,
    "clicks_hourly": """
        CREATE TABLE IF NOT EXISTS clicks_hourly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT DEFAULT 'luma',
            hour DATETIME,
            page_url TEXT,
            x INTEGER,
            y INTEGER,
            count INTEGER DEFAULT 0
        )
    """,
    "geo_summary": """
        CREATE TABLE IF NOT EXISTS geo_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT DEFAULT 'luma',
            hour DATETIME,
            lat REAL,
            lng REAL,
            city TEXT,
            region TEXT,
            country TEXT,
            count INTEGER DEFAULT 0
        )
    """,
}

INDEXES = [
    "CREATE INDEX IF NOT EXISTS ix_events_site ON events(site_id)",
    "CREATE INDEX IF NOT EXISTS ix_events_ts ON events(timestamp)",
    "CREATE INDEX IF NOT EXISTS ix_hs_site ON hourly_summary(site_id)",
    "CREATE INDEX IF NOT EXISTS ix_hs_hour ON hourly_summary(hour)",
    "CREATE INDEX IF NOT EXISTS ix_rs_site ON referrer_summary(site_id)",
    "CREATE INDEX IF NOT EXISTS ix_rs_hour ON referrer_summary(hour)",
    "CREATE INDEX IF NOT EXISTS ix_ch_site ON clicks_hourly(site_id)",
    "CREATE INDEX IF NOT EXISTS ix_ch_hour ON clicks_hourly(hour)",
    "CREATE INDEX IF NOT EXISTS ix_gs_site ON geo_summary(site_id)",
    "CREATE INDEX IF NOT EXISTS ix_gs_hour ON geo_summary(hour)",
]


def backup_db(path, label):
    """Back up a database file before merging."""
    if not os.path.exists(path):
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"{label}_{ts}.db")
    shutil.copy2(path, backup_path)
    print(f"  📦 Backed up {label} → {backup_path}")


def get_table_columns(conn, table_name):
    """Get column names for a table."""
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


def get_tables(conn):
    """Get all table names in a database."""
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [row[0] for row in cursor.fetchall()]


def migrate_luma_events(src_conn, dst_conn):
    """Migrate Luma's 'events' table → unified 'events' table."""
    tables = get_tables(src_conn)
    if "events" not in tables:
        print("  ⚠️  No 'events' table in Luma DB, skipping.")
        return 0

    src_cols = get_table_columns(src_conn, "events")
    # Map Luma columns to unified schema
    col_mapping = {}
    for col in src_cols:
        if col == "id":
            continue  # Skip primary key
        col_mapping[col] = col  # Direct mapping for Luma (it's the base schema)

    rows = src_conn.execute("SELECT * FROM events").fetchall()
    count = 0
    for row in rows:
        row_dict = dict(zip(src_cols, row))
        dst_conn.execute(
            """INSERT INTO events (site_id, session_id, event_type, page_url, referrer, user_agent,
               ip_address, ip_hash, screen_width, screen_height, vp_width, vp_height,
               click_x, click_y, timestamp, is_bot, country, city, region, lat, lng)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "luma",
                row_dict.get("session_id"),
                row_dict.get("event_type", "pageview"),
                row_dict.get("page_url"),
                row_dict.get("referrer"),
                row_dict.get("user_agent"),
                row_dict.get("ip_address"),
                row_dict.get("ip_hash"),
                row_dict.get("screen_width"),
                row_dict.get("screen_height"),
                row_dict.get("vp_width"),
                row_dict.get("vp_height"),
                row_dict.get("click_x"),
                row_dict.get("click_y"),
                row_dict.get("timestamp"),
                row_dict.get("is_bot", 0),
                row_dict.get("country"),
                row_dict.get("city"),
                row_dict.get("region"),
                row_dict.get("lat"),
                row_dict.get("lng"),
            )
        )
        count += 1
    return count


def migrate_substack_events(src_conn, dst_conn):
    """Migrate Substack's 'raw_events' table → unified 'events' table."""
    tables = get_tables(src_conn)
    if "raw_events" not in tables:
        print("  ⚠️  No 'raw_events' table in Substack DB, skipping.")
        return 0

    src_cols = get_table_columns(src_conn, "raw_events")
    rows = src_conn.execute("SELECT * FROM raw_events").fetchall()
    count = 0
    for row in rows:
        row_dict = dict(zip(src_cols, row))
        # Map Substack columns → unified columns
        dst_conn.execute(
            """INSERT INTO events (site_id, session_id, event_type, page_url, referrer, user_agent,
               ip_address, ip_hash, screen_width, screen_height, vp_width, vp_height,
               click_x, click_y, timestamp, is_bot, country, city, region, lat, lng)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "substack",
                row_dict.get("session_id"),
                row_dict.get("event_type", "pageview"),
                row_dict.get("page_url"),
                row_dict.get("referrer"),
                row_dict.get("user_agent"),
                row_dict.get("ip_address"),  # already hashed in substack
                row_dict.get("ip_address"),  # use as ip_hash too
                None,  # screen_width (not in substack)
                None,  # screen_height (not in substack)
                row_dict.get("vp_width"),
                row_dict.get("vp_height"),
                int(row_dict["click_x"]) if row_dict.get("click_x") is not None else None,
                int(row_dict["click_y"]) if row_dict.get("click_y") is not None else None,
                row_dict.get("timestamp"),
                row_dict.get("is_bot", 0),
                row_dict.get("country"),
                row_dict.get("city"),
                None,  # region (not in substack)
                row_dict.get("latitude"),   # substack uses 'latitude' not 'lat'
                row_dict.get("longitude"),  # substack uses 'longitude' not 'lng'
            )
        )
        count += 1
    return count


def migrate_luma_summaries(src_conn, dst_conn):
    """Migrate Luma's summary tables (hourly_summary, referrer_summary, clicks_hourly, geo_summary)."""
    counts = {}
    tables = get_tables(src_conn)

    # hourly_summary
    if "hourly_summary" in tables:
        src_cols = get_table_columns(src_conn, "hourly_summary")
        rows = src_conn.execute("SELECT * FROM hourly_summary").fetchall()
        for row in rows:
            d = dict(zip(src_cols, row))
            dst_conn.execute(
                "INSERT INTO hourly_summary (site_id, hour, page_url, pageviews, unique_visitors, clicks) VALUES (?,?,?,?,?,?)",
                ("luma", d.get("hour"), d.get("page_url"), d.get("pageviews", 0), d.get("unique_visitors", 0), d.get("clicks", 0))
            )
        counts["hourly_summary"] = len(rows)

    # referrer_summary
    if "referrer_summary" in tables:
        src_cols = get_table_columns(src_conn, "referrer_summary")
        rows = src_conn.execute("SELECT * FROM referrer_summary").fetchall()
        for row in rows:
            d = dict(zip(src_cols, row))
            dst_conn.execute(
                "INSERT INTO referrer_summary (site_id, hour, referrer, count) VALUES (?,?,?,?)",
                ("luma", d.get("hour"), d.get("referrer"), d.get("count", 0))
            )
        counts["referrer_summary"] = len(rows)

    # clicks_hourly
    if "clicks_hourly" in tables:
        src_cols = get_table_columns(src_conn, "clicks_hourly")
        rows = src_conn.execute("SELECT * FROM clicks_hourly").fetchall()
        for row in rows:
            d = dict(zip(src_cols, row))
            dst_conn.execute(
                "INSERT INTO clicks_hourly (site_id, hour, page_url, x, y, count) VALUES (?,?,?,?,?,?)",
                ("luma", d.get("hour"), d.get("page_url"), d.get("x"), d.get("y"), d.get("count", 0))
            )
        counts["clicks_hourly"] = len(rows)

    # geo_summary
    if "geo_summary" in tables:
        src_cols = get_table_columns(src_conn, "geo_summary")
        rows = src_conn.execute("SELECT * FROM geo_summary").fetchall()
        for row in rows:
            d = dict(zip(src_cols, row))
            dst_conn.execute(
                "INSERT INTO geo_summary (site_id, hour, lat, lng, city, region, country, count) VALUES (?,?,?,?,?,?,?,?)",
                ("luma", d.get("hour"), d.get("lat"), d.get("lng"), d.get("city"), d.get("region"), d.get("country"), d.get("count", 0))
            )
        counts["geo_summary"] = len(rows)

    return counts


def migrate_substack_summaries(src_conn, dst_conn):
    """Migrate Substack's summary tables (hourly_pageviews → hourly_summary, hourly_referrers → referrer_summary)."""
    counts = {}
    tables = get_tables(src_conn)

    # hourly_pageviews → hourly_summary
    if "hourly_pageviews" in tables:
        src_cols = get_table_columns(src_conn, "hourly_pageviews")
        rows = src_conn.execute("SELECT * FROM hourly_pageviews").fetchall()
        for row in rows:
            d = dict(zip(src_cols, row))
            dst_conn.execute(
                "INSERT INTO hourly_summary (site_id, hour, page_url, pageviews, unique_visitors, clicks) VALUES (?,?,?,?,?,?)",
                ("substack", d.get("hour_bucket"), d.get("page_url", "/"), d.get("count", 0), 0, 0)
            )
        counts["hourly_pageviews → hourly_summary"] = len(rows)

    # hourly_referrers → referrer_summary
    if "hourly_referrers" in tables:
        src_cols = get_table_columns(src_conn, "hourly_referrers")
        rows = src_conn.execute("SELECT * FROM hourly_referrers").fetchall()
        for row in rows:
            d = dict(zip(src_cols, row))
            dst_conn.execute(
                "INSERT INTO referrer_summary (site_id, hour, referrer, count) VALUES (?,?,?,?)",
                ("substack", d.get("hour_bucket"), d.get("referrer", ""), d.get("count", 0))
            )
        counts["hourly_referrers → referrer_summary"] = len(rows)

    return counts


def main():
    print("=" * 60)
    print("  🔄 Analytics Database Merger")
    print("=" * 60)
    print()

    # Check source databases exist
    luma_exists = os.path.exists(LUMA_DB)
    substack_exists = os.path.exists(SUBSTACK_DB)

    if not luma_exists and not substack_exists:
        print("❌ Neither database found!")
        print(f"   Expected: {LUMA_DB}")
        print(f"   Expected: {SUBSTACK_DB}")
        sys.exit(1)

    print(f"  📂 Luma DB:     {LUMA_DB} {'✅ found' if luma_exists else '⚠️ not found'}")
    print(f"  📂 Substack DB: {SUBSTACK_DB} {'✅ found' if substack_exists else '⚠️ not found'}")
    print()

    # Back up existing databases
    print("📦 Creating backups...")
    if luma_exists:
        backup_db(LUMA_DB, "luma")
    if substack_exists:
        backup_db(SUBSTACK_DB, "substack")
    print()

    # Create unified output database
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)

    dst_conn = sqlite3.connect(OUTPUT_DB)
    print("🏗️  Creating unified schema...")
    for table_name, ddl in SCHEMA.items():
        dst_conn.execute(ddl)
        print(f"  ✅ Created table: {table_name}")
    for idx_sql in INDEXES:
        dst_conn.execute(idx_sql)
    dst_conn.commit()
    print()

    # Migrate Luma data
    if luma_exists:
        print("📥 Migrating Luma data...")
        src_conn = sqlite3.connect(LUMA_DB)
        src_tables = get_tables(src_conn)
        print(f"  Found tables: {', '.join(src_tables)}")

        event_count = migrate_luma_events(src_conn, dst_conn)
        print(f"  ✅ events: {event_count} rows")

        summary_counts = migrate_luma_summaries(src_conn, dst_conn)
        for table, cnt in summary_counts.items():
            print(f"  ✅ {table}: {cnt} rows")

        dst_conn.commit()
        src_conn.close()
        print()

    # Migrate Substack data
    if substack_exists:
        print("📥 Migrating Substack data...")
        src_conn = sqlite3.connect(SUBSTACK_DB)
        src_tables = get_tables(src_conn)
        print(f"  Found tables: {', '.join(src_tables)}")

        event_count = migrate_substack_events(src_conn, dst_conn)
        print(f"  ✅ raw_events → events: {event_count} rows")

        summary_counts = migrate_substack_summaries(src_conn, dst_conn)
        for table, cnt in summary_counts.items():
            print(f"  ✅ {table}: {cnt} rows")

        dst_conn.commit()
        src_conn.close()
        print()

    # Print final summary
    print("📊 Final database summary:")
    for table_name in SCHEMA:
        count = dst_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        site_counts = dst_conn.execute(f"SELECT site_id, COUNT(*) FROM {table_name} GROUP BY site_id").fetchall()
        site_str = ", ".join([f"{s[0]}={s[1]}" for s in site_counts]) if site_counts else "empty"
        print(f"  {table_name}: {count} total ({site_str})")

    dst_conn.close()

    # Replace original with merged
    print()
    print(f"🔁 Replacing {LUMA_DB} with merged database...")
    if os.path.exists(LUMA_DB):
        os.remove(LUMA_DB)
    shutil.move(OUTPUT_DB, LUMA_DB)

    print()
    print("=" * 60)
    print("  ✅ Migration complete! Unified analytics.db is ready.")
    print("=" * 60)
    print()
    print("  Next steps:")
    print("    1. Run: python -m uvicorn main:app --reload")
    print("    2. Test: http://localhost:8000/stats?site=luma")
    print("    3. Test: http://localhost:8000/stats?site=substack")


if __name__ == "__main__":
    main()

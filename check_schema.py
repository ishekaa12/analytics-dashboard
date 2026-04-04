import sqlite3
conn = sqlite3.connect("analytics.db")
cursor = conn.cursor()
cursor.execute("select sql from sqlite_master where type='table'")
for r in cursor.fetchall():
    print(r[0])
    print()
conn.close()

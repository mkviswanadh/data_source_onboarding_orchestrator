import sqlite3
from datetime import datetime
import sys
import os

def log_ingestion(target_schema: str, target_table: str,
                  status: str, message: str = ""):
    db_path = os.path.join(os.path.dirname(__file__), "audit.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ingestion_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        target_schema TEXT,
        target_table TEXT,
        status TEXT,
        message TEXT,
        ts TIMESTAMP
    )
    """)

    cursor.execute("""
    INSERT INTO ingestion_audit(target_schema, target_table, status, message, ts)
    VALUES (?, ?, ?, ?, ?)
    """, (target_schema, target_table, status, message, datetime.utcnow()))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python log_to_audit.py <target_schema> <target_table> <status> [message]")
        sys.exit(1)

    schema = sys.argv[1]
    table = sys.argv[2]
    status = sys.argv[3]
    message = sys.argv[4] if len(sys.argv) > 4 else ""
    log_ingestion(schema, table, status, message)

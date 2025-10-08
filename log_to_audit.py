import sqlite3
from datetime import datetime
import sys
import os

def log_ingestion(
    source_name: str,
    source_schema: str,
    source_table: str,
    target_schema: str,
    target_table: str,
    status: str,
    message: str = ""
):
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, "audit.db")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT,
            source_schema TEXT,
            source_table TEXT,
            target_schema TEXT,
            target_table TEXT,
            status TEXT NOT NULL,
            message TEXT,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        cursor.execute("""
        INSERT INTO ingestion_audit (
            source_name, source_schema, source_table,
            target_schema, target_table, status, message, ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source_name, source_schema, source_table,
            target_schema, target_table,
            status, message, datetime.utcnow()
        ))

        conn.commit()
        conn.close()

        print(f"✅ Audit logged: {source_name}.{source_schema}.{source_table} → {target_schema}.{target_table} [{status}]")

    except Exception as e:
        print(f"❌ Error logging ingestion audit: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 7:
        print("❌ Usage: python log_to_audit.py <source_name> <source_schema> <source_table> <target_schema> <target_table> <status> [message]")
        sys.exit(1)

    source_name = sys.argv[1]
    source_schema = sys.argv[2]
    source_table = sys.argv[3]
    target_schema = sys.argv[4]
    target_table = sys.argv[5]
    status = sys.argv[6]
    message = sys.argv[7] if len(sys.argv) > 7 else ""

    log_ingestion(source_name, source_schema, source_table, target_schema, target_table, status, message)

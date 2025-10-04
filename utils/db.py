from sqlalchemy import create_engine
import pandas as pd
import os
import urllib.parse

def get_engine():
    # Config
    user = os.getenv("DB_USER", "ingestion_user")
    raw_password = os.getenv("DB_PASSWORD", "Test123")
    encoded_password = urllib.parse.quote_plus(raw_password)
    host = os.getenv("DB_HOST", "localhost")
    db = os.getenv("DB_NAME", "ai_tdv_finacle")
    db_uri = f"mysql+pymysql://{user}:{encoded_password}@{host}/{db}"
    return create_engine(db_uri)

def run_query(query: str):
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

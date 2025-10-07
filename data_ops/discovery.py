from sqlalchemy import create_engine, inspect
import os
import urllib.parse


def get_msql_connection():
    # Config
    user = os.getenv("DB_USER", "ingestion_user")
    raw_password = os.getenv("DB_PASSWORD", "Test123")
    encoded_password = urllib.parse.quote_plus(raw_password)
    host = os.getenv("DB_HOST", "localhost")
    db = os.getenv("DB_NAME", "ai_tdv_finacle")
    db_uri = f"mysql+pymysql://{user}:{encoded_password}@{host}/{db}"
    return db_uri


# Example DB configurations
DB_CONFIGS = {
    "mysql_source": get_msql_connection()
}

def _discover_tables(filter_func=None):
    """
    Shared logic to scan DBs and return tables matching a filter_func
    filter_func: function(table_name) -> bool
    """
    results = []
    for source_name, conn_str in DB_CONFIGS.items():
        try:
            engine = create_engine(conn_str)
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            for table in tables:
                if filter_func is None or filter_func(table):
                    results.append({
                        "source_name": source_name,
                        "schema": engine.url.database,
                        "table": table
                    })
        except Exception as e:
            print(f"Error inspecting {source_name}: {e}")
    return results

def discover_sources_full(filter_func=None):
    """
    Action: Full discovery with schema and table info.
    Returns list of dicts with: schema, table, source_name
    """
    return _discover_tables(filter_func)

def discover_sources(filter_func=None):
    """
    Discover available sources in the database, return a list of source names.
    """
    results = set()
    available_sources = _discover_tables(lambda t: "transactions" in t.lower())
    for source in available_sources:
        source_name = source["source_name"]
        if filter_func is None or filter_func(source["table"]):  
            results.add(source_name)  

    return list(results)  

def check_table_in_sources(table_name: str):
    """Check if a table with the exact name exists in any source"""
    return _discover_tables(lambda t: t.lower() == table_name.lower())

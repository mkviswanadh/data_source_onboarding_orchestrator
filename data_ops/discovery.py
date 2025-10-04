from sqlalchemy import create_engine, inspect

# Example DB configurations
DB_CONFIGS = {
    "mysql_finance": "mysql+pymysql://user:password@mysql-host/finance",
    "postgres_sales": "postgresql+psycopg2://user:password@postgres-host/sales"
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

def discover_sources():
    """Discover tables with 'transaction' in the name (case-insensitive)"""
    return _discover_tables(lambda t: "transaction" in t.lower())

def check_table_in_sources(table_name: str):
    """Check if a table with the exact name exists in any source"""
    return _discover_tables(lambda t: t.lower() == table_name.lower())

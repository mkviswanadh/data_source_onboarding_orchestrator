import pandas as pd
from sqlalchemy import create_engine
from data_ops.discovery import DB_CONFIGS
from ydata_profiling import ProfileReport
import tempfile
import os

def profile_table(source: str, schema: str, table: str):
    conn_str = DB_CONFIGS[source]
    engine = create_engine(conn_str)

    # Load sample or full data
    query = f"SELECT * FROM {schema}.{table} LIMIT 5000"
    df = pd.read_sql(query, engine)

    # Basic statistics
    row_count = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {schema}.{table}", engine).iloc[0, 0]

    # Generate profiling report
    profile = ProfileReport(df, title=f"Profiling Report: {schema}.{table}", minimal=True)

    # Save HTML report to temp file
    html_report_path = os.path.join(tempfile.gettempdir(), f"profile_{schema}_{table}.html")
    profile.to_file(html_report_path)

    summary_stats = {
        "row_count": int(row_count),
        "num_columns": df.shape[1],
        "nulls_by_column": df.isnull().sum().to_dict(),
        "sample_rows": df.head(5).to_dict(orient="records"),
        "profile_report_path": html_report_path
    }

    return summary_stats

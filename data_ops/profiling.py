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
    profile = ProfileReport(df, title=f"Profiling Report: {schema}.{table}", minimal=False)

    # Save HTML report to temp file
    html_report_path = os.path.join(tempfile.gettempdir(), f"profile_{schema}_{table}.html")
    profile.to_file(html_report_path)

    # Read HTML content from file with UTF-8 encoding to handle special characters
    try:
        with open(html_report_path, "r", encoding="utf-8") as f:
            html_content = f.read()
    except UnicodeDecodeError as e:
        print(f"Error reading the file: {e}")
        html_content = ""

    summary_stats = {
        "row_count": int(row_count),
        "num_columns": df.shape[1],
        "nulls_by_column": df.isnull().sum().to_dict(),
        "sample_rows": df.head(5).to_dict(orient="records"),
        "profile_report_html": html_content  # Pass the HTML content here
    }

    return summary_stats,html_report_path

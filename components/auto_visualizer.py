import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from utils.db import run_query

def auto_render_output(query: str):
    df = run_query(query)

    st.write("### Query Result:")
    st.dataframe(df)

    if df.empty:
        st.warning("⚠️ Query returned no data.")
        return

    # If single value — show it as a metric
    if df.shape == (1, 1):
        val = df.iloc[0, 0]
        col_name = df.columns[0]
        st.metric(label=f"{col_name}", value=val)
        return


    # Simple heuristic to decide what to plot
    num_cols = df.select_dtypes(include=['number']).columns.tolist()
    cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()

    # Example: Bar plot if 1 categorical + 1 numeric
    if len(cat_cols) == 1 and len(num_cols) == 1:
        cat_col = cat_cols[0]
        num_col = num_cols[0]
        st.write(f"### Bar Chart: {num_col} by {cat_col}")
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.barplot(data=df, x=cat_col, y=num_col, ax=ax)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=45)
        st.pyplot(fig)
        plt.close(fig)

    # Line plot if 1 datetime + 1 numeric
    elif any(df.dtypes == 'datetime64[ns]') and len(num_cols) >= 1:
        dt_col = df.select_dtypes(include=['datetime']).columns[0]
        num_col = num_cols[0]
        st.write(f"### Line Chart: {num_col} over {dt_col}")
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.lineplot(data=df, x=dt_col, y=num_col, ax=ax)
        st.pyplot(fig)
        plt.close(fig)

    # Pie chart if it's a share breakdown
    elif len(cat_cols) == 1 and len(num_cols) == 1 and df.shape[0] <= 10:
        labels = df[cat_cols[0]]
        sizes = df[num_cols[0]]
        fig, ax = plt.subplots()
        ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        ax.axis('equal')
        st.write(f"### Pie Chart: {num_cols[0]} by {cat_cols[0]}")
        st.pyplot(fig)
        plt.close(fig)

    else:
        st.info("ℹ️ Showing data table. No suitable plot type detected.")
        st.dataframe(df)

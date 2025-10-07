import streamlit as st
import re
from utils.llm_agent import query_llm
from components.auto_visualizer import auto_render_output


# Delay import to avoid circular import issues
def get_conversation_manager():
    from conv_manager import ConversationManager
    return ConversationManager()

conversation_manager = get_conversation_manager()

# Streamlit page config
st.set_page_config(page_title="Autonomous Data Onboarding", page_icon="🤖", layout="wide")

# App Header
st.title("🔄 Autonomous Data Onboarding")
st.markdown("This tool helps you **explore**, **profile**, and **configure data ingestion pipelines** interactively.")

# Sidebar Navigation
st.sidebar.title("Navigation")
app_mode = st.sidebar.radio("Select Mode", ["Introduction", "Explore Data", "Data Analytics", "Configure Ingestion"])

# Initialize Session State
if "conversation_history" not in st.session_state:
    st.session_state.conversation_history = []

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# ----------------------------- #
#        INTRODUCTION
# ----------------------------- #
if app_mode == "Introduction":
    st.subheader("How it works:")
    st.markdown("""
    1. **Explore Data**: Discover available data sources and tables.
    2. **Profile Data**: Analyze the schema, data quality, and sample data.
    3. **Data Analytics**: Run your analytics queries, aggregations and visualize data.
    4. **Configure Ingestion**: Specify target schema and table, and configure the refresh schedule.
    5. **GitHub Integration**: Automatically create a GitHub PR for your ingestion pipeline.
    """)
    st.markdown("Let's get started by exploring data!")

# ----------------------------- #
#        EXPLORE DATA
# ----------------------------- #
elif app_mode == "Explore Data":
    st.subheader("Chat with the Assistant")

    # Chat History
    if st.session_state.conversation_history:
        st.text_area("Conversation", "\n".join(st.session_state.conversation_history), height=300)

    # Capture user input
    if "explore_step" not in st.session_state:
        st.session_state.explore_step = "awaiting_query"

    if st.session_state.explore_step == "awaiting_query":
        user_input = st.text_input("Type your query to explore data:", key="explore_user_input")

        if user_input:
            st.session_state.conversation_history.append(f"**You:** {user_input}")
            assistant_response = conversation_manager.user_message(user_input)
            st.session_state.conversation_history.append(f"**Assistant:** {assistant_response}")

            # Extract keyword (e.g., transactions, sales)
            keywords = ["transactions", "sales", "inventory", "marketing"]
            found_keyword = next((k for k in keywords if k in user_input.lower()), None)

            if found_keyword:
                st.session_state.explore_keyword = found_keyword
                st.session_state.explore_step = "select_source"
            else:
                st.warning("Please mention a valid domain like 'transactions', 'sales', etc.")

    # Step 2: Select source from matching tables
    if st.session_state.explore_step == "select_source":
        st.markdown(f"### Showing sources for domain: **{st.session_state.explore_keyword}**")
        st.session_state.filtered_sources = conversation_manager.run_action(
            "discover_sources_full",
            filter_func=lambda t: st.session_state.explore_keyword in t.lower()
        )

        if not st.session_state.filtered_sources:
            st.error("No matching sources found.")
        else:
            source_names = sorted(set(src["source_name"] for src in st.session_state.filtered_sources))
            st.session_state.selected_source = st.selectbox("Select a data source:", source_names)

            if st.session_state.selected_source:
                st.session_state.explore_step = "select_schema"

    # Step 3: Select schema
    if st.session_state.explore_step == "select_schema":
        matching_schemas = sorted(set(
            src["schema"] for src in st.session_state.filtered_sources
            if src["source_name"] == st.session_state.selected_source
        ))
        st.session_state.selected_schema = st.selectbox("Select a schema:", matching_schemas)

        if st.session_state.selected_schema:
            st.session_state.explore_step = "select_table"

    # Step 4: Select table
    if st.session_state.explore_step == "select_table":
        matching_tables = sorted([
            src["table"] for src in st.session_state.filtered_sources
            if src["source_name"] == st.session_state.selected_source and
               src["schema"] == st.session_state.selected_schema
        ])
        st.session_state.selected_table = st.selectbox("Select a table:", matching_tables)

        if st.session_state.selected_table:
            st.session_state.explore_step = "profiling"

    # Step 5: Show profiling
    if st.session_state.explore_step == "profiling":
        st.markdown(f"**Profiling:** `{st.session_state.selected_source}.{st.session_state.selected_schema}.{st.session_state.selected_table}`")

        with st.spinner("Profiling data, please wait..."):
            profiling_result, html_report_path = conversation_manager.run_action(
                "profile_table",
                source=st.session_state.selected_source,
                schema=st.session_state.selected_schema,
                table=st.session_state.selected_table,
            )

        profiling_html = profiling_result.get("profile_report_html")
        if profiling_html:
            st.components.v1.html(profiling_html, height=1400, scrolling=True)
        else:
            st.error("No profiling report available.")

    # Reset Button
    if st.button("🔁 Reset Exploration"):
        for key in [
            "explore_step", "explore_keyword", "explore_user_input",
            "filtered_sources", "selected_source", "selected_schema", "selected_table"
        ]:
            st.session_state.pop(key, None)
        st.success("Exploration reset. Start by entering a new query.")


# ----------------------------- #
#        DATA ANALYTICS
# ----------------------------- #
elif app_mode == "Data Analytics":
    st.subheader("Do you have any analytics queries?")

    user_input = st.chat_input("How can I help you today with data insights...")

    if user_input:
        st.session_state.chat_history.append({"role": "user", "text": user_input})

        with st.spinner("DataMate is thinking..."):
            llm_response = query_llm(user_input)
            st.session_state.chat_history.append({"role": "assistant", "text": llm_response})

            sql_match = re.search(r"(SELECT[\s\S]+?;)", llm_response, re.IGNORECASE)
            if sql_match:
                sql_query = sql_match.group(1).strip()
                st.markdown(f"**Generated SQL:** `{sql_query}`")
                auto_render_output(sql_query)
            else:
                st.error("⚠️ LLM did not generate a valid SQL query.")
                st.markdown("### Raw LLM Output:")
                st.code(llm_response)

# ----------------------------- #
#      CONFIGURE INGESTION
# ----------------------------- #
elif app_mode == "Configure Ingestion":
    st.subheader("Configure Your Ingestion Pipeline")

    all_sources = conversation_manager.run_action("discover_sources_full")
    unique_sources = sorted(set([src["source_name"] for src in all_sources]))
    selected_source = st.selectbox("Select source:", unique_sources)

    if selected_source:
        schemas = sorted(set([src["schema"] for src in all_sources if src["source_name"] == selected_source]))
        selected_schema = st.selectbox("Select source schema:", schemas)

        if selected_schema:
            tables = sorted(
                [src["table"] for src in all_sources if src["source_name"] == selected_source and src["schema"] == selected_schema]
            )
            selected_table = st.selectbox("Select source table:", tables)

            st.markdown("---")
            domain = st.text_input("Enter Domain for Ingestion", value="transactions")
            target_schema = st.text_input(f"Enter Target Schema for {domain}", value="analytics")
            target_table = st.text_input(f"Enter Target Table for {domain}", value="curated_table")

            refresh_schedule = st.selectbox("Select Refresh Schedule", ["one-time", "daily", "monthly", "quarterly"])
            load_strategy = st.selectbox("Select Load Strategy", ["overwrite", "incremental"])

            st.markdown("### Ingestion Configuration Summary")
            st.write(f"- Source: `{selected_source}`")
            st.write(f"- Source Schema: `{selected_schema}`")
            st.write(f"- Source Table: `{selected_table}`")
            st.write(f"- Domain: {domain}")
            st.write(f"- Target Schema: {target_schema}")
            st.write(f"- Target Table: {target_table}")
            st.write(f"- Refresh Schedule: {refresh_schedule}")
            st.write(f"- Load Strategy: {load_strategy}")

            if st.button("Create Ingestion YAML and PR"):
                with st.spinner("Generating ingestion YAML and creating GitHub PR..."):
                    ingestion_yaml = conversation_manager.run_action(
                        "build_ingest_yaml",
                        source_name=selected_source,
                        source_schema=selected_schema,
                        source_table=selected_table,
                        target_schema=target_schema,
                        target_table=target_table,
                        refresh=refresh_schedule,
                        load_strategy=load_strategy,
                        domain=domain,
                    )

                    pr_title = f"Ingestion Pipeline for {domain} Data"
                    pr_body = f"Automated PR for ingesting {domain} data into {target_schema}.{target_table}"
                    pr_branch = f"ingestion/{domain}/{target_schema}/{target_table}"

                    pr_response = conversation_manager.run_action(
                        "create_github_pr",
                        repo_full_name="mkviswanadh/data_source_onboarding_orchestrator",
                        pr_branch=pr_branch,
                        yaml_content=ingestion_yaml,
                        pr_title=pr_title,
                        pr_body=pr_body,
                    )

                    if pr_response:
                        st.success(f"✅ GitHub PR created successfully! [View PR]({pr_response.html_url})")
                    else:
                        st.error("❌ Failed to create GitHub PR.")

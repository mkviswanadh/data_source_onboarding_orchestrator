import streamlit as st
from conversation_manager import ConversationManager
import time
import shutil
import re
from utils.llm_agent import query_llm
from components.auto_visualizer import auto_render_output


# Initialize the Conversation Manager
conversation_manager = ConversationManager()

# Set page title and icon
st.set_page_config(page_title="Autonomous Data Onboarding", page_icon=":robot:")

# Streamlit App Title and Header
st.title("🔄 Autonomous Data Onboarding")
st.markdown("This tool helps you **explore**, **profile**, and **configure data ingestion pipelines** interactively.")

# Side bar for navigation
st.sidebar.title("Navigation")
app_mode = st.sidebar.radio("Select Mode", ["Introduction", "Explore Data", "Data Analytics", "Configure Ingestion"])

# Initialize or continue conversation in session state
if "conversation_history" not in st.session_state:
    st.session_state["conversation_history"] = []

if app_mode == "Introduction":
    st.subheader("How it works:")
    st.markdown("""
    1. **Explore Data**: Discover available data sources and tables.
    2. **Profile Data**: Analyze the schema, data quality, and sample data.
    3. **Configure Ingestion**: Specify target schema and table, and configure the refresh schedule.
    4. **GitHub Integration**: Automatically create a GitHub PR for your ingestion pipeline.
    """)
    st.markdown("Let's get started by exploring data!")

elif app_mode == "Explore Data":
    # Chatbot Interface
    st.subheader("Chat with the Assistant")

    user_input = st.text_input("Type your query here:", key="user_input", placeholder="Ask me about the data...")

    if user_input:
        assistant_response = conversation_manager.user_message(user_input)
        st.session_state.conversation_history.append(f"**You:** {user_input}")
        st.session_state.conversation_history.append(f"**Assistant:** {assistant_response}")
        st.text_area("Conversation", "\n".join(st.session_state.conversation_history), height=300)

        # Step-by-step process for data exploration
        if "explore" in user_input.lower():
            domain = st.text_input("Please specify the domain of data you want to explore (e.g., transactions, sales, etc.):")
            if domain:
                st.write(f"Exploring data for domain: {domain}")
                sources = conversation_manager.run_action("discover_sources", filter_func=lambda table: domain in table)
                st.write(f"Available sources for {domain}: {', '.join(sources)}")
                selected_source = st.selectbox(f"Select a source schema for {domain}", sources)
                
                if selected_source:
                    st.write(f"You selected: {selected_source}")
                    table = st.text_input(f"Please specify the table name for {domain}:")
                    if table:
                        st.spinner("Profiling data, please wait...")
                        profiling_result,html_report_path = conversation_manager.run_action(
                            "profile_table", 
                            source=selected_source, 
                            schema="ai_tdv_finacle", 
                            table=table
                        )
                        st.write("Profiling Result:")
                        # Access the HTML content from the profiling result
                        profiling_html = profiling_result.get("profile_report_html")

                        # If the HTML content exists, render it in Streamlit
                        if profiling_html:
                            st.components.v1.html(profiling_html, height=1200, width=1200)
                        else:
                            st.write("No profiling report available.")
            else:
                st.error("Please specify a valid domain.")

elif app_mode == "Data Analytics":
    st.subheader("Do you have any analytics queries?")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    user_input = st.chat_input("How can I help you today with data insights...")
    
    if user_input:
        st.session_state.chat_history.append({"role": "user", "text": user_input})
        
        with st.spinner("DataMate is thinking..."):
            llm_response = query_llm(user_input)
            st.session_state.chat_history.append({"role": "assistant", "text": llm_response})
            # Try to extract SQL from the response
            sql_match = re.search(r"(SELECT[\s\S]+?;)", llm_response, re.IGNORECASE)

            if sql_match:
                sql_query = sql_match.group(1).strip()
                st.markdown(f"**Generated SQL:** `{sql_query}`")
                auto_render_output(sql_query)
            else:
                st.error("⚠️ LLM did not generate a valid SQL query.")
                st.markdown("### Raw LLM Output:")
                st.code(llm_response)

elif app_mode == "Configure Ingestion":
    st.subheader("Configure Your Ingestion Pipeline")

    # Check if the source and schema were selected in the Explore Data mode
    if "selected_source_name" in st.session_state and "selected_schema_name" in st.session_state:
        source_name = st.session_state.selected_source_name
        schema_name = st.session_state.selected_schema_name

        st.write(f"Selected Source: {source_name}")
        st.write(f"Selected Schema: {schema_name}")

    # Allow the user to select and configure ingestion options
    domain = st.selectbox("Select Domain for Ingestion", ["transactions", "sales", "inventory", "marketing"])
    target_schema = st.text_input(f"Enter Target Schema for {domain}", "analytics")
    target_table = st.text_input(f"Enter Target Table for {domain}", "curated_table")

    refresh_schedule = st.selectbox("Select Refresh Schedule", ["one-time", "daily", "monthly", "quarterly"])
    load_strategy = st.selectbox("Select Load Strategy", ["overwrite", "incremental"])

    # Display the summary of the configurations selected
    st.write(f"**Ingestion Configuration Summary**")
    st.write(f"- Domain: {domain}")
    st.write(f"- Target Schema: {target_schema}")
    st.write(f"- Target Table: {target_table}")
    st.write(f"- Refresh Schedule: {refresh_schedule}")
    st.write(f"- Load Strategy: {load_strategy}")

    if st.button("Create Ingestion YAML and PR"):
        with st.spinner("Generating ingestion YAML and creating GitHub PR..."):
            ingestion_yaml = conversation_manager.run_action(
                "build_ingest_yaml",
                source_name=domain,
                source_schema="source_schema",
                source_table="source_table",
                target_schema=target_schema,
                target_table=target_table,
                refresh=refresh_schedule,
                load_strategy=load_strategy,
                domain=domain
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
                pr_body=pr_body
            )

            if pr_response:
                st.success(f"GitHub PR created successfully! [View PR]({pr_response.html_url})")
            else:
                st.error("Failed to create GitHub PR.")

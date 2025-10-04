# data_source_onboarding_orchestrator
 Build an agentic AI-powered platform that autonomously connects to multiple data sources(SQL/NoSQL), samples and profiles the data, suggests data quality checks and enables users to explore the data with natural language queries. 

# Data Onboarding Prototype with Llama / Model Context Protocol

This is a prototype for an autonomous data onboarding pipeline using a conversational interface (Streamlit + Llama3.8 or equivalent).  
It supports Discovery, Exploration, Ingestion orchestration via GitHub + Airflow.

## How to Run

1. Create a Python virtual environment and install dependencies:

   ```bash
   python3 -m venv data_onboard_venv
   data_onboard_venv/Scripts/activate
   pip install -r requirements.txt

2. Configure your credentials:

    . In llama_client.py, set your LLM API key or endpoint

    . In data_ops/discovery.py, update DB_CONFIGS to point to real source databases

    . In github_integration/pr_creator.py, set your GitHub token or credentials

    . In .github/workflows/ingestion.yml, ensure secrets like GITHUB_TOKEN are defined in your repo settings

    . You will also need to configure Airflow — connection IDs, API endpoints, and where DAGs are deployed

3. Run the Streamlit chatbot:
   streamlit run app.py

4. Interact with the bot to discover, explore, and ingest data.

5. Review GitHub PRs, merge, and watch DAGs being generated and triggered by GitHub Actions.


### Steps to Turn Into a GitHub Repo & Deploy

1. Create a new GitHub repository (e.g. data-onboarding-prototype)

2. Copy the above folder structure and files into your local copy

3. Push to GitHub

4. In your GitHub repo settings, add secrets:

    GITHUB_TOKEN

    (Optional) Airflow API credentials

5. Configure your Airflow environment:

    a) Set up the dags folder path that matches .github/workflows/ingestion.yml (line /usr/local/airflow/dags in example)
    b) Ensure Airflow’s REST API is enabled and accessible
    c) Ensure required Airflow connection IDs exist for source/target systems

6. Run the Streamlit app (streamlit run app.py)

7. Use the chatbot to formulate an ingestion PR

8. Merge the PR in GitHub → triggers workflow → DAG generation → DAG trigger → audit log
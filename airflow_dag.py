import os
import sys
from jinja2 import Environment, FileSystemLoader
import yaml

def parse_yaml(yaml_path):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)

def convert_schedule(schedule_str):
    # Map the friendly names to cron expressions
    schedule_map = {
        "one-time": "None",
        "daily": "'@daily'",
        "monthly": "'@monthly'",
        "quarterly": "'0 0 1 */3 *'"
    }
    return schedule_map.get(schedule_str, "'@daily'")

def main(yaml_path: str, output_dag_path: str, airflow_dags_folder: str):
    cfg = parse_yaml(yaml_path)
    ingest = cfg["ingestion"]

    dag_id = f"ingest__{ingest['source']['name']}__{ingest['target']['table']}"
    schedule = convert_schedule(ingest["refresh_schedule"])

    params = {
        "dag_id": dag_id,
        "schedule": schedule,
        "source_conn": f"{{{{ conn_{ingest['source']['name']} }}}}",
        "target_conn": f"{{{{ conn_{ingest['source']['name']} }}}}",  # Default fixed target Airflow connection
        "source_schema": ingest["source"]["schema"],
        "source_table": ingest["source"]["table"],
        "target_schema": ingest["target"]["schema"],
        "target_table": ingest["target"]["table"],
    }

    env = Environment(loader=FileSystemLoader("airflow_templates"))
    template = env.get_template("ingest_template.py.j2")
    rendered = template.render(**params)

    with open(output_dag_path, "w") as f:
        f.write(rendered)

    os.system(f"cp {output_dag_path} {airflow_dags_folder}")
    print(f"✅ DAG generated and copied: {dag_id}")

    return dag_id

if __name__ == "__main__":
    yaml_path = sys.argv[1]
    output_dag_path = sys.argv[2]
    airflow_dags_folder = sys.argv[3]
    main(yaml_path, output_dag_path, airflow_dags_folder)

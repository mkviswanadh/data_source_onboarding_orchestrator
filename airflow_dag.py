import os
import sys
from jinja2 import Environment, FileSystemLoader
import yaml

def parse_yaml(yaml_path):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)

def main(yaml_path: str, output_dag_path: str, airflow_dags_folder: str):
    cfg = parse_yaml(yaml_path)
    ingest = cfg["ingestion"]

    dag_id = f"ingest__{ingest['source']['name']}__{ingest['target']['table']}"
    schedule = ingest["refresh_schedule"]

    # Jinja template variables
    params = {
        "dag_id": dag_id,
        "schedule": schedule,
        "source_conn": f"{{{{ conn_{ingest['source']['name']} }}}}",  # Airflow connection ID
        "target_conn": f"{{{{ conn_{ingest['target']['name']} }}}}",
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
    print(f"Generated DAG: {dag_id}")

    return dag_id

if __name__ == "__main__":
    yaml_path = sys.argv[1]
    output_dag_path = sys.argv[2]
    airflow_dags_folder = sys.argv[3]
    main(yaml_path, output_dag_path, airflow_dags_folder)

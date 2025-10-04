import yaml
from datetime import datetime

def build_ingestion_yaml(source_name: str, source_schema: str, source_table: str,
                         target_schema: str, target_table: str,
                         domain: str, description: str,
                         refresh: str, load_strategy: str) -> str:
    config = {
        "ingestion": {
            "source": {
                "name": source_name,
                "schema": source_schema,
                "table": source_table
            },
            "target": {
                "schema": target_schema,
                "table": target_table
            },
            "domain": domain,
            "description": description,
            "refresh_schedule": refresh,
            "load_strategy": load_strategy,
            "created_at": datetime.utcnow().isoformat()
        }
    }
    return yaml.dump(config, sort_keys=False)

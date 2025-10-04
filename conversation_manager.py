from llama_client import LlamaClient
from data_ops.discovery import discover_sources, check_table_in_sources
from data_ops.profiling import profile_table
from data_ops.ingestion import build_ingestion_yaml

class ConversationManager:
    def __init__(self):
        self.llama = LlamaClient()
        self.context = []   # conversation history
        self.state = {}     # state: selected source, schema, etc.

    def user_message(self, text: str) -> str:
        # Append to the conversation context
        self.context.append({"role": "user", "content": text})
        # Get response from the LLM
        resp = self.llama.chat(self.context)
        assistant_text = resp["text"]
        self.context.append({"role": "assistant", "content": assistant_text})
        # Optionally you can parse for tags or structured commands
        return assistant_text

    def run_action(self, action: str, **kwargs):
        if action == "discover_sources":
            return discover_sources()
        elif action == "check_table":
            return check_table_in_sources(kwargs["table"])
        elif action == "profile_table":
            return profile_table(
                kwargs["source"], kwargs["schema"], kwargs["table"]
            )
        elif action == "build_ingest_yaml":
            return build_ingestion_yaml(
                source_name=kwargs["source_name"],
                source_schema=kwargs["source_schema"],
                source_table=kwargs["source_table"],
                target_schema=kwargs["target_schema"],
                target_table=kwargs["target_table"],
                domain=kwargs.get("domain", ""),
                description=kwargs.get("description", ""),
                refresh=kwargs.get("refresh", ""),
                load_strategy=kwargs.get("load_strategy", "incremental")
            )
        else:
            raise ValueError("Unknown action: " + action)

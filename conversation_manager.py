from llama_client import LlamaClient
from data_ops.discovery import discover_sources, check_table_in_sources
from data_ops.profiling import profile_table
from data_ops.ingestion import build_ingestion_yaml
from github_integration.pr_creator import create_ingestion_pr

class ConversationManager:
    def __init__(self):
        self.llama = LlamaClient()  # Llama LLM client
        self.context = []  # Conversation history
        self.state = {}  # Track state (e.g., selected source, schema, etc.)

    def user_message(self, text: str) -> str:
        """Handles the user message and fetches response from the LLM."""
        self.context.append({"role": "user", "content": text})
        response = self.llama.chat(self.context)
        assistant_text = response.get("text", "")
        self.context.append({"role": "assistant", "content": assistant_text})
        return assistant_text

    def run_action(self, action: str, **kwargs):
        """Runs the specified action based on the user input."""
        if action == "discover_sources":
            sources = discover_sources()
            self.state['sources'] = sources  # Store sources in state
            return sources
        elif action == "check_table":
            return check_table_in_sources(kwargs["table"])
        elif action == "profile_table":
            profiling_result = profile_table(
                kwargs["source"], kwargs["schema"], kwargs["table"]
            )
            self.state['profiling_result'] = profiling_result  # Store profiling result
            return profiling_result
        elif action == "build_ingest_yaml":
            ingestion_yaml = build_ingestion_yaml(
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
            self.state['ingestion_yaml'] = ingestion_yaml  # Store YAML for later use
            return ingestion_yaml
        elif action == "create_github_pr":
            pr_response = create_ingestion_pr(
                repo_full_name=kwargs["repo_full_name"],
                pr_branch=kwargs["pr_branch"],
                yaml_content=kwargs["yaml_content"],
                pr_title=kwargs["pr_title"],
                pr_body=kwargs["pr_body"]
            )
            self.state['pr_url'] = pr_response.html_url  # Store PR URL for tracking
            return pr_response
        else:
            raise ValueError(f"Unknown action: {action}")

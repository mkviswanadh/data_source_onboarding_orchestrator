from sqlalchemy import create_engine
from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.agent_toolkits.sql.base import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from langchain_community.llms import Ollama
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from utils.db import get_engine


def get_llm_agent():
    engine = get_engine()
    db = SQLDatabase(engine)

    llm = Ollama(model="llama3:8b")  # Make sure Ollama is running
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    
    agent_executor = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        verbose=True
    )
    return agent_executor

from sqlalchemy import create_engine
from langchain_community.llms import Ollama
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain_community.utilities import SQLDatabase
from utils.db import get_engine

def get_sql_chain():
    engine = get_engine()
    db = SQLDatabase(engine, include_tables=["daily_transactions"])
    table_info = db.get_table_info()

    # Enhanced LLM prompt with more guidance
    prompt = PromptTemplate(
        input_variables=["question"],
        template="""
                    You are a senior data analyst with read-only access to a SQL database.
                    Use the schema and metadata below to answer the user's data question accurately.

                    ONLY use available tables and columns. DO NOT fabricate any table or column names.

                    If unsure, ask the user to clarify instead of guessing.

                    Schema Info:
                    -------------
                    {table_info}

                    Instructions:
                    - Do not explain the query.
                    - Only return a valid SQL query (no markdown).
                    - Return aggregations (SUM, AVG, COUNT, etc.) or filters (WHERE, GROUP BY) as needed.
                    - Do not include extra text or commentary.

                    User's Question:
                    {question}

                    SQL Query:
                """,
            )

    llm = Ollama(model="llama3:8b")
    chain = LLMChain(
        llm=llm,
        prompt=prompt.partial(table_info=table_info),
        verbose=True
    )

    return chain

from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain_community.llms import Ollama

def classify_intent(question: str) -> str:
    """Classify the user's intent using the LLM: schema_lookup vs analytics_query."""
    llm = Ollama(model="llama3:8b")
    
    prompt = PromptTemplate(
        input_variables=["question"],
        template="""
                    You are a smart assistant that classifies user questions about a SQL database.

                    Possible intents:
                    - schema_lookup: Asking about tables, columns, schema structure.
                    - analytics_query: Asking for insights, aggregations, filters, comparisons.
                    - unknown: Question doesn't match any intent or not related to SQL.

                    Classify this question: "{question}"

                    Respond with only one word: schema_lookup, analytics_query, or unknown.
                """
    )

    chain = LLMChain(llm=llm, prompt=prompt)
    result = chain.run(question).strip().lower()
    return result


def query_llm(question: str):
    agent = get_sql_chain()
    return agent.run(question).strip()


# For testing
if __name__ == "__main__":
    print(query_llm("What is the total number of transactions?"))

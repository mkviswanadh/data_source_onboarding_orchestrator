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

def get_sql_chain():
    engine = get_engine()

    # Only show the real table
    db = SQLDatabase(engine, include_tables=["daily_transactions"])

    llm = Ollama(model="llama3:8b")

    prompt = PromptTemplate(
        input_variables=["question"],
        template="""
        You are a data analyst with access to the following SQL table schema:

        {table_info}

        Write a SQL query to answer the user's question using ONLY the available tables and columns.
        Do not invent table or column names.Do not explain the query. Do not include markdown. Only output a valid SQL query

        Question: {question}
        SQL:
        """,
    )
    chain = LLMChain(
        llm=llm,
        prompt=prompt.partial(table_info=db.get_table_info()), 
        verbose=True
    )

    return chain

def query_llm(question: str):
    agent = get_sql_chain()
    return agent.run(question).strip()


# For testing
if __name__ == "__main__":
    print(query_llm("What is the total number of transactions?"))

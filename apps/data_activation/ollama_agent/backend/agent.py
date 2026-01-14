
import os
import sys
import argparse
from typing import Optional

# Add project root to sys.path to allow importing from utils
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../.."))
if project_root not in sys.path:
    sys.path.append(project_root)

# Correctly handle imports depending on context
try:
    from langchain_community.llms import Ollama
    from langchain.agents import initialize_agent, Tool, AgentType
    from langchain.tools import tool
    from utils.generic_ingestor import ingest_file
    from utils.dbt_runner import run_dbt_command
    from langchain_community.utilities import SQLDatabase
    from langchain_community.agent_toolkits import create_sql_agent
except ImportError:
    # Fallback to absolute imports if running from root
    from langchain_community.llms import Ollama
    from langchain.agents import initialize_agent, Tool, AgentType
    from langchain.tools import tool
    from jimwurst.utils.generic_ingestor import ingest_file
    from jimwurst.utils.dbt_runner import run_dbt_command
    from langchain_community.utilities import SQLDatabase
    from langchain_community.agent_toolkits import create_sql_agent

@tool
def ingest_data_tool(file_path: str):
    """
    Ingests a CSV file into the database. 
    Input should be the full absolute path to the CSV file.
    """
    return ingest_file(file_path)

@tool
def run_transformations_tool(command: str = "build"):
    """
    Runs dbt transformations to process data. 
    Input can be 'run' or 'build'. Default is 'build'.
    """
    return run_dbt_command(command)

class JimwurstAgent:
    def __init__(self, model_name: str = "qwen2.5:3b"):
        self.model_name = model_name
        self.llm = Ollama(model=model_name)
        self.agent = self._setup_agent()

    def check_ollama_connection(self):
        """Checks if Ollama is reachable."""
        try:
            import requests
            response = requests.get("http://localhost:11434")
            if response.status_code == 200:
                return True
        except Exception:
            pass
        return False

    def _get_sql_agent(self):
        """Creates an SQL agent for querying the database."""
        from utils.ingestion_utils import load_env
        load_env()
        
        DB_HOST = os.getenv("DB_HOST", "localhost")
        DB_NAME = os.getenv("POSTGRES_DB", "jimwurst")
        DB_USER = os.getenv("POSTGRES_USER", "jimwurst")
        DB_PASS = os.getenv("POSTGRES_PASSWORD", "jimwurst")
        DB_PORT = os.getenv("DB_PORT", "5432")
        
        db_uri = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        db = SQLDatabase.from_uri(db_uri)
        
        # Custom prefix to teach the agent about the data warehouse schema structure
        sql_prefix = """You are an agent designed to interact with a SQL database.
Given an input question, create a syntactically correct PostgreSQL query to run, then look at the results of the query and return the answer.

IMPORTANT - DATA WAREHOUSE SCHEMA STRUCTURE:
The database follows a layered data warehouse architecture:

1. INGESTED DATA (Raw Sources):
   - Located in schemas that start with 's_' (e.g., s_substack, s_linkedin, s_telegram, s_bolt, etc.)
   - These contain raw data ingested from various applications
   - To find all ingested data sources, use:
     SELECT DISTINCT table_schema, table_name 
     FROM information_schema.tables 
     WHERE table_schema LIKE 's_%' 
     ORDER BY table_schema, table_name;

2. CURATED & TRANSFORMED DATA:
   - Located in the 'intermediate' schema
   - Contains data that has been cleaned and transformed
   - To find curated tables, use:
     SELECT table_name FROM information_schema.tables WHERE table_schema = 'intermediate';

3. INSIGHT-READY DATA (Analytics):
   - Located in the 'marts' schema
   - Contains final analytical models ready for reporting and insights
   - To find insight-ready tables, use:
     SELECT table_name FROM information_schema.tables WHERE table_schema = 'marts';

When asked about:
- "What data is being ingested" → Query schemas starting with 's_'
- "What data is curated/transformed" → Query the 'intermediate' schema
- "What data is ready for insights" → Query the 'marts' schema

Always use information_schema to discover tables and schemas. Never assume table names exist without checking first.

Unless the user specifies a specific number of examples they wish to obtain, always limit your query to at most 5 results.
You can order the results by a relevant column to return the most interesting examples in the database.
Never query for all the columns from a specific table, only ask for the relevant columns given the question.
You have access to tools for interacting with the database.
Only use the given tools. Only use the information returned by the tools to construct your final answer.
You MUST double check your query before executing it. If you get an error while executing a query, rewrite the query and try again.

DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.

If the question does not seem related to the database, just return "I don't know" as the answer.
"""
        
        return create_sql_agent(
            llm=self.llm,
            toolkit=None,
            db=db,
            verbose=True,
            agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
            handle_parsing_errors=True,
            prefix=sql_prefix,
        )

    def _setup_agent(self):
        tools = [
            ingest_data_tool,
            run_transformations_tool
        ]
        
        sql_agent_executor = self._get_sql_agent()
        
        @tool
        def query_database_tool(query: str):
            """
            Useful for when you need to answer questions about data in the database. 
            Input should be a natural language question.
            The tool will convert it to SQL, execute it, and return the answer.
            """
            return sql_agent_executor.invoke({"input": query})

        tools.append(query_database_tool)

        return initialize_agent(
            tools, 
            self.llm, 
            agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, 
            verbose=True,
            handle_parsing_errors=True
        )

    def chat(self, prompt: str) -> str:
        """Sends a message to the agent and returns the response."""
        try:
            response = self.agent.invoke({"input": prompt})
            return response['output']
        except Exception as e:
            return f"Error: {e}"

def main():
    parser = argparse.ArgumentParser(description="Jimwurst AI Client")
    parser.add_argument("--prompt", type=str, help="The prompt to send to the AI")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    parser.add_argument("--model", type=str, default="qwen2.5:3b", help="Ollama model to use")
    
    args = parser.parse_args()
    
    agent = JimwurstAgent(model_name=args.model)

    if not agent.check_ollama_connection():
        print("\n\033[91mError: Could not connect to Ollama.\033[0m")
        print("Please ensure Ollama is running by executing: \033[1mollama serve\033[0m")
        print("Keep that terminal open and run this agent in a new terminal.\n")
        return

    if args.interactive:
        print(f"Jimwurst AI Client ({args.model}) (Type 'exit' to quit)")
        while True:
            user_input = input(">> ")
            if user_input.lower() in ["exit", "quit"]:
                break
            print(agent.chat(user_input))
    
    elif args.prompt:
        print(agent.chat(args.prompt))
    else:
        print("Please provide a prompt using --prompt or run with --interactive")

if __name__ == "__main__":
    main()

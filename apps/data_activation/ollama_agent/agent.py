
import os
import sys
import argparse

# Add project root to sys.path to allow importing from utils
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../.."))
if project_root not in sys.path:
    sys.path.append(project_root)

from langchain_community.llms import Ollama
from langchain.agents import initialize_agent, Tool, AgentType
from langchain.tools import tool
from utils.generic_ingestor import ingest_file
from utils.dbt_runner import run_dbt_command
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent

# Initialize LLM
llm = Ollama(model="qwen2.5:3b") # Default to qwen2.5:3b, user can change

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

def get_sql_agent(llm):
    """Creates an SQL agent for querying the database."""
    from utils.ingestion_utils import load_env
    load_env()
    
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_NAME = os.getenv("POSTGRES_DB", "jimwurst_db")
    DB_USER = os.getenv("POSTGRES_USER", "jimwurst_user")
    DB_PASS = os.getenv("POSTGRES_PASSWORD", "jimwurst_password")
    DB_PORT = os.getenv("DB_PORT", "5432")
    
    db_uri = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    db = SQLDatabase.from_uri(db_uri)
    
    return create_sql_agent(
        llm=llm,
        toolkit=None, # SQLDatabaseToolkit is created automatically bycreate_sql_agent if db is passed but toolkit is cleaner
        db=db,
        verbose=True,
        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    )


def check_ollama_connection():
    """Checks if Ollama is reachable."""
    try:
        # Simple test generation to check connection
        # Using a direct request to avoid LangChain wrapping which hides the root cause sometimes
        import requests
        response = requests.get("http://localhost:11434")
        if response.status_code == 200:
            return True
    except Exception:
        pass
    return False

def main():
    parser = argparse.ArgumentParser(description="Jimwurst AI Client")
    parser.add_argument("--prompt", type=str, help="The prompt to send to the AI")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    
    args = parser.parse_args()
    
    # Check Ollama Connection
    if not check_ollama_connection():
        print("\n\033[91mError: Could not connect to Ollama.\033[0m")
        print("Please ensure Ollama is running by executing: \033[1mollama serve\033[0m")
        print("Keep that terminal open and run this agent in a new terminal.\n")
        return

    # Define Tools
    tools = [
        ingest_data_tool,
        run_transformations_tool
    ]
    
    # We can mix SQL agent skills into the main agent or have the main agent delegate.
    # For simplicity, let's give the main agent a tool to "ask database".
    
    sql_agent_executor = get_sql_agent(llm)
    
    @tool
    def query_database_tool(query: str):
        """
        Useful for when you need to answer questions about data in the database. 
        Input should be a natural language question.
        The tool will convert it to SQL, execute it, and return the answer.
        """
        return sql_agent_executor.invoke({"input": query})

    tools.append(query_database_tool)

    agent = initialize_agent(
        tools, 
        llm, 
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, 
        verbose=True,
        handle_parsing_errors=True
    )

    if args.interactive:
        print("Jimwurst AI Client (Type 'exit' to quit)")
        while True:
            user_input = input(">> ")
            if user_input.lower() in ["exit", "quit"]:
                break
            try:
                response = agent.invoke({"input": user_input})
                print(response['output'])
            except Exception as e:
                print(f"Error: {e}")
    
    elif args.prompt:
        response = agent.invoke({"input": args.prompt})
        print(response['output'])
    else:
        print("Please provide a prompt using --prompt or run with --interactive")

if __name__ == "__main__":
    main()

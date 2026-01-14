
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

CRITICAL WORKFLOW - READ THIS CAREFULLY:
1. FIRST: Use sql_db_list_tables to see what tables actually exist in the database
2. THEN: Generate and execute queries based on the ACTUAL tables you discovered
3. FINALLY: Return the RESULTS in natural language, NOT the SQL query itself

Your final answer must ALWAYS be the query RESULTS, never SQL code (unless the user explicitly asks to see the query).

DATA WAREHOUSE SCHEMA STRUCTURE:
The database (jimwurst_db) follows a layered data warehouse architecture:

1. INGESTED DATA (Raw Sources):
   - Schemas starting with 's_' (e.g., s_substack, s_linkedin, s_telegram, s_bolt)
   - These contain raw data ingested from various applications

2. CURATED & TRANSFORMED DATA:
   - Schema: 'intermediate'
   - Contains data that has been cleaned and transformed

3. INSIGHT-READY DATA (Analytics):
   - Schema: 'marts'
   - Contains final analytical models ready for reporting and insights

ANSWERING METADATA QUESTIONS:
When asked "What data is being ingested?" or "What data is curated?" or "What data is ready for insights?":

Step 1: Use sql_db_list_tables to discover all tables
Step 2: Filter the results to show:
   - For ingested data: tables in schemas starting with 's_'
   - For curated data: tables in 'intermediate' schema
   - For insights data: tables in 'marts' schema
Step 3: Return a natural language summary like:
   "The following data sources are being ingested: [list of schemas/applications based on s_* schemas]"

IMPORTANT RULES:
- ALWAYS use sql_db_list_tables FIRST before writing any queries
- Use sql_db_schema to understand table structure if needed
- NEVER assume table names - always check what actually exists
- Return RESULTS in natural language, not SQL code
- Limit results to 5 rows unless user asks for more
- Only query relevant columns, not SELECT *
- NO DML statements (INSERT, UPDATE, DELETE, DROP)

If the question is not database-related, return "I don't know".
"""
        
        
        # Custom error handler for parsing errors
        def handle_parsing_error(error) -> str:
            return f"I encountered an issue processing that request. Let me try a simpler approach. Error: {str(error)[:100]}"
        
        return create_sql_agent(
            llm=self.llm,
            toolkit=None,
            db=db,
            verbose=True,
            agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
            handle_parsing_errors=handle_parsing_error,
            prefix=sql_prefix,
            max_iterations=10,
            max_execution_time=60,
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
            try:
                result = sql_agent_executor.invoke({"input": query})
                # Extract the output if it's a dict
                if isinstance(result, dict) and 'output' in result:
                    return result['output']
                return str(result)
            except Exception as e:
                return f"I encountered an error querying the database: {str(e)[:200]}. Please try rephrasing your question."

        tools.append(query_database_tool)

        # Custom error handler for the main agent
        def handle_main_agent_error(error) -> str:
            return f"I had trouble processing that. Let me try again with a simpler approach."

        return initialize_agent(
            tools, 
            self.llm, 
            agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, 
            verbose=True,
            handle_parsing_errors=handle_main_agent_error,
            max_iterations=10,
            max_execution_time=120,
        )

    def chat(self, prompt: str, callbacks=None):
        """Sends a message to the agent and returns the response.
        
        Args:
            prompt: The user's question/prompt
            callbacks: Optional list of LangChain callbacks for streaming output
            
        Returns:
            tuple: (response_text, intermediate_steps) if callbacks provided, else just response_text
        """
        try:
            config = {"callbacks": callbacks} if callbacks else {}
            response = self.agent.invoke({"input": prompt}, config=config)
            
            # Return both output and intermediate steps if available
            if isinstance(response, dict):
                output = response.get('output', str(response))
                intermediate_steps = response.get('intermediate_steps', [])
                return output if not callbacks else (output, intermediate_steps)
            return str(response)
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

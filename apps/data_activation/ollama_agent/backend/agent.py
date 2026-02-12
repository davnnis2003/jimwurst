
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
    Ingests a CSV or Excel file into the database.
    Input MUST be ONLY the full absolute path to the file as stored on the server,
    typically under ~/.jimwurst_data/<filename> when uploaded via the app.

    CRITICAL:
    - Do NOT append any extra text such as encoding notes or explanations.
    - Do NOT wrap the path in quotes or backticks.
    - If you want to mention encoding, do so in natural language outside the tool input.
    """
    # Defensive cleaning in case the model adds extra text
    cleaned = file_path.strip()
    # Strip common accidental wrappers, but only if they match
    if cleaned and cleaned[0] in ("`", '"', "'") and cleaned[-1] == cleaned[0]:
        cleaned = cleaned[1:-1].strip()
    # If the model appended notes like " (using 'utf-8' encoding)", try a safe fallback:
    # only strip that suffix if the original path does NOT exist but the trimmed one DOES.
    if " (using" in cleaned:
        original = cleaned
        candidate = cleaned.split(" (using", 1)[0].strip()
        try:
            from os import path as _path
            if not _path.exists(original) and _path.exists(candidate):
                cleaned = candidate
        except Exception:
            # If anything goes wrong, keep the original string so we fail loudly instead of silently truncating.
            cleaned = original

    # Normalize any accidental /jimwurst_data/ paths to the canonical ~/.jimwurst_data location
    try:
        import os as _os
        home = _os.path.expanduser("~")
        canonical_dir = _os.path.join(home, ".jimwurst_data")

        # Case 1: model invented something like "/Users/jimwurst_data/<file>"
        marker = "/jimwurst_data/"
        if marker in cleaned and ".jimwurst_data" not in cleaned:
            after = cleaned.split(marker, 1)[1]
            cleaned = _os.path.join(canonical_dir, after)

        # Case 2: model used "~/jimwurst_data" instead of "~/.jimwurst_data"
        tilde_marker = "~/jimwurst_data/"
        if cleaned.startswith(tilde_marker):
            after = cleaned[len(tilde_marker):]
            cleaned = _os.path.join(canonical_dir, after)
    except Exception:
        # If normalization fails for any reason, fall back to the cleaned string as-is.
        pass

    return ingest_file(cleaned)

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

    def _get_sql_agent(self, db):
        """Creates an SQL agent for querying the database."""
        # Custom error handler for parsing errors
        def handle_parsing_error(error) -> str:
            error_str = str(error)
            if "SELECT" in error_str or "Action:" in error_str:
                return f"I had a parsing error. I will try to be more concise. Error: {error_str[:50]}"
            return f"I encountered an issue. Let me try a simpler approach. Error: {error_str[:100]}"
        
        # Helper to clean SQL from the agent
        def clean_sql(query: str) -> str:
            query = query.strip()
            # Remove markdown code blocks if present
            if "```" in query:
                import re
                match = re.search(r"```(?:sql)?\s*(.*?)\s*```", query, re.DOTALL | re.IGNORECASE)
                if match:
                    query = match.group(1)
                else:
                    query = query.replace("```sql", "").replace("```", "")
            
            # Remove leading "sql" if the model adds it outside backticks
            if query.lower().startswith("sql"):
                query = query[3:].strip()
                
            return query.strip("` \n\t;")

        # Custom prefix to teach the agent about the data warehouse schema structure
        sql_prefix = """You are an agent designed to interact with a SQL database.
All relevant schemas (marts, s_*) are in your search path.

DATA CATEGORIZATION:
1. RAW DATA: Located in `s_` schemas (e.g., s_substack, s_linkedin). This is the landing zone for the ingestion system.
2. CURATED DATA: Located in the `marts` schema. This is cleaned, modeled data ready for insight generation and analysis.

CRITICAL SCHEMA PRIORITIES:
1. MART SYSTEM: Use the `marts` schema for all analytical questions and insights. This is your primary source of truth.
2. INGESTION SYSTEM: Use the `s_` schemas only when asked for "raw data" or when debugging ingestion.
3. IGNORE: Do NOT use or refer to the `staging` or `intermediate` schemas.

FILE INGESTION MODEL:
1. Uploaded files from the UI are stored on disk under the user's home directory in a dedicated folder: `~/.jimwurst_data/<filename>`.
2. The ingestion tool (`ingest_data_tool`) expects absolute file paths that point to this folder (or other server-accessible locations), NOT arbitrary client-side paths like `~/Downloads/...`.
3. If the user mentions a file path in `Downloads` or any non-`~/.jimwurst_data` location, DO NOT try to verify the file's existence. Instead, explain that files must be uploaded via the app so they are stored in `~/.jimwurst_data`, and then ingested from there.
4. Do NOT infer, assume, or check file existence from the database schemas; ingestion writes raw data into `s_` schemas, while table presence is independent from the original filesystem path.

CRITICAL RULES:
1. When asked "what data is there" or to "check data", you MUST list tables from both `marts` and `s_` schemas. 
2. Explicitly label tables as "Raw Data" or "Curated/Ready for Insights".
3. SOURCE ATTRIBUTION: For "Raw Data", identify the source application based on the schema (e.g., `s_linkedin` is LinkedIn, `s_spotify` is Spotify, `s_substack` is Substack, `s_bolt` is Bolt, `s_apple_health` is Apple Health). Tell the user which app generated the data.
4. HALLUCINATION PREVENTION: If a schema (e.g., `marts`) is empty or `sql_db_list_tables` returns nothing, state clearly that no data is available. DO NOT invent information.
5. RESPONSE FORMAT: Always provide a concise **Executive Summary** followed by **Bullet Points**.
6. NEVER include markdown backticks (```) or the word "sql" in your tool inputs. Only provide raw SQL.
7. PUBLIC SCHEMA IS EMPTY. Do NOT query `information_schema` filtering for `table_schema = 'public'`.

WORKFLOW:
1. Use `sql_db_list_tables` to see available tables.
2. Categorize them into Raw (s_*) and Curated (marts).
3. Tell the user what raw data is available and what curated data is ready for insight generation.
4. Execute queries on `marts` for insights.
"""

        # Custom Toolkit with cleaned query tool
        from langchain_community.agent_toolkits import SQLDatabaseToolkit
        from langchain.tools import Tool as LangChainTool
        
        toolkit = SQLDatabaseToolkit(db=db, llm=self.llm)
        original_tools = toolkit.get_tools()
        cleaned_tools = []
        
        for t in original_tools:
            if t.name == "sql_db_query":
                # Create a wrapper that cleans the SQL before calling the original tool
                def wrapped_query(query: str, tool=t):
                    cleaned = clean_sql(query)
                    return tool.run(cleaned)
                
                # Create a new Tool with the same metadata but our wrapped function
                new_tool = LangChainTool(
                    name=t.name,
                    description=t.description,
                    func=wrapped_query
                )
                cleaned_tools.append(new_tool)
            else:
                cleaned_tools.append(t)

        return create_sql_agent(
            llm=self.llm,
            toolkit=None,  # Pass tools directly
            tools=cleaned_tools,
            db=db,
            verbose=True,
            agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
            handle_parsing_errors=handle_parsing_error,
            prefix=sql_prefix,
            max_iterations=10,
            max_execution_time=60,
        )

    def _setup_agent(self):
        from utils.ingestion_utils import load_env
        load_env()
        
        DB_HOST = os.getenv("DB_HOST", "localhost")
        DB_NAME = os.getenv("POSTGRES_DB", "jimwurst")
        DB_USER = os.getenv("POSTGRES_USER", "jimwurst")
        DB_PASS = os.getenv("POSTGRES_PASSWORD", "jimwurst")
        DB_PORT = os.getenv("DB_PORT", "5432")
        
        # Define the schemas we want to include in our search path (Focusing only on what matters)
        schemas = "public,marts,s_spotify,s_linkedin,s_substack,s_telegram,s_bolt,s_apple_health,s_google_sheet"
        
        # Update URI to include search_path
        db_uri = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?options=-csearch_path%3D{schemas}"
        db = SQLDatabase.from_uri(db_uri)

        sql_agent_executor = self._get_sql_agent(db)

        @tool
        def query_database_tool(query_description: str):
            """Useful for when you need to answer questions about data in the Data Warehouse. 
            Input should be a natural language question about the data."""
            try:
                result = sql_agent_executor.invoke({"input": query_description})
                if isinstance(result, dict):
                    return result.get("output", str(result))
                return str(result)
            except Exception as e:
                return f"Error querying database: {str(e)}"

        tools = [
            ingest_data_tool,
            run_transformations_tool,
            query_database_tool
        ]

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

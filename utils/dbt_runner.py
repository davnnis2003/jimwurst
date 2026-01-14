
import subprocess
import os

def run_dbt_command(command: str = "build") -> str:
    """
    Runs a dbt command in the data_transformation app.
    Default command is 'build'.
    """
    dbt_dir = os.path.join(os.path.dirname(__file__), '../apps/data_transformation/dbt')
    
    try:
        # Ensure we capture stdout and stderr
        result = subprocess.run(
            ["dbt", command],
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            check=False # Don't raise exception on non-zero exit, just return output
        )
        
        output = result.stdout
        if result.stderr:
            output += "\nErrors:\n" + result.stderr
            
        return output

    except Exception as e:
        return f"Error running dbt: {str(e)}"

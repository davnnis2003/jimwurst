from typing import Any, Dict, List, Optional
from langchain.callbacks.base import BaseCallbackHandler
import streamlit as st

class StreamlitThinkingCallback(BaseCallbackHandler):
    """Callback Handler that prints to a Streamlit container."""
    
    def __init__(self, container):
        """Initialize the callback handler."""
        self.container = container
        self.text = ""
        self.placeholder = container.empty()

    def on_llm_start(self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any) -> None:
        """Run when LLM starts running."""
        # self.text += f"**Thinking...**\n\n"
        # self.placeholder.markdown(self.text)
        pass

    def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs: Any) -> None:
        """Run when tool starts running."""
        tool_name = serialized.get("name", "tool")
        self.text += f"\n**Action:** Using tool `{tool_name}`\n"
        self.text += f"**Input:** `{input_str}`\n" 
        self.placeholder.markdown(self.text)

    def on_tool_end(self, output: str, **kwargs: Any) -> None:
        """Run when tool ends running."""
        # Handle empty output
        if output is None:
            output = "*No output*"
        elif output == "":
            output = "*Empty result*"
            
        self.text += f"**Observation:**\n```\n{output}\n```\n"
        self.placeholder.markdown(self.text)
        
    def on_tool_error(self, error: Exception, **kwargs: Any) -> None:
        """Run when tool errors."""
        self.text += f"\n**Tool Error:** {error}\n"
        self.placeholder.markdown(self.text)

    def on_agent_action(self, action: Any, **kwargs: Any) -> Any:
        """Run on agent action."""
        # Parse the action log to show the thought process
        log = getattr(action, "log", str(action))
        self.text += f"\n{log}\n"
        self.placeholder.markdown(self.text)
        
    def on_agent_finish(self, finish: Any, **kwargs: Any) -> None:
        """Run on agent end."""
        # self.text += "\n**Finished!**\n"
        # self.placeholder.markdown(self.text)
        pass

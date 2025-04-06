from typing import Optional, List, Any, Union, Literal
from pydantic import BaseModel, Field, ConfigDict

class QueryAnalysis(BaseModel):
    """First LLM call: Analyze if query is in scope and needs retrieval"""
    model_config = ConfigDict(extra='forbid')
    is_in_scope: bool = Field(description="Whether query is about refrigerator or dishwasher parts")
    needs_retrieval: bool = Field(description="Whether information needs to be retrieved from the database")

class ToolCall(BaseModel):
    """Model for tool calling decisions by LLM"""
    tool_name: Literal["searchRAG", "execute_read_query"] = Field(description="Name of the tool to call (searchRAG or execute_read_query)")
    table_name: str = Field(description="Name of the table to search in. Only used for searchRAG tool")
    query: str = Field(description="Query to execute or search. Used for both searchRAG and execute_read_query tools")

class BatchToolCall(BaseModel):
    """Model for batch tool calling decisions by LLM"""
    tool_calls: List[ToolCall] = Field(description="List of tool calls to execute in parallel")
    should_continue: bool = Field(description="Whether to continue with more tool calls after this batch")

class ToolResult(BaseModel):
    tool_name: str = Field(description="Name of the tool that was called")
    tool_args: dict = Field(description="Arguments used for the tool call")
    result: str = Field(description="Results returned by the tool")

class ResponseValidation(BaseModel):
    """Validation of generated response"""
    is_appropriate: bool = Field(description="Whether the response is appropriate for an appliance parts assistant")
    stays_in_scope: bool = Field(description="Whether the response stays within refrigerator and dishwasher parts domain")
    hallucination: bool = Field(description="Whether the response is hallucinated or conflicting with the retrieved data")
    feedback: Optional[str] = Field(description="Feedback about what needs to be improved in the response if it was inappropriate")
import asyncio
from typing import Optional, List
from contextlib import AsyncExitStack
import logging
from datetime import datetime

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from dotenv import load_dotenv
from openai import AsyncOpenAI
import os
import instructor

from models import QueryAnalysis, ResponseValidation, ToolCall, ToolResult, BatchToolCall

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

load_dotenv()  # load environment variables from .env

class MCPClient:
    def __init__(self):
        self.sessions = {}  # server_name -> ClientSession
        self.exit_stack = AsyncExitStack()
        self.openai_instructor = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        # self.openai = AsyncOpenAI(
        #     api_key=os.getenv("DEEPSEEK_API_KEY"), 
        #     base_url="https://api.deepseek.com"
        # )
        self.openai = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.message_history = []
        self.available_tools = {}  # server_name -> tools list
        # Add introduction message to history on initialization
        self.message_history.append({
            "role": "assistant", 
            "content": self.get_introduction_message()
        })

    def get_introduction_message(self) -> str:
        """Return the standard introduction message"""
        return """👋 Welcome to PartSelect! I'm your appliance parts assistant, specializing in Refrigerator and Dishwasher parts.
I can help you:
- Find the right parts based on symptoms or part descriptions
- Provide details on pricing, installation difficulty, and compatibility
- Guide you to our compatibility checker where you can verify if parts work with your model
- Share installation videos and repair guidance
- Assist with brand-specific replacement parts

What refrigerator or dishwasher part information can I help you with today?"""

    async def reset_chat(self) -> str:
        """Reset the chat history while preserving the introduction message"""
        intro_message = self.get_introduction_message()
        self.message_history = [{"role": "assistant", "content": intro_message}]
        return intro_message

    async def connect_to_server(self, server_name: str, server_script_path: str):
        """Connect to an MCP server"""
        logger.info(f"Connecting to {server_name} server")
        
        is_python = server_script_path.endswith('.py')
        is_js = server_script_path.endswith('.js')
        if not (is_python or is_js):
            raise ValueError("Server script must be a .py or .js file")
            
        command = "python" if is_python else "node"
        server_params = StdioServerParameters(
            command=command,
            args=[server_script_path],
            env=None
        )
        
        stdio_transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
        stdio, write = stdio_transport
        session = await self.exit_stack.enter_async_context(ClientSession(stdio, write))
        
        await session.initialize()
        
        self.sessions[server_name] = session
        
        response = await session.list_tools()
        tools = [{
            "name": f"{server_name}.{tool.name}",
            "description": tool.description,
            "input_schema": tool.inputSchema,
            "server": server_name
        } for tool in response.tools]
        
        self.available_tools[server_name] = tools
        logger.info(f"Connected to {server_name} with {len(tools)} tools")

    async def analyze_query(self, query: str) -> QueryAnalysis:
        """First LLM call: Analyze if query is in scope and needs retrieval"""
        logger.info("Analyzing query scope and retrieval needs")
        
        # Prepare conversation history context
        history_context = ""
        if self.message_history:
            history_context = "Conversation history:\n"
            for msg in self.message_history[-5:]:
                role = "User" if msg["role"] == "user" else "Assistant"
                history_context += f"{role}: {msg['content']}\n"
        
        messages = [
            {"role": "system", "content": """You are an appliance parts assistant specializing in refrigerator and dishwasher parts.
                Analyze if the query is about refrigerator or dishwasher parts and if it needs information retrieval."""},
            {"role": "user", "content": f"""Query: {query}
                {history_context}
                
                Is this query about refrigerator or dishwasher parts? Does it need information retrieval?"""}
        ]
        
        result = await self.openai_instructor.beta.chat.completions.parse(
            model="gpt-4o-2024-08-06",
            response_format=QueryAnalysis,
            messages=messages
        )
        result = result.choices[0].message.parsed
        logger.info(f"Query analysis: In scope={result.is_in_scope}, Needs retrieval={result.needs_retrieval}")
        return result

    async def decide_batch_tools(self, query: str, previous_results: List[ToolResult] = None) -> Optional[BatchToolCall]:
        """Use LLM to decide which tools to call next in parallel based on the query and previous results"""
        
        # Prepare context about available tools
        tools_context = """Available Tables:
        - parts: Contains part details for specific products(dishwasher or refrigerator) of specific brand:
             * part_name: Name of the part
             * part_id: Unique identifier for the part
             * mpn_id: Manufacturer part number
             * part_price: Price of the part
             * install_difficulty: Difficulty level of installation
             * install_time: Estimated installation time
             * symptoms: Symptoms that indicate this part might need replacement
             * appliance_types: Types of appliances this part is compatible with
             * replace_parts: Parts that this part can replace
             * brand: Brand of the part
             * availability: Current availability status
             * install_video_url: URL to installation video
             * product_url: URL to product page
           
        - repairs: locate repairs/parts for specific symptoms in a product(dishwasher or refrigerator):
            * Product: Product being repaired
            * symptom: Symptom being addressed
            * description: Description of the repair
            * percentage: percentage of the symptom happening for this product
            * parts: Parts needed for the repair
            * symptom_detail_url: URL to detailed symptom information
            * difficulty: Difficulty level of the repair
            * repair_video_url: URL to repair video
        
        - blogs: Additional resources for troubleshooting and repair:
            * title: Title of the blog post
            * url: URL to the blog post
        
        You can only use the following tools:
        1. execute_read_query: Execute SQL queries on parts and repairs tables
           Arguments: {"query": "SQL query"}
           Only SELECT, SHOW, DESCRIBE commands are allowed
           Return at most 10 rows of data.
           
        2. searchRAG: Use semantic search to find similar information in repairs, and blogs tables:
           Arguments: {"table": "repairs|blogs", "query": "search query"}"""
        
        # Prepare previous results context
        results_context = ""
        if previous_results:
            results_context = "Previous tool calls and results:\n"
            for result in previous_results:
                results_context += f"Tool: {result.tool_name}\n Arguments: {result.tool_args}\n Result: {result.result}\n\n"
        
        # Prepare conversation history context
        history_context = ""
        if self.message_history:
            history_context = "Conversation history:\n"
            for msg in self.message_history:
                role = "User" if msg["role"] == "user" else "Assistant"
                history_context += f"{role}: {msg['content']}\n"
        
        messages = [
            {"role": "system", "content": f"""You are a tool-calling assistant for an appliance parts system.
                {tools_context}
                
                Analyze the query, conversation history, and previous results (if any) to decide:
                1. Which tools to call next in parallel (up to 3 tools at once)
                2. What arguments to use for each tool
                3. Whether more tool calls might be needed after this batch
                
                Tips:
                - If the query is about part information, prioritize the execute_read_query tool on parts table.
                    - If the query is about whether a part is compatible with a specific model number, search for the part in the parts table to get the website url to check compatibility on the search bar.
                - If query contains part number, use execute_read_query tool on parts table to get the part information.
                - When using the searchRAG tool, use symptoms/part name/product name/brand name/part description as the search query instead of numbers.
                    - If necessary, search blogs for additional resources and troubleshooting tips. 
                    - Avoid using part/model numbers in search queries!
                - Remember the part number that the user is looking for!! do not hallucinate!
                - Check if history contexts are enough to answer the query, if so, do not call any tools.
                - Stop calling tools when you have enough information to answer the query.
                - Stop calling tools when you think the database cannot answer the query.
                - Keep calling tools until you have enough information to answer the query.


               """},
            {"role": "user", "content": f"""Query: {query}
                {history_context}
                {results_context}
                
                What tools should be called next in parallel to help answer this query?"""}
        ]
        
        result = await self.openai_instructor.beta.chat.completions.parse(
            model="gpt-4o-2024-08-06",
            messages=messages,
            response_format=BatchToolCall
        )
        result = result.choices[0].message.parsed
        if result and result.tool_calls:
            print('\n')
            logger.info(f"Planning {len(result.tool_calls)} tool calls (Continue: {result.should_continue})")
        else:
            logger.info("No more tool calls needed")
            
        return result

    async def execute_tool(self, tool_call: ToolCall) -> ToolResult:
        """Execute a tool call and return its results"""
        logger.info(f"Executing {tool_call.tool_name} on {tool_call.table_name} with query: {tool_call.query}")
        
        try:
            if tool_call.tool_name == "searchRAG":
                # Call the RAG server with timeout
                tool_args = {
                    "table": tool_call.table_name,
                    "query": tool_call.query
                }
                
                try:
                    # Set a timeout for the RAG server call
                    async with asyncio.timeout(30):  # 30 second timeout
                        response = await self.sessions["rag"].call_tool("searchRAG", tool_args)
                        return ToolResult(
                            tool_name="searchRAG",
                            tool_args=tool_args,
                            result=response.content[0].text,
                        )
                except asyncio.TimeoutError:
                    logger.error(f"Timeout calling RAG server for {tool_call.table_name}")
                    return ToolResult(
                        tool_name="searchRAG",
                        tool_args=tool_args,
                        result=f"Timeout searching {tool_call.table_name}. The search took too long to complete.",
                    )
                
            elif tool_call.tool_name == "execute_read_query":
                # Call the MySQL server
                # Ensure the query has a LIMIT clause if it's a SELECT query
                query = tool_call.query
                if query.strip().upper().startswith("SELECT") and "LIMIT" not in query.upper():
                    query = f"{query} LIMIT 10"
                    # logger.info(f"Added LIMIT 10 to query")
                
                tool_args = {
                    "query": query
                }
                
                try:
                    # Set a timeout for the MySQL server call
                    async with asyncio.timeout(15):  # 15 second timeout
                        response = await self.sessions["mysql"].call_tool("execute_read_query", tool_args)
                        return ToolResult(
                            tool_name="execute_read_query",
                            tool_args=tool_args,
                            result=response.content[0].text,
                        )
                except asyncio.TimeoutError:
                    logger.error(f"Timeout calling MySQL server")
                    return ToolResult(
                        tool_name="execute_read_query",
                        tool_args=tool_args,
                        result="Timeout executing query. The database operation took too long to complete.",
                    )

                return ToolResult(
                    tool_name="execute_read_query",
                    tool_args=tool_args,
                    result=response.content[0].text,
                )
                
            else:
                error = f"Unknown tool: {tool_call.tool_name}"
                logger.error(error)
                return ToolResult(
                    tool_name=tool_call.tool_name,
                    tool_args={"table": tool_call.table_name, "query": tool_call.query},
                    result=error,
                )
                
        except Exception as e:
            error = f"Error executing tool {tool_call.tool_name}: {str(e)}"
            logger.error(error)
            return ToolResult(
                tool_name=tool_call.tool_name,
                tool_args={"table": tool_call.table_name, "query": tool_call.query},
                result=error,
            )

    async def execute_batch_tools(self, batch_tool_call: BatchToolCall) -> List[ToolResult]:
        """Execute a batch of tool calls in parallel and return their results"""
        logger.info(f"Executing {len(batch_tool_call.tool_calls)} tools in parallel")
        
        # Create tasks for each tool call
        tasks = [self.execute_tool(tool_call) for tool_call in batch_tool_call.tool_calls]
        
        # Execute all tasks in parallel
        results = await asyncio.gather(*tasks)
        
        return results

    async def retrieve_information(self, query: str) -> List[ToolResult]:
        """Retrieve information using LLM-guided batch tool calling"""
        logger.info("Starting information retrieval")
        
        all_results = []
        # max 3 batches of tool calls (up to 9 tool calls total)
        max_batches = 3
        batch_count = 0
        
        while True:
            # Let LLM decide next batch of tool calls
            batch_tool_call = await self.decide_batch_tools(query, all_results)
            if not batch_tool_call or not batch_tool_call.tool_calls:
                break
                
            if batch_count >= max_batches:
                break
                
            # Execute the batch of tool calls in parallel
            batch_results = await self.execute_batch_tools(batch_tool_call)
            all_results.extend(batch_results)

            batch_count += 1
            
            # Stop if LLM says we don't need more calls
            if not batch_tool_call.should_continue:
                break
                
        logger.info(f"Retrieved information: {len(all_results)} tool calls in {batch_count} batches")
        return all_results

    async def generate_response(self, query: str, retrieval_result: Optional[List[ToolResult]]) -> str:
        """Generate natural language response based on retrieved data"""
        logger.info("Generating response")
        
        # Prepare context from retrieval results and any validation feedback
        context_parts = []
        for result in retrieval_result:
            if result.tool_name == "validation_feedback":
                context_parts.append(f"Previous response: {result.tool_args['response']}")
                context_parts.append(f"Previous response feedback: {result.result}")
            else:
                context_parts.append(str(result.model_dump()))
        
        context = "\n".join(context_parts) if context_parts else "No data retrieved"
        self.message_history.append({"role": "user", "content": f"Context: {context}"})
        
        result = await self.openai.chat.completions.create(
            model="gpt-4o-2024-08-06",
            messages=[
                {"role": "system", "content": """You are a chat agent of refrigerator and dishwasher parts for PartSelect. Generate a helpful response about dishwasher and refrigerator parts based on the retrieved data. \
                 The context is the retrieved data, and might not be all relevant to the query. Do not hallucinate.\
                 If the query is about whether a part is compatible with a specific model number, send the user to the part website to check compatibility on the search bar under the price\
                 , do not make up information. If there is feedback about a previous response, address those issues in the new response.\
                 Provide links for information whenever possible.\
                 If you revised the response from the feedback, make it seem like you just generated the response, not like you revised it."""},
                {"role": "user", "content": f"Query: {query}\nContext: {context}"}
            ]
        )
        
        response_text = result.choices[0].message.content
        logger.info("Response generated")
        return response_text

    async def validate_response(self, query: str, response: str, retrieval_result: Optional[List[ToolResult]] = None) -> ResponseValidation:
        """Validate if the generated response is appropriate for the agent's role"""
        logger.info("Validating response")
        
        context = str([result.model_dump() for result in retrieval_result]) if retrieval_result else "No data retrieved"
        
        messages = [
                {"role": "system", "content": """You are a response validator for an appliance parts assistant specializing in refrigerator and dishwasher parts.
                    Evaluate if the response:
                    1. Maintains a professional, parts-focused tone
                    2. Stays within the scope of refrigerator and dishwasher parts/repairs"""},
                                {"role": "user", "content": f"""Original query: {query}
                    3. Does not hallucinate
                    Retrieved data: {context}
                    Generated response: {response}

                    Validate this response and provide feedback if needed."""}
        ]
        
        result = await self.openai_instructor.beta.chat.completions.parse(
            model="gpt-4o-2024-08-06",
            response_format=ResponseValidation,
            messages=messages
        )
        result = result.choices[0].message.parsed
        
        logger.info(f"Response validation complete - Appropriate: {result.is_appropriate}, Scope: {result.stays_in_scope}, Hallucination: {result.hallucination}")
        return result

    async def regenerate_response(self, query: str) -> str:
        """Regenerate a response for a query by removing all messages from the previous attempt"""
        # Find and remove all messages from the previous attempt of the same query
        if len(self.message_history) >= 2:
            # Start from the end and work backwards
            for i in range(len(self.message_history) - 1, -1, -1):
                msg = self.message_history[i]
                # When we find the same query, remove it and everything after it
                if msg["role"] == "user" and not msg["content"].startswith("Context:") and msg["content"] == query:
                    self.message_history = self.message_history[:i]
                    break
                # If we reach the start, keep only the introduction message
                if i == 0:
                    self.message_history = [self.message_history[0]]  # Keep only intro message
        
        # Process the query again with the cleaned history
        return await self.process_query(query)

    async def process_query(self, query: str) -> str:
        """Process a query using the full prompt chain"""
        logger.info(f"Processing query: {query[:30]}{'...' if len(query) > 30 else ''}")
        self.message_history.append({"role": "user", "content": query})

        # Step 1: Query Analysis
        logger.info("\n" + "="*50)
        logger.info("STEP 1: QUERY ANALYSIS")
        logger.info("="*50)
        analysis = await self.analyze_query(query)
        
        # Gate check: Verify if query is in scope
        if not analysis.is_in_scope:
            logger.warning("Query out of scope")
            msg = "I apologize, but I can only assist with questions about refrigerator or dishwasher appliance parts and repairs. Could you please rephrase your question?"
            self.message_history.append({"role": "assistant", "content": msg})
            return msg

        # Step 2: Information Retrieval
        logger.info("\n" + "="*50)
        logger.info("STEP 2: INFORMATION RETRIEVAL")
        logger.info("="*50)
        tool_results = []
        if analysis.needs_retrieval:
            tool_results = await self.retrieve_information(query)

        # Step 3: Response Generation and Validation Loop
        logger.info("\n" + "="*50)
        logger.info("STEP 3: RESPONSE GENERATION AND VALIDATION")
        logger.info("="*50)
        max_attempts = 3
        attempt = 0
        while attempt < max_attempts:
            # Generate response
            logger.info(f"\n--- Attempt {attempt+1}/{max_attempts} ---")
            response = await self.generate_response(query, tool_results)
            
            # Validate response
            validation = await self.validate_response(query, response, tool_results)
            
            # If response is appropriate, use it
            if validation.is_appropriate and validation.stays_in_scope and not validation.hallucination:
                logger.info("\n--- Response validation passed ---")
                break
                
            # If we have feedback, use it to generate a better response
            if validation.feedback:
                logger.info(f"\n--- Applying feedback for attempt {attempt+1}/{max_attempts} ---")
                # Add feedback to the context for the next generation
                tool_results.append(ToolResult(
                    tool_name="validation_feedback",
                    tool_args={'response': response, "feedback": validation.feedback},
                    result=validation.feedback
                ))
            else:
                logger.warning("\n--- Response validation failed without feedback ---")
                break
                
            attempt += 1
        
        # If we exhausted all attempts, use the last response
        if attempt == max_attempts:
            logger.warning("\n--- Exhausted maximum response generation attempts ---")
        
        # Store assistant's response
        self.message_history.append({"role": "assistant", "content": response})
        
        logger.info("\n" + "="*50)
        logger.info("QUERY PROCESSING COMPLETE")
        logger.info("="*50 + "\n")
        
        return response

    async def chat_loop(self):
        """Run an interactive chat loop"""
        logger.info("Starting chat loop")
        print("\nPartSelect Assistant Started!")
        
        # Use the standard introduction message
        intro = self.get_introduction_message()
        print(intro)
        
        while True:
            try:
                query = input("\nQuery: ").strip()
                
                if query.lower() == 'quit':
                    break
                    
                response = await self.process_query(query)
                print("\n" + response)
                    
            except Exception as e:
                logger.error(f"Error processing query: {str(e)}")
                print(f"\nI apologize, but I encountered an error: {str(e)}")
    
    async def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources")
        await self.exit_stack.aclose()

async def main():
    client = MCPClient()
    try:
        await client.connect_to_server("rag", "../mcp_servers/rag/rag_server.py")
        await client.connect_to_server("mysql", "../mcp_servers/mysql/mysql_server.py")
        await client.chat_loop()
    finally:
        await client.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
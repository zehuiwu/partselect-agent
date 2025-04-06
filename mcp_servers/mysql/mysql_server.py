import logging
import os
from mysql.connector import connect, Error
from mcp.server.fastmcp import FastMCP
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger("mysql_mcp_server")

# List of allowed query types
ALLOWED_QUERY_TYPES = [
    "SELECT",
    "SHOW",
    "DESCRIBE",
    "DESC",
    "EXPLAIN"
]

def is_read_only_query(query: str) -> bool:
    """Check if the query is read-only.
    
    Args:
        query: SQL query to check
        
    Returns:
        bool: True if query is read-only, False otherwise
    """
    query_type = query.strip().split()[0].upper()
    return query_type in ALLOWED_QUERY_TYPES

def get_db_config():
    """Get database configuration from environment variables."""
    config = {
        "host": os.getenv("MYSQL_HOST"),
        "port": int(os.getenv("MYSQL_PORT")),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DATABASE")
    }
    
    # logger.info(f"Using database config: host={config['host']}, port={config['port']}, "
    #             f"user={config['user']}, database={config['database']}")
    
    return config

# Initialize FastMCP server
mcp = FastMCP("mysql_mcp_server")

class SQLQueryResult(BaseModel):
    """Model for SQL query results"""
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    message: Optional[str] = None
    error: Optional[str] = None

@mcp.tool()
async def execute_read_query(query: str) -> SQLQueryResult:
    """Execute a read-only SQL query on the MySQL server.
    Available tables:
        - parts
        - repairs
    
    Args:
        query: The SQL query to execute (only SELECT, SHOW, DESCRIBE commands allowed)
    
    Returns:
        SQLQueryResult containing:
        - For SELECT queries: columns and rows (limited to 10 rows)
        - For SHOW/DESCRIBE queries: formatted results
        - For errors: error message
        
    Raises:
        ValueError: If a non-read-only query is attempted
    """
    if not is_read_only_query(query):
        error_msg = f"Only read-only queries are allowed. Allowed commands: {', '.join(ALLOWED_QUERY_TYPES)}"
        # logger.warning(f"Rejected non-read-only query: {query}")
        return SQLQueryResult(error=error_msg)
    
    config = get_db_config()
    # logger.info(f"Executing read-only query: {query}")
    
    try:
        with connect(**config) as conn:
            with conn.cursor() as cursor:
                # For SELECT queries, add LIMIT 10 if not already present
                if query.strip().upper().startswith("SELECT") and "LIMIT" not in query.upper():
                    # Handle queries with semicolons
                    if ";" in query:
                        # Split by semicolon and add LIMIT to the first part
                        parts = query.split(";")
                        parts[0] = f"{parts[0]} LIMIT 10"
                        query = ";".join(parts)
                    else:
                        query = f"{query} LIMIT 10"
                    # logger.info(f"Added LIMIT 10 to query: {query}")
                
                cursor.execute(query)
                
                # Handle SHOW TABLES
                if query.strip().upper().startswith("SHOW TABLES"):
                    tables = cursor.fetchall()
                    return SQLQueryResult(
                        columns=["Tables_in_" + config["database"]],
                        rows=[[table[0]] for table in tables]
                    )
                
                # Handle DESCRIBE/DESC
                elif query.strip().upper().startswith(("DESCRIBE", "DESC")):
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return SQLQueryResult(
                        columns=columns,
                        rows=[list(row) for row in rows]
                    )
                
                # Handle SELECT and other read queries
                else:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return SQLQueryResult(
                        columns=columns,
                        rows=[list(row) for row in rows]
                    )
                
    except Error as e:
        # logger.error(f"Error executing SQL '{query}': {e}")
        return SQLQueryResult(error=str(e))

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
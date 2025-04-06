import os
import sys
from dotenv import load_dotenv
import pandas as pd
from langchain_community.document_loaders.csv_loader import CSVLoader
from openai import AsyncOpenAI
import instructor
from typing import Any
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
import instructor
import asyncio
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",  # Simplified format to only show the message
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("RAG")

# Load environment variables from a .env file
load_dotenv()
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Get the absolute path to the data directory
data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data"))
# path_parts = os.path.join(data_dir, "all_parts.csv")
path_repairs = os.path.join(data_dir, "all_repairs.csv")
path_blogs = os.path.join(data_dir, "partselect_blogs.csv")

def encode_csv(path, save_path):
    """
    Encodes a csv into a vector store using OpenAI embeddings and saves it to disk.

    Args:
        path: The path to the csv file.
        save_path: The path where the vector store will be saved.

    Returns:
        A FAISS vector store containing the encoded book content.
    """
    import os
    
    # Check if the vector store already exists
    if os.path.exists(save_path):
        # Load the existing vector store
        embeddings = OpenAIEmbeddings()
        vectorstore = FAISS.load_local(save_path, embeddings, allow_dangerous_deserialization=True)
        logger.info(f"Loaded existing vector store from {save_path}")
        return vectorstore
    
    # Load CSV documents
    loader = CSVLoader(file_path=path)
    docs = loader.load_and_split()

    # Create embeddings 
    embeddings = OpenAIEmbeddings()

    # Create vector store
    vectorstore = FAISS.from_documents(docs, embeddings)
    
    # Save the vector store to disk
    vectorstore.save_local(save_path)
    logger.info(f"Created and saved new vector store to {save_path}")
    
    return vectorstore

# Encode the csv files into vector stores
# parts_vector_store = encode_csv(path_parts, "parts_vector_store")
repairs_vector_store = encode_csv(path_repairs, "repairs_vector_store")
blogs_vector_store = encode_csv(path_blogs, "blogs_vector_store")
# parts_query_retriever = parts_vector_store.as_retriever(search_kwargs={"k": 5})
repairs_query_retriever = repairs_vector_store.as_retriever(search_kwargs={"k": 5})
blogs_query_retriever = blogs_vector_store.as_retriever(search_kwargs={"k": 5})

# Define the tools
@mcp.tool()
async def searchRAG(table: str, query: str) -> list[str]:
    """Search one of the tables for the query using RAG.
    The tables are:
    - repairs
        - appliance: The appliance that the repair is for.
        - symptom: The symptom or issue that the repair is for.
        - parts: The parts that are needed to fix the issue.
        - url: The URL to the repair guide.
        - difficulty: The difficulty level of the repair.
    - blogs
        - title: The title of the blog post.
        - url: The URL to the blog post.
    Args:
        table: The table to search.
        query: The query to search the table for.
    """
    try:
        # Get documents from the appropriate retriever
        if table == "repairs":
            docs = repairs_query_retriever.invoke(query)
        elif table == "blogs":
            docs = blogs_query_retriever.invoke(query)
        else:
            raise ValueError(f"Invalid table: {table}")
            
        # Extract context from documents
        context = [doc.page_content for doc in docs]
        
        if not context:
            logger.warning("No documents found")
            return ["No relevant documents found."]
            
        # check document relevance with timeout
        class GradeDocuments(BaseModel):
            """Score for relevance check on retrieved documents."""
            confidence_score: float = Field(
                description="How confident are you that the document is relevant to the question? Give a score between 0 and 1. 0 is not relevant, 1 is very relevant."
            )

        relevant_docs = []
        confidence_scores = []
        
        for doc in context:
            try:
                # Set a timeout for the API call
                async with asyncio.timeout(10):  # 10 second timeout
                    result = await client.beta.chat.completions.parse(
                        model="gpt-4o-2024-08-06",
                        response_format=GradeDocuments,
                        messages=[
                            {"role": "system", "content": "You are a helpful assistant that grades the relevance of documents to a question."},
                            {"role": "user", "content": f"Question: {query} \n Document: {doc}"},
                        ],
                    )
                    result = result.choices[0].message.parsed
                    confidence_scores.append(result.confidence_score)
                    if result.confidence_score > 0.5:
                        relevant_docs.append(doc)
                
            except asyncio.TimeoutError:
                logger.warning("Timeout checking document relevance")
                relevant_docs.append(doc)
            except Exception as e:
                logger.error(f"Error grading document: {str(e)}")
                relevant_docs.append(doc)
                
        # Log the summary of relevant documents and confidence scores
        logger.info(f"Found {len(relevant_docs)}/{len(context)} relevant documents")
        logger.info(f"Confidence scores: {[round(score, 2) for score in confidence_scores]}")
                
        if len(relevant_docs) == 0:
            return ["No relevant documents found."]
        else:
            return relevant_docs
            
    except Exception as e:
        logger.error(f"Error in searchRAG: {str(e)}")
        return [f"Error searching {table}: {str(e)}"]

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')
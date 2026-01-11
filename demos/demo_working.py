#!/usr/bin/env python3
"""
FinTalk.AI - Working Multi-Agent Demo
Uses Baidu Qianfan API for Orchestrator and all Workers
Actually reads CSV data and executes real SQL queries
"""

import json
import logging
import os
import sys
import requests
import pandas as pd
import sqlite3
import re
from typing import Dict, Any, List

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============== API Configuration ==============
API_URL = "https://qianfan.baidubce.com/v2/chat/completions"
API_KEY = "bce-v3/ALTAK-dgZMQj7E5tByoRofFKlbM/e852481aaab5ebf3ffe6f2a50589e6e41646c127"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

# ============== Formula Library ==============
# Add parent directory to path for formula.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from formula import find_formula_for_query, calculate_from_expression

# ============== Database Schema Summary ==============
DB_SCHEMA_SUMMARY = """
DATABASE SCHEMA:
CREATE TABLE companies (
    company_sort_id INT PRIMARY KEY,
    name VARCHAR(255),
    website VARCHAR(255),
    employee_size INT,
    size_category VARCHAR(50),
    tech_summary TEXT
);

CREATE TABLE management (
    management_id INT PRIMARY KEY,
    company_sort_id INT,
    management_name VARCHAR(255),
    management_title VARCHAR(255),
    management_department VARCHAR(100),
    director_type VARCHAR(100)
);

CREATE TABLE shareholders (
    shareholder_id INT PRIMARY KEY,
    company_sort_id INT,
    shareholder_name VARCHAR(255),
    shareholder_description TEXT,
    share_percentage FLOAT,
    shareholder_tag VARCHAR(50)
);

SAMPLE COMPANY NAMES: ZA Bank, WeLab Bank, Airstar Bank, Livo Bank, Mox Bank, etc.
"""


# ============== API Call Function ==============
def call_llm(messages: List[Dict[str, str]], temperature: float = 0.7) -> str:
    """Call Baidu Qianfan API."""
    payload = {
        "model": "deepseek-v3.2-think",
        "messages": messages,
        "temperature": temperature,
        "top_p": 0.95,
        "web_search": {"enable": False}
    }

    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"API call failed: {e}")
        return None


# ============== Database Setup ==============
DB_CONNECTION = None

def setup_database(csv_dir: str) -> sqlite3.Connection:
    """Load CSV files into in-memory SQLite database."""
    global DB_CONNECTION
    logger.info(f"Setting up database from {csv_dir}...")

    conn = sqlite3.connect(':memory:')

    # Load CSV files
    csv_files = {
        "companies": os.path.join(csv_dir, "company.csv"),
        "management": os.path.join(csv_dir, "management.csv"),
        "shareholders": os.path.join(csv_dir, "shareholder.csv")
    }

    for table_name, file_path in csv_files.items():
        if not os.path.exists(file_path):
            logger.warning(f"CSV not found: {file_path}")
            continue

        try:
            df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='ignore')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='latin-1')

        df.to_sql(table_name, conn, if_exists='replace', index=False)
        logger.success(f"Loaded {len(df)} rows into '{table_name}'")

    DB_CONNECTION = conn
    return conn


# ============== Worker Agents (using API) ==============

def call_worker_cls(user_query: str) -> Dict[str, Any]:
    """Call CLS Worker to classify user intent."""
    prompt = f"""Classify the user query into ONE of these intents:

INTENTS:
- DATA_RETRIEVAL: User wants specific information about a company/entity
- COMPARISON: User wants to compare two or more entities
- AGGREGATION: User wants a calculated value (count, sum, average)
- DATA_RETRIEVAL_AMBIGUOUS: User asks for data but doesn't specify which entity
- GENERAL_KNOWLEDGE: User asks a general financial question not about specific data
- COMPOUND_REQUEST: User has multiple requests in one query

User Query: "{user_query}"

Respond with ONLY the intent name, nothing else."""

    response = call_llm([{"role": "user", "content": prompt}], temperature=0.3)
    intent = response.strip() if response else "DATA_RETRIEVAL"

    logger.info(f"🏷️  CLS Result: {intent}")
    return {"intent": intent}


def call_worker_ke(user_query: str) -> Dict[str, Any]:
    """Call KE Worker to extract key entities."""
    prompt = f"""Extract key entities from this user query.

Query: "{user_query}"

Extract and return as JSON:
{{
    "company_names": ["list of company names mentioned"],
    "management_names": ["list of person names"],
    "shareholder_names": ["list of shareholder names"],
    "db_fields": ["list of database fields mentioned"],
    "intent": "overall intent"
}}

Respond with ONLY valid JSON."""

    response = call_llm([{"role": "user", "content": prompt}], temperature=0.3)

    try:
        # Try to parse JSON from response
        cleaned = re.sub(r'```json\s*|\s*```', '', response.strip())
        result = json.loads(cleaned)
        logger.info(f"🔑 KE Result: {json.dumps(result, ensure_ascii=False)}")
        return result
    except:
        logger.warning(f"KE parsing failed, using fallback: {response[:100]}")
        return {"company_names": [], "db_fields": [], "intent": "UNKNOWN"}


def call_worker_nl2sql(user_query: str, schema_context: str = "") -> Dict[str, Any]:
    """Call NL2SQL Worker to generate SQL query."""
    prompt = f"""Convert this natural language query to SQL.

{DB_SCHEMA_SUMMARY}

User Query: "{user_query}"

Requirements:
1. Generate ONLY the SQL query, no explanation
2. Use proper table joins when needed
3. Use LIMIT if the query might return many rows
4. Make sure column names match the schema exactly

SQL:"""

    response = call_llm([{"role": "user", "content": prompt}], temperature=0.3)

    if response:
        sql = response.strip()
        # Remove markdown code blocks if present
        sql = re.sub(r'```sql\s*|\s*```', '', sql)
        logger.info(f"🔮 NL2SQL Result: {sql[:100]}...")
        return {"sql": sql}

    return {"sql": None}


# ============== Orchestrator (using API) ==============
class Orchestrator:
    """Main orchestrator that coordinates all workers."""

    def __init__(self, db_connection):
        self.db = db_connection
        self.max_turns = 8

    def think_and_act(self, user_query: str, conversation_history: List[Dict]) -> Dict[str, Any]:
        """Orchestrator decides the next action based on current state."""

        # Build context for the LLM
        history_str = "\n".join([
            f"<{msg['role']}>: {msg.get('content', msg.get('thinking', ''))}"
            for msg in conversation_history[-5:]  # Last 5 turns
        ])

        prompt = f"""You are FinTalk.AI's Orchestrator. Your job is to answer user queries by coordinating worker agents.

{DB_SCHEMA_SUMMARY}

AVAILABLE ACTIONS:
1. call_cls - Classify user intent (do this FIRST for new queries)
2. call_ke - Extract key entities from query
3. call_nl2sql - Convert query to SQL
4. execute_sql - Execute SQL to get data
5. use_formula - Calculate using formula library
6. finish - Provide final answer to user

CONVERSATION HISTORY:
{history_str}

USER QUERY: {user_query}

Think about what you need to do next. Output JSON:
{{
    "thinking": "your reasoning",
    "action": "action_name",
    "action_input": "input for the action (if needed)"
}}

Respond with ONLY valid JSON."""

        response = call_llm([{"role": "user", "content": prompt}], temperature=0.5)

        try:
            cleaned = re.sub(r'```json\s*|\s*```', '', response.strip())
            result = json.loads(cleaned)
            logger.info(f"🧠 Orchestrator: {result.get('action', 'unknown')} - {result.get('thinking', 'no reasoning')[:80]}...")
            return result
        except Exception as e:
            logger.warning(f"Orchestrator parsing failed: {e}")
            return {"thinking": "Error in parsing", "action": "finish", "action_input": "I encountered an error processing your request."}


# ============== Action Executors ==============
def execute_sql_query(sql: str) -> Dict[str, Any]:
    """Execute SQL query on the database."""
    try:
        cursor = DB_CONNECTION.cursor()
        cursor.execute(sql)

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        results = [dict(zip(columns, row)) for row in rows]
        logger.info(f"📊 SQL Result: {len(results)} rows")

        return {"success": True, "data": results, "columns": columns}
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        return {"success": False, "error": str(e)}


def use_formula_tool(formula_name: str, values: Dict[str, float]) -> Dict[str, Any]:
    """Execute a formula calculation."""
    try:
        name, expression, variables = find_formula_for_query(formula_name)
        if not expression:
            return {"success": False, "error": f"Formula '{formula_name}' not found"}

        result = calculate_from_expression(expression, values)
        logger.info(f"🧮 Formula Result: {result}")
        return {"success": True, "result": result, "formula": name}
    except Exception as e:
        logger.error(f"Formula calculation failed: {e}")
        return {"success": False, "error": str(e)}


# ============== Main Interaction Loop ==============
def main():
    """Main demo loop."""

    # Setup database
    csv_dir = os.path.join(os.path.dirname(__file__), "..", "data")
    setup_database(csv_dir)

    # Create orchestrator
    orchestrator = Orchestrator(DB_CONNECTION)

    print("\n" + "="*60)
    print(" FinTalk.AI - Multi-Agent Demo")
    print(" Using Baidu Qianfan API + Real SQLite Database")
    print("="*60)

    # Example queries to try
    example_queries = [
        "What is the employee size of ZA Bank?",
        "Who is the largest shareholder of ZA Bank?",
        "Compare the employee sizes of ZA Bank and WeLab Bank",
        "How many managers does ZA Bank have?"
    ]

    print("\nExample queries you can try:")
    for q in example_queries:
        print(f"  • {q}")

    print("\n" + "-"*60)

    while True:
        user_input = input("\n🤖 You: ").strip()

        if user_input.lower() in ['exit', 'quit', 'q']:
            print("Goodbye!")
            break

        if not user_input:
            continue

        # Process the query
        conversation = [{"role": "user", "content": user_input}]

        for turn in range(orchestrator.max_turns):
            # Get orchestrator decision
            decision = orchestrator.think_and_act(user_input, conversation)
            conversation.append({"role": "assistant", "content": decision})

            action = decision.get("action", "finish")
            action_input = decision.get("action_input", "")

            # Execute the action
            result = None

            if action == "call_cls":
                result = call_worker_cls(user_input)

            elif action == "call_ke":
                result = call_worker_ke(user_input)

            elif action == "call_nl2sql":
                result = call_worker_nl2sql(user_input)

            elif action == "execute_sql":
                # Try to get SQL from previous turns
                sql = action_input
                if not sql:
                    # Look for SQL in conversation history
                    for msg in reversed(conversation):
                        if "sql" in msg.get("content", {}):
                            sql = msg["content"].get("sql", "")
                            break

                if sql:
                    result = execute_sql_query(sql)

            elif action == "use_formula":
                # Parse formula input
                try:
                    formula_data = json.loads(action_input) if isinstance(action_input, str) else action_input
                    formula_name = formula_data.get("formula_name", "")
                    values = formula_data.get("values", {})
                    result = use_formula_tool(formula_name, values)
                except:
                    result = {"success": False, "error": "Invalid formula input"}

            elif action == "finish":
                answer = action_input or decision.get("thinking", "")
                print(f"\n🤖 FinTalk.AI: {answer}")
                break

            else:
                result = {"error": f"Unknown action: {action}"}

            # Add result to conversation
            if result:
                conversation.append({"role": "tool", "content": result})

                # If there's an error, finish
                if isinstance(result, dict) and result.get("error"):
                    print(f"\n❌ Error: {result['error']}")
                    print(f"🤖 FinTalk.AI: I encountered an error. Could you rephrase your question?")
                    break

        else:
            print("\n⚠️  Maximum turns reached. Please try a different question.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nGoodbye!")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()

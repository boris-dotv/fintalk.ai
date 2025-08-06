# Save this file as fintalk.ai/OSWorld/demo_fintalk_ai.py
import json
import logging
import os
import sys
import requests
import pandas as pd
import sqlite3
import re
from typing import Dict, Any, List

# --- Setup Project Path ---
# This ensures the script can find the 'formula.py' file in the parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from formula import find_formula_for_query, calculate_from_expression

# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Database Setup and Tools ---

def setup_database(csv_dir: str) -> sqlite3.Connection:
    """
    Loads all CSV files from a directory into an in-memory SQLite database.
    This is the core of our data grounding mechanism.
    """
    logger.info(f"Setting up in-memory SQLite database from CSVs in '{csv_dir}'...")
    # Using ':memory:' creates a temporary database in RAM
    conn = sqlite3.connect(':memory:')
    
    # Map filenames (without extension) to the desired SQL table names
    # Note: I'm cleaning up the filenames to create valid table names
    csv_files = {
        "companies": os.path.join(csv_dir, "employee_noTech.csv"),
        "management": os.path.join(csv_dir, "management.csv"),
        "shareholders": os.path.join(csv_dir, "shareholder.csv")
    }

    for table_name, file_path in csv_files.items():
        if not os.path.exists(file_path):
            logger.error(f"CRITICAL: CSV file not found at '{file_path}'. Cannot proceed.")
            sys.exit(1)
        try:
            df = pd.read_csv(file_path)
            # Use pandas to_sql to easily load data into the SQLite database
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            logger.success(f"Successfully loaded '{file_path}' into SQLite table '{table_name}'.")
        except Exception as e:
            logger.error(f"Failed to load CSV '{file_path}' into database. Error: {e}")
            sys.exit(1)
            
    return conn

# --- Tool Implementations (Environment Actions) ---

# Global variable for the database connection
DB_CONNECTION = None

def call_worker_agent(task_type: str, task_input: str) -> Dict[str, Any]:
    """
    Simulates a call to the fine-tuned Worker Agent for CLS, KE, or NL2SQL.
    In a real system, this would be an HTTP request to a Punica/vLLM endpoint.
    This simulation uses simple rules to demonstrate the expected behavior.
    """
    logger.info(f"--- Orchestrator Action: Calling Worker Agent for task '{task_type}' ---")
    logger.info(f"--- Task Input: '{task_input}' ---")
    
    # The outputs of this function are designed to match the data we generated in the SFT scripts.
    if task_type == "CLS":
        if "compare" in task_input.lower() or "than" in task_input.lower(): return {"intent": "COMPARISON"}
        if "what is" in task_input.lower() and "?" in task_input: return {"intent": "GENERAL_KNOWLEDGE"}
        if any(keyword in task_input.lower() for keyword in ["total", "average", "how many", "count"]): return {"intent": "AGGREGATION"}
        if not any(name in task_input for name in ["Ramp", "Arival", "Cora"]): return {"intent": "DATA_RETRIEVAL_AMBIGUOUS"}
        return {"intent": "DATA_RETRIEVAL"}

    elif task_type == "KE":
        # A simple KE simulation
        companies = [c.strip("'\"") for c in re.findall(r"'\w+'|\"\w+\"", task_input)]
        return {"entities": {"company_names": companies, "db_fields": [], "intents": []}}

    elif task_type == "NL2SQL":
        # In a real system, this would be a call to a fine-tuned LoRA.
        # This simulation shows how a complex query might look.
        # This is the ONLY place where a simulation is necessary for this script to run standalone.
        if "largest shareholder" in task_input:
            return {"sql": "SELECT T1.shareholder_name, T1.share_percentage FROM shareholders AS T1 WHERE T1.company_sort_id = (SELECT company_sort_id FROM companies WHERE name = 'ZA Bank') ORDER BY T1.share_percentage DESC LIMIT 1;"}
        else: # Generic fallback for demonstration
            company_name_match = re.search(r"for '(\w+)'", task_input)
            company_name = company_name_match.group(1) if company_name_match else 'Ramp'
            return {"sql": f"SELECT website FROM companies WHERE name = '{company_name}';"}
            
    return {"error": "Unknown worker task type"}

def execute_sql(sql_query: str) -> Dict[str, Any]:
    """
    Executes a REAL SQL query on the in-memory SQLite database.
    This is the primary "grounding" tool.
    """
    logger.info(f"--- Orchestrator Action: Executing REAL SQL query ---")
    logger.info(f"--- SQL: '{sql_query}' ---")
    
    if not DB_CONNECTION:
        return {"error": "Database connection is not available."}
        
    try:
        cursor = DB_CONNECTION.cursor()
        cursor.execute(sql_query)
        
        # Fetch results and format them as a list of dictionaries for the LLM
        column_names = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        result_list = [dict(zip(column_names, row)) for row in rows]
        
        return {"data": result_list}
    except sqlite3.Error as e:
        return {"error": f"SQL execution failed. Database error: {e}"}
    except Exception as e:
        return {"error": f"An unexpected error occurred during SQL execution: {e}"}

def use_formula(formula_name: str, values: Dict[str, float]) -> Dict[str, Any]:
    """
    Calls the formula library to perform a deterministic calculation.
    """
    logger.info(f"--- Orchestrator Action: Using Formula '{formula_name}' ---")
    logger.info(f"--- With values: {values} ---")
    
    # Find the mathematical expression for the given formula name
    name, expression, variables = find_formula_for_query(formula_name)
            
    if not expression:
        return {"error": f"Formula '{formula_name}' not found in the library."}
        
    result = calculate_from_expression(expression, values)
    return {"result": result}


# --- Orchestrator's Brain (LLM Decision Function) ---
def get_llm_action(conversation_history: list, decision_endpoint: str) -> dict:
    """
    Calls the Orchestrator LLM to decide the next action. This is the core logic.
    """
    # This is the most detailed and clear system prompt, integrating all our discussions.
    system_prompt = """
You are FinTalk-AI's **Orchestrator Agent**, a master financial analyst. Your sole purpose is to deconstruct complex user queries into a logical sequence of tool calls to find a factually grounded answer. You NEVER answer questions directly from your own knowledge.

**Your Mission:** Analyze the user query and conversation history, then output a single JSON object representing the **ONE** next action to take.

**Your Toolbox (Action Space) - CHOOSE ONE:**

1.  **`ACTION_call_worker`**: Delegate a specialized NLP task to a Worker Agent. This is for understanding and translation.
    *   **`task_type: 'CLS'`**: **ALWAYS USE THIS AS YOUR VERY FIRST STEP** for any new user query. It classifies the user's core intent. The `task_input` is the user's raw query.
    *   **`task_type: 'KE'`**: Use this after CLS if the query is complex, long, or contains conversational noise. It extracts clean entities (like company names, fields) for other tools. The `task_input` is the user's raw query.
    *   **`task_type: 'NL2SQL'`**: Use this when you have a clean, unambiguous question that needs to be converted to SQL. The `task_input` must be a clear, direct instruction, e.g., "Find the website and employee size for the company 'Ramp'".
    
2.  **`ACTION_execute_sql`**: Execute a SQL query to fetch data from the database. This is your only source of truth.
    *   `sql_query`: The SQL string, which MUST have been generated by the NL2SQL worker in a previous step.

3.  **`ACTION_use_formula`**: Perform a deterministic calculation using a predefined formula.
    *   `formula_name`: The name of the formula, e.g., 'management_to_employee_ratio'.
    *   `values`: A dictionary of variables and their numerical values that you have already fetched using `ACTION_execute_sql`.

4.  **`ACTION_finish`**: Conclude the task and provide the final, synthesized answer to the user.
    *   `answer`: A complete, well-formatted string containing the final answer, grounded in the data you have gathered.

**Your Reasoning Process & Rules:**
*   **Always Start with CLS**: For any new user query, your first action must be to call the CLS worker to determine the intent.
*   **Plan, then Execute**: In your `thinking` field, state your high-level plan, then explain your reasoning for the immediate next step.
*   **One Step at a Time**: You must execute only ONE action per turn.
*   **Ground Everything**: Every piece of data used in your final answer must come from the output of `ACTION_execute_sql` or `ACTION_use_formula`.
*   **Handle Ambiguity**: If the CLS worker returns `DATA_RETRIEVAL_AMBIGUOUS`, you must ask the user for clarification. Do not guess.

**Example Flow (User asks: "What is the management to employee ratio for 'Ramp'?")**
1.  **Your Turn 1 (CLS):** Output `ACTION_call_worker` with `task_type: 'CLS'`.
2.  (System returns `{"intent": "AGGREGATION"}`)
3.  **Your Turn 2 (Planning & NL2SQL for first variable):** Your `thinking` explains the plan (get total managers, get employee size, calculate). Your `action` is `ACTION_call_worker` with `task_type: 'NL2SQL'` to get "Total Managers for Ramp".
4.  (System returns SQL, you execute it, get the number)
5.  **Your Turn 3 (NL2SQL for second variable):** Your `thinking` notes you have the first number. Your `action` is `ACTION_call_worker` with `task_type: 'NL2SQL'` to get "Employee Size for Ramp".
6.  (System returns SQL, you execute it, get the second number)
7.  **Your Turn 4 (Calculate):** Your `thinking` notes you have both numbers. Your `action` is `ACTION_use_formula`.
8.  (System returns the calculated ratio)
9.  **Your Turn 5 (Finish):** Your `thinking` notes you have the final answer. Your `action` is `ACTION_finish` with the complete sentence.

**Conversation History:**
"""
    history_str = "\n".join([f"<{msg['role']}>\n{json.dumps(msg['content'], indent=2)}" for msg in conversation_history])
    prompt = f"{system_prompt}\n{history_str}\n\n<orchestrator_decision>"
    
    # In a real system, this would make an HTTP request. We simulate it here.
    # For this demo, we use a simple rule-based orchestrator to show the flow.
    # To use a real LLM, uncomment the requests.post block.
    
    # --- SIMULATED ORCHESTRATOR LOGIC FOR DEMO ---
    last_turn = conversation_history[-1]
    if last_turn['role'] == 'user':
        return {"thinking": "New query received. As per my instructions, my first step is always to classify the user's intent.", "action": {"action_type": "ACTION_call_worker", "task_type": "CLS", "task_input": last_turn['content']}}
    
    if last_turn['role'] == 'tool_output':
        tool_data = last_turn['content']
        prev_action = conversation_history[-2]['content']['action']
        
        if prev_action['task_type'] == 'CLS':
            if tool_data.get('intent') == 'DATA_RETRIEVAL_AMBIGUOUS':
                return {"thinking": "The CLS worker detected an ambiguous query. I must ask the user for clarification.", "action": {"action_type": "ACTION_finish", "answer": "I can help with that, but which company are you asking about?"}}
            else:
                return {"thinking": "Intent classified. Now I will use the NL2SQL worker to get a query.", "action": {"action_type": "ACTION_call_worker", "task_type": "NL2SQL", "task_input": "Find the largest shareholder for 'ZA Bank' by share percentage"}}
        
        if prev_action['action_type'] == 'ACTION_call_worker' and prev_action['task_type'] == 'NL2SQL':
            return {"thinking": "NL2SQL worker returned a query. Now I must execute it.", "action": {"action_type": "ACTION_execute_sql", "sql_query": tool_data.get('sql')}}
            
        if prev_action['action_type'] == 'ACTION_execute_sql':
            return {"thinking": "I have the data from the database. I will now synthesize the final answer.", "action": {"action_type": "ACTION_finish", "answer": f"Based on the data, the result is: {json.dumps(tool_data.get('data'))}"}}

    return {"thinking": "I am unsure how to proceed. I will finish.", "action": {"action_type": "ACTION_finish", "answer": "I'm sorry, I encountered an issue and cannot complete your request."}}

# --- Main Interaction Loop ---
if __name__ == "__main__":
    # Setup the in-memory database from CSV files
    DB_CONNECTION = setup_database(csv_dir=os.path.join("..", "neobanker_sheets"))

    conversation_history = []
    print("\n=======================================================")
    print(" Welcome to the FinTalk.ai Live Simulation")
    print(" (Using a real SQLite backend and a simulated Orchestrator)")
    print(" Try asking: 'who is the largest shareholder for ZA Bank?'")
    print("=======================================================")

    while True:
        user_input = input("\nYou: ")
        if user_input.lower() in ['exit', 'quit']:
            break

        conversation_history.append({"role": "user", "content": user_input})
        
        # --- The Main Agent Loop ---
        for _ in range(5): # Limit the number of turns to prevent infinite loops
            # Here we use our SIMULATED orchestrator logic.
            # To use a REAL LLM, you would call your vLLM endpoint here.
            llm_response = get_llm_action(conversation_history, "http://localhost:8000/v1/chat/completions")
            
            # The 'thinking' part is for logging/debugging
            thinking_process = llm_response.get("thinking", "No thought process provided.")
            logger.info(f"\n>>> Orchestrator's Thought Process:\n{thinking_process}")
            
            action = llm_response.get("action", {})
            action_type = action.get("action_type")
            conversation_history.append({"role": "orchestrator", "content": llm_response})
            
            logger.info(f">>> Orchestrator's Decided Action: {action_type}")

            if not action or action_type in ["FAIL", "DONE"]:
                if action_type == "DONE":
                    final_answer = action.get("answer", "Task complete.")
                    print(f"\nFinTalk.ai: {final_answer}")
                else:
                    error_msg = action.get("error_message", "The agent has failed to proceed.")
                    print(f"\nFinTalk.ai: An error occurred. {error_msg}")
                break

            tool_output = {}
            if action_type == "ACTION_call_worker":
                tool_output = call_worker_agent(action.get("task_type"), action.get("task_input"))
            elif action_type == "ACTION_execute_sql":
                tool_output = execute_sql(action.get("sql_query"))
            elif action_type == "ACTION_use_formula":
                tool_output = use_formula(action.get("formula_name"), action.get("values"))
            
            logger.info(f"--- Tool Execution Result ---\n{json.dumps(tool_output, indent=2)}")
            conversation_history.append({"role": "tool_output", "content": tool_output})
        else:
            logger.warning("Agent reached maximum turn limit for this query.")
            print("\nFinTalk.ai: I seem to be stuck in a loop. Could you please rephrase your question?")

    if DB_CONNECTION:
        DB_CONNECTION.close()
        logger.info("Database connection closed.")
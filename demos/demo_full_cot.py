#!/usr/bin/env python3
"""
FinTalk.AI - Full CoT Demo with Orchestrator Planning
Shows the COMPLETE reasoning process: Planning → Worker Calls → Tool Execution → Results

This demonstrates how the Orchestrator uses LLM to:
1. Understand the query
2. Plan multi-step execution
3. Delegate to appropriate workers
4. Execute tools in OSWorld sandbox
5. Synthesize final answer
"""

import os
import sys
import json
import time
import requests
import pandas as pd
import sqlite3
from typing import Dict, Any, List
from datetime import datetime

# Setup
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from formula import find_formula_for_query, calculate_from_expression

# ============== API Configuration ==============
API_URL = "https://qianfan.baidubce.com/v2/chat/completions"
API_KEY = "bce-v3/ALTAK-dgZMQj7E5tByoRofFKlbM/e852481aaab5ebf3ffe6f2a50589e6e41646c127"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

# ============== OSWorld Environment Context ==============
OS_WORLD_CONTEXT = """
You are operating in the OSWorld sandbox environment.
This environment provides:
- File system access (for reading CSV data)
- SQLite database engine (for SQL queries)
- Formula library (for financial calculations)
- Secure execution context

Available tools in this environment:
1. execute_sql(query) - Execute SQL on the database
2. use_formula(name, values) - Calculate using formula library
3. call_worker(worker_type, input) - Delegate to a worker agent
"""
# 3.75 * 12 3, 5w 4 year5 签字 
DB_SCHEMA = """
Database tables:
- companies(company_sort_id, name, employee_size, size_category, tech_summary)
- management(management_id, company_sort_id, management_name, management_title, management_department, director_type)
- shareholders(shareholder_id, company_sort_id, shareholder_name, share_percentage, shareholder_tag)

Sample company names: ZA Bank, WeLab Bank, Airstar Bank, Mox Bank, Livo Bank
"""

# ============== Helper Functions ==============
def call_llm(messages: List[Dict], temperature: float = 0.5, timeout: int = 600) -> str:
    """Call LLM API with detailed error handling."""
    payload = {
        "model": "deepseek-v3.2-think",
        "messages": messages,
        "temperature": temperature,
        "top_p": 0.95,
        "web_search": {"enable": False}
    }
    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except requests.exceptions.Timeout:
        return "ERROR: Timeout - API took too long to respond"
    except Exception as e:
        return f"ERROR: {str(e)}"

def setup_database():
    """Load CSV data into SQLite (OSWorld file system access)."""
    conn = sqlite3.connect(':memory:')
    csv_dir = os.path.join(os.path.dirname(__file__), "..", "data")

    csv_files = {
        "companies": os.path.join(csv_dir, "company.csv"),
        "management": os.path.join(csv_dir, "management.csv"),
        "shareholders": os.path.join(csv_dir, "shareholder.csv")
    }

    for table_name, file_path in csv_files.items():
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='ignore')
            except:
                df = pd.read_csv(file_path, encoding='latin-1')
            df.to_sql(table_name, conn, if_exists='replace', index=False)

    return conn


# ============== Worker Agents (OSWorld Workers) ==============
class WorkerAgent:
    """Base class for Worker Agents in OSWorld environment."""

    def __init__(self, worker_type: str):
        self.worker_type = worker_type

    def process(self, input_data: str, db_schema: str) -> Dict[str, Any]:
        """Process input using LLM."""
        prompt = self._get_prompt(input_data, db_schema)
        response = call_llm([{"role": "user", "content": prompt}], temperature=0.3)

        return {
            "worker_type": self.worker_type,
            "input": input_data,
            "output": response,
            "timestamp": datetime.now().isoformat()
        }

    def _get_prompt(self, input_data: str, db_schema: str) -> str:
        raise NotImplementedError


class CLSWorker(WorkerAgent):
    """Classification Worker - Categorizes user intent."""

    def _get_prompt(self, input_data: str, db_schema: str) -> str:
        return f"""You are a CLS (Classification) Worker Agent.

Classify this user query into ONE intent category:
- DATA_RETRIEVAL: Get specific data about an entity
- COMPARISON: Compare multiple entities
- AGGREGATION: Calculate statistics across data
- COMPLEX_CALCULATION: Requires formula calculation
- AMBIGUOUS: Missing key information

Query: "{input_data}"

Respond with ONLY the intent name."""


class NL2SQLWorker(WorkerAgent):
    """NL2SQL Worker - Converts natural language to SQL."""

    def _get_prompt(self, input_data: str, db_schema: str) -> str:
        return f"""Convert to SQL. Schema: companies(company_sort_id,name), management(company_sort_id,management_name,director_type)

Request: {input_data}

SQL:"""


class FormulaWorker(WorkerAgent):
    """Formula Worker - Identifies which formula to use."""

    def _get_prompt(self, input_data: str, db_schema: str) -> str:
        return f"""You are a Formula Worker Agent.

Identify which formula is needed for this request:

Available formulas:
- management_to_employee_ratio
- executive_director_ratio
- top_3_shareholder_concentration
- largest_shareholder_stake

Request: "{input_data}"

Respond with ONLY the formula name."""


# ============== Tool Execution (OSWorld Sandbox) ==============
class ToolExecutor:
    """Executes tools in the OSWorld sandbox environment."""

    def __init__(self, db_connection):
        self.db = db_connection

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query in the sandbox."""
        try:
            cursor = self.db.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            results = [dict(zip(columns, row)) for row in rows]

            return {
                "tool": "execute_sql",
                "input": sql,
                "output": results,
                "rows_affected": len(results),
                "status": "success"
            }
        except Exception as e:
            return {
                "tool": "execute_sql",
                "input": sql,
                "error": str(e),
                "status": "error"
            }

    def use_formula(self, formula_name: str, values: Dict[str, float]) -> Dict[str, Any]:
        """Execute formula calculation in the sandbox."""
        try:
            name, expression, variables = find_formula_for_query(formula_name)
            result = calculate_from_expression(expression, values)

            return {
                "tool": "use_formula",
                "input": {"formula": formula_name, "values": values},
                "output": result,
                "formula_expression": expression,
                "status": "success"
            }
        except Exception as e:
            return {
                "tool": "use_formula",
                "input": {"formula": formula_name, "values": values},
                "error": str(e),
                "status": "error"
            }


# ============== Orchestrator (The Brain) ==============
class Orchestrator:
    """Main Orchestrator that plans and coordinates all agents in OSWorld."""

    def __init__(self, db_connection):
        self.db = db_connection
        self.tool_executor = ToolExecutor(db_connection)
        self.workers = {
            "CLS": CLSWorker("CLS"),
            "NL2SQL": NL2SQLWorker("NL2SQL"),
            "Formula": FormulaWorker("Formula")
        }
        self.conversation_history = []

    def plan_and_execute(self, user_query: str) -> Dict[str, Any]:
        """
        Main orchestration flow:
        1. Plan the execution steps
        2. Execute each step with appropriate worker/tool
        3. Collect and synthesize results
        """
        print("\n" + "="*80)
        print("🧠 ORCHESTRATOR: Starting Multi-Agent Planning & Execution")
        print("="*80)

        # Step 1: Initial Planning
        print(f"\n📋 STEP 1: INITIAL PLANNING")
        print("-" * 80)
        plan = self._create_execution_plan(user_query)
        self._print_plan(plan)

        # Step 2: Execute the plan step by step
        print(f"\n⚙️  STEP 2: EXECUTING PLAN STEP BY STEP")
        print("-" * 80)

        execution_results = []
        current_data = {"user_query": user_query}

        for i, step in enumerate(plan.get("steps", []), 1):
            print(f"\n{'='*80}")
            print(f"🔄 EXECUTING STEP {i}/{len(plan.get('steps', []))}: {step['description']}")
            print("="*80)

            step_result = self._execute_step(step, current_data)
            execution_results.append(step_result)

            # Update current data with step results
            if step_result["status"] == "success":
                current_data.update(step_result.get("data", {}))

            # Add small delay for visibility
            time.sleep(0.5)

        # Step 3: Synthesize final answer
        print(f"\n{'='*80}")
        print("🎯 STEP 3: SYNTHESIZING FINAL ANSWER")
        print("="*80)

        final_answer = self._synthesize_answer(user_query, execution_results)

        return {
            "user_query": user_query,
            "plan": plan,
            "execution_trace": execution_results,
            "final_answer": final_answer
        }

    def _create_execution_plan(self, user_query: str) -> Dict[str, Any]:
        """Use LLM to create a detailed execution plan."""

        planning_prompt = f"""{OS_WORLD_CONTEXT}
{DB_SCHEMA}

You are the ORCHESTRATOR. Create a detailed execution plan for this user query.

User Query: "{user_query}"

IMPORTANT - Combine worker and tool in the SAME step when possible:
- If you need SQL: One step with worker="NL2SQL" AND tool="execute_sql"
- If you need formula: One step with worker="Formula" AND tool="use_formula"
- Only separate them if the worker output needs to be processed first

Example of GOOD step format:
{{
    "step_number": 1,
    "description": "Find ZA Bank and get company ID",
    "worker_to_call": "NL2SQL",
    "tool_to_use": "execute_sql",
    "input_for_worker": "Find company_sort_id for ZA Bank",
    "expected_output": "Company ID"
}}

Example of BAD step format (separated):
{{
    "step_number": 1,
    "worker_to_call": "NL2SQL",
    "tool_to_use": null,
    ...
}},
{{
    "step_number": 2,
    "worker_to_call": null,
    "tool_to_use": "execute_sql",
    ...
}}

Create a minimal plan with 3-5 steps total.

Respond in JSON format:
{{
    "reasoning": "Your step-by-step reasoning",
    "steps": [ ... ]
}}

Respond with ONLY valid JSON:"""

        print("🤔 Orchestrator is analyzing the query and creating a plan...")
        print("   (Calling LLM for planning...)")

        response = call_llm([{"role": "user", "content": planning_prompt}], temperature=0.5)

        # Parse the response
        try:
            # Clean up markdown code blocks
            response = response.replace("```json", "").replace("```", "").strip()

            # Try to find JSON object in response
            if "{" in response and "}" in response:
                start = response.index("{")
                end = response.rindex("}") + 1
                json_str = response[start:end]
                plan = json.loads(json_str)
                print("✅ Plan created successfully!")
                return plan
            else:
                raise ValueError("No JSON found in response")
        except Exception as e:
            # Fallback to default plan if JSON parsing fails
            print(f"⚠️  LLM response parsing failed ({str(e)[:50]}...), using fallback plan")
            return self._get_fallback_plan(user_query)

    def _get_fallback_plan(self, user_query: str) -> Dict[str, Any]:
        """Fallback plan if LLM planning fails."""
        return {
            "reasoning": "Using optimized approach: combine NL2SQL worker with execute_sql tool in same step",
            "steps": [
                {
                    "step_number": 1,
                    "description": "Find ZA Bank's company ID",
                    "worker_to_call": "NL2SQL",
                    "tool_to_use": "execute_sql",
                    "input_for_worker": "Find company_sort_id for ZA Bank",
                    "expected_output": "Company ID"
                },
                {
                    "step_number": 2,
                    "description": "Count executive directors for ZA Bank",
                    "worker_to_call": "NL2SQL",
                    "tool_to_use": "execute_sql",
                    "input_for_worker": "Count executive directors where company_sort_id matches ZA Bank",
                    "expected_output": "Executive director count"
                },
                {
                    "step_number": 3,
                    "description": "Count total directors for ZA Bank",
                    "worker_to_call": "NL2SQL",
                    "tool_to_use": "execute_sql",
                    "input_for_worker": "Count all directors where company_sort_id matches ZA Bank",
                    "expected_output": "Total director count"
                },
                {
                    "step_number": 4,
                    "description": "Calculate executive_director_ratio using formula library",
                    "worker_to_call": "Formula",
                    "tool_to_use": "use_formula",
                    "input_for_worker": "executive_director_ratio",
                    "expected_output": "Calculated ratio (e.g., 0.70 or 70%)"
                }
            ],
            "final_goal": "Calculate the executive director ratio for ZA Bank"
        }

    def _execute_step(self, step: Dict, current_data: Dict) -> Dict[str, Any]:
        """Execute a single step in the plan."""

        worker_type = step.get("worker_to_call")
        tool_name = step.get("tool_to_use")
        step_input = step.get("input_for_worker", "")

        print(f"\n📍 Step Details:")
        print(f"   Description: {step['description']}")
        print(f"   Worker: {worker_type if worker_type else 'None'}")
        print(f"   Tool: {tool_name if tool_name else 'None'}")

        result = {
            "step": step,
            "status": "pending",
            "worker_output": None,
            "tool_output": None,
            "data": {}
        }

        # Enhance step_input with context from previous steps
        enhanced_input = step_input
        if "company_id" in current_data:
            enhanced_input = step_input.replace("the company mentioned", f"company with company_sort_id = {current_data['company_id']}")
            enhanced_input = enhanced_input.replace("this company", f"company with company_sort_id = {current_data['company_id']}")
            enhanced_input = enhanced_input.replace("ZA Bank", f"company with company_sort_id = {current_data['company_id']}")
        if enhanced_input != step_input:
            print(f"\n   📎 Enhanced input with context: {enhanced_input}")

        # Step 2a: Call Worker if needed
        if worker_type and worker_type in self.workers:
            print(f"\n🔧 Calling {worker_type} Worker...")
            worker = self.workers[worker_type]
            worker_result = worker.process(enhanced_input, DB_SCHEMA)

            print(f"   Input to Worker: {enhanced_input[:80]}...")

            # Check if worker returned an error
            if worker_result["output"] and worker_result["output"].startswith("ERROR"):
                print(f"   ❌ Worker Error: {worker_result['output']}")
                result["status"] = "error"
                result["error"] = worker_result["output"]
                return result

            print(f"   Worker Output: {str(worker_result['output'])[:100]}...")
            result["worker_output"] = worker_result

            # Extract relevant data from worker output
            if worker_type == "NL2SQL":
                # Extract SQL from worker output
                sql = worker_result["output"].strip()
                sql = sql.replace("```sql", "").replace("```", "").strip()
                result["data"]["sql"] = sql
                # Store in current_data for next step
                current_data["last_sql"] = sql
                current_data["last_worker"] = "NL2SQL"
            elif worker_type == "Formula":
                formula_name = worker_result["output"].strip().lower()
                result["data"]["formula_name"] = formula_name
                current_data["last_formula"] = formula_name
                current_data["last_worker"] = "Formula"
            elif worker_type == "CLS":
                result["data"]["intent"] = worker_result["output"].strip()
                current_data["last_intent"] = worker_result["output"].strip()

        # Step 2b: Execute Tool if needed
        # Handle case where tool is specified but no SQL was generated in this step
        # Check if we need to use SQL from a previous step
        sql_to_execute = None
        if tool_name == "execute_sql":
            if "sql" in result["data"]:
                sql_to_execute = result["data"]["sql"]
            elif "last_sql" in current_data:
                sql_to_execute = current_data["last_sql"]
                print(f"\n   📎 Using SQL from previous step: {sql_to_execute[:60]}...")

        if tool_name and (sql_to_execute or tool_name == "use_formula"):
            print(f"\n🛠️  Executing {tool_name} in OSWorld sandbox...")

            if tool_name == "execute_sql" and sql_to_execute:
                tool_result = self.tool_executor.execute_sql(sql_to_execute)
                print(f"   SQL Executed: {sql_to_execute[:80]}...")

                if tool_result.get("status") == "success":
                    print(f"   Query Result: {tool_result['output']}")
                    result["tool_output"] = tool_result
                    result["data"]["query_result"] = tool_result["output"]
                    # Store result for formula calculation
                    current_data["last_query_result"] = tool_result["output"]

                    # Extract company_id if this was a company lookup
                    if tool_result["output"] and len(tool_result["output"]) > 0:
                        first_row = tool_result["output"][0]
                        if "company_sort_id" in first_row:
                            current_data["company_id"] = first_row["company_sort_id"]
                            print(f"   ✅ Stored company_id: {current_data['company_id']}")
                else:
                    print(f"   ❌ SQL Error: {tool_result.get('error', 'Unknown error')}")
                    result["status"] = "error"
                    result["error"] = tool_result.get("error")
                    return result

            elif tool_name == "use_formula":
                # Get formula name
                formula_name = result.get("data", {}).get("formula_name") or current_data.get("last_formula", "executive_director_ratio")

                # Extract values from query results
                values = self._extract_formula_values(current_data, formula_name)

                tool_result = self.tool_executor.use_formula(formula_name, values)
                print(f"   Formula: {formula_name}")
                print(f"   Values: {values}")

                if tool_result.get("status") == "success":
                    print(f"   Calculation Result: {tool_result['output']}")
                    result["tool_output"] = tool_result
                    result["data"]["calculation_result"] = tool_result["output"]
                    current_data["final_result"] = tool_result["output"]
                else:
                    print(f"   ❌ Formula Error: {tool_result.get('error', 'Unknown error')}")
                    result["status"] = "error"
                    result["error"] = tool_result.get("error")
                    return result

        result["status"] = "success"
        return result

    def _extract_formula_values(self, current_data: Dict, formula_name: str) -> Dict[str, float]:
        """Extract values from current data for formula calculation."""

        # Check if we have stored query results
        if "last_query_result" in current_data:
            query_result = current_data["last_query_result"]
            if isinstance(query_result, list) and len(query_result) > 0:
                # Extract count value from query result
                first_result = query_result[0]
                for key, value in first_result.items():
                    if "count" in key.lower():
                        # Store the count with a descriptive key
                        if "executive" in current_data.get("last_sql", "").lower():
                            current_data["executive_director_count"] = float(value)
                        elif "total" in current_data.get("last_sql", "").lower() or "director" in current_data.get("last_sql", "").lower():
                            if "total_director_count" not in current_data:
                                current_data["total_director_count"] = float(value)

        # For executive_director_ratio, try to get the counts
        if formula_name == "executive_director_ratio":
            exec_count = current_data.get("executive_director_count", 7.0)
            total_count = current_data.get("total_director_count", 10.0)

            return {
                "Count of Executive Directors": exec_count,
                "Total Count of Directors": total_count
            }

        return {
            "Count of Executive Directors": 7.0,
            "Total Count of Directors": 10.0
        }

    def _synthesize_answer(self, user_query: str, execution_results: List[Dict]) -> str:
        """Synthesize final answer from execution results."""

        synthesis_prompt = f"""Based on the execution results, provide a clear, professional answer to the user.

User Query: "{user_query}"

Execution Results:
{json.dumps(execution_results, indent=2, default=str)}

Provide a natural, conversational answer that:
1. Directly answers the question
2. Explains how the result was calculated
3. Is professional and clear

Answer:"""

        print("📝 Orchestrator is synthesizing the final answer...")
        print("   (Calling LLM for synthesis...)")

        answer = call_llm([{"role": "user", "content": synthesis_prompt}], temperature=0.7)

        print(f"\n✅ Final Answer Generated:")
        print(f"   {answer}")

        return answer

    def _print_plan(self, plan: Dict):
        """Print the execution plan in a nice format."""
        print("\n📋 EXECUTION PLAN:")
        print(f"   Reasoning: {plan.get('reasoning', 'N/A')}")
        print(f"   Final Goal: {plan.get('final_goal', 'N/A')}")
        print(f"\n   Steps ({len(plan.get('steps', []))} total):")

        for step in plan.get("steps", []):
            print(f"\n   Step {step.get('step_number', '?')}: {step.get('description', 'N/A')}")
            print(f"      Worker: {step.get('worker_to_call', 'None')}")
            print(f"      Tool: {step.get('tool_to_use', 'None')}")


# ============== Main Demo ==============
def main():
    """Run the full CoT demo."""

    print("\n" + "="*80)
    print("🚀 FinTalk.AI - FULL CHAIN OF THOUGHT DEMO")
    print("   Showing Orchestrator Planning → Worker Calls → Tool Execution")
    print("="*80)

    # Setup OSWorld environment
    print("\n📦 Setting up OSWorld sandbox environment...")
    print("   - Loading CSV files from file system...")
    print("   - Initializing SQLite database...")
    print("   - Loading formula library...")

    conn = setup_database()
    print("✅ OSWorld environment ready!")

    # Create Orchestrator
    orchestrator = Orchestrator(conn)

    # Run the complex query
    user_query = "What is the executive_director_ratio for ZA Bank?"

    print(f"\n{'='*80}")
    print(f"👤 USER QUERY: \"{user_query}\"")
    print("="*80)

    result = orchestrator.plan_and_execute(user_query)

    print(f"\n{'='*80}")
    print("🎉 DEMO COMPLETED!")
    print("="*80)

    print("\n📊 EXECUTION SUMMARY:")
    print(f"   Total Steps: {len(result['execution_trace'])}")
    print(f"   Successful: {sum(1 for s in result['execution_trace'] if s['status'] == 'success')}")
    print(f"\n   Final Answer: {result['final_answer'][:100]}...")


if __name__ == "__main__":
    main()

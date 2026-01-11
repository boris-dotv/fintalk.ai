#!/usr/bin/env python3
"""
FinTalk.AI - OSWorld Integrated Demo
Demonstrates FinTalk.AI running in OSWorld secure sandbox environment.

This demo shows:
1. OSWorld environment initialization
2. Sandboxed SQL execution
3. Safe formula calculations
4. Multi-agent orchestration in OSWorld
5. Standardized task evaluation
"""

import os
import sys
import json
import time
import logging
from typing import Dict, Any, List

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Add OSWorld to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'OSWorld')))

from formula import find_formula_for_query, calculate_from_expression
from OSWorld.osworld_adapter import FinTalkOSWorldAdapter, SAMPLE_TASKS

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ============== LLM API Configuration ==============
API_URL = "https://qianfan.baidubce.com/v2/chat/completions"
API_KEY = "bce-v3/ALTAK-dgZMQj7E5tByoRofFKlbM/e852481aaab5ebf3ffe6f2a50589e6e41646c127"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

def call_llm(prompt: str, temperature: float = 0.3, timeout: int = 30) -> str:
    """Call LLM API."""
    import requests
    payload = {
        "model": "deepseek-v3.2-think",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "web_search": {"enable": False}
    }
    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=timeout)
        return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"LLM API error: {e}")
        return f"ERROR: {str(e)}"


# ============== FinTalk Agent with OSWorld ==============
class FinTalkAgent:
    """
    FinTalk.AI Agent running in OSWorld environment.
    """

    def __init__(self, use_osworld: bool = True):
        """
        Initialize the agent with OSWorld adapter.

        Args:
            use_osworld: Whether to use OSWorld (fallback to local if unavailable)
        """
        logger.info("🤖 Initializing FinTalk.AI Agent...")

        # Initialize OSWorld adapter
        self.adapter = FinTalkOSWorldAdapter(use_osworld=use_osworld)

        # Database schema for context
        self.db_schema = """
DATABASE SCHEMA:
- companies(company_sort_id, name, employee_size, website, tech_summary)
- management(management_id, company_sort_id, management_name, management_title, director_type)
- shareholders(shareholder_id, company_sort_id, shareholder_name, share_percentage, shareholder_tag)
"""

        logger.info("✅ FinTalk.AI Agent initialized")

    def process_query(self, user_query: str, show_steps: bool = True) -> Dict[str, Any]:
        """
        Process a user query using multi-agent orchestration.

        Args:
            user_query: Natural language query
            show_steps: Whether to show step-by-step execution

        Returns:
            Query results and metadata
        """
        if show_steps:
            print("\n" + "="*80)
            print("🤖 FinTalk.AI - OSWorld Integrated Agent")
            print("="*80)
            print(f"\n👤 User Query: \"{user_query}\"")
            print(f"\n🖥️  Environment: {'OSWorld Sandbox' if self.adapter.use_osworld else 'Local SQLite'}")

        # Step 1: Create execution plan using LLM
        if show_steps:
            print("\n" + "="*80)
            print("🧠 STEP 1: Creating Execution Plan")
            print("="*80)

        plan = self._create_plan(user_query)

        if show_steps:
            print(f"\n📋 Execution Plan:")
            print(f"   Reasoning: {plan['reasoning']}")
            print(f"   Steps: {len(plan['steps'])}")
            for i, step in enumerate(plan['steps'], 1):
                print(f"   {i}. {step['description']}")

        # Step 2: Execute plan step by step
        if show_steps:
            print("\n" + "="*80)
            print("⚙️  STEP 2: Executing Plan in OSWorld Environment")
            print("="*80)

        results = {"user_query": user_query, "environment": "OSWorld" if self.adapter.use_osworld else "Local"}

        for i, step in enumerate(plan["steps"], 1):
            if show_steps:
                print(f"\n📍 Executing Step {i}/{len(plan['steps'])}: {step['description']}")
                print("   " + "-"*76)

            time.sleep(0.3)  # Visibility

            step_result = self._execute_step(step, results, show_steps=show_steps)
            results.update(step_result)

        # Step 3: Generate final answer
        if show_steps:
            print("\n" + "="*80)
            print("✨ STEP 3: Generating Final Answer")
            print("="*80)

        final_answer = self._generate_final_answer(user_query, results, show_steps=show_steps)

        results["final_answer"] = final_answer

        if show_steps:
            print("\n" + "="*80)
            print("🎉 Query Completed in OSWorld Environment")
            print("="*80)

        return results

    def _create_plan(self, user_query: str) -> Dict[str, Any]:
        """Create execution plan using LLM."""
        planning_prompt = f"""You are FinTalk.AI's Orchestrator. Create a step-by-step execution plan for this query.

{self.db_schema}

User Query: "{user_query}"

Create a JSON plan with this structure:
{{
    "reasoning": "step-by-step reasoning",
    "steps": [
        {{"step_number": 1, "description": "...", "sql": "SELECT ...", "purpose": "..."}},
        {{"step_number": 2, "description": "...", "sql": "SELECT ...", "purpose": "..."}},
        {{"step_number": 3, "description": "...", "action": "calculate_formula", "formula": "...", "purpose": "..."}}
    ]
}}

Respond with ONLY valid JSON."""

        response = call_llm(planning_prompt, temperature=0.5)

        # Parse JSON response
        try:
            response = response.replace("```json", "").replace("```", "").strip()
            if "{" in response and "}" in response:
                start = response.index("{")
                end = response.rindex("}") + 1
                plan = json.loads(response[start:end])
                return plan
        except:
            pass

        # Fallback plan
        return self._fallback_plan(user_query)

    def _fallback_plan(self, user_query: str) -> Dict[str, Any]:
        """Fallback plan if LLM fails."""
        if "executive_director_ratio" in user_query.lower():
            return {
                "reasoning": "Calculate executive director ratio",
                "steps": [
                    {"step_number": 1, "description": "Find company ID", "sql": "SELECT company_sort_id FROM companies WHERE name LIKE '%ZA Bank%'", "purpose": "Get company identifier"},
                    {"step_number": 2, "description": "Count executive directors", "sql": "SELECT COUNT(*) FROM management WHERE company_sort_id = 1 AND director_type LIKE '%Executive%'", "purpose": "Get executive count"},
                    {"step_number": 3, "description": "Count total directors", "sql": "SELECT COUNT(*) FROM management WHERE company_sort_id = 1 AND director_type IS NOT NULL", "purpose": "Get total director count"},
                    {"step_number": 4, "description": "Calculate ratio", "action": "calculate_formula", "formula": "executive_director_ratio", "purpose": "Apply formula"}
                ]
            }
        else:
            return {
                "reasoning": "Retrieve company information",
                "steps": [
                    {"step_number": 1, "description": "Query company data", "sql": "SELECT * FROM companies WHERE name LIKE '%ZA Bank%'", "purpose": "Get company information"}
                ]
            }

    def _execute_step(self, step: Dict, results: Dict, show_steps: bool = True) -> Dict[str, Any]:
        """Execute a single step in the plan."""
        result = {}

        # SQL execution step
        if "sql" in step:
            sql = step["sql"]
            if show_steps:
                print(f"   🔧 Executing SQL in {'OSWorld' if self.adapter.use_osworld else 'Local'}:")
                print(f"   SQL: {sql[:100]}...")

            query_result = self.adapter.execute_sql(sql)

            if query_result is None:
                print(f"   ❌ SQL execution failed")
                return {"error": "SQL execution failed"}

            if show_steps:
                print(f"   ✅ Result: {len(query_result)} rows")
                if query_result and len(query_result) > 0:
                    print(f"   📊 Sample: {query_result[0]}")

            result["query_result"] = query_result

            # Store intermediate data for subsequent steps
            if "company_sort_id" in query_result[0]:
                results["company_id"] = query_result[0]["company_sort_id"]

            if "executive_count" in str(sql).lower():
                results["executive_count"] = query_result[0].get("COUNT(*)", query_result[0].get("executive_count", 0))

            if "total_directors" in str(sql).lower() or "COUNT" in str(sql):
                results["total_directors"] = query_result[0].get("COUNT(*)", query_result[0].get("total_directors", 0))

        # Formula calculation step
        elif step.get("action") == "calculate_formula":
            formula_name = step.get("formula", "")

            if show_steps:
                print(f"   🧮 Calculating formula: {formula_name}")
                print(f"   Values: {results}")

            try:
                # Get formula
                _, expression, _ = find_formula_for_query(formula_name)

                # Prepare values
                if "executive_director_ratio" in formula_name:
                    values = {
                        "Count of Executive Directors": results.get("executive_count", 0),
                        "Total Count of Directors": results.get("total_directors", 1)
                    }

                result_value = calculate_from_expression(expression, values)

                if show_steps:
                    print(f"   ✅ Calculation result: {result_value:.4f} ({result_value:.2%})")

                result["calculation_result"] = result_value

            except Exception as e:
                if show_steps:
                    print(f"   ❌ Calculation error: {e}")
                result["error"] = str(e)

        return result

    def _generate_final_answer(self, user_query: str, results: Dict, show_steps: bool = True) -> str:
        """Generate natural language answer using LLM."""
        if show_steps:
            print(f"\n   🤖 Calling LLM to synthesize answer...")
            print("   (This may take a few seconds...)")

        synthesis_prompt = f"""Based on the OSWorld sandbox execution results, provide a professional answer.

Query: {user_query}

Results: {json.dumps(results, indent=2, default=str)}

Provide a clear, professional answer with:
1. Direct answer to the question
2. Supporting data
3. Business context if relevant

Answer:"""

        answer = call_llm(synthesis_prompt, temperature=0.7)

        if show_steps:
            print(f"\n   ✅ Answer:")
            print(f"\n🤖 {answer}")

        return answer

    def evaluate_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate a task using OSWorld's evaluation framework.

        Args:
            task: Task definition in OSWorld format

        Returns:
            Evaluation results
        """
        print("\n" + "="*80)
        print("📊 OSWorld Task Evaluation")
        print("="*80)
        print(f"\nTask ID: {task.get('id', 'unknown')}")
        print(f"Instruction: {task.get('instruction', '')}")

        # Process the query
        result = self.process_query(
            task['instruction'],
            show_steps=False
        )

        # Evaluate
        evaluation = self.adapter.evaluate_task(
            task_id=task['id'],
            instruction=task['instruction'],
            expected_output=task.get('evaluator', {}).get('expected')
        )

        print(f"\n✅ Evaluation Complete:")
        print(f"   Status: {evaluation.get('status', 'unknown')}")
        print(f"   Metrics: {evaluation.get('metrics', {})}")

        return evaluation

    def close(self):
        """Close the agent and cleanup OSWorld environment."""
        print("\n🧹 Cleaning up...")
        self.adapter.close()
        print("✅ Cleanup complete")


# ============== Demo Functions ==============
def demo_basic_query():
    """Demonstrate basic query in OSWorld."""
    print("\n" + "="*80)
    print("🎯 DEMO 1: Basic Data Retrieval in OSWorld")
    print("="*80)

    agent = FinTalkAgent(use_osworld=True)

    query = "What is the employee size of ZA Bank?"
    result = agent.process_query(query)

    agent.close()


def demo_complex_query():
    """Demonstrate complex query with formula calculation in OSWorld."""
    print("\n" + "="*80)
    print("🎯 DEMO 2: Complex Query with Formula in OSWorld")
    print("="*80)

    agent = FinTalkAgent(use_osworld=True)

    query = "Calculate the executive_director_ratio for ZA Bank"
    result = agent.process_query(query)

    agent.close()


def demo_task_evaluation():
    """Demonstrate OSWorld task evaluation."""
    print("\n" + "="*80)
    print("🎯 DEMO 3: OSWorld Task Evaluation")
    print("="*80)

    agent = FinTalkAgent(use_osworld=True)

    # Use predefined task
    task = SAMPLE_TASKS[0].to_osworld_format()
    evaluation = agent.evaluate_task(task)

    agent.close()


# ============== Main ==============
def main():
    """Run OSWorld integrated demo."""
    print("\n" + "="*80)
    print("🚀 FinTalk.AI - OSWorld Integrated Demo")
    print("   Multi-Agent Financial Q&A in OSWorld Secure Sandbox")
    print("="*80)

    print("\n📋 Available Demos:")
    print("   1. Basic Query (Data Retrieval)")
    print("   2. Complex Query (Formula Calculation)")
    print("   3. Task Evaluation (OSWorld Framework)")
    print("   4. Run All")

    choice = input("\nSelect demo (1-4): ").strip()

    if choice == "1":
        demo_basic_query()
    elif choice == "2":
        demo_complex_query()
    elif choice == "3":
        demo_task_evaluation()
    elif choice == "4":
        demo_basic_query()
        demo_complex_query()
        demo_task_evaluation()
    else:
        print("Running default demo (Basic Query)...")
        demo_basic_query()

    print("\n\n✅ Demo completed!")
    print("\n💡 Key Features Demonstrated:")
    print("   ✅ OSWorld environment initialization")
    print("   ✅ Sandboxed SQL execution")
    print("   ✅ Safe formula calculations")
    print("   ✅ Multi-agent orchestration")
    print("   ✅ Standardized task evaluation")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Demo interrupted by user")
    except Exception as e:
        logger.error(f"Demo error: {e}")
        import traceback
        traceback.print_exc()

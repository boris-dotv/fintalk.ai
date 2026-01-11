#!/usr/bin/env python3
"""
FinTalk.AI - Docker OSWorld Integrated Demo
This demo shows FinTalk.AI running in a Docker-based OSWorld sandbox.

Features:
1. Docker container isolation for security
2. Sandboxed SQL execution
3. Safe formula calculations
4. Multi-agent orchestration
5. Automatic fallback to local mode if Docker fails
"""

import os
import sys
import json
import time
import logging
from typing import Dict, Any, List

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from formula import find_formula_for_query, calculate_from_expression
from OSWorld.docker_osworld_adapter import DockerOSWorldAdapter

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# LLM API
API_URL = "https://qianfan.baidubce.com/v2/chat/completions"
API_KEY = "bce-v3/ALTAK-dgZMQj7E5tByoRofFKlbM/e852481aaab5ebf3ffe6f2a50589e6e41646c127"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

def call_llm(prompt: str, temperature: float = 0.3) -> str:
    """Call LLM API."""
    import requests
    payload = {
        "model": "deepseek-v3.2-think",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "web_search": {"enable": False}
    }
    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
        return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"LLM API error: {e}")
        return f"ERROR: {str(e)}"


class FinTalkDockerAgent:
    """
    FinTalk.AI Agent with Docker OSWorld integration.
    """

    def __init__(self):
        """Initialize the agent with Docker OSWorld adapter."""
        print("\n🤖 Initializing FinTalk.AI Agent with Docker OSWorld...")

        # Initialize Docker OSWorld adapter
        self.adapter = DockerOSWorldAdapter()

        # Get environment info
        self.env_info = self.adapter.get_container_info()
        self.env_mode = "Docker Container" if self.env_info else "Local SQLite"

        print(f"✅ Agent initialized in {self.env_mode} mode")

        if self.env_info:
            print(f"   Container: {self.env_info['name']}")
            print(f"   Status: {self.env_info['status']}")

    def process_query(self, user_query: str) -> Dict[str, Any]:
        """Process a user query with full orchestration."""
        print("\n" + "="*80)
        print("🤖 FinTalk.AI - Docker OSWorld Agent")
        print("="*80)
        print(f"\n👤 Query: \"{user_query}\"")
        print(f"🐳 Environment: {self.env_mode}")

        # Step 1: Planning
        print("\n" + "="*80)
        print("🧠 STEP 1: Creating Execution Plan")
        print("="*80)

        plan = self._create_plan(user_query)

        print(f"\n📋 Plan:")
        print(f"   Reasoning: {plan['reasoning']}")
        print(f"   Steps: {len(plan['steps'])}")
        for i, step in enumerate(plan['steps'], 1):
            print(f"   {i}. {step['description']}")

        # Step 2: Execution
        print("\n" + "="*80)
        print(f"⚙️  STEP 2: Executing in {self.env_mode}")
        print("="*80)

        results = {"query": user_query, "environment": self.env_mode}

        for i, step in enumerate(plan["steps"], 1):
            print(f"\n📍 Step {i}/{len(plan['steps'])}: {step['description']}")
            print("   " + "-"*76)
            time.sleep(0.3)

            step_result = self._execute_step(step, results)
            results.update(step_result)

        # Step 3: Final Answer
        print("\n" + "="*80)
        print("✨ STEP 3: Generating Final Answer")
        print("="*80)

        final_answer = self._generate_final_answer(user_query, results)
        results["final_answer"] = final_answer

        print("\n" + "="*80)
        print("🎉 Query Completed")
        print("="*80)

        return results

    def _create_plan(self, user_query: str) -> Dict:
        """Create execution plan using LLM."""
        planning_prompt = f"""Create a step-by-step execution plan for this query.

Database Schema:
- companies(company_sort_id, name, employee_size)
- management(management_id, company_sort_id, management_name, director_type)
- shareholders(shareholder_id, company_sort_id, shareholder_name, share_percentage)

Query: "{user_query}"

Return JSON:
{{
    "reasoning": "step-by-step reasoning",
    "steps": [
        {{"step_number": 1, "description": "...", "sql": "SELECT ...", "purpose": "..."}},
        ...
    ]
}}"""

        response = call_llm(planning_prompt, temperature=0.5)

        try:
            response = response.replace("```json", "").replace("```", "").strip()
            if "{" in response and "}" in response:
                start = response.index("{")
                end = response.rindex("}") + 1
                return json.loads(response[start:end])
        except:
            pass

        return self._fallback_plan(user_query)

    def _fallback_plan(self, query: str) -> Dict:
        """Fallback plan."""
        if "executive_director_ratio" in query.lower():
            return {
                "reasoning": "Calculate executive director ratio",
                "steps": [
                    {"step_number": 1, "description": "Find company ID", "sql": "SELECT company_sort_id FROM companies WHERE name LIKE '%ZA Bank%'", "purpose": "Get company identifier"},
                    {"step_number": 2, "description": "Count executive directors", "sql": "SELECT COUNT(*) as count FROM management WHERE company_sort_id = 1 AND director_type LIKE '%Executive%'", "purpose": "Get executive count"},
                    {"step_number": 3, "description": "Count total directors", "sql": "SELECT COUNT(*) as count FROM management WHERE company_sort_id = 1 AND director_type IS NOT NULL", "purpose": "Get total directors"},
                    {"step_number": 4, "description": "Calculate ratio", "action": "calculate_formula", "formula": "executive_director_ratio", "purpose": "Apply formula"}
                ]
            }
        else:
            return {
                "reasoning": "Retrieve data",
                "steps": [
                    {"step_number": 1, "description": "Query company data", "sql": "SELECT * FROM companies WHERE name LIKE '%ZA Bank%'", "purpose": "Get company info"}
                ]
            }

    def _execute_step(self, step: Dict, results: Dict) -> Dict:
        """Execute a step."""
        result = {}

        if "sql" in step:
            sql = step["sql"]
            print(f"   🔧 SQL: {sql[:80]}...")

            query_result = self.adapter.execute_sql(sql)

            if query_result is None:
                print(f"   ❌ Query failed")
                return {"error": "Query failed"}

            print(f"   ✅ Result: {len(query_result)} rows")
            if query_result:
                print(f"   📊 Sample: {query_result[0]}")

            result["query_result"] = query_result

            # Store intermediate data
            if query_result and "company_sort_id" in query_result[0]:
                results["company_id"] = query_result[0]["company_sort_id"]

            if query_result and "count" in query_result[0]:
                if "executive" in step.get("description", "").lower():
                    results["executive_count"] = query_result[0]["count"]
                else:
                    results["total_directors"] = query_result[0]["count"]

        elif step.get("action") == "calculate_formula":
            formula_name = step.get("formula", "")
            print(f"   🧮 Formula: {formula_name}")

            try:
                _, expression, _ = find_formula_for_query(formula_name)

                values = {
                    "Count of Executive Directors": results.get("executive_count", 0),
                    "Total Count of Directors": results.get("total_directors", 1)
                }

                result_value = calculate_from_expression(expression, values)
                print(f"   ✅ Result: {result_value:.4f} ({result_value:.2%})")

                result["calculation_result"] = result_value

            except Exception as e:
                print(f"   ❌ Error: {e}")
                result["error"] = str(e)

        return result

    def _generate_final_answer(self, query: str, results: Dict) -> str:
        """Generate final answer."""
        print(f"\n   🤖 Synthesizing answer...")

        synthesis_prompt = f"""Based on the query execution, provide a professional answer.

Query: {query}
Results: {json.dumps(results, indent=2, default=str)}

Provide a clear, professional answer."""

        answer = call_llm(synthesis_prompt, temperature=0.7)

        print(f"\n🤖 {answer}")

        return answer

    def close(self):
        """Cleanup."""
        print("\n🧹 Cleaning up...")
        self.adapter.close()
        print("✅ Cleanup complete")


# ============== Demo Functions ==============
def demo_basic():
    """Basic query demo."""
    agent = FinTalkDockerAgent()
    agent.process_query("What is the employee size of ZA Bank?")
    agent.close()


def demo_complex():
    """Complex query with formula."""
    agent = FinTalkDockerAgent()
    agent.process_query("Calculate the executive_director_ratio for ZA Bank")
    agent.close()


def demo_comparison():
    """Comparison query."""
    agent = FinTalkDockerAgent()

    query = "Compare the top_3_shareholder_concentration between ZA Bank and WeLab Bank"
    agent.process_query(query)

    agent.close()


# ============== Main ==============
def main():
    """Run demo."""
    print("\n" + "="*80)
    print("🚀 FinTalk.AI - Docker OSWorld Demo")
    print("   Multi-Agent Financial Q&A in Docker Sandbox")
    print("="*80)

    print("\n📋 Demos:")
    print("   1. Basic Query")
    print("   2. Complex Query (Formula)")
    print("   3. Comparison Query")
    print("   4. Run All")

    choice = input("\nSelect demo (1-4): ").strip()

    if choice == "1":
        demo_basic()
    elif choice == "2":
        demo_complex()
    elif choice == "3":
        demo_comparison()
    elif choice == "4":
        demo_basic()
        demo_complex()
        demo_comparison()
    else:
        print("Running basic demo...")
        demo_basic()

    print("\n\n✅ Demo completed!")
    print("\n💡 Features:")
    print("   ✅ Docker container sandbox")
    print("   ✅ Sandboxed SQL execution")
    print("   ✅ Safe formula calculations")
    print("   ✅ Multi-agent orchestration")
    print("   ✅ Automatic fallback")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted")
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()

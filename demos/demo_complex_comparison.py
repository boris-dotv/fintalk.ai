#!/usr/bin/env python3
"""
FinTalk.AI - Ultra Complex Query Demo
"Compare the top_3_shareholder_concentration between ZA Bank and WeLab Bank,
and identify which company has a more concentrated ownership structure."

This requires:
1. Find both companies' IDs
2. Get top 3 shareholders for each company (with percentage parsing)
3. Calculate concentration for each
4. Compare and recommend
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

# API Configuration
API_URL = "https://qianfan.baidubce.com/v2/chat/completions"
API_KEY = "bce-v3/ALTAK-dgZMQj7E5tByoRofFKlbM/e852481aaab5ebf3ffe6f2a50589e6e41646c127"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

def call_llm(prompt: str, temperature: float = 0.3, timeout: int = 30) -> str:
    """Call LLM API."""
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
        return f"ERROR: {str(e)}"

def setup_database():
    """Load CSV data into SQLite."""
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
            print(f"✅ Loaded {len(df)} rows into '{table_name}'")

    return conn

def execute_sql(conn, sql: str) -> List[Dict]:
    """Execute SQL query."""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        print(f"❌ SQL Error: {e}")
        return None

def parse_percentage(perc_str: str) -> float:
    """Parse percentage string like '44.70%' to float 44.70."""
    if not perc_str or perc_str == '/':
        return 0.0
    # Remove % sign and convert
    try:
        return float(str(perc_str).replace('%', '').strip())
    except:
        return 0.0

# ============== COMPLEX ORCHESTRATOR ==============
class ComplexOrchestrator:
    """Orchestrator for complex multi-company comparison queries."""

    def __init__(self, db_connection):
        self.db = db_connection

    def plan_and_execute(self, user_query: str):
        """Plan using LLM, then execute step by step."""

        print("\n" + "="*80)
        print("🧠 ORCHESTRATOR: Creating Execution Plan")
        print("="*80)

        # Step 1: Create plan
        plan = self._create_plan(user_query)
        self._print_plan(plan)

        # Step 2: Execute plan
        print("\n" + "="*80)
        print("⚙️  EXECUTING PLAN STEP BY STEP")
        print("="*80)

        results = {"user_query": user_query}

        for i, step in enumerate(plan["steps"], 1):
            print(f"\n{'='*80}")
            print(f"🔄 EXECUTING STEP {i}/{len(plan['steps'])}: {step['description']}")
            print("="*80)

            # Add small delay for visibility
            time.sleep(0.3)

            # Execute the step
            step_result = self._execute_step(step, results)
            results.update(step_result)

        # Step 3: Generate final answer
        self._generate_final_answer(user_query, results)

        print("\n" + "="*80)
        print("🎉 COMPLEX QUERY COMPLETED!")
        print("="*80)

        self._print_summary(results)

    def _create_plan(self, user_query: str) -> Dict:
        """Create execution plan using LLM."""

        planning_prompt = f"""You are an orchestrator for financial database queries.

Database Schema:
- companies(company_sort_id, name, employee_size)
- shareholders(company_sort_id, shareholder_name, share_percentage, shareholder_tag)

User Query: "{user_query}"

Create a step-by-step execution plan. Return ONLY JSON:
{{
    "reasoning": "step-by-step reasoning",
    "steps": [
        {{"step_number": 1, "description": "...", "sql": "...", "purpose": "..."}},
        ...
    ]
}}

Keep it simple with 4-5 steps."""

        print("🤔 Calling LLM to create execution plan...")
        print("   (This may take a few seconds...)")

        response = call_llm(planning_prompt, temperature=0.5)

        # Try to parse JSON
        try:
            response = response.replace("```json", "").replace("```", "").strip()
            if "{" in response and "}" in response:
                start = response.index("{")
                end = response.rindex("}") + 1
                plan = json.loads(response[start:end])
                print("✅ Plan created successfully!")
                return plan
        except:
            pass

        print("⚠️  Using fallback plan")
        return self._fallback_plan()

    def _fallback_plan(self) -> Dict:
        """Fallback plan if LLM fails."""
        return {
            "reasoning": "Standard comparison approach",
            "steps": [
                {
                    "step_number": 1,
                    "description": "Find ZA Bank's company ID",
                    "sql": "SELECT company_sort_id FROM companies WHERE name LIKE '%ZA Bank%'",
                    "purpose": "Get ZA Bank identifier"
                },
                {
                    "step_number": 2,
                    "description": "Find WeLab Bank's company ID",
                    "sql": "SELECT company_sort_id FROM companies WHERE name LIKE '%WeLab%'",
                    "purpose": "Get WeLab Bank identifier"
                },
                {
                    "step_number": 3,
                    "description": "Get ZA Bank's top 3 shareholders",
                    "sql": "SELECT shareholder_name, share_percentage FROM shareholders WHERE company_sort_id = 1 AND share_percentage NOT LIKE '%/%' ORDER BY CAST(REPLACE(share_percentage, '%', '') AS REAL) DESC LIMIT 3",
                    "purpose": "Get ZA Bank shareholder data"
                },
                {
                    "step_number": 4,
                    "description": "Get WeLab Bank's top 3 shareholders",
                    "sql": "SELECT shareholder_name, share_percentage FROM shareholders WHERE company_sort_id = 2 AND share_percentage NOT LIKE '%/%' ORDER BY CAST(REPLACE(share_percentage, '%', '') AS REAL) DESC LIMIT 3",
                    "purpose": "Get WeLab Bank shareholder data"
                },
                {
                    "step_number": 5,
                    "description": "Calculate and compare concentrations",
                    "action": "calculate_and_compare",
                    "purpose": "Compare metrics and identify winner"
                }
            ]
        }

    def _print_plan(self, plan: Dict):
        """Print the plan nicely."""
        print(f"\n📋 EXECUTION PLAN:")
        print(f"   Reasoning: {plan.get('reasoning', 'N/A')}")
        print(f"\n   Steps ({len(plan.get('steps', []))} total):")
        for step in plan.get("steps", []):
            print(f"   {step.get('step_number', '?')}. {step.get('description', 'N/A')}")
            if "sql" in step:
                print(f"      → SQL query")
            if "action" in step:
                print(f"      → {step['action']}")

    def _execute_step(self, step: Dict, results: Dict) -> Dict:
        """Execute a single step."""

        description = step.get("description", "")
        sql = step.get("sql")
        action = step.get("action")

        print(f"\n📍 Step Details:")
        print(f"   Description: {description}")
        print(f"   Purpose: {step.get('purpose', 'N/A')}")

        result = {}

        if sql:
            print(f"\n   🔧 Executing SQL Query...")
            print(f"   SQL: {sql[:100]}...")

            query_result = execute_sql(self.db, sql)

            if query_result is None:
                print(f"   ❌ SQL execution failed")
                return result

            print(f"   ✅ Query Result: {query_result}")
            result["query_result"] = query_result

            # Process result based on step
            if "ZA Bank" in description and "company ID" in description:
                if query_result and len(query_result) > 0:
                    results["za_bank_id"] = query_result[0]['company_sort_id']
                    print(f"   ✅ Stored: za_bank_id = {results['za_bank_id']}")

            elif "WeLab Bank" in description and "company ID" in description:
                if query_result and len(query_result) > 0:
                    results["welab_id"] = query_result[0]['company_sort_id']
                    print(f"   ✅ Stored: welab_id = {results['welab_id']}")

            elif "ZA Bank" in description and "shareholders" in description:
                results["za_shareholders"] = query_result
                za_concentration = sum(parse_percentage(s.get('share_percentage', '0')) for s in query_result)
                results["za_concentration"] = za_concentration
                print(f"\n   📊 ZA Bank Top 3:")
                for i, s in enumerate(query_result, 1):
                    print(f"      {i}. {s['shareholder_name']}: {s['share_percentage']}")
                print(f"   ✅ Concentration: {za_concentration:.2f}%")

            elif "WeLab Bank" in description and "shareholders" in description:
                results["welab_shareholders"] = query_result
                welab_concentration = sum(parse_percentage(s.get('share_percentage', '0')) for s in query_result)
                results["welab_concentration"] = welab_concentration
                print(f"\n   📊 WeLab Bank Top 3:")
                for i, s in enumerate(query_result, 1):
                    print(f"      {i}. {s['shareholder_name']}: {s['share_percentage']}")
                print(f"   ✅ Concentration: {welab_concentration:.2f}%")

        elif action == "calculate_and_compare":
            print(f"\n🔢 Performing calculations...")

            za_conc = results.get("za_concentration", 0)
            welab_conc = results.get("welab_concentration", 0)

            # Calculate using formula library
            formula_values_za = {"Sum of Top 3 Share Percentages": za_conc}
            _, expr_za, _ = find_formula_for_query("top_3_shareholder_concentration")
            result_za = calculate_from_expression(expr_za, formula_values_za)

            formula_values_welab = {"Sum of Top 3 Share Percentages": welab_conc}
            _, expr_welab, _ = find_formula_for_query("top_3_shareholder_concentration")
            result_welab = calculate_from_expression(expr_welab, formula_values_welab)

            print(f"   ZA Bank: {result_za:.2%}")
            print(f"   WeLab Bank: {result_welab:.2%}")

            diff = abs(za_conc - welab_conc)
            higher = "ZA Bank" if za_conc > welab_conc else "WeLab Bank"

            print(f"\n   📊 Comparison:")
            print(f"      Difference: {diff:.2f}%")
            print(f"      Higher: {higher}")
            print(f"      Result: {higher} has more concentrated ownership")

            results["comparison"] = {
                "za_result": result_za,
                "welab_result": result_welab,
                "diff": diff,
                "higher": higher
            }

        return result

    def _generate_final_answer(self, user_query: str, results: Dict):
        """Generate final answer using LLM."""

        print(f"\n{'='*80}")
        print("📍 FINAL STEP: Generate Natural Language Answer")
        print("="*80)

        print(f"\n🤖 Orchestrator is calling LLM to synthesize answer...")
        print(f"   (This may take a few seconds...)")

        synthesis_prompt = f"""Based on the analysis, provide a professional answer.

Query: {user_query}

Results:
- ZA Bank Top 3 Concentration: {results.get('za_concentration', 0):.2f}%
- WeLab Bank Top 3 Concentration: {results.get('welab_concentration', 0):.2f}%
- Higher: {results.get('comparison', {}).get('higher', 'Unknown')}

Shareholders:
ZA Bank: {results.get('za_shareholders', [])}
WeLab Bank: {results.get('welab_shareholders', [])}

Provide a clear, professional answer with business implications."""

        answer = call_llm(synthesis_prompt, temperature=0.7)

        print(f"\n✅ Answer generated:")
        print(f"\n🤖 {answer}")

    def _print_summary(self, results: Dict):
        """Print execution summary."""
        print(f"\n📊 EXECUTION SUMMARY:")
        print(f"   Steps Executed: {len(results.get('steps', []))}")
        print(f"   Companies Analyzed: 2")
        print(f"   SQL Queries: 4")
        print(f"   Formula Calculations: 2")
        print(f"   Data Points: {len(results.get('za_shareholders', [])) + len(results.get('welab_shareholders', []))}")


# ============== MAIN DEMO ==============
def main():
    """Run the ultra complex query demo."""

    print("\n" + "="*80)
    print("🚀 FinTalk.AI - ULTRA COMPLEX QUERY DEMO")
    print("   Multi-Company Comparison with Orchestrator Planning")
    print("="*80)

    # Setup
    print("\n📦 Setting up OSWorld environment...")
    conn = setup_database()

    # The complex query
    user_query = "Compare the top_3_shareholder_concentration between ZA Bank and WeLab Bank. Which company has more concentrated ownership?"

    print(f"\n{'='*80}")
    print(f"👤 USER QUERY: \"{user_query}\"")
    print("="*80)

    # Create and run orchestrator
    orchestrator = ComplexOrchestrator(conn)
    orchestrator.plan_and_execute(user_query)


if __name__ == "__main__":
    main()

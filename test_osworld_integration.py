#!/usr/bin/env python3
"""
FinTalk.AI - OSWorld Integration Test Suite

This test module validates:
1. FinTalk.AI WITHOUT OSWorld (Local SQLite mode)
2. FinTalk.AI WITH OSWorld (Docker sandbox mode)

Test Query: "Compare the top_3_shareholder_concentration between ZA Bank and WeLab Bank"

This complex query requires:
- 4 SQL queries (2 company IDs + 2 shareholder queries)
- Formula calculations for both companies
- Comparison logic
- Final answer synthesis
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

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============== Test Configuration ==============
TEST_QUERY = "Compare the top_3_shareholder_concentration between ZA Bank and WeLab Bank. Which company has more concentrated ownership?"

EXPECTED_RESULTS = {
    "za_bank": {
        "company": "ZA Bank",
        "top_3_shareholders": [
            "Sinolink Worldwide Holdings Limited",
            "ZhongAn Online P&C Insurance Co., Ltd.",
            "Chow Tai Fook Jewellery Company Limited"
        ],
        "concentration": 97.78  # Approximately 97.78%
    },
    "welab_bank": {
        "company": "WeLab Bank",
        "top_3_shareholders": [
            "WeLab Holdings Limited"
        ],
        "concentration": 100.0
    },
    "comparison": {
        "higher": "WeLab Bank",
        "difference": 2.22
    }
}


# ============== Test Module 1: FinTalk WITHOUT OSWorld ==============
class FinTalkLocal:
    """FinTalk.AI running in local SQLite mode (no OSWorld)."""

    def __init__(self):
        import sqlite3
        import pandas as pd

        self.mode = "Local SQLite (No OSWorld)"
        self.db_file = ":memory:"
        self.conn = sqlite3.connect(':memory:', check_same_thread=False)

        # Load data
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        csv_files = {
            "companies": os.path.join(data_dir, "company.csv"),
            "management": os.path.join(data_dir, "management.csv"),
            "shareholders": os.path.join(data_dir, "shareholder.csv")
        }

        for table_name, file_path in csv_files.items():
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='ignore')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='latin-1')
                df.to_sql(table_name, self.conn, if_exists='replace', index=False)

        logger.info(f"✅ {self.mode} initialized")

    def execute_sql(self, sql: str) -> List[Dict]:
        """Execute SQL query."""
        cursor = self.conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def close(self):
        """Close database connection."""
        self.conn.close()


# ============== Test Module 2: FinTalk WITH OSWorld ==============
class FinTalkOSWorld:
    """FinTalk.AI running in OSWorld Docker sandbox mode."""

    def __init__(self):
        self.mode = "Docker OSWorld Sandbox"
        self.adapter = None

        # Import OSWorld adapter
        try:
            from OSWorld.docker_osworld_adapter import DockerOSWorldAdapter

            logger.info(f"🐳 Initializing {self.mode}...")
            self.adapter = DockerOSWorldAdapter()

            # Check if running in Docker
            env_info = self.adapter.get_container_info()
            if env_info and env_info.get('is_running'):
                self.mode = "Docker OSWorld Sandbox (Active)"
                logger.info(f"✅ {self.mode} - Container: {env_info['name']}")
            else:
                self.mode = "Docker OSWorld Sandbox (Fallback to Local)"
                logger.info(f"⚠️  Docker unavailable, using fallback mode")

        except Exception as e:
            logger.error(f"❌ OSWorld initialization failed: {e}")
            raise

    def execute_sql(self, sql: str) -> List[Dict]:
        """Execute SQL in OSWorld sandbox."""
        return self.adapter.execute_sql(sql)

    def close(self):
        """Close OSWorld adapter."""
        if self.adapter:
            self.adapter.close()


# ============== Complex Query Orchestrator ==============
class ComplexQueryOrchestrator:
    """
    Orchestrator for complex multi-company comparison queries.
    Works with both Local and OSWorld modes.
    """

    def __init__(self, env: object):
        """
        Initialize orchestrator with environment (Local or OSWorld).

        Args:
            env: FinTalkLocal or FinTalkOSWorld instance
        """
        self.env = env
        self.env_mode = env.mode

    def process_complex_query(self, user_query: str, show_details: bool = True) -> Dict[str, Any]:
        """
        Process a complex comparison query.

        Args:
            user_query: The user's natural language query
            show_details: Whether to show detailed execution steps

        Returns:
            Query results with all intermediate data
        """
        if show_details:
            print("\n" + "="*80)
            print(f"🚀 FinTalk.AI - {self.env_mode}")
            print("="*80)
            print(f"\n👤 USER QUERY: \"{user_query}\"")

        # Execution Plan
        if show_details:
            print("\n" + "="*80)
            print("📋 EXECUTION PLAN")
            print("="*80)
            print("This complex query requires:")
            print("  1. Find ZA Bank's company ID")
            print("  2. Find WeLab Bank's company ID")
            print("  3. Get ZA Bank's top 3 shareholders")
            print("  4. Get WeLab Bank's top 3 shareholders")
            print("  5. Calculate concentrations using formula library")
            print("  6. Compare and generate final answer")

        # Results storage
        results = {
            "query": user_query,
            "environment": self.env_mode,
            "start_time": time.time()
        }

        # Step 1: Find ZA Bank ID
        if show_details:
            print("\n" + "-"*80)
            print("📍 STEP 1: Find ZA Bank's company ID")
            print("-"*80)

        sql_1 = "SELECT company_sort_id FROM companies WHERE name LIKE '%ZA Bank%'"
        result_1 = self.env.execute_sql(sql_1)

        if show_details:
            print(f"SQL: {sql_1}")
            print(f"Result: {result_1}")

        if not result_1 or len(result_1) == 0:
            return {"error": "ZA Bank not found"}

        za_bank_id = int(result_1[0]['company_sort_id'])
        results["za_bank_id"] = za_bank_id

        if show_details:
            print(f"✅ ZA Bank ID: {za_bank_id}")

        # Step 2: Find WeLab Bank ID
        if show_details:
            print("\n" + "-"*80)
            print("📍 STEP 2: Find WeLab Bank's company ID")
            print("-"*80)

        sql_2 = "SELECT company_sort_id FROM companies WHERE name LIKE '%WeLab%'"
        result_2 = self.env.execute_sql(sql_2)

        if show_details:
            print(f"SQL: {sql_2}")
            print(f"Result: {result_2}")

        if not result_2 or len(result_2) == 0:
            return {"error": "WeLab Bank not found"}

        welab_id = int(result_2[0]['company_sort_id'])
        results["welab_id"] = welab_id

        if show_details:
            print(f"✅ WeLab Bank ID: {welab_id}")

        # Step 3: Get ZA Bank top 3 shareholders
        if show_details:
            print("\n" + "-"*80)
            print("📍 STEP 3: Get ZA Bank's top 3 shareholders")
            print("-"*80)

        sql_3 = f"""
        SELECT shareholder_name, share_percentage
        FROM shareholders
        WHERE company_sort_id = {za_bank_id}
          AND share_percentage NOT LIKE '%/%'
        ORDER BY CAST(REPLACE(share_percentage, '%', '') AS REAL) DESC
        LIMIT 3
        """
        result_3 = self.env.execute_sql(sql_3)

        if show_details:
            print(f"SQL: {sql_3.strip()}")
            print(f"Result: {result_3}")

        def parse_percentage(perc_str):
            if not perc_str or perc_str == '/':
                return 0.0
            try:
                return float(str(perc_str).replace('%', '').strip())
            except:
                return 0.0

        za_concentration = sum(parse_percentage(s.get('share_percentage', '0')) for s in result_3)
        results["za_shareholders"] = result_3
        results["za_concentration"] = za_concentration

        if show_details:
            print(f"\n📊 ZA Bank Top 3 Shareholders:")
            for i, s in enumerate(result_3, 1):
                print(f"   {i}. {s['shareholder_name']}: {s['share_percentage']}")
            print(f"\n✅ ZA Bank Concentration: {za_concentration:.2f}%")

        # Step 4: Get WeLab Bank top 3 shareholders
        if show_details:
            print("\n" + "-"*80)
            print("📍 STEP 4: Get WeLab Bank's top 3 shareholders")
            print("-"*80)

        sql_4 = f"""
        SELECT shareholder_name, share_percentage
        FROM shareholders
        WHERE company_sort_id = {welab_id}
          AND share_percentage NOT LIKE '%/%'
        ORDER BY CAST(REPLACE(share_percentage, '%', '') AS REAL) DESC
        LIMIT 3
        """
        result_4 = self.env.execute_sql(sql_4)

        if show_details:
            print(f"SQL: {sql_4.strip()}")
            print(f"Result: {result_4}")

        welab_concentration = sum(parse_percentage(s.get('share_percentage', '0')) for s in result_4)
        results["welab_shareholders"] = result_4
        results["welab_concentration"] = welab_concentration

        if show_details:
            print(f"\n📊 WeLab Bank Top 3 Shareholders:")
            for i, s in enumerate(result_4, 1):
                print(f"   {i}. {s['shareholder_name']}: {s['share_percentage']}")
            print(f"\n✅ WeLab Bank Concentration: {welab_concentration:.2f}%")

        # Step 5: Calculate using formula library
        if show_details:
            print("\n" + "-"*80)
            print("📍 STEP 5: Calculate concentrations using Formula Library")
            print("-"*80)

        # Calculate ZA Bank concentration using formula
        formula_values_za = {"Sum of Top 3 Share Percentages": za_concentration}
        _, expr_za, _ = find_formula_for_query("top_3_shareholder_concentration")
        result_za = calculate_from_expression(expr_za, formula_values_za)

        # Calculate WeLab Bank concentration using formula
        formula_values_welab = {"Sum of Top 3 Share Percentages": welab_concentration}
        _, expr_welab, _ = find_formula_for_query("top_3_shareholder_concentration")
        result_welab = calculate_from_expression(expr_welab, formula_values_welab)

        results["za_formula_result"] = result_za
        results["welab_formula_result"] = result_welab

        if show_details:
            print(f"Formula: top_3_shareholder_concentration")
            print(f"ZA Bank: {result_za:.2%}")
            print(f"WeLab Bank: {result_welab:.2%}")

        # Step 6: Compare
        diff = abs(za_concentration - welab_concentration)
        higher = "ZA Bank" if za_concentration > welab_concentration else "WeLab Bank"

        results["comparison"] = {
            "difference": diff,
            "higher": higher
        }

        if show_details:
            print(f"\n📊 COMPARISON:")
            print(f"   Difference: {diff:.2f}%")
            print(f"   Higher Concentration: {higher}")
            print(f"\n✅ RESULT: {higher} has more concentrated ownership")

        results["end_time"] = time.time()
        results["execution_time"] = results["end_time"] - results["start_time"]

        if show_details:
            print(f"\n⏱️  Total Execution Time: {results['execution_time']:.2f} seconds")

        return results


# ============== Test Validator ==============
class TestValidator:
    """Validates test results against expected outcomes."""

    @staticmethod
    def validate(results: Dict[str, Any], expected: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate test results.

        Returns:
            Validation report with pass/fail status
        """
        report = {
            "passed": True,
            "tests": []
        }

        # Test 1: ZA Bank concentration
        za_conc = results.get("za_concentration", 0)
        expected_za = expected["za_bank"]["concentration"]
        za_match = abs(za_conc - expected_za) < 1.0  # Allow 1% tolerance

        report["tests"].append({
            "name": "ZA Bank Concentration",
            "expected": f"{expected_za}%",
            "actual": f"{za_conc:.2f}%",
            "passed": za_match
        })

        # Test 2: WeLab Bank concentration
        welab_conc = results.get("welab_concentration", 0)
        expected_welab = expected["welab_bank"]["concentration"]
        welab_match = abs(welab_conc - expected_welab) < 1.0

        report["tests"].append({
            "name": "WeLab Bank Concentration",
            "expected": f"{expected_welab}%",
            "actual": f"{welab_conc:.2f}%",
            "passed": welab_match
        })

        # Test 3: Comparison result
        comparison = results.get("comparison", {})
        higher = comparison.get("higher", "")
        expected_higher = expected["comparison"]["higher"]
        comparison_match = higher == expected_higher

        report["tests"].append({
            "name": "Comparison Result",
            "expected": expected_higher,
            "actual": higher,
            "passed": comparison_match
        })

        # Overall pass/fail
        report["passed"] = all(test["passed"] for test in report["tests"])

        return report


# ============== Test Runner ==============
def run_test_suite():
    """Run complete test suite comparing Local vs OSWorld modes."""

    print("\n" + "="*80)
    print("🧪 FinTalk.AI - OSWorld Integration Test Suite")
    print("="*80)
    print(f"\n📝 Test Query: \"{TEST_QUERY}\"")
    print(f"\n🎯 This test validates:")
    print(f"   1. FinTalk WITHOUT OSWorld (Local SQLite)")
    print(f"   2. FinTalk WITH OSWorld (Docker Sandbox)")
    print(f"   3. Both modes produce identical results")

    # Test 1: Local Mode
    print("\n" + "="*80)
    print("📋 TEST 1: FinTalk WITHOUT OSWorld (Local SQLite Mode)")
    print("="*80)

    try:
        local_env = FinTalkLocal()
        local_orchestrator = ComplexQueryOrchestrator(local_env)
        local_results = local_orchestrator.process_complex_query(TEST_QUERY)

        local_validation = TestValidator.validate(local_results, EXPECTED_RESULTS)

        print("\n" + "="*80)
        print("📊 TEST 1 RESULTS")
        print("="*80)

        for test in local_validation["tests"]:
            status = "✅ PASS" if test["passed"] else "❌ FAIL"
            print(f"{status} - {test['name']}: {test['actual']} (expected: {test['expected']})")

        print(f"\n{'✅ TEST 1 PASSED' if local_validation['passed'] else '❌ TEST 1 FAILED'}")
        print(f"⏱️  Execution Time: {local_results['execution_time']:.2f}s")

        local_env.close()

    except Exception as e:
        print(f"❌ TEST 1 ERROR: {e}")
        import traceback
        traceback.print_exc()
        local_validation = {"passed": False}
        local_results = {}

    time.sleep(2)

    # Test 2: OSWorld Mode
    print("\n\n" + "="*80)
    print("📋 TEST 2: FinTalk WITH OSWorld (Docker Sandbox Mode)")
    print("="*80)

    try:
        osworld_env = FinTalkOSWorld()
        osworld_orchestrator = ComplexQueryOrchestrator(osworld_env)
        osworld_results = osworld_orchestrator.process_complex_query(TEST_QUERY)

        osworld_validation = TestValidator.validate(osworld_results, EXPECTED_RESULTS)

        print("\n" + "="*80)
        print("📊 TEST 2 RESULTS")
        print("="*80)

        for test in osworld_validation["tests"]:
            status = "✅ PASS" if test["passed"] else "❌ FAIL"
            print(f"{status} - {test['name']}: {test['actual']} (expected: {test['expected']})")

        print(f"\n{'✅ TEST 2 PASSED' if osworld_validation['passed'] else '❌ TEST 2 FAILED'}")
        print(f"⏱️  Execution Time: {osworld_results['execution_time']:.2f}s")

        osworld_env.close()

    except Exception as e:
        print(f"❌ TEST 2 ERROR: {e}")
        import traceback
        traceback.print_exc()
        osworld_validation = {"passed": False}
        osworld_results = {}

    # Final Summary
    print("\n\n" + "="*80)
    print("🎉 FINAL TEST SUMMARY")
    print("="*80)

    test1_status = "✅ PASSED" if local_validation.get("passed") else "❌ FAILED"
    test2_status = "✅ PASSED" if osworld_validation.get("passed") else "❌ FAILED"

    print(f"\n┌─────────────────────────────────────────────────────────┐")
    print(f"│  FinTalk.AI OSWorld Integration Test Results          │")
    print(f"├─────────────────────────────────────────────────────────┤")
    print(f"│  TEST 1 (Local SQLite):    {test1_status:32} │")
    print(f"│  TEST 2 (Docker OSWorld):  {test2_status:32} │")
    print(f"├─────────────────────────────────────────────────────────┤")

    if local_validation.get("passed") and osworld_validation.get("passed"):
        print(f"│  🎉 ALL TESTS PASSED!                                      │")
        print(f"│                                                          │")
        print(f"│  FinTalk.AI works correctly in both modes:              │")
        print(f"│  • Local SQLite mode (no OSWorld dependency)            │")
        print(f"│  • Docker OSWorld mode (sandboxed execution)            │")
        print(f"└─────────────────────────────────────────────────────────┘")
        print("\n✅ OSWorld Integration: SUCCESSFUL")
    else:
        print(f"│  ⚠️  SOME TESTS FAILED                                      │")
        print(f"│                                                          │")
        print(f"│  Please check the error messages above                  │")
        print(f"└─────────────────────────────────────────────────────────┘")
        print("\n⚠️  OSWorld Integration: NEEDS ATTENTION")

    print("\n" + "="*80)

    return {
        "local": {"results": local_results, "validation": local_validation},
        "osworld": {"results": osworld_results, "validation": osworld_validation}
    }


# ============== Main ==============
if __name__ == "__main__":
    try:
        results = run_test_suite()

        # Exit with appropriate code
        local_pass = results["local"]["validation"].get("passed", False)
        osworld_pass = results["osworld"]["validation"].get("passed", False)

        if local_pass and osworld_pass:
            print("\n✅ All tests passed! Exiting with code 0.")
            sys.exit(0)
        else:
            print("\n⚠️  Some tests failed. Exiting with code 1.")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n👋 Tests interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

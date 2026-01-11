#!/usr/bin/env python3
"""
Financial Function Registry - 金融Function注册表
定义和执行金融领域的Function Calling
"""

import logging
import json
from typing import Dict, Any, List, Optional
import sqlite3

logger = logging.getLogger(__name__)


# ============== 金融Function定义 ==============
FINANCIAL_FUNCTIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_company_info",
            "description": "获取公司的基本信息",
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "公司名称，如：ZA Bank, WeLab Bank"
                    }
                },
                "required": ["company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_executive_director_ratio",
            "description": "计算执行董事比率",
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {
                        "type": "string",
                        "description": "公司名称"
                    }
                },
                "required": ["company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_top_shareholders",
            "description": "获取前N大股东",
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {"type": "string"},
                    "top_n": {"type": "integer", "description": "前N大，默认3"}
                },
                "required": ["company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "calculate_shareholder_concentration",
            "description": "计算股东集中度",
            "parameters": {
                "type": "object",
                "properties": {
                    "company_name": {"type": "string"},
                    "top_n": {"type": "integer"}
                },
                "required": ["company_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "compare_companies",
            "description": "比较两个公司",
            "parameters": {
                "type": "object",
                "properties": {
                    "company1": {"type": "string"},
                    "company2": {"type": "string"},
                    "metric": {"type": "string", "description": "指标名称"}
                },
                "required": ["company1", "company2", "metric"]
            }
        }
    }
]


class FinancialFunctionRegistry:
    """金融Function注册表和执行器"""

    def __init__(self, db_connection=None, osworld_adapter=None):
        """
        初始化

        Args:
            db_connection: SQLite连接
            osworld_adapter: OSWorld适配器
        """
        self.db = db_connection
        self.osworld = osworld_adapter
        logger.info("✅ FinancialFunctionRegistry initialized")

    def get_functions(self) -> List[Dict]:
        """获取所有可用函数"""
        return FINANCIAL_FUNCTIONS

    def execute(self, function_name: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行function

        Args:
            function_name: 函数名
            parameters: 参数字典

        Returns:
            执行结果
        """
        logger.info(f"   🔧 Executing function: {function_name} with params {parameters}")

        try:
            if function_name == "get_company_info":
                return self._get_company_info(parameters["company_name"])

            elif function_name == "get_executive_director_ratio":
                return self._get_executive_director_ratio(parameters["company_name"])

            elif function_name == "get_top_shareholders":
                return self._get_top_shareholders(
                    parameters["company_name"],
                    parameters.get("top_n", 3)
                )

            elif function_name == "calculate_shareholder_concentration":
                return self._calculate_concentration(
                    parameters["company_name"],
                    parameters.get("top_n", 3)
                )

            elif function_name == "compare_companies":
                return self._compare_companies(
                    parameters["company1"],
                    parameters["company2"],
                    parameters["metric"]
                )

            else:
                return {"error": f"Unknown function: {function_name}"}

        except Exception as e:
            logger.error(f"Function execution error: {e}")
            return {"error": str(e)}

    def _get_company_id(self, company_name: str) -> Optional[int]:
        """获取公司ID"""
        company_map = {
            "za bank": 1,
            "welab bank": 2,
            "airstar bank": 3,
            "livo bank": 4,
            "mox bank": 5
        }

        for key, value in company_map.items():
            if key in company_name.lower():
                return value
        return None

    def _execute_sql(self, sql: str) -> List[Dict]:
        """执行SQL"""
        if self.osworld:
            return self.osworld.execute_sql(sql)
        else:
            cursor = self.db.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    def _get_company_info(self, company_name: str) -> Dict:
        """获取公司信息"""
        company_id = self._get_company_id(company_name)
        if not company_id:
            return {"error": f"Company not found: {company_name}"}

        result = self._execute_sql(
            f"SELECT * FROM companies WHERE company_sort_id = {company_id}"
        )

        if result:
            return {"company": company_name, "info": result[0], "status": "success"}
        return {"error": f"No data for {company_name}"}

    def _get_executive_director_ratio(self, company_name: str) -> Dict:
        """获取执行董事比率"""
        import sys
        sys.path.append("..")
        from formula import find_formula_for_query, calculate_from_expression

        company_id = self._get_company_id(company_name)
        if not company_id:
            return {"error": f"Company not found: {company_name}"}

        exec_result = self._execute_sql(
            f"SELECT COUNT(*) as count FROM management WHERE company_sort_id = {company_id} AND director_type LIKE '%Executive%'"
        )
        total_result = self._execute_sql(
            f"SELECT COUNT(*) as count FROM management WHERE company_sort_id = {company_id} AND director_type IS NOT NULL"
        )

        if exec_result and total_result:
            exec_count = exec_result[0]["count"]
            total_count = total_result[0]["count"]

            _, expression, _ = find_formula_for_query("executive_director_ratio")
            values = {
                "Count of Executive Directors": exec_count,
                "Total Count of Directors": total_count
            }
            ratio = calculate_from_expression(expression, values)

            return {
                "company": company_name,
                "executive_directors": exec_count,
                "total_directors": total_count,
                "ratio": ratio,
                "ratio_percentage": f"{ratio:.2%}",
                "status": "success"
            }
        return {"error": f"Failed to get data for {company_name}"}

    def _get_top_shareholders(self, company_name: str, top_n: int) -> Dict:
        """获取前N大股东"""
        company_id = self._get_company_id(company_name)
        if not company_id:
            return {"error": f"Company not found: {company_name}"}

        result = self._execute_sql(
            f"SELECT shareholder_name, share_percentage FROM shareholders WHERE company_sort_id = {company_id} AND share_percentage NOT LIKE '%/%' ORDER BY CAST(REPLACE(share_percentage, '%', '') AS REAL) DESC LIMIT {top_n}"
        )

        return {
            "company": company_name,
            "top_n_shareholders": result,
            "count": len(result) if result else 0,
            "status": "success"
        }

    def _calculate_concentration(self, company_name: str, top_n: int) -> Dict:
        """计算股东集中度"""
        shareholders_data = self._get_top_shareholders(company_name, top_n)

        if "error" in shareholders_data:
            return shareholders_data

        def parse_pct(perc_str):
            if not perc_str or perc_str == '/':
                return 0.0
            try:
                return float(str(perc_str).replace('%', '').strip())
            except:
                return 0.0

        concentration = sum(
            parse_pct(s.get('share_percentage', '0'))
            for s in shareholders_data["top_n_shareholders"]
        )

        return {
            "company": company_name,
            "top_n": top_n,
            "concentration": concentration,
            "concentration_percentage": f"{concentration:.2f}%",
            "shareholders": shareholders_data["top_n_shareholders"],
            "status": "success"
        }

    def _compare_companies(self, company1: str, company2: str, metric: str) -> Dict:
        """比较两个公司"""
        # 简化实现，只比较concentration
        data1 = self._calculate_concentration(company1, 3)
        data2 = self._calculate_concentration(company2, 3)

        if "error" in data1 or "error" in data2:
            return {"error": "Failed to get comparison data"}

        conc1 = data1["concentration"]
        conc2 = data2["concentration"]
        higher = company1 if conc1 > conc2 else company2

        return {
            "metric": metric,
            "company1": company1,
            "company2": company2,
            f"{company1}_concentration": conc1,
            f"{company2}_concentration": conc2,
            "higher": higher,
            "comparison": f"{higher} has higher concentration",
            "status": "success"
        }

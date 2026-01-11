#!/usr/bin/env python3
"""
Enhanced FinTalk.AI - 主入口
整合所有MCP核心功能模块
"""

import os
import sys
import json
import time
import logging
from typing import Dict, Any, Optional

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import core modules
from enhanced_core import (
    ParallelExecutor,
    QueryRewriter,
    QueryArbitrator,
    RejectionDetector,
    CorrelationChecker,
    FinancialFunctionRegistry,
    StreamingNLG,
    ConversationManager
)

# Import existing components
from formula import find_formula_for_query, calculate_from_expression
from OSWorld.docker_osworld_adapter import DockerOSWorldAdapter

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============== Configuration ==============
API_URL = "https://qianfan.baidubce.com/v2/chat/completions"
API_KEY = "bce-v3/ALTAK-dgZMQj7E5tByoRofFKlbM/e852481aaab5ebf3ffe6f2a50589e6e41646c127"


# ============== LLM Caller ==============
def llm_caller(prompt: str, temperature: float = 0.3) -> str:
    """LLM调用函数"""
    import requests
    payload = {
        "model": "deepseek-v3.2-think",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "web_search": {"enable": False}
    }
    try:
        response = requests.post(API_URL, headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}"
        }, json=payload, timeout=30)
        return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"LLM API error: {e}")
        return ""


# ============== Enhanced FinTalk.AI ==============
class EnhancedFinTalkAI:
    """
    增强版FinTalk.AI

    集成所有MCP核心功能：
    1. ✅ 并行模型调用
    2. ✅ Query改写
    3. ✅ 仲裁机制
    4. ✅ 拒识检测
    5. ✅ 相关性判断
    6. ✅ Function Calling
    7. ✅ 流式输出
    8. ✅ NLU/NLG
    9. ✅ 对话管理
    """

    def __init__(self, use_osworld: bool = True):
        """初始化增强版FinTalk.AI"""
        logger.info("\n" + "="*80)
        logger.info("🚀 Enhanced FinTalk.AI - MCP Architecture")
        logger.info("="*80)

        # 初始化OSWorld或本地数据库
        if use_osworld:
            self.adapter = DockerOSWorldAdapter()
            self.db = None
            self.env_mode = "Docker OSWorld"
        else:
            self.adapter = None
            self._init_local_db()
            self.env_mode = "Local SQLite"

        # 初始化所有核心模块
        self.parallel_executor = ParallelExecutor(max_workers=10)
        self.query_rewriter = QueryRewriter(llm_caller)
        self.arbitrator = QueryArbitrator(llm_caller)
        self.rejection_detector = RejectionDetector(llm_caller)
        self.correlation_checker = CorrelationChecker(llm_caller)
        self.function_registry = FinancialFunctionRegistry(self.db, self.adapter)
        self.nlg = StreamingNLG(API_URL, API_KEY)
        self.conversation_manager = ConversationManager()

        logger.info(f"✅ Environment: {self.env_mode}")
        logger.info(f"✅ All modules initialized")

    def _init_local_db(self):
        """初始化本地数据库"""
        import pandas as pd
        import sqlite3

        self.db = sqlite3.connect(':memory:', check_same_thread=False)
        data_dir = "data"

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
                df.to_sql(table_name, self.db, if_exists='replace', index=False)
                logger.info(f"   Loaded {len(df)} rows into '{table_name}'")

    def process_query(self,
                     user_query: str,
                     sender_id: str = "test",
                     stream_output: bool = False) -> Dict[str, Any]:
        """
        处理用户query（使用所有MCP核心功能）

        Args:
            user_query: 用户query
            sender_id: 用户ID
            stream_output: 是否流式输出

        Returns:
            处理结果
        """
        start_time = time.time()

        logger.info(f"\n{'='*80}")
        logger.info(f"👤 User Query: {user_query}")
        logger.info(f"💬 Context: {self.conversation_manager.get_context_summary()}")
        logger.info(f"{'='*80}")

        # 获取对话历史
        history_text = self.conversation_manager.get_history_text(n_turns=3)
        prev_query = self.conversation_manager.get_last_query()

        # ============== STEP 1: 并行模型调用 ==============
        logger.info(f"\n📍 STEP 1: Parallel Model Calls")

        def task_rewrite():
            return self.query_rewriter.rewrite(user_query, history_text)

        def task_arbitrate():
            return self.arbitrator.arbitrate(user_query, history_text)

        def task_rejection():
            return self.rejection_detector.should_accept(user_query)

        def task_correlation():
            return self.correlation_checker.is_correlated(
                prev_query or "", user_query
            )

        # 并行执行所有任务
        tasks = {
            "rewrite": task_rewrite,
            "arbitrate": task_arbitrate,
            "rejection": task_rejection,
            "correlation": task_correlation
        }

        parallel_results = self.parallel_executor.execute_parallel(tasks, timeout=60)

        # 提取结果
        rewritten_query = parallel_results["rewrite"].result
        arbitration = parallel_results["arbitrate"].result
        accept = parallel_results["rejection"].result
        is_correlated = parallel_results["correlation"].result

        logger.info(f"\n📊 Parallel Results:")
        logger.info(f"   Rewrite: {user_query} -> {rewritten_query}")
        logger.info(f"   Type: {arbitration.query_type}")
        logger.info(f"   Accept: {accept}")
        logger.info(f"   Correlated: {is_correlated}")

        # ============== STEP 2: 处理拒识 ==============
        if not accept:
            result = {
                "query": user_query,
                "rewritten_query": rewritten_query,
                "status": "rejected",
                "answer": "抱歉，我只能回答与金融数据分析相关的问题。",
                "execution_time": time.time() - start_time
            }
            logger.info(f"❌ Query rejected")
            return result

        # ============== STEP 3: 根据仲裁结果处理 ==============
        logger.info(f"\n📍 STEP 2: Execute by Type ({arbitration.query_type})")

        if arbitration.query_type == "task":
            # 任务导向 - 使用Function Calling
            answer = self._handle_task_query(rewritten_query, stream_output)

        elif arbitration.query_type == "knowledge":
            # 知识查询
            answer = self._handle_knowledge_query(rewritten_query)

        elif arbitration.query_type == "small_talk":
            # 闲聊
            answer = self._handle_small_talk(rewritten_query)

        else:
            # 无效输入
            answer = "抱歉，我无法理解您的问题。"

        # ============== STEP 3: 更新对话历史 ==============
        self.conversation_manager.add_turn(
            user_query,
            answer,
            arbitration.query_type
        )

        result = {
            "query": user_query,
            "rewritten_query": rewritten_query,
            "status": "success",
            "query_type": arbitration.query_type,
            "answer": answer,
            "execution_time": time.time() - start_time
        }

        logger.info(f"\n✅ Completed in {result['execution_time']:.2f}s")
        logger.info(f"💬 Answer: {answer[:100]}...")

        return result

    def _handle_task_query(self, query: str, stream_output: bool) -> str:
        """处理任务查询"""
        logger.info(f"   🎯 Handling as task query")

        # 提取function call
        func_call = self._extract_function_call(query)

        if func_call:
            # 执行function
            func_result = self.function_registry.execute(
                func_call["function_name"],
                func_call.get("parameters", {})
            )

            if "error" not in func_result:
                # 使用NLG生成答案
                answer = self.nlg.generate_answer(query, func_result)
                return answer
            else:
                return f"抱歉，{func_result['error']}"

        # 尝试通用处理
        return self._handle_general_query(query)

    def _handle_knowledge_query(self, query: str) -> str:
        """处理知识查询"""
        logger.info(f"   📚 Handling as knowledge query")

        return llm_caller(
            f"用简单的话解释这个金融概念（100字以内）：{query}",
            temperature=0.7
        )

    def _handle_small_talk(self, query: str) -> str:
        """处理闲聊"""
        logger.info(f"   💬 Handling as small talk")

        responses = {
            "hello": "你好！我是FinTalk.AI，你的金融数据分析助手。有什么可以帮助你的吗？",
            "hi": "你好！我是FinTalk.AI，请问有什么金融数据相关的问题需要查询？",
            "thank": "不客气！如果你还有其他问题，随时可以问我。",
            "bye": "再见！祝你一切顺利！"
        }

        query_lower = query.lower()
        for key, response in responses.items():
            if key in query_lower:
                return response

        return llm_caller(f"简要友好地回复：{query}", temperature=0.7)

    def _extract_function_call(self, query: str) -> Optional[Dict]:
        """提取function call"""
        functions_json = json.dumps(self.function_registry.get_functions(), ensure_ascii=False)

        prompt = f"""You are a financial function calling expert. Extract the function call from this query.

Available functions:
{functions_json}

Query: {query}

Return JSON format:
{{
    "function_name": "function_name",
    "parameters": {{"key": "value"}}
}}

If no function matches, return {{"function_name": "none"}}"""

        result = llm_caller(prompt, temperature=0.1)

        try:
            result = result.replace("```json", "").replace("```", "").strip()
            if "{" in result and "}" in result:
                start = result.index("{")
                end = result.rindex("}") + 1
                func_call = json.loads(result[start:end])

                if func_call.get("function_name") != "none":
                    logger.info(f"   🔧 Function: {func_call['function_name']}")
                    return func_call

        except Exception as e:
            logger.warning(f"Function extraction failed: {e}")

        return None

    def _handle_general_query(self, query: str) -> str:
        """处理通用查询"""
        # 简单处理：如果有公司名，返回公司信息
        if "za bank" in query.lower():
            func_result = self.function_registry.execute("get_company_info", {"company_name": "ZA Bank"})
        elif "welab" in query.lower():
            func_result = self.function_registry.execute("get_company_info", {"company_name": "WeLab Bank"})
        else:
            return "请指定你想查询的公司名称（如：ZA Bank 或 WeLab Bank）"

        if "error" not in func_result:
            return self.nlg.generate_answer(query, func_result)
        else:
            return func_result["error"]

    def close(self):
        """清理资源"""
        if self.adapter:
            self.adapter.close()
        elif hasattr(self, 'db') and self.db:
            self.db.close()
        logger.info("✅ Resources cleaned up")


# ============== Demo ==============
def demo_enhanced():
    """演示增强版FinTalk.AI"""

    print("\n" + "="*80)
    print("🚀 Enhanced FinTalk.AI - MCP Core Features")
    print("   并行调用 | Query改写 | 仲裁 | 拒识 | Function Calling | 流式输出 | 对话管理")
    print("="*80)

    # 初始化
    client = EnhancedFinTalkAI(use_osworld=True)

    # 测试用例
    test_cases = [
        "Hello!",
        "What is ZA Bank's employee size?",
        "And WeLab?",
        "Calculate executive_director_ratio for ZA Bank",
        "How is executive_director_ratio calculated?",
        "Compare ZA Bank and WeLab Bank on executive_director_ratio"
    ]

    for i, query in enumerate(test_cases, 1):
        print(f"\n{'─'*80}")
        print(f"Test {i}/{len(test_cases)}: {query}")
        print(f"{'─'*80}")

        result = client.process_query(query, stream_output=False)

        print(f"\nStatus: {result['status']}")
        print(f"Answer: {result['answer']}")
        print(f"Time: {result['execution_time']:.2f}s")

    # 显示对话历史
    print(f"\n{'='*80}")
    print("💬 Conversation History:")
    print(f"{'='*80}")
    for turn in client.conversation_manager.history:
        print(f"User: {turn.user}")
        print(f"Assistant: {turn.assistant[:100]}...")
        print(f"Type: {turn.query_type}\n")

    client.close()

    print("="*80)
    print("✅ Demo completed!")
    print("="*80)


if __name__ == "__main__":
    try:
        demo_enhanced()
    except KeyboardInterrupt:
        print("\n\n👋 Demo interrupted")
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()

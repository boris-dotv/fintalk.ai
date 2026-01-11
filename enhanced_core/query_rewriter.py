#!/usr/bin/env python3
"""
Query Rewriter - Query改写模块
基于对话历史改写用户query
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class QueryRewriter:
    """Query改写器"""

    def __init__(self, llm_caller):
        """
        初始化

        Args:
            llm_caller: LLM调用函数
        """
        self.llm_caller = llm_caller
        logger.info("✅ QueryRewriter initialized")

    def rewrite(self, query: str, history: str) -> str:
        """
        改写query

        Args:
            query: 用户query
            history: 对话历史

        Returns:
            改写后的query
        """
        if not history:
            return query

        prompt = self._build_rewrite_prompt(query, history)
        rewritten = self.llm_caller(prompt, temperature=0.3)

        # 防止误改写
        if not rewritten or self._is_bad_rewrite(query, rewritten):
            logger.info(f"   ✏️  No rewrite needed: {query}")
            return query

        logger.info(f"   ✏️  Rewrite: {query} -> {rewritten}")
        return rewritten

    def _build_rewrite_prompt(self, query: str, history: str) -> str:
        """构建改写prompt"""
        return f"""# Role: Financial Query Rewrite Expert

Rewrite the user's query based on conversation history.

## Rules:
1. Replace pronouns (he, she, it, that, this) with specific entities
2. Complete incomplete queries
3. Add missing context
4. Output ONLY the rewritten query

## Examples:
History:
User: What is ZA Bank's employee size?
Assistant: ZA Bank has 501-1,000 employees.
User: How about WeLab?
Output: What is WeLab Bank's employee size?

Conversation History:
{history}

Current Query: {query}

Rewritten Query:"""

    def _is_bad_rewrite(self, original: str, rewritten: str) -> bool:
        """检查是否是错误的改写"""
        if not rewritten:
            return True

        # 检查字符重叠度
        overlap = len(set(rewritten).intersection(original))
        if overlap < len(original) / 4:
            return True

        return False

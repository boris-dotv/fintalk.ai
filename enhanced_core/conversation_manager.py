#!/usr/bin/env python3
"""
Conversation Manager - 对话管理模块
管理对话历史、上下文、槽位
"""

import logging
import time
from typing import Dict, Any, List, Optional, Deque
from collections import deque
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ConversationTurn:
    """对话轮次"""
    user: str
    assistant: str = ""
    timestamp: float = field(default_factory=time.time)
    query_type: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class ConversationManager:
    """
    对话管理器

    功能：
    1. 管理对话历史
    2. 维护上下文信息
    3. 槽位管理
    """

    def __init__(self, max_history: int = 10):
        """
        初始化

        Args:
            max_history: 最大历史轮次
        """
        self.history: Deque[ConversationTurn] = deque(maxlen=max_history)
        self.context: Dict[str, Any] = {
            "last_company": None,
            "last_query_type": None,
            "entities": []
        }
        self.slots: Dict[str, Any] = {}
        logger.info("✅ ConversationManager initialized")

    def add_turn(self, user_query: str, assistant_answer: str,
                query_type: str = "", metadata: Dict = None):
        """
        添加对话轮次

        Args:
            user_query: 用户query
            assistant_answer: 助手回答
            query_type: query类型
            metadata: 元数据
        """
        turn = ConversationTurn(
            user=user_query,
            assistant=assistant_answer,
            query_type=query_type,
            metadata=metadata or {}
        )
        self.history.append(turn)
        self._update_context(user_query)

    def _update_context(self, query: str):
        """更新上下文"""
        # 提取公司名
        companies = ["ZA Bank", "WeLab Bank", "Airstar Bank", "Livo Bank", "Mox Bank"]
        for company in companies:
            if company.lower() in query.lower():
                self.context["last_company"] = company
                if company not in self.context["entities"]:
                    self.context["entities"].append(company)

    def get_history_text(self, n_turns: int = 3) -> str:
        """
        获取用于prompt的对话历史文本

        Args:
            n_turns: 最近N轮

        Returns:
            格式化的历史文本
        """
        recent_turns = list(self.history)[-n_turns:]
        parts = []
        for turn in recent_turns:
            parts.append(f"User: {turn.user}")
            if turn.assistant:
                parts.append(f"Assistant: {turn.assistant}")
        return "\n".join(parts)

    def get_context_summary(self) -> str:
        """获取上下文摘要"""
        parts = []
        if self.context["last_company"]:
            parts.append(f"Last company: {self.context['last_company']}")
        if self.context["entities"]:
            parts.append(f"Entities: {', '.join(self.context['entities'])}")
        return "; ".join(parts) if parts else "No context"

    def get_last_query(self) -> Optional[str]:
        """获取上一个用户query"""
        if self.history:
            return self.history[-1].user
        return None

    def clear(self):
        """清空对话历史"""
        self.history.clear()
        self.context = {"last_company": None, "last_query_type": None, "entities": []}
        self.slots = {}
        logger.info("🗑️  Conversation history cleared")

    def get_stats(self) -> Dict[str, Any]:
        """获取对话统计"""
        return {
            "total_turns": len(self.history),
            "last_company": self.context.get("last_company"),
            "entities_count": len(self.context.get("entities", [])),
            "slots_count": len(self.slots)
        }

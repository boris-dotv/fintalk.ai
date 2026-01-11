#!/usr/bin/env python3
"""
Streaming NLG - 流式输出和自然语言生成模块
"""

import logging
import json
from typing import Generator, Optional, Dict, Any

logger = logging.getLogger(__name__)


class StreamingNLG:
    """
    流式NLG生成器

    功能：
    1. 流式输出LLM响应
    2. 自然语言答案生成
    3. 友好性回复
    """

    def __init__(self, api_url: str, api_key: str):
        """
        初始化

        Args:
            api_url: API地址
            api_key: API密钥
        """
        self.api_url = api_url
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        logger.info("✅ StreamingNLG initialized")

    def generate_streaming(self, prompt: str) -> Generator[str, None, None]:
        """
        生成流式响应

        Args:
            prompt: 提示词

        Yields:
            文本片段
        """
        import requests

        payload = {
            "model": "deepseek-v3.2-think",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "stream": True,
            "web_search": {"enable": False}
        }

        try:
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                stream=True,
                timeout=60
            )

            for line in response.iter_lines(decode_unicode=False):
                if not line:
                    continue

                line = line.decode('utf-8')
                if line.startswith('data: '):
                    data_str = line[6:]
                    if data_str == '[DONE]':
                        break

                    try:
                        data = json.loads(data_str)
                        if 'choices' in data and len(data['choices']) > 0:
                            delta = data['choices'][0].get('delta', {})
                            content = delta.get('content', '')
                            if content:
                                yield content

                    except json.JSONDecodeError:
                        continue

        except Exception as e:
            logger.error(f"Streaming error: {e}")
            yield f"[Error: {str(e)}]"

    def generate_answer(self, query: str, data: Dict[str, Any]) -> str:
        """
        从查询结果生成自然语言答案

        Args:
            query: 用户query
            data: 查询结果数据

        Returns:
            自然语言答案
        """
        import requests

        nlg_prompt = f"""# Role: Financial Data Analyst

Based on the query result, provide a clear and professional answer.

Query: {query}
Result: {json.dumps(data, indent=2, default=str)}

Provide a concise answer (under 100 words) that:
1. Directly answers the question
2. Includes key data points
3. Is professional and friendly

Answer:"""

        payload = {
            "model": "deepseek-v3.2-think",
            "messages": [{"role": "user", "content": nlg_prompt}],
            "temperature": 0.7,
            "web_search": {"enable": False}
        }

        try:
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            return response.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.error(f"NLG generation error: {e}")
            return f"Based on the data, {query}"

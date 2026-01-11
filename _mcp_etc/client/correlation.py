import requests
import json
import time
import os
from typing import Any
import prompts
from utils import logger
from utils.redis_tool import RedisClient


TIMEOUT = 2.0
REDIS_KEY = "voice:last_service:{}"
_redis_client = RedisClient() 

DOUBAO_API_KEY = os.environ["API_KEY"]
DOUBAO_URL = os.environ["BASE_URL"]
CORRELATION_SYSTEM = prompts.CORRELATION_SYSTEM
CORRELATION_PROMPT = prompts.CORRELATION_PROMPT



def request_correlation(query, sender_id):
    try:
        last = _redis_client.get(REDIS_KEY.format(sender_id))
        if not last:
            return "否"

        _, last_query, last_reject, _ = last.split("#")
        if len(query) > 1 and last_query == query:
            return "是"

        if last_reject == "N":
            return "否"

        headers = {
            "Content-Type": "application/json",
            "Authorization": DOUBAO_API_KEY
        }
        messages = [
            {"role": "system", "content": CORRELATION_SYSTEM},
            {"role": "user", "content": CORRELATION_PROMPT.format(last_query, query)}
        ]

        body = dict(
            model="ep-20241203180921-h2kgz",
            messages=messages,
            temperature=0,
        )
        response = requests.post(
            DOUBAO_URL,
            headers=headers,
            json=body,
            timeout=TIMEOUT
        )
        response = response.json()
        answer = response["choices"][0]["message"]["content"]
        if answer and len(answer) > 0:
            answer = answer[0]
        if answer != "是":
            answer = "否"

        logger.info(f"相关性LLM判定结果: {answer}")
        return answer

    except Exception:
        logger.error("Call Correlation API failed.")
        return "否"


if __name__ == "__main__":
    sender_id = "123"
    while True:
        query = input("输入：")
        res = request_correlation(query, sender_id)
        print(res)


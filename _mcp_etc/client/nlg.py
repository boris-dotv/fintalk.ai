import requests
import json
import time
import os
from typing import Any
import prompts
from utils import logger


TIMEOUT = 10.0
DOUBAO_API_KEY = os.environ["API_KEY"]
DOUBAO_URL = os.environ["BASE_URL"]
NLG_PROMPT = prompts.NLG_PROMPT


def request_nlg(query, tool_response):
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": DOUBAO_API_KEY
        }
        messages = [
            {"role": "user", "content": NLG_PROMPT.format(query, tool_response)}
        ]

        body = dict(
            model="ep-20241203180921-h2kgz",
            messages=messages,
        )
        response = requests.post(
            DOUBAO_URL,
            headers=headers,
            json=body,
            timeout=TIMEOUT
        )
        response = response.json()
        answer = response["choices"][0]["message"]["content"]
        logger.info(f"NLG结果: {answer}")
        return answer

    except Exception:
        logger.error("Call NLG API failed.")
        return ""


if __name__ == "__main__":
    
    query = "how's the weather"
    tool_response = "城市：北京市\n天气：阴\n温度：21度\n风向：东北\n风力：1-3级"

    res = request_nlg(query, tool_response)
    print(res)


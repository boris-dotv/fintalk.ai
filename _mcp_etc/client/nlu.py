import json
import requests
import re
import os
from utils import logger
from typing import List


NLU_URL = os.environ["NLU_URL"]


def request_nlu(query, trace_id, enable_dm=True):
    headers = {
        "Content-Type":"application/json"
    }
    payload = json.dumps({
        "query": query,
        "trace_id": trace_id,
        "enable_dm": enable_dm
    })
    try:
        response = requests.post(
            NLU_URL,
            headers=headers,
            data=payload
        )
        res = response.json()
        logger.info(f"NLU模型的输出：{res}")
    except Exception as e:
        logger.error(f"call NLU failed:{e}")
        res = {}
    return res


if __name__ == '__main__':
    while True:
        query = input("Input:")
        res = request_nlu(query, "123")
        print(res)


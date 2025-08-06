import os
import json
import re
import requests
import prompts
from utils import logger
from utils.redis_tool import RedisClient


TTL = 40
MAX_HISTORY = 6
DOUBAO_API_KEY = os.environ["API_KEY"]
DOUBAO_URL = os.environ["BASE_URL"]
REDIS_KEY = "voice:rewrite_history:{}"
_redis_client = RedisClient() 


def request_rewrite(query, last_answer, sender_id):

    headers = {
        "Authorization": DOUBAO_API_KEY,
        "Content-Type": "application/json"
    }
    history = _redis_client.get(REDIS_KEY.format(sender_id))
    if history:
        history = json.loads(history)
    else:
        history = []
    history = history[-MAX_HISTORY:]

    if history and last_answer:
        history[-1]["content"] = last_answer

    messages_header = [
        {"role": "system", "content": prompts.REWRITE_SYSTEM_PROMPT}
    ]
    if not history:
        result = "否"
    else:
        split_history = [history[i:i+2] for i in range(0, len(history), 2)]
        history_msgs = []
        for index, item in enumerate(split_history):
            if index == len(split_history) - 1:
                if item[1]["content"]:
                    msg = "A：{}\nB：{}".format(item[0]["content"], item[1]["content"])
                else:
                    msg = "A：{}".format(item[0]["content"])
            else:
                if item[1]["content"]:
                    msg = "A：{}\nB：{}".format(item[0]["content"], item[1]["content"])
                else:
                    msg = "A：{}".format(item[0]["content"])
            history_msgs.append(msg)
        history_msgs = "\n".join(history_msgs)

        prompt = "#对话历史#\n{}\nA：{}\n".format(history_msgs, query)
        logger.info(f"对话历史：{prompt}")
        messages_now = [
            {"role": "user", "content": prompt}
        ]
        messages = messages_header + messages_now

        data = {
            "model": "ep-20250206092527-ms2qn",
            "messages": messages,
            "temperature": 0.001,
            "top_p": 0,
        }

        response = requests.post(
            DOUBAO_URL,
            headers=headers,
            data=json.dumps(data),
        )
        res = response.content.decode('utf-8')
        res = json.loads(res)
        result = res['choices'][0]['message']['content']

        # 防止误改
        if len(set(result).intersection(query)) < len(query) / 4:
            result = "否"

    if result == "否":
        result = query

    logger.info("改写后：{}".format(result))

    history.append({"role": "user", "content": result})
    history.append({"role": "assistant", "content": ""})

    _redis_client.set(REDIS_KEY.format(sender_id), json.dumps(history, ensure_ascii=False), ex=TTL)

    return result

import os
import json
import re
import time
import requests
import prompts
from utils import logger
from utils.redis_tool import RedisClient


MAX_HIS = 6
TTL = 45
REMIND_TIMEOUT = 2.5
REDIS_KEY = "voice:chat_history:{}"
_redis_client = RedisClient() 


DEEPSEEK_API_KEY = os.environ["API_KEY"]
DEEPSEEK_BASE_URL = os.environ["BASE_URL"]
SYSTEM_PROMPT = prompts.BOT_CHAT_SYSTEM_PROMPT


def request_chat(query, sender_id, multiturn=True):
    if multiturn:
        history = _redis_client.get(REDIS_KEY.format(sender_id))
        if history:
            history = json.loads(history)
        else:
            history = []
    else:
        history = []
    headers = {
        "Authorization": f"{DEEPSEEK_API_KEY}", 
        "Content-Type": "application/json"
    }
    messages_header = [
        {"role": "system", "content": SYSTEM_PROMPT}
    ]
    messages_now = [
        {"role": "user", "content": query}
    ]
    messages = messages_header + history + messages_now
    logger.info(f'request message:{messages}')
    data = {
        "model": "deepseek-chat",
        "messages": messages,
        "stream": True
    }
    try:
        response = requests.post(
            DEEPSEEK_BASE_URL,
            headers=headers,
            data=json.dumps(data),
            stream=True
        )
        # --- 在这里添加状态码检查 ---
        response.raise_for_status() # 如果HTTP状态码不是2xx，将抛出HTTPError异常
        logger.info(f"API请求成功，状态码: {response.status_code}")
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Bot Chat error: {e}")
        return "N"
    except Exception as e:
        logger.error("Bot Chat error:" + str(e))
        return "N"


def process_chat(response, query, sender_id):
    if response is None:
        response = "抱歉，此为敏感信息，请您换个问题"
        yield response
    if response == "N":
        response = "抱歉，网络有点问题，请您再试一下"
        yield response
    else:
        counter = 1
        uttrance = ""
        answer = ""
        for r in response.iter_lines(chunk_size=1, decode_unicode=False, delimiter=b'\n'):
            r = r.decode("utf-8").strip()
            if not r:
                continue
            r = json.loads(r.lstrip("data: "))
            if r["choices"][0].get("finish_reason", {}) == "stop":
                break
            text = r["choices"][0]["delta"]["content"]
            uttrance += text
            answer += text
            if re.search('，|。|？|；', text):
                yield uttrance
                uttrance = ""
                counter = 1
            if counter % 5 == 0:
                yield uttrance
                uttrance = ""
            counter += 1

        if uttrance and uttrance != "  " and uttrance != " ":
            yield uttrance

        logger.info(f"bot_Chat Result: {answer}")
        history = _redis_client.get(REDIS_KEY.format(sender_id))
        if history:
            history = json.loads(history)
        else:
            history = []
        history.append({"role": "user", "content": query})
        history.append({"role": "assistant", "content": answer})
        history = history[-MAX_HIS:]
        history_str = json.dumps(history, ensure_ascii=False)
        _redis_client.set(REDIS_KEY.format(sender_id), history_str, ex=TTL)


if __name__ == '__main__':
    while 1:
        query = input("-->")
        res = request_chat(query, '1', '2')
        for frame in process_chat(res, query, '1'):
            print(frame)


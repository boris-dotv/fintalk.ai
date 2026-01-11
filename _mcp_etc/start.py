


import json
import os
import copy
import traceback
import time
import redis
import requests
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify, make_response
from flask_socketio import SocketIO, emit

import prompts
from utils import logger
from utils.redis_tool import RedisClient
from client.arbitration import request_arbitration
from client.stream_chat import request_chat, process_chat
from client.reject import request_reject
from client.nlu import request_nlu
from client.rewrite import request_rewrite
from client.correlation import request_correlation


socketio = SocketIO(cors_allowed_origins='*', async_mode='threading')
app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
socketio.init_app(app)


TTL = 40
REDIS_KEY = "voice:last_service:{}"
redis_client = RedisClient() 
thread_pool = ThreadPoolExecutor(max_workers=10)


@app.route("/health", methods=["GET"])
def check():
    response = make_response(
        jsonify(health="healthy"),
        200,
        {'content-type': 'application/json'}
    )
    return response


@socketio.on('connect')
def connected_msg():
    manager = socketio.server.manager
    connections_count = len(manager.rooms['/']) - 1
    logger.info(f'当前连接数: {connections_count}')
    logger.info('client connected.')


@socketio.on('disconnect')
def disconnect_msg():
    logger.info('client disconnected.')


def send_msg(nlu_result, func, frame, seq, cost, status):
    if func == "CHAT":
        intent = "闲聊百科" 
        intent_id = "439"
    else:
        intent = "拒识" 
        intent_id = "440"

    nlu_result["intent"] = intent
    nlu_result["intent_id"] = intent_id
    nlu_result["func"] = func
    nlu_result["frame"] = frame
    nlu_result["seq"] = seq
    nlu_result["cost"] = cost
    nlu_result["status"] = status

    emit(
        "request_nlu",
        json.dumps(nlu_result, ensure_ascii=False),
        broadcast=False
    )


def handle_chat(handler_bot, nlu_result, query, sender_id, begin):

    # 开始帧
    seq = 1
    nlu_result_begin = copy.deepcopy(nlu_result)
    send_msg(nlu_result_begin, "CHAT", "", seq, time.time() - begin, status=0)

    # 中间帧
    full_answer = ""
    for value in process_chat(handler_bot.result(), query, sender_id):
        nlu_result_chat = copy.deepcopy(nlu_result)
        send_msg(nlu_result_chat, "CHAT", value, seq, time.time() - begin, status=1)
        seq += 1
        full_answer += value
        logger.info(f"Chat Frame:{seq},content:{value}")

    # 结束帧
    if seq > 1:
        nlu_result_end = copy.deepcopy(nlu_result)
        send_msg(nlu_result_begin, "CHAT", "", seq, time.time() - begin, status=2)
        logger.info(f"Chat cost time: {time.time() - begin}")
        return True, full_answer
    else:
        logger.info(f"Chat cost time: {time.time() - begin}")
        return False, full_answer



@socketio.on('request_nlu')
def inference(req):
    begin = time.time()
    json_info = json.loads(req)
    query = json_info.get("query")
    enable_dm = json_info.get("enable_dm")
    sender_id = json_info.get("sender_id", "test")
    trace_id = json_info.get("trace_id", "123")

    nlu_template = {
        "query": query,
        "tarce_id": trace_id,
        "intent": "",
        "intent_id": "",
        "function": "",
        "slots": {},
        "cost": time.time() - begin
    }
    try:
        ori_query = query
        logger.session.trace_id = trace_id
        logger.info("Request Params: {}".format(json_info))

        last_info = redis_client.get(REDIS_KEY.format(sender_id))
        last_domain, last_query, last_reject, last_answer = "", "", "", ""
        if last_info:
            last_domain, last_query, last_reject, last_answer = last_info.split("#")

        # Query改写
        query = request_rewrite(query, last_answer, sender_id)

        # 调用nlu语义
        handler_nlu = thread_pool.submit(request_nlu, query, trace_id, enable_dm)

        # 调用仲裁
        handler_arbitration = thread_pool.submit(request_arbitration, ori_query, sender_id)

        # 调拒识模型
        handler_reject = thread_pool.submit(request_reject, query, trace_id)

        # 调用相关性模型
        handler_correlation = thread_pool.submit(request_correlation, ori_query, sender_id)

        # 调用百科闲聊
        handler_bot = thread_pool.submit(request_chat, ori_query, sender_id)

        # 获取仲裁结果
        arbitration_result = handler_arbitration.result()

        logger.info(
            f"TraceID:{trace_id}, query:{query}, arbitration result: {arbitration_result}, cost time: {time.time() - begin}")

        # 开始仲裁
        if arbitration_result == "task":
            nlu_result = handler_nlu.result()
            # 技能
            if nlu_result.get("function", "") not in ["Unknown"]:
                redis_client.set(REDIS_KEY.format(sender_id), f"SKILL#{query}#1#", ex=TTL)
                emit(
                    "request_nlu",
                    json.dumps(
                        nlu_result,
                        ensure_ascii=False
                    ),
                    broadcast=False
                )
            else:
                send_msg(nlu_result, "REJECT", prompts.DEFAULT_NLG, 1, time.time() - begin, status=-1)
                logger.info(f"Query {query} has been rejected.")
        else:
            # 拒识
            reject_result = handler_reject.result()
            if reject_result == 0:
                correlation_result = handler_correlation.result()
                if correlation_result == "是":
                    reject_result = 1 
            if reject_result == 0:
                send_msg(nlu_template, "REJECT", "", 1, time.time() - begin, status=-1)
                logger.info(f"Query {query} has been rejected.")
            else:
                # 百科闲聊兜底
                is_hit_chat, full_answer = handle_chat(handler_bot, nlu_template, ori_query, sender_id, begin)
                if is_hit_chat:
                    redis_client.set(REDIS_KEY.format(sender_id), f"CHAT#{query}#{reject_result}#{full_answer}", ex=TTL)

    except Exception as e:
        logger.error(
            'TraceID:{}, Internal Server Error!'.format(trace_id))
        logger.error('{}'.format(e))
        traceback.print_exc()
        send_msg(nlu_template, "REJECT", "", 1, time.time() - begin, status=-1)

if __name__ == "__main__":
    socketio.run(
        app,
        allow_unsafe_werkzeug=True,
        host='0.0.0.0',
        port=os.getenv("FLASK_SERVER_PORT", 8080)
    )


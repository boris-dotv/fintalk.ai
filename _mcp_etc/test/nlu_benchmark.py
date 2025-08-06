# -*- coding:utf-8 -*-


"""
运行命令：
locust -f nlu_benchmark.py  --host http://127.0.0.1:8009  --headless -u 10 -r 5 -t 60s
"""

import random
import uuid
from locust import HttpUser, task, between


fd = open("../train/data/intent/test.txt")
samples = [k.split("\t")[0] for k in fd]

class User(HttpUser):
    wait_time = between(3, 5)

    @task
    def task_post_archive(self):
        trace_id = f'cevi{uuid.uuid4().hex}'
        testServer = 'http://127.0.0.1:8009'
        path = '/chatnlu-server/v1'
        url = f'{testServer}{path}'
        headers = {
            'Content-Type': 'application/json'        }
        data = {
            "query": random.choice(samples),
            "trace_id": trace_id
        }
        self.client.post(url, json=data, headers=headers)




"""
运行命令：
locust -f intent_benchmark.py  --host http://127.0.0.1:8008  --headless -u 1000 -r 100 -t 60s
"""

import random
import uuid
from locust import HttpUser, task, between


fd = open("../train/data/intent/test.txt")
samples = [k.split("\t")[0] for k in fd]

class User(HttpUser):
    wait_time = between(1, 1.5)

    @task
    def task_post_archive(self):
        trace_id = f'cevi{uuid.uuid4().hex}'
        testServer = 'http://127.0.0.1:8008'
        path = '/intent-server/v1'
        url = f'{testServer}{path}'
        headers = {
            'Content-Type': 'application/json'        }
        data = {
            "query": random.choice(samples),
            "trace_id": trace_id
        }
        self.client.post(url, json=data, headers=headers)


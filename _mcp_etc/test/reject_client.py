# -*- coding:utf-8 -*-


import os
import requests
import uuid
import json
from tqdm import tqdm


THRSHOLD = 0.5
URL = os.environ["REJECT_URL"]


def get_completion(query):
    headers = {'Content-Type': 'application/json'}
    data = {"query": query, "thres": THRSHOLD, "trace_id": str(uuid.uuid1())}
    response = requests.post(url=URL, headers=headers, data=json.dumps(data))
    return response.json()

if __name__ == '__main__':
    fd = open("../train/data/reject/test.txt")
    data = fd.readlines()
    right = 0
    total = 0
    for index in tqdm(range(len(data))):
        line = data[index]
        text, label = line.strip().split("\t")
        label = int(label)
        response = get_completion(text)
        # print(text, response)
        if response["data"] == label:
            right += 1
        total += 1
    print("test avg acc:", right/total)

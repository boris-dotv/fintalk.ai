# -*- coding:utf-8 -*-



import os
import requests
import uuid
import json
from tqdm import tqdm


URL = os.environ["NLU_URL"]


def get_completion(query):
    headers = {'Content-Type': 'application/json'}
    data = {"query": query, "trace_id": str(uuid.uuid1()), "enable_dm": False}
    response = requests.post(url=URL, headers=headers, data=json.dumps(data))
    return response.json()

if __name__ == '__main__':
    fd = open("data/single_slots_new.txt")
    data = fd.readlines()
    intent_right = 0
    slots_right = 0
    total = 0
    for idx in tqdm(range(len(data))):
        line = data[idx]
        text, label, slots = line.strip().split("\t")
        response = get_completion(text)
        pred_slots = response["slots"]
        slots = json.loads(slots)
        if slots == pred_slots:
            slots_right += 1 
        if response["intent_id"] == label:
            intent_right += 1
        total += 1
    print("test intent acc:", intent_right/total, "slots acc:", slots_right/total)

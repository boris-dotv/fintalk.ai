# -*- coding: utf-8 -*-


"""计算任务型对话agent端到端准确率得分
"""

import json
fd = open("test/result/multi_test_output.txt")
for line in fd:
    info = json.loads(line)
    query = info["query"]    
    res = ""
    for msg in info["res"]:
        if msg["intent"] == "闲聊百科":
            res += msg["frame"].replace("\n", " ") 
        else:
            res += msg["intent"] + "\t" + json.dumps(msg["slots"], ensure_ascii=False)
    print(query + "############" + res)

# 对输出结果进行标注, 需要考虑输出质量，500条多轮case需人工check，约20min

# 对标注后的结果进行统计
print("\n" + "="*50)
with open("test/result/multi_test_output_labeled.txt") as file_handler:
    right, total = 0, 0
    for line in file_handler:
        if line.startswith("1"):
            right += 1
        total += 1
    print("端到端准确率：{}%".format(round(right/total * 100, 3)))
print("="*50)

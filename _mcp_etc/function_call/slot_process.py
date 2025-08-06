import json


def value_process(key, value):
    position_map = {
        "主驾": "MAIN",
        "副驾": "VICE",
        "左侧": "LEFT",
        "右侧": "RIGHT",
        "前排": "FRONT",
        "后排": "REAR",
        "左后": "LEFT_REAR",
        "右后": "RIGHT_REAR",
        "主对角": "MAIN_DIAGONAL",
        "副对角": "VICE_DIAGONAL",
        "所有": "ALL",
        "吹脚": "FOOT",
        "吹脸": "FACE",
        "吹窗": "WINDOW",
        "吹脸吹脚": "FACE_AND_FOOT",
        "吹窗吹脚": "WINDOW_AND_FOOT",
        "左前": "MAIN",
        "右前": "VICE",
        "主副驾": "FRONT"
    }
    if key in ["NUMBER", "RATIO"]:
        if '%' in value:
            value = float(eval(value.replace('%', '')) / 100)
        else:
            value = float(eval(value))
    elif key in ["POSITION"]:
        value = position_map.get(value, value)
    elif key == "对话时长":
        value = value.replace("秒", "")
    elif key == "Extreme":
        if value in ["最大", "最高", "最强", "最亮", "最热"]:
            value = "最大"
        if value in ["最小", "最低", "最弱", "最暗", "最冷"]:
            value = "最小"
    return value


def intent_slot(function, map_intent, slot_map):
    try:
        predict_e = function[0].get("function", {}).get("name", "NULL")
        predict_z = map_intent.get(predict_e, predict_e)
        slots_predict = function[0].get("function", {}).get("arguments", "{}")
        slots_predict = json.loads(slots_predict)
        # 先做槽位名的转换
        result = predict_z + "-"
        slot_lst = []
        dict_slot = slot_map.get(predict_e)
        if slots_predict:
            for key, value in slots_predict.items():
                if value and isinstance(dict_slot, dict) and value != "不限":
                    key = dict_slot.get(key, key)
                    value = value_process(key, value)
                    slot_lst.append({key: value})
                    result = result + f"{key}:{str(value)}" + ","
                else:
                    continue
            result = result.rsplit(",", 1)[0]
        if ":" not in result:
            result = result + "无"
    except Exception as e:
        return "未知-无"

    return result

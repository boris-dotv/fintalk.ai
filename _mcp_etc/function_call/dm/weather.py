import json
from datetime import datetime
from sinan import Sinan
from client.nlg import request_nlg
from mcp_core.mcp_client import MCPClient


mcp_client = MCPClient()


async def process(func_name, query, slots):

    if func_name not in ["Query_Weather", "Query_Timely_Weather"]:
        return

    # 调用MCP接口
    date_parsed = Sinan(slots.get("date", "")).parse()
    if "datetime" in date_parsed:
        slots["date"] = date_parsed["datetime"][0].split(" ")[0]
    else:
        slots["date"] = datetime.now().strftime("%Y-%m-%d")
    if "city" not in slots:
        slots["city"] = "北京"
    await mcp_client.connect_to_server("../mcp_core/amp_server.py")
    tool_response = await mcp_client.execute("maps_weather", slots)
    tool_response = json.loads(tool_response)
    nlg = request_nlg(query, tool_response)

    return (tool_response, nlg)





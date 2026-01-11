#!/usr/bin/env python3
"""
Enhanced FinTalk.AI - 带MCP功能的交互式Demo
展示本地函数 + MCP外部工具的完整能力
"""

import os
import sys
import time

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from enhanced_fintalk import EnhancedFinTalkAI
from mcp_integration import MCPClient, MCPFunctionRegistry


def print_section(title):
    """打印分节标题"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)


def print_feature(feature, description):
    """打印功能说明"""
    print(f"\n✨ {feature}")
    print(f"   {description}")


def demo_with_mcp():
    """带MCP功能的交互式demo"""

    print("\n" + "🚀"*40)
    print(" "*10 + "Enhanced FinTalk.AI - 本地函数 + MCP外部工具")
    print(" "*15 + "完整交互式体验")
    print("🚀"*40)

    print_section("🎯 系统初始化")

    # 初始化主系统
    client = EnhancedFinTalkAI(use_osworld=False)

    # 初始化MCP客户端
    print("\n📡 Initializing MCP (Model Context Protocol)...")
    mcp_client = MCPClient(log_dir="mcp_integration/logs")

    print_feature("✅ 本地数据库函数", "查询公司、管理层、股东数据")
    print_feature("✅ MCP外部工具", "Web搜索、股票价格、新闻、GitHub搜索")
    print_feature("✅ MCP日志记录", "所有MCP通信记录到 mcp_integration/logs/")

    print_section("📝 使用说明")

    print("""
支持的功能：

【本地数据库查询】
1. 🏢 公司信息
   - "What is ZA Bank's employee size?"
   - "Tell me about WeLab Bank"

2. 📊 金融指标
   - "Calculate executive_director_ratio for ZA Bank"
   - "What's the shareholder concentration?"

3. 🔍 数据比较
   - "Compare ZA Bank and WeLab Bank"

【MCP外部工具】
4. 🔍 Web搜索
   - "Search for latest news about virtual banks in Hong Kong"
   - "What are the recent developments in digital banking?"

5. 📈 股票价格
   - "Get stock price for 03863.HK"
   - "What's the current price of ZA Bank's parent company?"

6. 📰 金融新闻
   - "Get recent news about virtual banks"
   - "Latest news on fintech regulation"

7. 💻 GitHub搜索
   - "Search GitHub for MCP implementations"
   - "Find examples of LangChain financial applications"

【特殊命令】
- 'mcp': 查看MCP日志摘要
- 'mcp full': 查看完整MCP日志
- 'tools': 列出所有可用工具
- 'history': 查看对话历史
- 'clear': 清空对话历史
- 'quit' 或 'exit': 退出
    """)

    print_section("💬 开始对话")

    while True:
        try:
            user_input = input("\n👤 You: ").strip()

            if not user_input:
                continue

            if user_input.lower() in ['quit', 'exit', 'q']:
                print("\n👋 Goodbye!")
                break

            # 特殊命令
            if user_input.lower() == 'mcp':
                print(mcp_client.get_logs_summary())
                continue

            if user_input.lower() == 'mcp full':
                print(mcp_client.view_logs())
                continue

            if user_input.lower() == 'tools':
                print_section("🔧 可用工具")

                print("\n【本地函数】")
                local_funcs = client.function_registry.get_functions()
                for func in local_funcs:
                    print(f"  • {func['function']['name']}: {func['function']['description']}")

                print("\n【MCP外部工具】")
                mcp_funcs = mcp_client.get_tools()
                for func in mcp_funcs:
                    print(f"  • {func['name']}: {func['description']}")
                continue

            if user_input.lower() == 'history':
                print_section("💬 对话历史")
                for i, turn in enumerate(client.conversation_manager.history, 1):
                    print(f"\n轮次 {i}:")
                    print(f"  👤 User: {turn.user}")
                    print(f"  🤖 Assistant: {turn.assistant[:100]}...")
                    print(f"  📋 Type: {turn.query_type}")
                continue

            if user_input.lower() == 'clear':
                client.conversation_manager.clear()
                print("\n🗑️  对话历史已清空")
                continue

            # 处理query
            print("\n🤖 Processing...\n")
            start_time = time.time()

            # 检查是否需要MCP工具
            mcp_keywords = ['search', 'web', 'news', 'stock', 'github', 'price']
            use_mcp = any(keyword in user_input.lower() for keyword in mcp_keywords)

            if use_mcp:
                # 使用MCP工具
                print("📡 Using MCP external tools...")

                # 简单的MCP工具匹配
                if 'search' in user_input.lower() and 'github' in user_input.lower():
                    result = mcp_client.call_tool("search_github", {"query": user_input})
                elif 'search' in user_input.lower() or 'web' in user_input.lower():
                    result = mcp_client.call_tool("web_search", {"query": user_input})
                elif 'stock' in user_input.lower() or 'price' in user_input.lower():
                    # 提取股票代码
                    symbol = "03863.HK"  # 默认
                    for word in user_input.split():
                        if '.HK' in word.upper():
                            symbol = word.upper()
                    result = mcp_client.call_tool("get_stock_price", {"symbol": symbol})
                elif 'news' in user_input.lower():
                    result = mcp_client.call_tool("get_financial_news", {"topic": "virtual banks"})
                else:
                    result = {"error": "No matching MCP tool"}

                if "error" not in result:
                    answer = f"✅ MCP Tool Result:\n{json.dumps(result, indent=2, ensure_ascii=False)}"
                    status = "success"
                    query_type = "mcp_tool"
                else:
                    answer = f"❌ {result['error']}"
                    status = "error"
                    query_type = "error"

                result_data = {
                    "query": user_input,
                    "status": status,
                    "query_type": query_type,
                    "answer": answer,
                    "execution_time": time.time() - start_time
                }

            else:
                # 使用本地处理
                result_data = client.process_query(user_input, stream_output=False)
                answer = result_data['answer']

            elapsed = time.time() - start_time

            # 显示结果
            print("\n" + "─"*80)
            print(f"📊 Status: {result_data['status']}")
            if result_data['status'] == 'success':
                print(f"🎯 Type: {result_data.get('query_type', 'unknown')}")
                if use_mcp:
                    print(f"📡 Via: MCP External Tool")
            print(f"⏱️  Time: {result_data['execution_time']:.2f}s")
            print("─"*80)

            print(f"\n🤖 Assistant: {answer}")

        except KeyboardInterrupt:
            print("\n\n👋 Demo interrupted")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()

    # 显示统计
    print_section("📊 会话统计")

    # 对话统计
    stats = client.conversation_manager.get_stats()
    print(f"\n💬 对话统计:")
    print(f"   总轮次: {stats['total_turns']}")
    print(f"   上一个公司: {stats.get('last_company', 'N/A')}")
    print(f"   实体数量: {stats.get('entities_count', 0)}")

    # MCP统计
    print(f"\n📡 MCP统计:")
    print(mcp_client.get_logs_summary())

    client.close()

    print("\n" + "="*80)
    print("✅ Demo completed! Thank you for trying Enhanced FinTalk.AI!")
    print("="*80 + "\n")


if __name__ == "__main__":
    import json
    try:
        demo_with_mcp()
    except Exception as e:
        print(f"\n❌ Fatal Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

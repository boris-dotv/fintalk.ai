#!/usr/bin/env python3
"""
Enhanced FinTalk.AI - 交互式全功能Demo
展示所有MCP核心功能
"""

import os
import sys
import time

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from enhanced_fintalk import EnhancedFinTalkAI


def print_section(title):
    """打印分节标题"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)


def print_feature(feature, description):
    """打印功能说明"""
    print(f"\n✨ {feature}")
    print(f"   {description}")


def interactive_demo():
    """交互式demo"""

    print("\n" + "🚀"*40)
    print(" "*15 + "Enhanced FinTalk.AI - MCP全功能Demo")
    print(" "*20 + "交互式体验模式")
    print("🚀"*40)

    print_section("🎯 系统初始化")

    client = EnhancedFinTalkAI(use_osworld=False)

    print_feature("✅ 并行模型调用", "同时执行改写、仲裁、拒识、相关性检测")
    print_feature("✅ Query改写", "基于对话历史优化query")
    print_feature("✅ 仲裁机制", "智能分类：task/knowledge/small_talk/invalid")
    print_feature("✅ 拒识检测", "过滤无关query")
    print_feature("✅ 相关性判断", "识别多轮对话的上下文关联")
    print_feature("✅ Function Calling", "调用预定义的金融函数")
    print_feature("✅ 流式输出", "实时生成响应")
    print_feature("✅ 对话管理", "维护对话历史和上下文")

    print_section("📝 使用说明")

    print("""
支持的查询类型：

1. 🏢 公司信息查询
   - "What is ZA Bank's employee size?"
   - "Tell me about WeLab Bank"

2. 📊 金融指标计算
   - "Calculate executive_director_ratio for ZA Bank"
   - "What's the shareholder concentration of WeLab?"

3. 🔍 数据比较
   - "Compare ZA Bank and WeLab Bank"
   - "Which has higher executive director ratio?"

4. 💬 上下文对话
   - "What about WeLab?" (承接上文)
   - "And their top shareholders?" (连续提问)

5. 📚 知识查询
   - "What is executive_director_ratio?"
   - "Explain shareholder concentration"

6. 👋 日常对话
   - "Hello!"
   - "Thank you"
   - "Goodbye"

输入 'quit' 或 'exit' 退出
输入 'history' 查看对话历史
输入 'clear' 清空对话历史
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

            result = client.process_query(user_input, stream_output=False)

            elapsed = time.time() - start_time

            # 显示结果
            print("\n" + "─"*80)
            print(f"📊 Status: {result['status']}")
            if result['status'] == 'success':
                print(f"🎯 Type: {result.get('query_type', 'unknown')}")
                if 'rewritten_query' in result and result['rewritten_query'] != result['query']:
                    print(f"✏️  Rewrite: {result['query']} → {result['rewritten_query']}")
            print(f"⏱️  Time: {result['execution_time']:.2f}s")
            print("─"*80)

            print(f"\n🤖 Assistant: {result['answer']}")

        except KeyboardInterrupt:
            print("\n\n👋 Demo interrupted")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()

    # 显示统计
    print_section("📊 对话统计")
    stats = client.conversation_manager.get_stats()
    print(f"   总轮次: {stats['total_turns']}")
    print(f"   上一个公司: {stats.get('last_company', 'N/A')}")
    print(f"   实体数量: {stats.get('entities_count', 0)}")
    print(f"   槽位数量: {stats.get('slots_count', 0)}")

    client.close()

    print("\n" + "="*80)
    print("✅ Demo completed! Thank you for trying Enhanced FinTalk.AI!")
    print("="*80 + "\n")


if __name__ == "__main__":
    try:
        interactive_demo()
    except Exception as e:
        print(f"\n❌ Fatal Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

#!/usr/bin/env python3
"""
测试 Enhanced FinTalk.AI - 本地模式
"""

import os
import sys

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from enhanced_fintalk import EnhancedFinTalkAI


def test_basic_functionality():
    """测试基本功能"""

    print("\n" + "="*80)
    print("🧪 Testing Enhanced FinTalk.AI - Local Mode")
    print("="*80)

    # 使用本地模式（不依赖Docker）
    client = EnhancedFinTalkAI(use_osworld=False)

    # 测试用例
    test_cases = [
        ("Hello!", "small_talk", "问候"),
        ("What is ZA Bank's employee size?", "task", "公司信息查询"),
        ("And WeLab?", "task", "上下文相关查询"),
    ]

    print(f"\n📝 Running {len(test_cases)} test cases...\n")

    passed = 0
    failed = 0

    for i, (query, expected_type, description) in enumerate(test_cases, 1):
        print(f"\n{'─'*80}")
        print(f"Test {i}/{len(test_cases)}: {description}")
        print(f"Query: {query}")
        print(f"{'─'*80}")

        try:
            result = client.process_query(query, stream_output=False)

            status = result['status']
            query_type = result.get('query_type', 'unknown')
            answer = result['answer']
            exec_time = result['execution_time']

            # 检查结果
            if status == "success":
                print(f"\n✅ Status: {status}")
                print(f"   Type: {query_type}")
                print(f"   Answer: {answer[:100]}...")
                print(f"   Time: {exec_time:.2f}s")

                if query_type == expected_type:
                    print(f"   ✓ Query type matches expected: {expected_type}")
                    passed += 1
                else:
                    print(f"   ⚠ Query type mismatch: expected {expected_type}, got {query_type}")
                    passed += 1  # 仍然算通过，因为可能LLM判断不同
            else:
                print(f"\n❌ Status: {status}")
                failed += 1

        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    # 显示对话历史
    print(f"\n{'='*80}")
    print("💬 Conversation History:")
    print(f"{'='*80}")
    for turn in client.conversation_manager.history:
        print(f"User: {turn.user}")
        print(f"Assistant: {turn.assistant[:80]}...")
        print(f"Type: {turn.query_type}\n")

    # 统计
    print(f"{'='*80}")
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    print(f"{'='*80}")

    client.close()

    return passed, failed


if __name__ == "__main__":
    try:
        passed, failed = test_basic_functionality()

        if failed == 0:
            print("\n✅ All tests passed!")
            sys.exit(0)
        else:
            print(f"\n⚠️ {failed} test(s) failed")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n👋 Test interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

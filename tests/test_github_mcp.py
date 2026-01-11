#!/usr/bin/env python3
"""
测试 GitHub Repository Manager - MCP完整功能
使用 .env 文件管理密钥
"""

import os
import sys
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# Setup path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mcp_integration import MCPClient


def main():
    print('🚀 Testing GitHub Repository Manager...\n')
    print(f'🔑 Using GitHub Token: {os.getenv("GITHUB_TOKEN")[:15]}...')

    # 初始化MCP客户端
    mcp_client = MCPClient(log_dir="mcp_integration/logs")

    print(f'\n✅ MCP Client initialized')
    print(f'📋 Available tools: {len(mcp_client.get_tools())}')
    print('\n🔧 Available MCP Tools:')
    for tool in mcp_client.get_tools():
        print(f'  • {tool["name"]}: {tool["description"]}')

    # 测试1: 获取仓库文件
    print('\n📝 Test 1: Get file from repository...')
    result = mcp_client.call_tool('github_repo_manager', {
        'action': 'get_file',
        'path': 'mcp_test.py'  # 读取刚才创建的文件
    })
    print(f'   Status: {result["status"]}')
    if result['status'] == 'success':
        print(f'   File: {result["path"]}')
        print(f'   Size: {len(result["content"])} bytes')
        print(f'   URL: {result["url"]}')
    else:
        print(f'   Error: {result.get("error", "Unknown error")}')

    # 测试2: 更新文件
    print('\n📝 Test 2: Update file...')
    new_content = '''#!/usr/bin/env python3
"""
Test file created via MCP GitHub Repository Manager
Updated via MCP!
"""
print("Hello from MCP - Updated!")
'''
    result = mcp_client.call_tool('github_repo_manager', {
        'action': 'update_file',
        'path': 'mcp_test.py',
        'content': new_content,
        'message': 'Update test file via MCP'
    })
    print(f'   Status: {result["status"]}')
    if result['status'] == 'success':
        print(f'   Updated: {result["path"]}')
        print(f'   URL: {result["url"]}')

    # 测试3: 创建分支
    print('\n📝 Test 3: Create branch...')
    result = mcp_client.call_tool('github_repo_manager', {
        'action': 'create_branch',
        'branch': 'test-mcp-feature'
    })
    print(f'   Status: {result["status"]}')
    if result['status'] == 'success':
        print(f'   Branch: {result["branch"]} created from {result["from"]}')
    else:
        print(f'   Note: {result.get("error", "Branch may already exist")}')

    # 显示日志
    print('\n📋 MCP Log Summary:')
    print(mcp_client.get_logs_summary())

    print('\n✅ All GitHub repo manager tests completed!')


if __name__ == "__main__":
    main()

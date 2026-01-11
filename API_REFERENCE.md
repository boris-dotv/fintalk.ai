# FinTalk.AI - API Reference

## 📡 当前使用的API

### MCP外部工具（目前都是模拟数据）

| 工具名称 | 描述 | 当前状态 | 真实API推荐 |
|---------|------|---------|------------|
| `web_search` | Web搜索 | ✅ Mock数据 | Google Custom Search API / Bing Search API |
| `get_stock_price` | 股票价格查询 | ✅ Mock数据 | Alpha Vantage (免费) / Yahoo Finance API |
| `get_financial_news` | 金融新闻 | ✅ Mock数据 | NewsAPI.org (免费额度) / Bing News API |
| `search_github` | GitHub代码搜索 | ✅ Mock数据 | GitHub REST API (免费) |

## 🎯 系统中定义的所有Action

### 本地函数（5个）
```python
1. get_company_info          # 获取公司基本信息
2. get_executive_director_ratio  # 计算执行董事比率
3. get_top_shareholders       # 获取前N大股东
4. calculate_shareholder_concentration  # 计算股东集中度
5. compare_companies          # 比较两个公司
```

### MCP外部工具（4个）
```python
1. web_search              # Web搜索
2. get_stock_price         # 股票价格
3. get_financial_news      # 金融新闻
4. search_github           # GitHub搜索
```

### MCP核心功能（8个模块）
```python
1. ParallelExecutor        # 并行模型调用
2. QueryRewriter          # Query改写
3. QueryArbitrator        # 仲裁机制
4. RejectionDetector      # 拒识检测
5. CorrelationChecker     # 相关性判断
6. StreamingNLG           # 流式输出/NLG
7. ConversationManager    # 对话管理
8. FinancialFunctionRegistry  # Function注册
```

## 📁 文件结构

```
fintalk.ai/
├── enhanced_fintalk.py          # ⭐ 最完备的系统入口
├── demo_with_mcp.py             # ⭐ 功能最全的体验中心
├── enhanced_core/               # MCP核心功能模块
│   ├── __init__.py
│   ├── parallel_executor.py
│   ├── query_rewriter.py
│   ├── arbitrator.py
│   ├── rejection_detector.py
│   ├── correlation_checker.py
│   ├── function_registry.py
│   ├── conversation_manager.py
│   └── streaming_nlg.py
├── mcp_integration/             # MCP外部工具集成
│   ├── __init__.py
│   ├── mcp_client.py
│   └── logs/                    # MCP通信日志
└── demos/                       # 其他demo（移到这个目录）
```

## 🚀 运行完整功能体验

```bash
/Users/dotvigor/dotvigor/venv/bin/python demo_with_mcp.py
```

## 💡 超级难题示例

```python
"""
综合问题示例：
"Compare ZA Bank and WeLab Bank on executive_director_ratio,
then search for latest news about virtual banks in Hong Kong,
get stock price for 03863.HK (ZA Bank's parent company),
and search GitHub for MCP implementation examples.
Finally, summarize all findings in a professional report."

这个问题会调用：
1. ✅ 本地函数: compare_companies (需要调用 executive_director_ratio × 2)
2. ✅ MCP工具: get_financial_news
3. ✅ MCP工具: get_stock_price
4. ✅ MCP工具: search_github
5. ✅ NLG: 生成专业报告
"""
```

# FinTalk.AI - 项目结构说明

## 🚀 快速开始

### 运行完整Demo
```bash
python run.py
# 选择 [1] 运行完整Demo
```

### 直接运行MCP Demo
```bash
python demos/demo_with_mcp.py
```

### 测试GitHub集成
```bash
python tests/test_github_mcp.py
```

---

## 📁 目录结构

```
fintalk.ai/
├── 🎯 主入口
│   ├── run.py                      # 统一入口（推荐使用）
│   ├── enhanced_fintalk.py         # 主程序
│   └── formula.py                  # 金融公式库
│
├── 🧠 MCP核心功能 (enhanced_core/)
│   ├── parallel_executor.py        # 并行模型调用
│   ├── query_rewriter.py          # Query改写
│   ├── arbitrator.py              # 仲裁机制
│   ├── rejection_detector.py      # 拒识检测
│   ├── correlation_checker.py     # 相关性判断
│   ├── function_registry.py       # Function注册表
│   ├── conversation_manager.py    # 对话管理
│   └── streaming_nlg.py           # 流式输出/NLG
│
├── 📡 MCP外部工具 (mcp_integration/)
│   ├── mcp_client.py              # MCP客户端（真实API）
│   └── logs/                       # MCP通信日志
│
├── 🎪 Demos (demos/)
│   ├── demo_with_mcp.py           # ⭐ MCP完整功能Demo（推荐）
│   ├── demo_complex_comparison.py # 复杂比较Demo
│   ├── demo_complex_query.py      # 复杂查询Demo
│   ├── demo_docker_osworld.py     # Docker OSWorld Demo
│   ├── demo_full_cot.py           # 完整CoT Demo
│   ├── demo_with_osworld.py       # OSWorld集成Demo
│   └── demo_working.py            # 工作版本Demo
│
├── 🧪 Tests (tests/)
│   ├── test_github_mcp.py         # GitHub MCP测试
│   └── mcp_test.py                # MCP基础测试
│
├── 🐳 OSWorld (OSWorld/)
│   ├── docker_osworld_adapter.py  # Docker适配器
│   ├── osworld_adapter.py         # 通用适配器
│   └── desktop_env/               # OSWorld核心框架
│
├── 📊 数据 (data/)
│   ├── company.csv                # 公司数据
│   ├── management.csv             # 管理层数据
│   └── shareholder.csv            # 股东数据
│
├── ⚙️ 配置
│   ├── .env                        # 环境变量（包含真实密钥）
│   ├── .env.example               # 环境变量模板
│   └── requirements.txt            # Python依赖
│
└── 📖 文档
    ├── README.md                   # 项目说明
    ├── API_REFERENCE.md           # API参考文档
    └── STRUCTURE.md               # 本文件
```

---

## 🔑 环境变量配置

复制 `.env.example` 为 `.env` 并填入你的API密钥：

```bash
cp .env.example .env
```

### 需要配置的API密钥

| API | 必需 | 获取方式 |
|-----|------|---------|
| `GITHUB_TOKEN` | ✅ | GitHub Settings → Developer settings → Personal access tokens |
| `GOOGLE_API_KEY` | ❌ | Google Cloud Console → Custom Search API |
| `ALPHA_VANTAGE_KEY` | ❌ | https://www.alphavantage.co/support/#api-key |
| `NEWS_API_KEY` | ❌ | https://newsapi.org/register |

---

## 🎯 功能清单

### ✅ 本地数据库功能
- [x] 公司信息查询
- [x] 管理层数据查询
- [x] 股东数据查询
- [x] 执行董事比率计算
- [x] 股东集中度计算
- [x] 公司数据比较

### ✅ MCP核心功能
- [x] 并行模型调用（4个任务并行）
- [x] Query改写（基于对话历史）
- [x] 仲裁机制（4种query类型分类）
- [x] 拒识检测（过滤无关查询）
- [x] 相关性判断（多轮对话）
- [x] Function Calling（5个金融函数）
- [x] 流式输出
- [x] 对话管理

### ✅ MCP外部工具
- [x] GitHub搜索（公开API，无需token）
- [x] GitHub仓库管理（需token）
  - [x] 读取文件
  - [x] 创建文件
  - [x] 更新文件
  - [x] 创建Issue
  - [x] 创建分支
- [ ] Google搜索（需API key）
- [ ] Alpha Vantage股票价格（需API key）
- [ ] NewsAPI金融新闻（需API key）

---

## 🚀 使用示例

### 本地数据库查询
```python
# 查询公司信息
"What is ZA Bank's employee size?"

# 计算金融指标
"Calculate executive_director_ratio for ZA Bank"

# 比较两家公司
"Compare ZA Bank and WeLab Bank"
```

### GitHub集成
```python
# 搜索GitHub
"Search GitHub for model context protocol"

# 读取仓库文件
"Get the content of enhanced_fintalk.py"

# 创建文件
"Create a file test.py with hello world code"
```

---

## 📝 开发日志

### 2025-01-11
- ✅ 集成MCP架构（从阿里专家代码学习）
- ✅ 移除所有mock数据，使用真实API
- ✅ 实现GitHub仓库管理功能
- ✅ 添加.env安全管理
- ✅ 清理项目结构，删除不需要的模块
- ✅ 创建统一入口 run.py

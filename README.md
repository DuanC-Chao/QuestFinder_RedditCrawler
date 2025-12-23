# Reddit数据爬取与处理管线

本项目提供完整的Reddit数据爬取、过滤、分类和入库流程，通过一键调用完成所有步骤。

## 快速开始（5分钟上手）

### 一键运行

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量（创建 .env 文件）
# DEEPSEEK_API_KEY=your_api_key_here
# SUPABASE_URL=your_supabase_url
# SUPABASE_ANON_KEY=your_anon_key
# SUPABASE_SERVICE_ROLE_KEY=your_service_role_key

# 3. 配置关键词文件（可选）
# - to_craw_query_seeds.txt: 搜索关键词（每行一个）
# - filter_keywords.txt: 标题过滤关键词（每行一个，或使用 * 表示不过滤）

# 4. 运行管线
python pipeline.py --task-id task001 --max-posts 100

# 5. 导入到Supabase（可选）
python import_to_supabase.py --task-id task001
```

### 常用命令

```bash
# 基本用法（LLM驱动过滤）
python pipeline.py --task-id task001

# 使用基于规则过滤（不需要API密钥）
python pipeline.py --task-id task001 --use-rule-based-filter

# 指定爬取数量
python pipeline.py --task-id task001 --max-posts 100

# 完整参数示例
python pipeline.py --task-id task001 \
  --max-posts 200 \
  --delay 5.0 \
  --use-rule-based-filter \
  --filter-keywords-file manual_filter_keywords.json
```

---

## 三层筛选机制详解

本项目的过滤管线采用**三层筛选机制**，逐层过滤，确保最终数据的质量和相关性。

### 架构概览

```
Reddit搜索 → 第一层筛选（Query Seeds） → 第二层筛选（标题关键词） → 第三层筛选（内容过滤） → 分类 → 入库
```

### 第一层：Query Seeds 搜索筛选

**位置**：爬取阶段（`reddit_html_crawler.py`）

**机制**：
- 使用 `to_craw_query_seeds.txt` 文件中的关键词
- 将关键词拼接到Reddit搜索URL：`https://old.reddit.com/search/?q={关键词}`
- **本质上是利用Reddit的内置搜索功能**进行初步筛选
- 只爬取Reddit搜索返回的结果

**配置文件**：`to_craw_query_seeds.txt`

**示例**：
```
AI Tool
Make PPT with AI
Make report with AI
AI Tool for paper searching
```

**特点**：
- ✅ 利用Reddit的搜索算法，结果相关性高
- ✅ 可以指定多个搜索关键词，脚本会平均分配爬取配额
- ⚠️ 依赖Reddit搜索的准确性

**作用**：**粗筛**，从Reddit全站缩小到与关键词相关的帖子

---

### 第二层：标题关键词过滤

**位置**：爬取阶段（`reddit_html_crawler.py`）

**机制**：
- 使用 `filter_keywords.txt` 文件中的关键词
- 对第一层筛选出的帖子，检查**标题**是否包含任一关键词
- 如果标题不包含任何关键词，则跳过该帖子

**配置文件**：`filter_keywords.txt`

**示例**：
```
tutorial
guide
example
how to
```

**特殊用法**：
- 使用 `*` 表示不过滤，爬取所有帖子
- 以 `#` 开头的行会被忽略

**匹配规则**：
- 不区分大小写
- 标题包含任一关键词即匹配

**特点**：
- ✅ 快速过滤，减少后续处理的数据量
- ✅ 基于标题，处理速度快
- ⚠️ 只检查标题，可能漏掉内容相关但标题不匹配的帖子

**作用**：**中筛**，基于标题快速过滤掉明显不相关的帖子

---

### 第三层：内容深度过滤

**位置**：过滤阶段（`post_filter.py` 或 `post_filter_rule_based.py`）

**机制**：
- 对第二层筛选后的帖子，进行**内容深度分析**
- 检查帖子的**标题、内容、所有评论**中是否包含特定信号
- 必须同时满足条件才会被标记为 `valid`

**两种模式可选**：

#### 模式A：LLM驱动过滤（默认）

**脚本**：`post_filter.py`

**机制**：
- 使用 DeepSeek API 进行智能判断
- 提取帖子的标题、内容、前三条评论
- 发送给 LLM，判断是否包含有效的AI工具使用经验
- LLM 返回 `is_valid: true/false`

**判断标准**（由 `filter_prompt.txt` 定义）：
1. 标题和内容是否关于AI Tool的使用讨论
2. 是否有评论分享了使用AI Tool解决具体问题的经验
3. 该帖子是否**不是**广告、营销

**特点**：
- ✅ 理解语义，准确性高
- ✅ 能识别隐含的AI工具使用场景
- ⚠️ 需要API密钥，有调用成本
- ⚠️ 速度相对较慢（需要API调用）

**使用方法**：
```bash
python pipeline.py --task-id task001
# 或
python post_filter.py --task-id task001 --threads 16
```

#### 模式B：基于规则过滤

**脚本**：`post_filter_rule_based.py`

**机制**：
- 从 `manual_filter_keywords.json` 读取关键词列表
- 提取 `ai_signal_block` 和 `recipe_signal_block` 关键词
- 检查帖子内容（标题+正文+所有评论）是否**同时包含**：
  - `ai_signal_block` 中的至少一个关键词（如 "AI", "ChatGPT", "LLM"）
  - `recipe_signal_block` 中的至少一个关键词（如 "prompt", "workflow", "template"）

**配置文件**：`manual_filter_keywords.json`

**示例配置**：
```json
{
  "global_constraints": {
    "ai_signal_block": [
      "AI",
      "ChatGPT",
      "LLM",
      "large language model"
    ],
    "recipe_signal_block": [
      "prompt",
      "workflow",
      "template",
      "step by step"
    ]
  }
}
```

**匹配规则**：
- 忽略大小写（"AI" 匹配 "ai", "Ai"）
- 忽略标点符号（"AI" 匹配 "A.I.", "ai-powered"）
- 支持短语匹配（"large language model" 作为整体匹配）
- 单词边界匹配（避免部分匹配）

**特点**：
- ✅ 不需要API密钥，无成本
- ✅ 速度快（本地匹配）
- ✅ 可解释性强（显示匹配的关键词）
- ⚠️ 准确性取决于关键词配置
- ⚠️ 无法理解语义

**使用方法**：
```bash
python pipeline.py --task-id task001 --use-rule-based-filter
# 或
python post_filter_rule_based.py --task-id task001 --threads 16
```

**作用**：**精筛**，确保最终数据包含有效的AI工具使用经验

---

### 三层筛选流程图

```
┌─────────────────────────────────────────────────────────────┐
│ Reddit全站                                                  │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 第一层：Query Seeds搜索筛选                                 │
│ - 使用Reddit内置搜索功能                                    │
│ - 配置文件：to_craw_query_seeds.txt                        │
│ - 示例：搜索 "AI Tool"、"Make PPT with AI" 等              │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 第二层：标题关键词过滤                                      │
│ - 检查标题是否包含关键词                                    │
│ - 配置文件：filter_keywords.txt                            │
│ - 示例：标题包含 "tutorial"、"guide" 等                     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ 第三层：内容深度过滤（二选一）                              │
│                                                             │
│  ┌────────────────────────┐  ┌──────────────────────────┐ │
│  │ LLM驱动过滤            │  │ 基于规则过滤             │ │
│  │ - 使用DeepSeek API     │  │ - 关键词匹配            │ │
│  │ - 理解语义             │  │ - 本地匹配              │ │
│  │ - 需要API密钥          │  │ - 不需要API密钥         │ │
│  └────────────────────────┘  └──────────────────────────┘ │
│                                                             │
│ 配置文件：filter_prompt.txt    配置文件：manual_filter_... │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Valid帖子（标记为 contains_valid_ai_tool_recipe: true）    │
└─────────────────────────────────────────────────────────────┘
```

---

### 筛选效果示例

假设Reddit全站有1000个帖子：

1. **第一层筛选后**：Reddit搜索 "AI Tool" 返回 200 个帖子
2. **第二层筛选后**：标题包含 "tutorial" 或 "guide" 的帖子，剩余 50 个
3. **第三层筛选后**：内容同时包含AI信号和Recipe信号的帖子，剩余 20 个

最终这20个帖子会被标记为 `valid`，进入后续的分类和入库流程。

---

## 安装和配置

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

创建 `.env` 文件，配置以下变量：

```env
# DeepSeek API（用于LLM驱动过滤和分类）
# 注意：如果使用基于规则过滤，可以不配置此项
DEEPSEEK_API_KEY=your_deepseek_api_key_here

# Supabase配置（用于入库）
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```

**注意**：
- 如果使用**LLM驱动过滤**（默认），必须配置 `DEEPSEEK_API_KEY`
- 如果使用**基于规则过滤**（`--use-rule-based-filter`），可以不配置 `DEEPSEEK_API_KEY`
- 分类阶段始终需要 `DEEPSEEK_API_KEY`（无论使用哪种过滤模式）

### 3. 配置关键词文件

#### `to_craw_query_seeds.txt` - 第一层搜索关键词

每行一个关键词，脚本会将这些关键词拼接到Reddit搜索URL中：

```
AI Tool
Make PPT with AI
Make report with AI
AI Tool for paper searching
```

#### `filter_keywords.txt` - 第二层标题过滤关键词

每行一个关键词，标题包含任一关键词的帖子会被爬取：

```
tutorial
guide
example
```

**特殊用法：**
- 使用 `*` 表示不过滤，爬取所有帖子
- 以 `#` 开头的行会被忽略

#### `manual_filter_keywords.json` - 第三层规则过滤关键词配置（可选）

如果使用基于规则过滤（`--use-rule-based-filter`），需要配置此文件：

```json
{
  "global_constraints": {
    "ai_signal_block": [
      "AI",
      "ChatGPT",
      "LLM",
      "large language model"
    ],
    "recipe_signal_block": [
      "prompt",
      "workflow",
      "template",
      "step by step"
    ]
  }
}
```

**说明：**
- `ai_signal_block`: AI信号关键词列表，Post必须包含其中至少一个
- `recipe_signal_block`: Recipe信号关键词列表，Post必须包含其中至少一个
- Post必须**同时包含**AI信号和Recipe信号才会被标记为valid
- 关键词匹配时忽略大小写和标点符号

---

## 参数说明

### 必需参数

- `--task-id`, `-t`: 任务ID（必需，用于命名输出文件）

### 文件路径参数

- `--query-seeds-file`, `-q`: 搜索关键词列表文件路径（默认: `to_craw_query_seeds.txt`）
- `--keywords-file`, `-k`: 标题过滤关键词列表文件路径（默认: `filter_keywords.txt`）

### 爬取参数

- `--delay`, `-d`: 请求之间的延迟（秒，默认3.0，建议3-5秒以避免限流）
- `--max-posts`, `-m`: 最大爬取帖子数量（默认不限制）
- `--crawl-threads`: 爬取阶段的并发线程数（默认8）

### 过滤参数

- `--filter-threads`: 过滤阶段的并发线程数（默认16）
- `--use-rule-based-filter`: **使用基于规则的过滤**（默认使用LLM驱动过滤）
- `--filter-keywords-file`: 规则过滤的关键词配置文件路径（默认: `manual_filter_keywords.json`）

### 分类参数

- `--classify-threads`: 分类阶段的并发线程数（默认16）
- `--max-chars`, `-c`: 分类时的最大字符数限制（可选）

### 其他参数

- `--user-agent`: 自定义User-Agent

---

## 过滤模式选择

管线脚本支持两种过滤模式，您可以根据需求选择：

### 1. LLM驱动过滤（默认）

**特点：**
- 使用 DeepSeek API 进行智能判断
- 理解语义，准确性高
- 需要配置 `DEEPSEEK_API_KEY`
- 有 API 调用成本
- 速度相对较慢（需要 API 调用）

**使用方法：**
```bash
# 默认就是LLM驱动过滤
python pipeline.py --task-id task001
```

**要求：**
- 必须在 `.env` 文件中配置 `DEEPSEEK_API_KEY`

### 2. 基于规则过滤

**特点：**
- 基于关键词匹配规则
- 不需要 API 密钥，无成本
- 速度快（本地匹配）
- 可解释性强（显示匹配的关键词）
- 准确性取决于关键词配置

**使用方法：**
```bash
# 使用基于规则过滤
python pipeline.py --task-id task001 --use-rule-based-filter

# 自定义关键词配置文件
python pipeline.py --task-id task001 --use-rule-based-filter --filter-keywords-file my_keywords.json
```

**规则说明：**
- 从 `manual_filter_keywords.json` 读取关键词
- 提取 `ai_signal_block` 和 `recipe_signal_block` 关键词列表
- Post 必须**同时包含**：
  - `ai_signal_block` 中的至少一个关键词（如 "AI", "ChatGPT", "LLM" 等）
  - `recipe_signal_block` 中的至少一个关键词（如 "prompt", "workflow", "template" 等）
- 匹配时忽略大小写和标点符号

### 如何选择？

| 场景 | 推荐模式 |
|------|---------|
| 需要高准确性，理解语义 | LLM驱动过滤 |
| 快速测试，不需要API | 基于规则过滤 |
| 批量处理，成本敏感 | 基于规则过滤 |
| 需要可解释性 | 基于规则过滤 |
| 关键词规则明确 | 基于规则过滤 |

---

## 工作流程

管线脚本按以下顺序执行三个步骤：

1. **爬取阶段** (`reddit_html_crawler.py`)
   - 从Reddit搜索页面爬取帖子
   - 提取帖子内容和评论树
   - 保存到 `Data/raw/{task_id}.json`

2. **过滤阶段**（可选择两种模式之一）
   - **LLM驱动过滤** (`post_filter.py`)：
     - 使用DeepSeek API智能判断
     - 理解语义，准确性高
     - 需要API密钥
   - **基于规则过滤** (`post_filter_rule_based.py`)：
     - 基于关键词匹配规则
     - 不需要API密钥，速度快
     - 可解释性强（显示匹配的关键词）
   - 两种模式都会更新 `Data/mask/{task_id}_mask.json`

3. **分类阶段** (`post_classifier.py`)
   - 使用DeepSeek API分类帖子
   - 生成质量分数、场景、类型等信息
   - 保存到 `Data/classifier_output/{task_id}_classifier.json`
   - 生成数据库就绪数据 `Data/ready_for_DB/{task_id}_ready.json`

---

## 错误处理

**重要**：如果中途任何阶段失败，管线会自动清理所有已创建的文件，确保不会留下不完整的数据。

例如：
- 如果爬取阶段失败，不会创建任何文件
- 如果过滤阶段失败，会删除已创建的raw文件
- 如果分类阶段失败，会删除已创建的raw和mask文件

---

## 导入到Supabase

管线完成后，可以使用以下命令将数据导入到Supabase：

```bash
python import_to_supabase.py --task-id task001
```

导入脚本支持：
- **增量导入**：默认跳过已存在的记录，不会覆盖
- **自动修复**：自动修复source_url字段（从搜索页面URL提取实际帖子URL）
- **数据验证**：导入前进行完整性检查

### 导入参数

```bash
# 基本用法（增量导入，不覆盖）
python import_to_supabase.py --task-id task001

# 更新已存在的记录（谨慎使用）
python import_to_supabase.py --task-id task001 --update-existing

# 自定义批次大小
python import_to_supabase.py --task-id task001 --batch-size 50
```

---

## 输出文件结构

```
Data/
├── raw/
│   └── {task_id}.json              # 原始爬取数据
├── mask/
│   └── {task_id}_mask.json         # 过滤结果（valid/invalid标记）
├── classifier_output/
│   └── {task_id}_classifier.json   # 分类结果（质量分数、场景、类型）
└── ready_for_DB/
    └── {task_id}_ready.json        # 数据库就绪数据
```

---

## 注意事项

1. **Reddit限流**：Reddit有严格的限流策略，建议使用较大的delay值（3-5秒）
2. **API密钥**：
   - 使用LLM驱动过滤时，必须在 `.env` 文件中配置 `DEEPSEEK_API_KEY`
   - 使用基于规则过滤时，不需要API密钥
3. **任务ID唯一性**：每个任务ID只能使用一次，不能重复
4. **网络稳定性**：确保网络连接稳定，避免中途断网导致失败
5. **数据量**：如果爬取大量数据，建议分批处理，避免单次处理时间过长
6. **过滤模式**：
   - LLM驱动过滤：准确性高，但需要API密钥，有成本
   - 基于规则过滤：速度快，无成本，但准确性取决于关键词配置

---

## 故障排除

### 问题：Reddit限流错误（429）

**解决方案**：
- 增加 `--delay` 参数值（建议5秒或更高）
- 减少 `--crawl-threads` 参数值（建议4或更低）
- 等待一段时间后重试

### 问题：DeepSeek API调用失败

**解决方案**：
- 检查 `.env` 文件中的 `DEEPSEEK_API_KEY` 是否正确
- 检查API密钥是否有效且有足够的配额
- 检查网络连接是否正常
- **替代方案**：使用基于规则过滤，不需要API密钥：
  ```bash
  python pipeline.py --task-id task001 --use-rule-based-filter
  ```

### 问题：任务ID已存在

**解决方案**：
- 使用不同的任务ID
- 或删除已存在的文件：`Data/raw/{task_id}.json`

### 问题：导入到Supabase失败

**解决方案**：
- 检查 `.env` 文件中的Supabase配置是否正确
- 检查数据库表是否存在
- 检查数据格式是否正确（运行管线脚本会自动验证）

---

## 示例

### 示例1：爬取100个帖子（LLM驱动过滤）

```bash
python pipeline.py --task-id task001 --max-posts 100
```

### 示例2：爬取100个帖子（基于规则过滤）

```bash
python pipeline.py --task-id task001 --max-posts 100 --use-rule-based-filter
```

### 示例3：使用自定义关键词文件

```bash
# LLM驱动过滤
python pipeline.py --task-id task002 \
  --query-seeds-file my_seeds.txt \
  --keywords-file my_keywords.txt

# 基于规则过滤
python pipeline.py --task-id task002 \
  --query-seeds-file my_seeds.txt \
  --keywords-file my_keywords.txt \
  --use-rule-based-filter \
  --filter-keywords-file my_filter_keywords.json
```

### 示例4：完整流程（爬取 → 导入）

```bash
# 1. 运行管线（LLM驱动过滤）
python pipeline.py --task-id task001 --max-posts 100

# 或者使用基于规则过滤
python pipeline.py --task-id task001 --max-posts 100 --use-rule-based-filter

# 2. 导入到Supabase
python import_to_supabase.py --task-id task001
```

### 示例5：独立使用过滤脚本

```bash
# LLM驱动过滤
python post_filter.py --task-id task001 --threads 16

# 基于规则过滤
python post_filter_rule_based.py --task-id task001 --threads 16
```

---

## 许可证

MIT License

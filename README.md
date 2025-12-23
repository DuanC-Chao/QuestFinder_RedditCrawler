# Reddit数据爬取与处理管线

本项目提供完整的Reddit数据爬取、过滤、分类和入库流程，通过一键调用完成所有步骤。

## 快速开始

### 一键运行

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量（创建 .env 文件）
# DEEPSEEK_API_KEY=your_api_key_here
# SUPABASE_URL=your_supabase_url
# SUPABASE_ANON_KEY=your_anon_key
# SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
# REDDIT_CLIENT_ID=your_reddit_client_id
# REDDIT_CLIENT_SECRET=your_reddit_client_secret
# REDDIT_USER_AGENT=python:QuestFinderCrawler:v1.0.0 (by u/QuestFinder)

# 3. 配置关键词文件
# - to_craw_query_seeds.txt: 搜索关键词（每行一个）
# - filter_keywords.txt: 标题过滤关键词（每行一个，或使用 * 表示不过滤）

# 4. 运行新管线
python newpipeline.py --task-id task001 --max-first-level-comments 1000

# 5. 导入到Supabase（可选）
python import_to_supabase.py --task-id task001
```

### 常用命令

```bash
# 基本用法
python newpipeline.py --task-id task001 --max-first-level-comments 1000

# 指定爬取数量（一级评论数）
python newpipeline.py --task-id task001 --max-first-level-comments 500

# 自定义延迟和线程数
python newpipeline.py --task-id task001 \
  --max-first-level-comments 1000 \
  --delay 1.0 \
  --crawl-threads 8 \
  --classify-threads 16
```

---

## 新管线架构

### 数据流程

```
Reddit搜索 → 爬取帖子 → 提取第一层评论 → 过滤二级评论 → 分类 → 准备数据库数据 → 入库
```

### 核心概念

**重要**：新管线的数据模型发生了根本性变化：

- **旧模型**：以Post为单位，Post包含多个评论
- **新模型**：以第一层评论为单位，每个第一层评论被视为一个独立的"Post"

这意味着：
- 爬取时，每个第一层评论会作为独立项目保存到 `Data/raw/`
- 入库时，第一层评论进入 `crawled_posts` 表
- 二级及更深层级的评论进入 `crawled_comments` 表

---

## 管线步骤详解

### 步骤1：爬取Reddit数据

**脚本**：`reddit_html_crawler.py`

**功能**：
- 从Reddit搜索页面爬取帖子
- 提取每个帖子的第一层评论
- **将每个第一层评论作为独立项目保存**

**输出**：
- `Data/raw/{task_id}.json`：每个第一层评论作为独立项目

**关键参数**：
- `--max-first-level-comments`：最大一级评论数量（会平均分配给所有搜索关键词）
- `--delay`：请求延迟（秒，默认0.5）
- `--threads`：并发线程数（默认8）

**数据格式**：
```json
[
  {
    "post_id": 1,
    "platform": "reddit",
    "source_url": "https://reddit.com/...",
    "source_platform_id": "comment_id",
    "title": "原Post标题",
    "content_text": "第一层评论内容",
    "comments_tree": [
      {
        "id": "comment_id",
        "body": "评论内容",
        "score": 10,
        "replies": [...]
      }
    ],
    "query_seed": "AI Tool"
  }
]
```

---

### 步骤2：过滤评论

**脚本**：`comment_filter.py`

**功能**：
- 对每个第一层评论，如果其二级评论超过5个，只保留upvote最多的5个
- 过滤后的数据保存到 `Data/comment_filtered_raw/`

**输出**：
- `Data/comment_filtered_raw/{task_id}.json`

**关键参数**：
- `--max-second-level`：最多保留的二级评论数（默认5）

**作用**：
- 减少数据量，只保留高质量评论
- 为后续分类和入库做准备

---

### 步骤3：分类一级评论

**脚本**：`comment_classifier.py`

**功能**：
- 使用DeepSeek API对每个第一层评论进行分类
- 生成质量分数、场景、类型等信息

**输出**：
- `Data/classifier_output/{task_id}_classifier.json`

**分类维度**：
- `base_quality_score`：质量分数（1-100）
- `scene`：场景（Slides, Docs, Research, Automation）
- `post_type`：类型（Prompt-only, workflow, tip）

**提示词模板**：`classifier_prompt.txt`

**关键参数**：
- `--threads`：并发线程数（默认16）
- `--max-chars`：最大字符数限制（可选）

---

### 步骤4：准备数据库数据

**脚本**：`prepare_for_db.py`

**功能**：
- 将过滤后的第一层评论转换为 `crawled_posts` 表格式
- 将二级及更深层级评论转换为 `crawled_comments` 表格式
- 整合分类结果（scene, post_type, base_quality_score）

**输出**：
- `Data/ready_for_DB_posts/{task_id}_posts.json`：Posts数据（第一层评论）
- `Data/ready_for_DB_comments/{task_id}_comments.json`：Comments数据（二级及更深层级评论）

**关键字段映射**：
- Posts：第一层评论 → `crawled_posts` 表
- Comments：二级及更深层级评论 → `crawled_comments` 表
- 关联关系：通过 `_post_source_platform_id` 和 `_parent_source_comment_id` 建立

---

### 步骤5：导入到Supabase（可选）

**脚本**：`import_to_supabase.py`

**功能**：
- 将 `ready_for_DB_posts` 和 `ready_for_DB_comments` 数据导入到Supabase
- 自动建立外键关联（`post_id`, `parent_comment_id`）

**关键特性**：
- **增量导入**：默认跳过已存在的记录
- **自动关联**：自动建立posts和comments的关联关系
- **数据验证**：导入前进行完整性检查

**使用方法**：
```bash
python import_to_supabase.py --task-id task001
```

---

## 安装和配置

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

创建 `.env` 文件，配置以下变量：

```env
# DeepSeek API（用于分类）
DEEPSEEK_API_KEY=your_deepseek_api_key_here

# Supabase配置（用于入库）
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key

# Reddit API配置（用于爬取）
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=python:QuestFinderCrawler:v1.0.0 (by u/QuestFinder)
```

**Reddit API配置说明**：
1. 访问 https://www.reddit.com/prefs/apps
2. 创建新应用（选择"script"类型）
3. 获取 `client_id` 和 `client_secret`
4. 设置 `user_agent`（格式：`python:应用名:版本号 (by u/用户名)`）

### 3. 配置关键词文件

#### `to_craw_query_seeds.txt` - 搜索关键词

每行一个关键词，脚本会将这些关键词用于Reddit搜索：

```
AI Tool
LLM
large language model
language model
foundation model
```

#### `filter_keywords.txt` - 标题过滤关键词

每行一个关键词，标题包含任一关键词的帖子会被爬取：

```
tutorial
guide
example
```

**特殊用法**：
- 使用 `*` 表示不过滤，爬取所有帖子
- 以 `#` 开头的行会被忽略

---

## 参数说明

### 必需参数

- `--task-id`, `-t`：任务ID（必需，用于命名输出文件）

### 文件路径参数

- `--query-seeds-file`, `-q`：搜索关键词列表文件路径（默认: `to_craw_query_seeds.txt`）
- `--keywords-file`, `-k`：标题过滤关键词列表文件路径（默认: `filter_keywords.txt`）

### 爬取参数

- `--delay`, `-d`：请求之间的延迟（秒，默认0.5）
- `--max-first-level-comments`, `-m`：最大一级评论数量（默认不限制，会平均分配给所有搜索关键词）
- `--crawl-threads`：爬取阶段的并发线程数（默认8）

### 过滤参数

- `--max-second-level`：最多保留的二级评论数（默认5）

### 分类参数

- `--classify-threads`：分类阶段的并发线程数（默认16）
- `--max-chars`, `-c`：分类时的最大字符数限制（可选）

---

## 输出文件结构

```
Data/
├── raw/
│   └── {task_id}.json                    # 原始爬取数据（每个第一层评论作为独立项目）
├── comment_filtered_raw/
│   └── {task_id}.json                    # 过滤后的评论数据（每个第一层评论最多5个二级评论）
├── classifier_output/
│   └── {task_id}_classifier.json         # 分类结果（质量分数、场景、类型）
├── ready_for_DB_posts/
│   └── {task_id}_posts.json             # Posts数据（第一层评论，准备入库）
└── ready_for_DB_comments/
    └── {task_id}_comments.json          # Comments数据（二级及更深层级评论，准备入库）
```

---

## 数据模型说明

### 新模型：以第一层评论为中心

**核心变化**：
- 每个第一层评论被视为一个独立的"Post"
- 第一层评论的内容作为 `crawled_posts.content_text`
- 原Post的标题保留在 `crawled_posts.title`
- 二级及更深层级评论作为 `crawled_comments`

**数据关联**：
- `crawled_comments.post_id` → `crawled_posts.id`（通过UUID关联）
- `crawled_comments.parent_comment_id` → `crawled_comments.id`（建立评论树）

**关联字段**：
- `_post_source_platform_id`：第一层评论的ID（用于关联post）
- `_parent_source_comment_id`：父评论的ID（用于建立父子关系）

---

## 工作流程

新管线按以下顺序执行四个步骤：

1. **爬取阶段** (`reddit_html_crawler.py`)
   - 从Reddit搜索页面爬取帖子
   - 提取每个帖子的第一层评论
   - 将每个第一层评论作为独立项目保存
   - 输出：`Data/raw/{task_id}.json`

2. **过滤阶段** (`comment_filter.py`)
   - 对每个第一层评论，如果二级评论超过5个，只保留upvote最多的5个
   - 输出：`Data/comment_filtered_raw/{task_id}.json`

3. **分类阶段** (`comment_classifier.py`)
   - 使用DeepSeek API对每个第一层评论进行分类
   - 生成质量分数、场景、类型等信息
   - 输出：`Data/classifier_output/{task_id}_classifier.json`

4. **准备数据库数据** (`prepare_for_db.py`)
   - 将第一层评论转换为 `crawled_posts` 表格式
   - 将二级及更深层级评论转换为 `crawled_comments` 表格式
   - 整合分类结果
   - 输出：`Data/ready_for_DB_posts/{task_id}_posts.json` 和 `Data/ready_for_DB_comments/{task_id}_comments.json`

5. **导入到Supabase** (`import_to_supabase.py`) - 可选
   - 将数据导入到Supabase数据库
   - 自动建立外键关联

---

## 错误处理

**重要**：如果中途任何阶段失败，管线会自动清理所有已创建的文件，确保不会留下不完整的数据。

例如：
- 如果爬取阶段失败，不会创建任何文件
- 如果过滤阶段失败，会删除已创建的raw文件
- 如果分类阶段失败，会删除已创建的raw和filtered文件
- 如果准备数据库数据阶段失败，会删除所有中间文件

---

## 合并多个Task数据

如果需要对多个task的数据进行合并，可以使用 `merge_tasks.py`：

```bash
# 合并所有task到merged_task001
python merge_tasks.py --output-task-id merged_task001

# 合并指定的task列表
python merge_tasks.py --output-task-id merged_task001 --tasks task001 task002 task003

# 合并所有task，不去重
python merge_tasks.py --output-task-id merged_task001 --no-skip-duplicates
```

合并后的数据会保存到：
- `Data/ready_for_DB_posts/{output_task_id}_posts.json`
- `Data/ready_for_DB_comments/{output_task_id}_comments.json`

---

## 导入到Supabase

管线完成后，可以使用以下命令将数据导入到Supabase：

```bash
python import_to_supabase.py --task-id task001
```

导入脚本支持：
- **增量导入**：默认跳过已存在的记录，不会覆盖
- **自动关联**：自动建立posts和comments的关联关系
- **数据验证**：导入前进行完整性检查
- **字段处理**：`media_urls` 空列表转为 `null`，`content_type` 固定为 `"answer"`

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

## 注意事项

1. **Reddit API配置**：必须配置 `REDDIT_CLIENT_ID` 和 `REDDIT_CLIENT_SECRET`
2. **任务ID唯一性**：每个任务ID只能使用一次，不能重复
3. **数据模型变化**：新管线以第一层评论为中心，与旧管线的数据模型不同
4. **网络稳定性**：确保网络连接稳定，避免中途断网导致失败
5. **数据量**：如果爬取大量数据，建议分批处理，避免单次处理时间过长
6. **API密钥**：分类阶段需要 `DEEPSEEK_API_KEY`，入库阶段需要Supabase配置

---

## 故障排除

### 问题：Reddit API认证失败

**解决方案**：
- 检查 `.env` 文件中的 `REDDIT_CLIENT_ID` 和 `REDDIT_CLIENT_SECRET` 是否正确
- 检查 `REDDIT_USER_AGENT` 格式是否正确
- 确认Reddit应用类型为"script"

### 问题：DeepSeek API调用失败

**解决方案**：
- 检查 `.env` 文件中的 `DEEPSEEK_API_KEY` 是否正确
- 检查API密钥是否有效且有足够的配额
- 检查网络连接是否正常

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

### 示例1：基本用法

```bash
python newpipeline.py --task-id task001 --max-first-level-comments 1000
```

### 示例2：自定义参数

```bash
python newpipeline.py --task-id task001 \
  --max-first-level-comments 500 \
  --delay 1.0 \
  --crawl-threads 8 \
  --classify-threads 16 \
  --max-second-level 5
```

### 示例3：完整流程（爬取 → 导入）

```bash
# 1. 运行管线
python newpipeline.py --task-id task001 --max-first-level-comments 1000

# 2. 导入到Supabase
python import_to_supabase.py --task-id task001
```

### 示例4：合并多个task

```bash
# 1. 运行多个task
python newpipeline.py --task-id task001 --max-first-level-comments 500
python newpipeline.py --task-id task002 --max-first-level-comments 500

# 2. 合并数据
python merge_tasks.py --output-task-id merged_task001 --tasks task001 task002

# 3. 导入合并后的数据
python import_to_supabase.py --task-id merged_task001
```

---

## 辅助工具

### `merge_tasks.py` - 合并多个task数据

合并多个task的结果数据到一个新的task：

```bash
python merge_tasks.py --output-task-id merged_task001
```

### `view_ready_data.py` - 查看ready_for_DB数据

以人类可读的方式查看ready_for_DB数据：

```bash
python view_ready_data.py --task-id task001
```

### `extract_recipe.py` - 提取AI工具Recipe

从ready_for_DB数据中提取AI工具Recipe：

```bash
python extract_recipe.py --task-id task001 --threads 16
```

---

## 许可证

MIT License

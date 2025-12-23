# 评论树格式解析指南

本文档介绍如何编写脚本来解析以 `format_content_tree.py` 生成的评论树格式。

## 格式规范

### 1. 整体结构

格式化的字符串包含以下部分（按顺序）：

```
[POST_TITLE]
标题内容
[/POST_TITLE]

[POST_AUTHOR]
AUTHOR_NAME: author_name
AUTHOR_HANDLE: author_handle
[/POST_AUTHOR]

[POST_CONTENT]
帖子正文内容
[/POST_CONTENT]

[COMMENTS]
[COMMENT]
...
[/COMMENT]
[/COMMENTS]
```

### 2. 标记说明

- **开始标记**：`[TAG_NAME]` - 标记一个部分的开始
- **结束标记**：`[/TAG_NAME]` - 标记一个部分的结束
- **字段格式**：`KEY: VALUE` - 键值对格式，冒号后有一个空格
- **缩进规则**：每层缩进4个空格，用于表示评论的层级关系

### 3. 主要标记类型

| 标记 | 说明 | 是否必需 |
|------|------|----------|
| `[POST_TITLE]` | 帖子标题 | 可选 |
| `[POST_AUTHOR]` | 发帖者信息 | 可选 |
| `[POST_CONTENT]` | 帖子内容 | 可选 |
| `[COMMENTS]` | 评论树开始 | 可选 |
| `[COMMENT]` | 单个评论 | 在 `[COMMENTS]` 内 |

### 4. 评论字段

每个 `[COMMENT]` 块包含以下字段：

| 字段名 | 说明 | 类型 | 示例 |
|--------|------|------|------|
| `COMMENT_ID` | 评论的唯一ID | 字符串 | `COMMENT_ID: mblzfwa` |
| `AUTHOR_ID` | 评论者ID | 字符串 | `AUTHOR_ID: username` |
| `IS_SUBMITTER` | 是否为发帖者 | 布尔值 | `IS_SUBMITTER: true` 或 `IS_SUBMITTER: false` |
| `SCORE` | 点赞数 | 整数 | `SCORE: 10` |
| `CREATED_UTC` | 创建时间（ISO格式） | 字符串 | `CREATED_UTC: 2025-01-09T12:00:00` |
| `BODY` | 评论内容 | 字符串（可能多行） | `BODY: 评论内容...` |

**注意**：
- `BODY` 字段可能包含多行内容，后续行使用6个空格的缩进（`      `）
- 如果评论被删除，`BODY` 值为 `[deleted]`
- 字段顺序可能变化，应通过字段名解析，不要依赖顺序

### 5. 层级关系

- 第一层评论：无缩进，直接跟在 `[COMMENTS]` 后
- 第二层评论：4个空格缩进
- 第三层评论：8个空格缩进
- 以此类推，每层增加4个空格

**层级判断规则**：
- 计算 `[COMMENT]` 标记前的空格数
- 层级 = 空格数 / 4

## 解析算法

### Python 示例实现

```python
from typing import Dict, List, Any, Optional
import re


class ContentTreeParser:
    """评论树解析器"""
    
    def __init__(self):
        self.indent_size = 4
    
    def parse(self, content: str) -> Dict[str, Any]:
        """
        解析格式化的内容树
        
        Args:
            content: 格式化的字符串
            
        Returns:
            解析后的字典，包含 title, author, content, comments
        """
        lines = content.split('\n')
        result = {
            'title': None,
            'author': {},
            'content': None,
            'comments': []
        }
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # 解析帖子标题
            if line == '[POST_TITLE]':
                i += 1
                title_lines = []
                while i < len(lines) and lines[i].strip() != '[/POST_TITLE]':
                    title_lines.append(lines[i])
                    i += 1
                result['title'] = '\n'.join(title_lines).strip()
                i += 1
                continue
            
            # 解析发帖者信息
            if line == '[POST_AUTHOR]':
                i += 1
                while i < len(lines) and lines[i].strip() != '[/POST_AUTHOR]':
                    author_line = lines[i].strip()
                    if author_line.startswith('AUTHOR_NAME:'):
                        result['author']['name'] = author_line.split(':', 1)[1].strip()
                    elif author_line.startswith('AUTHOR_HANDLE:'):
                        result['author']['handle'] = author_line.split(':', 1)[1].strip()
                    i += 1
                i += 1
                continue
            
            # 解析帖子内容
            if line == '[POST_CONTENT]':
                i += 1
                content_lines = []
                while i < len(lines) and lines[i].strip() != '[/POST_CONTENT]':
                    content_lines.append(lines[i])
                    i += 1
                result['content'] = '\n'.join(content_lines).strip()
                i += 1
                continue
            
            # 解析评论树
            if line == '[COMMENTS]':
                i += 1
                comments, i = self._parse_comments(lines, i, depth=0)
                result['comments'] = comments
                i += 1  # 跳过 [/COMMENTS]
                continue
            
            i += 1
        
        return result
    
    def _parse_comments(self, lines: List[str], start_idx: int, depth: int) -> tuple[List[Dict], int]:
        """
        递归解析评论树
        
        Args:
            lines: 所有行的列表
            start_idx: 开始索引
            depth: 当前深度
            
        Returns:
            (评论列表, 下一个索引)
        """
        comments = []
        i = start_idx
        expected_indent = depth * self.indent_size
        
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            
            # 检查是否到达评论树结束标记
            if stripped == '[/COMMENTS]':
                break
            
            # 检查是否是当前层级的评论
            current_indent = len(line) - len(line.lstrip())
            
            # 如果缩进小于预期，说明已经回到上一层或更高层
            if current_indent < expected_indent and stripped:
                break
            
            # 如果缩进大于预期，说明是子评论，跳过（由递归处理）
            if current_indent > expected_indent:
                i += 1
                continue
            
            # 解析单个评论
            if stripped == '[COMMENT]':
                comment, i = self._parse_single_comment(lines, i, depth)
                comments.append(comment)
                continue
            
            i += 1
        
        return comments, i
    
    def _parse_single_comment(self, lines: List[str], start_idx: int, depth: int) -> tuple[Dict, int]:
        """
        解析单个评论
        
        Args:
            lines: 所有行的列表
            start_idx: [COMMENT] 标记的索引
            depth: 当前深度
            
        Returns:
            (评论字典, 下一个索引)
        """
        comment = {
            'comment_id': None,
            'author_id': None,
            'is_submitter': False,
            'score': 0,
            'created_utc': None,
            'body': None,
            'replies': []
        }
        
        i = start_idx + 1  # 跳过 [COMMENT]
        indent = depth * self.indent_size
        body_lines = []
        in_body = False
        
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            
            # 检查是否到达评论结束标记
            if stripped == '[/COMMENT]':
                # 如果有未处理的body内容，处理它
                if body_lines:
                    comment['body'] = '\n'.join(body_lines).strip()
                i += 1
                break
            
            # 解析字段
            if stripped.startswith('COMMENT_ID:'):
                comment['comment_id'] = stripped.split(':', 1)[1].strip()
                in_body = False
            elif stripped.startswith('AUTHOR_ID:'):
                comment['author_id'] = stripped.split(':', 1)[1].strip()
                in_body = False
            elif stripped.startswith('IS_SUBMITTER:'):
                value = stripped.split(':', 1)[1].strip().lower()
                comment['is_submitter'] = (value == 'true')
                in_body = False
            elif stripped.startswith('SCORE:'):
                try:
                    comment['score'] = int(stripped.split(':', 1)[1].strip())
                except ValueError:
                    comment['score'] = 0
                in_body = False
            elif stripped.startswith('CREATED_UTC:'):
                comment['created_utc'] = stripped.split(':', 1)[1].strip()
                in_body = False
            elif stripped.startswith('BODY:'):
                # BODY字段开始
                body_content = stripped.split(':', 1)[1].strip()
                body_lines = [body_content] if body_content else []
                in_body = True
            elif in_body:
                # BODY的多行内容（缩进6个空格）
                if line.startswith(' ' * (indent + 6)):
                    body_lines.append(line[(indent + 6):])
                else:
                    # 如果缩进不对，说明body结束了
                    in_body = False
                    # 回退一行，重新解析
                    i -= 1
                    continue
            
            i += 1
        
        # 解析子评论（replies）
        if i < len(lines):
            # 检查下一行是否是子评论
            next_line = lines[i] if i < len(lines) else ''
            next_indent = len(next_line) - len(next_line.lstrip())
            next_depth = depth + 1
            expected_next_indent = next_depth * self.indent_size
            
            if next_indent == expected_next_indent and next_line.strip() == '[COMMENT]':
                replies, i = self._parse_comments(lines, i, depth + 1)
                comment['replies'] = replies
        
        return comment, i


# 使用示例
def example_usage():
    """使用示例"""
    parser = ContentTreeParser()
    
    # 从文件或字符串读取格式化的内容
    with open('formatted_content.txt', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 解析
    result = parser.parse(content)
    
    # 访问解析结果
    print(f"标题: {result['title']}")
    print(f"发帖者: {result['author'].get('name')}")
    print(f"内容: {result['content'][:100]}...")
    print(f"评论数: {len(result['comments'])}")
    
    # 遍历评论树
    def print_comment(comment, depth=0):
        indent = "  " * depth
        print(f"{indent}- {comment['author_id']}: {comment['body'][:50]}...")
        for reply in comment['replies']:
            print_comment(reply, depth + 1)
    
    for comment in result['comments']:
        print_comment(comment)


if __name__ == '__main__':
    example_usage()
```

## 解析步骤总结

### 步骤1：分割和标记识别

1. 按行分割字符串
2. 识别开始标记 `[TAG_NAME]` 和结束标记 `[/TAG_NAME]`
3. 提取标记之间的内容

### 步骤2：解析帖子信息

1. 查找 `[POST_TITLE]`...`[/POST_TITLE]` 块，提取标题
2. 查找 `[POST_AUTHOR]`...`[/POST_AUTHOR]` 块，解析字段
3. 查找 `[POST_CONTENT]`...`[/POST_CONTENT]` 块，提取内容

### 步骤3：解析评论树

1. 查找 `[COMMENTS]` 标记
2. 递归解析 `[COMMENT]` 块：
   - 计算缩进确定层级
   - 解析字段（COMMENT_ID, AUTHOR_ID, IS_SUBMITTER, SCORE, CREATED_UTC, BODY）
   - 处理多行 BODY 内容
   - 递归解析子评论（replies）

### 步骤4：处理特殊情况

1. **多行BODY**：BODY字段的第一行在 `BODY:` 后，后续行缩进6个空格
2. **空值处理**：某些字段可能不存在，应使用默认值
3. **布尔值**：`IS_SUBMITTER` 值为 `true` 或 `false`（小写）
4. **删除的评论**：`BODY` 值为 `[deleted]`

## 常见问题

### Q1: 如何判断评论的层级？

**A**: 通过计算 `[COMMENT]` 标记前的空格数：
```python
indent = len(line) - len(line.lstrip())
depth = indent // 4
```

### Q2: 如何处理多行BODY内容？

**A**: BODY的第一行在 `BODY:` 后，后续行缩进6个空格（相对于当前层级）：
```python
if line.startswith('BODY:'):
    body_content = line.split(':', 1)[1].strip()
    body_lines = [body_content]
elif in_body and line.startswith(' ' * (indent + 6)):
    body_lines.append(line[(indent + 6):])
```

### Q3: 如何区分字段和内容？

**A**: 
- 字段格式：`KEY: VALUE`（冒号后有一个空格）
- 内容部分：在 `[TAG]` 和 `[/TAG]` 之间，不包含冒号格式的行

### Q4: 字段顺序是否固定？

**A**: 不固定。应通过字段名（KEY）解析，不要依赖顺序。

## 测试建议

1. **单元测试**：测试每个标记块的解析
2. **边界测试**：测试空内容、删除的评论、深层嵌套
3. **多行测试**：测试包含换行的BODY内容
4. **性能测试**：测试大量评论的解析性能

## 参考实现

完整的解析器实现请参考 `parse_content_tree.py` 文件，可以直接导入使用：

```python
from parse_content_tree import ContentTreeParser, parse_content_tree

# 方法1：使用便捷函数
result = parse_content_tree(formatted_content)

# 方法2：使用类
parser = ContentTreeParser()
result = parser.parse(formatted_content)

# 方法3：直接转换为JSON
json_str = parser.to_json(formatted_content)
```

## 实际使用场景

### 场景1：从数据库读取并解析

```python
import psycopg2
from parse_content_tree import parse_content_tree

# 从数据库读取content_text
conn = psycopg2.connect("...")
cursor = conn.cursor()
cursor.execute("SELECT content_text FROM crawled_posts WHERE id = %s", (post_id,))
row = cursor.fetchone()

if row:
    formatted_content = row[0]
    parsed = parse_content_tree(formatted_content)
    
    # 访问解析结果
    print(f"标题: {parsed['title']}")
    print(f"发帖者: {parsed['author']['name']}")
    
    # 遍历评论
    for comment in parsed['comments']:
        print(f"  - {comment['author_id']}: {comment['body'][:50]}")
```

### 场景2：统计评论信息

```python
from parse_content_tree import parse_content_tree

def count_comments(comments):
    """递归统计评论数"""
    count = len(comments)
    for comment in comments:
        count += count_comments(comment.get('replies', []))
    return count

def find_submitter_comments(comments):
    """查找所有发帖者的评论"""
    result = []
    for comment in comments:
        if comment.get('is_submitter'):
            result.append(comment)
        result.extend(find_submitter_comments(comment.get('replies', [])))
    return result

# 使用
parsed = parse_content_tree(content)
total_comments = count_comments(parsed['comments'])
submitter_comments = find_submitter_comments(parsed['comments'])
```

### 场景3：导出为其他格式

```python
from parse_content_tree import parse_content_tree
import json

def export_to_markdown(parsed):
    """导出为Markdown格式"""
    md = []
    md.append(f"# {parsed['title']}\n")
    md.append(f"**发帖者**: {parsed['author'].get('name', 'Unknown')}\n")
    md.append(f"\n{parsed['content']}\n")
    md.append("\n## 评论\n")
    
    def format_comment_md(comment, depth=0):
        indent = "  " * depth
        md.append(f"{indent}- **{comment['author_id']}** (点赞: {comment['score']})\n")
        md.append(f"{indent}  {comment['body']}\n\n")
        for reply in comment.get('replies', []):
            format_comment_md(reply, depth + 1)
    
    for comment in parsed['comments']:
        format_comment_md(comment)
    
    return "\n".join(md)

# 使用
parsed = parse_content_tree(content)
markdown = export_to_markdown(parsed)
```

## 性能优化建议

1. **流式解析**：对于非常大的内容，可以实现流式解析，逐行处理
2. **缓存结果**：如果多次访问同一内容，可以缓存解析结果
3. **部分解析**：如果只需要特定部分（如只读标题），可以实现部分解析

## 错误处理

解析器会尽可能容错，但建议在解析前进行基本验证：

```python
def validate_format(content: str) -> bool:
    """验证格式是否有效"""
    required_tags = ['[POST_TITLE]', '[POST_CONTENT]', '[COMMENTS]']
    for tag in required_tags:
        if tag not in content:
            return False
    return True

# 使用
if validate_format(content):
    parsed = parse_content_tree(content)
else:
    print("格式无效")
```


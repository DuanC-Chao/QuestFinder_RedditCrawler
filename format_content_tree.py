#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评论树格式化脚本
功能：将Reddit帖子的评论树结构转换为格式化的字符串
保留完整的树状信息：标题、内容、作者ID、评论者ID、评论内容

格式化后的字符串格式：
- 使用明确的标记和分隔符，既人可读又便于程序解析
- 每个部分都有清晰的边界标记
- 评论层级通过缩进和明确的标记表示

格式规范：
1. 帖子标题：以 [POST_TITLE] 开始，以 [/POST_TITLE] 结束
2. 发帖者信息：以 [POST_AUTHOR] 开始，以 [/POST_AUTHOR] 结束
3. 帖子内容：以 [POST_CONTENT] 开始，以 [/POST_CONTENT] 结束
4. 评论树：以 [COMMENTS] 开始，以 [/COMMENTS] 结束
5. 每个评论：以 [COMMENT] 开始，以 [/COMMENT] 结束
6. 评论字段：使用 KEY: VALUE 格式，每行一个字段
7. 评论层级：通过缩进（4个空格）表示，第一层无缩进，第二层4空格，第三层8空格，以此类推
8. 子评论：紧跟在父评论的 [/COMMENT] 之后，使用相同的 [COMMENT]...[/COMMENT] 格式

示例输出格式：
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
COMMENT_ID: comment_id_1
AUTHOR_ID: commenter1
IS_SUBMITTER: false
SCORE: 10
CREATED_UTC: 2025-01-09T12:00:00
BODY: 评论内容...
[/COMMENT]
    [COMMENT]
    COMMENT_ID: comment_id_2
    AUTHOR_ID: commenter2
    IS_SUBMITTER: false
    SCORE: 5
    CREATED_UTC: 2025-01-09T13:00:00
    BODY: 回复内容...
    [/COMMENT]
[COMMENT]
COMMENT_ID: comment_id_3
AUTHOR_ID: commenter3
IS_SUBMITTER: false
SCORE: 3
CREATED_UTC: 2025-01-09T14:00:00
BODY: 评论内容...
[/COMMENT]
[/COMMENTS]
"""

from typing import Dict, Any, List, Optional


class ContentTreeFormatter:
    """内容树格式化器"""
    
    def __init__(self):
        """初始化格式化器"""
        self.indent_size = 4  # 每层缩进4个空格
    
    def format_post_tree(self, post: Dict[str, Any]) -> str:
        """
        格式化整个帖子树（标题、内容、所有评论）
        
        Args:
            post: 帖子数据字典，包含title, content_text, author_name, author_handle, comments_tree等字段
            
        Returns:
            格式化后的字符串
        """
        parts = []
        
        # 1. 帖子标题
        title = post.get('title', '')
        if title:
            parts.append("[POST_TITLE]")
            parts.append(title)
            parts.append("[/POST_TITLE]")
            parts.append("")
        
        # 2. 发帖者信息
        author_name = post.get('author_name', post.get('author', ''))
        author_handle = post.get('author_handle', author_name)
        if author_name:
            parts.append("[POST_AUTHOR]")
            parts.append(f"AUTHOR_NAME: {author_name}")
            if author_handle and author_handle != author_name:
                parts.append(f"AUTHOR_HANDLE: {author_handle}")
            parts.append("[/POST_AUTHOR]")
            parts.append("")
        
        # 3. 帖子内容
        content_text = post.get('content_text', post.get('selftext', ''))
        if content_text:
            parts.append("[POST_CONTENT]")
            parts.append(content_text)
            parts.append("[/POST_CONTENT]")
            parts.append("")
        
        # 4. 评论树
        comments_tree = post.get('comments_tree', [])
        if comments_tree:
            parts.append("[COMMENTS]")
            for comment in comments_tree:
                comment_str = self._format_comment(comment, depth=0)
                parts.append(comment_str)
            parts.append("[/COMMENTS]")
        
        return "\n".join(parts)
    
    def _format_comment(self, comment: Dict[str, Any], depth: int = 0) -> str:
        """
        递归格式化单个评论及其子评论
        
        Args:
            comment: 评论字典
            depth: 当前深度（用于缩进）
            
        Returns:
            格式化后的评论字符串
        """
        parts = []
        indent = " " * (depth * self.indent_size)
        
        # 评论ID和作者信息
        comment_id = comment.get('id', '')
        author = comment.get('author', '[deleted]')
        is_submitter = comment.get('is_submitter', False)
        score = comment.get('score', 0)
        created_utc = comment.get('created_utc', '')
        body = comment.get('body', '')
        
        # 评论开始标记
        parts.append(f"{indent}[COMMENT]")
        
        # 评论字段（每行一个字段）
        parts.append(f"{indent}COMMENT_ID: {comment_id}")
        parts.append(f"{indent}AUTHOR_ID: {author}")
        parts.append(f"{indent}IS_SUBMITTER: {str(is_submitter).lower()}")
        if score is not None:
            parts.append(f"{indent}SCORE: {score}")
        if created_utc:
            parts.append(f"{indent}CREATED_UTC: {created_utc}")
        
        # 评论内容（多行内容需要特殊处理）
        if body and body not in ['[deleted]', '[removed]']:
            # 如果内容包含换行，需要每行都缩进
            body_lines = body.split('\n')
            if len(body_lines) == 1:
                # 单行内容
                parts.append(f"{indent}BODY: {body}")
            else:
                # 多行内容：第一行使用BODY:，后续行使用缩进
                parts.append(f"{indent}BODY: {body_lines[0]}")
                for line in body_lines[1:]:
                    parts.append(f"{indent}      {line}")
        else:
            parts.append(f"{indent}BODY: [deleted]")
        
        # 评论结束标记
        parts.append(f"{indent}[/COMMENT]")
        
        # 处理子评论（replies）
        replies = comment.get('replies', [])
        if replies and len(replies) > 0:
            for reply in replies:
                reply_str = self._format_comment(reply, depth + 1)
                parts.append(reply_str)
        
        return "\n".join(parts)


def format_post_content_tree(post: Dict[str, Any]) -> str:
    """
    格式化帖子内容树的便捷函数
    
    Args:
        post: 帖子数据字典
        
    Returns:
        格式化后的字符串
    """
    formatter = ContentTreeFormatter()
    return formatter.format_post_tree(post)


def main():
    """主函数 - 用于测试"""
    import json
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='格式化Reddit帖子评论树')
    parser.add_argument('--input', '-i', required=True,
                       help='输入的JSON文件路径（包含单个post对象）')
    parser.add_argument('--output', '-o', default=None,
                       help='输出文件路径（默认输出到stdout）')
    parser.add_argument('--post-index', type=int, default=0,
                       help='如果输入文件包含多个post，指定要格式化的post索引（默认0）')
    
    args = parser.parse_args()
    
    # 读取输入文件
    try:
        with open(args.input, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 如果data是列表，取指定索引的post
        if isinstance(data, list):
            if args.post_index >= len(data):
                print(f"错误: post索引 {args.post_index} 超出范围（共 {len(data)} 个post）")
                sys.exit(1)
            post = data[args.post_index]
        else:
            post = data
        
        # 格式化
        formatter = ContentTreeFormatter()
        formatted_text = formatter.format_post_tree(post)
        
        # 输出
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(formatted_text)
            print(f"已保存到: {args.output}")
        else:
            print(formatted_text)
            
    except FileNotFoundError:
        print(f"错误: 文件不存在: {args.input}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"错误: JSON解析失败: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评论树格式解析器
功能：解析 format_content_tree.py 生成的格式化字符串，还原为结构化数据
"""

from typing import Dict, List, Any, Optional, Tuple


class ContentTreeParser:
    """评论树解析器"""
    
    def __init__(self):
        """初始化解析器"""
        self.indent_size = 4  # 每层缩进4个空格
    
    def parse(self, content: str) -> Dict[str, Any]:
        """
        解析格式化的内容树
        
        Args:
            content: 格式化的字符串
            
        Returns:
            解析后的字典，包含 title, author, content, comments
            结构：
            {
                'title': str,
                'author': {
                    'name': str,
                    'handle': str
                },
                'content': str,
                'comments': [
                    {
                        'comment_id': str,
                        'author_id': str,
                        'is_submitter': bool,
                        'score': int,
                        'created_utc': str,
                        'body': str,
                        'replies': [...]  # 子评论列表，结构相同
                    },
                    ...
                ]
            }
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
    
    def _parse_comments(self, lines: List[str], start_idx: int, depth: int) -> Tuple[List[Dict], int]:
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
                if comment:
                    comments.append(comment)
                continue
            
            i += 1
        
        return comments, i
    
    def _parse_single_comment(self, lines: List[str], start_idx: int, depth: int) -> Tuple[Dict, int]:
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
                line_indent = len(line) - len(line.lstrip())
                if line_indent >= indent + 6:
                    # 移除缩进
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
    
    def to_dict(self, content: str) -> Dict[str, Any]:
        """
        解析并返回字典格式（parse的别名）
        
        Args:
            content: 格式化的字符串
            
        Returns:
            解析后的字典
        """
        return self.parse(content)
    
    def to_json(self, content: str) -> str:
        """
        解析并返回JSON格式
        
        Args:
            content: 格式化的字符串
            
        Returns:
            JSON字符串
        """
        import json
        result = self.parse(content)
        return json.dumps(result, ensure_ascii=False, indent=2)


def parse_content_tree(content: str) -> Dict[str, Any]:
    """
    解析内容树的便捷函数
    
    Args:
        content: 格式化的字符串
        
    Returns:
        解析后的字典
    """
    parser = ContentTreeParser()
    return parser.parse(content)


def main():
    """主函数 - 用于测试"""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='解析Reddit帖子评论树格式')
    parser.add_argument('--input', '-i', required=True,
                       help='输入的格式化文本文件路径')
    parser.add_argument('--output', '-o', default=None,
                       help='输出JSON文件路径（默认输出到stdout）')
    parser.add_argument('--format', choices=['dict', 'json'], default='json',
                       help='输出格式（默认json）')
    
    args = parser.parse_args()
    
    # 读取输入文件
    try:
        with open(args.input, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 解析
        parser = ContentTreeParser()
        
        if args.format == 'json':
            result = parser.to_json(content)
        else:
            import json
            result = json.dumps(parser.parse(content), ensure_ascii=False, indent=2)
        
        # 输出
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(result)
            print(f"已保存到: {args.output}")
        else:
            print(result)
            
    except FileNotFoundError:
        print(f"错误: 文件不存在: {args.input}")
        sys.exit(1)
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()


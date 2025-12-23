#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查看ready_for_DB数据脚本
功能：读取并美化打印ready_for_DB文件中的格式化内容树
"""

import os
import json
import argparse
from typing import List, Dict, Any, Optional
from parse_content_tree import ContentTreeParser


class ReadyDataViewer:
    """Ready数据查看器"""
    
    def __init__(self):
        """初始化查看器"""
        self.data_dir = "Data"
        self.ready_dir = os.path.join(self.data_dir, "ready_for_DB")
        self.parser = ContentTreeParser()
    
    def load_ready_data(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载ready_for_DB数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            数据列表
        """
        filename = f"{task_id}_ready.json"
        filepath = os.path.join(self.ready_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Ready文件不存在: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def print_post(self, post: Dict[str, Any], index: int, total: int, show_metadata: bool = True):
        """
        美化打印单个Post
        
        Args:
            post: Post数据字典
            index: 当前索引（从1开始）
            total: 总数
            show_metadata: 是否显示元数据（scene, post_type, score等）
        """
        print("\n" + "=" * 100)
        print(f"Post {index}/{total}")
        print("=" * 100)
        
        # 显示元数据
        if show_metadata:
            print("\n【元数据】")
            print(f"  平台: {post.get('platform', 'N/A')}")
            print(f"  来源URL: {post.get('source_url', 'N/A')}")
            print(f"  平台ID: {post.get('source_platform_id', 'N/A')}")
            print(f"  场景: {post.get('scene', 'N/A')}")
            print(f"  类型: {post.get('post_type', 'N/A')}")
            print(f"  质量分数: {post.get('base_quality_score', 'N/A')}")
            print(f"  点赞数: {post.get('likes', 0)}")
            print(f"  评论数: {post.get('comments_count', 0)}")
            print(f"  语言: {post.get('lang', 'N/A')}")
            print(f"  抓取时间: {post.get('fetched_at', 'N/A')}")
            print()
        
        # 解析并打印内容树
        content_text = post.get('content_text', '')
        if content_text:
            print("【内容树】")
            print("-" * 100)
            
            # 解析内容树
            try:
                parsed = self.parser.parse(content_text)
                self._print_parsed_tree(parsed)
            except Exception as e:
                print(f"⚠️  解析失败: {e}")
                print("\n原始内容（前500字符）:")
                print(content_text[:500])
                if len(content_text) > 500:
                    print(f"... (共 {len(content_text)} 字符)")
        else:
            print("【内容树】")
            print("  (无内容)")
        
        print("\n" + "=" * 100)
    
    def _print_parsed_tree(self, parsed: Dict[str, Any]):
        """
        打印解析后的内容树
        
        Args:
            parsed: 解析后的字典
        """
        # 帖子标题
        title = parsed.get('title')
        if title:
            print(f"\n📌 标题: {title}")
        
        # 发帖者信息
        author = parsed.get('author', {})
        author_name = author.get('name')
        if author_name:
            author_info = f"👤 发帖者: {author_name}"
            author_handle = author.get('handle')
            if author_handle and author_handle != author_name:
                author_info += f" (@{author_handle})"
            print(author_info)
        
        # 帖子内容
        content = parsed.get('content')
        if content and content.strip():
            print(f"\n📝 内容:")
            print("-" * 80)
            print(content)
            print("-" * 80)
        elif not title:
            # 如果没有标题也没有内容，说明可能是空帖子
            print("\n📝 内容: (无内容)")
        
        # 评论树
        comments = parsed.get('comments', [])
        if comments:
            # 计算总评论数（包括子评论）
            total_comments = self._count_comments(comments)
            print(f"\n💬 评论 ({len(comments)} 条顶级评论，共 {total_comments} 条):")
            print("-" * 80)
            self._print_comments(comments, depth=0)
        else:
            print("\n💬 评论: (无评论)")
    
    def _count_comments(self, comments: List[Dict[str, Any]]) -> int:
        """递归计算评论总数（包括子评论）"""
        count = len(comments)
        for comment in comments:
            replies = comment.get('replies', [])
            if replies:
                count += self._count_comments(replies)
        return count
    
    def _print_comments(self, comments: List[Dict[str, Any]], depth: int = 0):
        """
        递归打印评论树
        
        Args:
            comments: 评论列表
            depth: 当前深度
        """
        indent = "  " * depth
        
        for i, comment in enumerate(comments):
            # 评论头部
            author_id = comment.get('author_id', '[deleted]')
            is_submitter = comment.get('is_submitter', False)
            score = comment.get('score', 0)
            created_utc = comment.get('created_utc', '')
            body = comment.get('body', '[deleted]')
            comment_id = comment.get('comment_id', '')
            
            # 标记是否为发帖者
            submitter_mark = " [发帖者]" if is_submitter else ""
            
            # 打印评论
            print(f"\n{indent}┌─ 评论 #{i+1}")
            if comment_id:
                print(f"{indent}│  ID: {comment_id}")
            print(f"{indent}│  作者: {author_id}{submitter_mark}")
            if score:
                print(f"{indent}│  点赞: {score}")
            if created_utc:
                print(f"{indent}│  时间: {created_utc}")
            print(f"{indent}│  内容:")
            
            # 打印评论内容（多行处理）
            if body and body not in ['[deleted]', '[removed]']:
                body_lines = body.split('\n')
                for line in body_lines:
                    if line.strip():  # 跳过空行
                        print(f"{indent}│    {line}")
                    else:
                        print(f"{indent}│")
            else:
                print(f"{indent}│    {body}")
            
            # 打印子评论
            replies = comment.get('replies', [])
            if replies:
                print(f"{indent}│")
                print(f"{indent}│  └─ 回复 ({len(replies)} 条):")
                self._print_comments(replies, depth + 1)
            
            print(f"{indent}└─")
    
    def view_task(self, task_id: str, post_index: Optional[int] = None, show_metadata: bool = True):
        """
        查看任务的所有Post
        
        Args:
            task_id: 任务ID
            post_index: 如果指定，只显示该索引的Post（从1开始）
            show_metadata: 是否显示元数据
        """
        print(f"\n查看任务: {task_id}")
        print("=" * 100)
        
        # 加载数据
        try:
            ready_data = self.load_ready_data(task_id)
            print(f"✓ 加载了 {len(ready_data)} 条记录")
        except FileNotFoundError as e:
            print(f"✗ 错误: {e}")
            return
        
        if not ready_data:
            print("数据为空")
            return
        
        # 如果指定了post_index，只显示该Post
        if post_index is not None:
            if post_index < 1 or post_index > len(ready_data):
                print(f"错误: Post索引 {post_index} 超出范围（共 {len(ready_data)} 条）")
                return
            self.print_post(ready_data[post_index - 1], post_index, len(ready_data), show_metadata)
        else:
            # 显示所有Post
            for i, post in enumerate(ready_data, 1):
                self.print_post(post, i, len(ready_data), show_metadata)
                
                # 如果不是最后一个，询问是否继续
                if i < len(ready_data):
                    try:
                        user_input = input(f"\n按Enter继续查看下一个Post ({i+1}/{len(ready_data)})，输入q退出: ")
                        if user_input.lower() == 'q':
                            print("\n已退出")
                            break
                    except KeyboardInterrupt:
                        print("\n\n已中断")
                        break


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='查看ready_for_DB数据（美化打印格式化的内容树）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 查看所有Post
  python view_ready_data.py --task-id task001
  
  # 查看指定索引的Post（从1开始）
  python view_ready_data.py --task-id task001 --post-index 1
  
  # 不显示元数据，只显示内容
  python view_ready_data.py --task-id task001 --no-metadata
        """
    )
    
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    parser.add_argument('--post-index', '-p', type=int, default=None,
                       help='Post索引（从1开始，如果指定则只显示该Post）')
    parser.add_argument('--no-metadata', action='store_true',
                       help='不显示元数据（scene, post_type, score等），只显示内容树')
    
    args = parser.parse_args()
    
    try:
        viewer = ReadyDataViewer()
        viewer.view_task(args.task_id, args.post_index, show_metadata=not args.no_metadata)
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


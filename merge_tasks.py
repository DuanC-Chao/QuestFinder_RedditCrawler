#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
合并多个task的结果数据脚本
功能：合并Data/ready_for_DB_posts和Data/ready_for_DB_comments目录下的所有task数据
"""

import os
import json
import argparse
from typing import List, Dict, Any
from glob import glob


class TaskMerger:
    """Task数据合并器"""
    
    def __init__(self):
        """初始化合并器"""
        self.data_dir = "Data"
        self.posts_dir = os.path.join(self.data_dir, "ready_for_DB_posts")
        self.comments_dir = os.path.join(self.data_dir, "ready_for_DB_comments")
    
    def find_all_tasks(self) -> List[str]:
        """
        查找所有task ID
        
        Returns:
            task ID列表
        """
        tasks = set()
        
        # 从posts目录查找
        if os.path.exists(self.posts_dir):
            for filename in os.listdir(self.posts_dir):
                if filename.endswith('_posts.json'):
                    task_id = filename.replace('_posts.json', '')
                    tasks.add(task_id)
        
        # 从comments目录查找
        if os.path.exists(self.comments_dir):
            for filename in os.listdir(self.comments_dir):
                if filename.endswith('_comments.json'):
                    task_id = filename.replace('_comments.json', '')
                    tasks.add(task_id)
        
        return sorted(list(tasks))
    
    def load_posts(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载指定task的posts数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            posts数据列表
        """
        filename = f"{task_id}_posts.json"
        filepath = os.path.join(self.posts_dir, filename)
        
        if not os.path.exists(filepath):
            print(f"  ⚠️  Posts文件不存在: {filepath}")
            return []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"  ✓ 加载了 {len(data)} 条posts")
                return data
        except Exception as e:
            print(f"  ✗ 加载失败: {e}")
            return []
    
    def load_comments(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载指定task的comments数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            comments数据列表
        """
        filename = f"{task_id}_comments.json"
        filepath = os.path.join(self.comments_dir, filename)
        
        if not os.path.exists(filepath):
            print(f"  ⚠️  Comments文件不存在: {filepath}")
            return []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"  ✓ 加载了 {len(data)} 条comments")
                return data
        except Exception as e:
            print(f"  ✗ 加载失败: {e}")
            return []
    
    def merge_tasks(self, task_ids: List[str], output_task_id: str, 
                   skip_duplicates: bool = True) -> tuple:
        """
        合并多个task的数据
        
        Args:
            task_ids: 要合并的task ID列表
            output_task_id: 输出task ID
            skip_duplicates: 是否跳过重复记录（基于source_platform_id和source_comment_id）
            
        Returns:
            (merged_posts, merged_comments) 元组
        """
        print(f"\n开始合并 {len(task_ids)} 个task的数据...")
        print(f"输出task ID: {output_task_id}")
        print("=" * 80)
        
        all_posts = []
        all_comments = []
        
        # 用于去重的集合
        seen_post_ids = set()
        seen_comment_ids = set()
        
        # 逐个加载并合并
        for i, task_id in enumerate(task_ids, 1):
            print(f"\n[{i}/{len(task_ids)}] 处理task: {task_id}")
            
            # 加载posts
            posts = self.load_posts(task_id)
            for post in posts:
                post_id = post.get('source_platform_id', '')
                if skip_duplicates:
                    if post_id and post_id in seen_post_ids:
                        continue
                    if post_id:
                        seen_post_ids.add(post_id)
                all_posts.append(post)
            
            # 加载comments
            comments = self.load_comments(task_id)
            for comment in comments:
                comment_id = comment.get('source_comment_id', '')
                if skip_duplicates:
                    if comment_id and comment_id in seen_comment_ids:
                        continue
                    if comment_id:
                        seen_comment_ids.add(comment_id)
                all_comments.append(comment)
        
        print("\n" + "=" * 80)
        print(f"合并完成:")
        print(f"  - Posts: {len(all_posts)} 条")
        print(f"  - Comments: {len(all_comments)} 条")
        
        return all_posts, all_comments
    
    def save_merged_data(self, posts: List[Dict[str, Any]], 
                        comments: List[Dict[str, Any]], 
                        output_task_id: str):
        """
        保存合并后的数据
        
        Args:
            posts: posts数据列表
            comments: comments数据列表
            output_task_id: 输出task ID
        """
        # 确保目录存在
        if not os.path.exists(self.posts_dir):
            os.makedirs(self.posts_dir)
        if not os.path.exists(self.comments_dir):
            os.makedirs(self.comments_dir)
        
        # 保存posts
        posts_filename = f"{output_task_id}_posts.json"
        posts_filepath = os.path.join(self.posts_dir, posts_filename)
        with open(posts_filepath, 'w', encoding='utf-8') as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
        print(f"\n✓ Posts数据已保存到: {posts_filepath}")
        print(f"  共 {len(posts)} 条记录")
        
        # 保存comments
        comments_filename = f"{output_task_id}_comments.json"
        comments_filepath = os.path.join(self.comments_dir, comments_filename)
        with open(comments_filepath, 'w', encoding='utf-8') as f:
            json.dump(comments, f, ensure_ascii=False, indent=2)
        print(f"✓ Comments数据已保存到: {comments_filepath}")
        print(f"  共 {len(comments)} 条记录")
    
    def merge_all_tasks(self, output_task_id: str, skip_duplicates: bool = True):
        """
        合并所有找到的task数据
        
        Args:
            output_task_id: 输出task ID
            skip_duplicates: 是否跳过重复记录
        """
        # 查找所有task
        task_ids = self.find_all_tasks()
        
        if not task_ids:
            print("未找到任何task数据文件")
            return
        
        print(f"找到 {len(task_ids)} 个task:")
        for task_id in task_ids:
            print(f"  - {task_id}")
        
        # 合并数据
        posts, comments = self.merge_tasks(task_ids, output_task_id, skip_duplicates)
        
        # 保存合并后的数据
        if posts or comments:
            self.save_merged_data(posts, comments, output_task_id)
        else:
            print("\n⚠️  没有数据可保存")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='合并多个task的结果数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 合并所有task到merged_task001
  python merge_tasks.py --output-task-id merged_task001
  
  # 合并指定的task列表
  python merge_tasks.py --output-task-id merged_task001 --tasks task001 task002 task003
  
  # 合并所有task，不去重
  python merge_tasks.py --output-task-id merged_task001 --no-skip-duplicates
        """
    )
    
    parser.add_argument('--output-task-id', '-o', required=True,
                       help='输出task ID（必需）')
    parser.add_argument('--tasks', '-t', nargs='+', default=None,
                       help='要合并的task ID列表（默认：合并所有找到的task）')
    parser.add_argument('--skip-duplicates', action='store_true', default=True,
                       help='跳过重复记录（默认True，基于source_platform_id和source_comment_id）')
    parser.add_argument('--no-skip-duplicates', dest='skip_duplicates', action='store_false',
                       help='不去重，保留所有记录')
    
    args = parser.parse_args()
    
    merger = TaskMerger()
    
    if args.tasks:
        # 合并指定的task列表
        print(f"合并指定的 {len(args.tasks)} 个task: {args.tasks}")
        posts, comments = merger.merge_tasks(args.tasks, args.output_task_id, args.skip_duplicates)
        if posts or comments:
            merger.save_merged_data(posts, comments, args.output_task_id)
    else:
        # 合并所有task
        merger.merge_all_tasks(args.output_task_id, args.skip_duplicates)


if __name__ == "__main__":
    main()


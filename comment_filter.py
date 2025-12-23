#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评论过滤脚本
功能：对爬虫输出的第一层评论进行过滤，如果二级评论超过5个，只保留upvote数量最多的5个
"""

import os
import json
import argparse
from typing import List, Dict, Any


class CommentFilter:
    """评论过滤器"""
    
    def __init__(self):
        """初始化过滤器"""
        self.data_dir = "Data"
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.filtered_dir = os.path.join(self.data_dir, "comment_filtered_raw")
        
        # 确保输出目录存在
        os.makedirs(self.filtered_dir, exist_ok=True)
    
    def load_raw_data(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载原始爬虫数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            数据列表
        """
        filename = f"{task_id}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"原始数据文件不存在: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def filter_second_level_comments(self, comment: Dict[str, Any], max_second_level: int = 5) -> Dict[str, Any]:
        """
        过滤二级评论：如果二级评论超过max_second_level个，只保留upvote数量最多的
        
        Args:
            comment: 评论字典（可能包含replies字段）
            max_second_level: 最多保留的二级评论数（默认5）
            
        Returns:
            过滤后的评论字典
        """
        # 创建评论的副本，避免修改原始数据
        filtered_comment = comment.copy()
        
        # 获取二级评论（replies）
        replies = comment.get('replies', [])
        
        if len(replies) > max_second_level:
            # 按score（upvote数）降序排序
            sorted_replies = sorted(replies, key=lambda x: x.get('score', 0), reverse=True)
            
            # 只保留前max_second_level个
            top_replies = sorted_replies[:max_second_level]
            
            # 递归处理每个保留的二级评论（它们可能也有子评论）
            filtered_replies = []
            for reply in top_replies:
                filtered_reply = self.filter_second_level_comments(reply, max_second_level)
                filtered_replies.append(filtered_reply)
            
            filtered_comment['replies'] = filtered_replies
        else:
            # 如果二级评论数不超过限制，递归处理所有二级评论（它们可能也有子评论）
            filtered_replies = []
            for reply in replies:
                filtered_reply = self.filter_second_level_comments(reply, max_second_level)
                filtered_replies.append(filtered_reply)
            filtered_comment['replies'] = filtered_replies
        
        return filtered_comment
    
    def filter_comment_item(self, item: Dict[str, Any], max_second_level: int = 5) -> Dict[str, Any]:
        """
        过滤单个评论项目（第一层评论）
        
        Args:
            item: 评论项目字典
            max_second_level: 最多保留的二级评论数
            
        Returns:
            过滤后的评论项目字典
        """
        # 创建项目的副本
        filtered_item = item.copy()
        
        # 获取comments_tree（应该只包含一个第一层评论）
        comments_tree = item.get('comments_tree', [])
        
        if not comments_tree:
            return filtered_item
        
        # 处理第一个（也是唯一的）第一层评论
        first_level_comment = comments_tree[0]
        filtered_first_level = self.filter_second_level_comments(first_level_comment, max_second_level)
        
        # 更新comments_tree
        filtered_item['comments_tree'] = [filtered_first_level]
        
        # 更新comments计数
        filtered_item['comments'] = self._count_comments_in_tree([filtered_first_level])
        
        return filtered_item
    
    def _count_comments_in_tree(self, comments_tree: List[Dict[str, Any]]) -> int:
        """
        递归计算评论树中的评论总数（包括所有层级的子评论）
        
        Args:
            comments_tree: 评论树列表
            
        Returns:
            评论总数
        """
        if not comments_tree:
            return 0
        count = len(comments_tree)
        for comment in comments_tree:
            replies = comment.get('replies', [])
            if replies:
                count += self._count_comments_in_tree(replies)
        return count
    
    def filter_task(self, task_id: str, max_second_level: int = 5) -> List[Dict[str, Any]]:
        """
        过滤任务的所有评论
        
        Args:
            task_id: 任务ID
            max_second_level: 最多保留的二级评论数
            
        Returns:
            过滤后的数据列表
        """
        print(f"\n开始过滤评论: {task_id}")
        print("=" * 80)
        
        # 加载原始数据
        try:
            raw_data = self.load_raw_data(task_id)
            print(f"✓ 加载了 {len(raw_data)} 个评论项目")
        except FileNotFoundError as e:
            print(f"✗ 错误: {e}")
            return []
        
        if not raw_data:
            print("数据为空")
            return []
        
        # 过滤每个评论项目
        filtered_data = []
        filtered_count = 0
        
        for i, item in enumerate(raw_data, 1):
            # 获取原始二级评论数
            comments_tree = item.get('comments_tree', [])
            if comments_tree:
                first_level_comment = comments_tree[0]
                original_replies_count = len(first_level_comment.get('replies', []))
                
                # 过滤
                filtered_item = self.filter_comment_item(item, max_second_level)
                filtered_data.append(filtered_item)
                
                # 统计过滤情况
                filtered_first_level = filtered_item['comments_tree'][0]
                filtered_replies_count = len(filtered_first_level.get('replies', []))
                
                if original_replies_count > max_second_level:
                    filtered_count += 1
                    print(f"  项目 {i}: 二级评论 {original_replies_count} → {filtered_replies_count} (保留upvote最多的{max_second_level}个)")
            else:
                filtered_data.append(item)
        
        print(f"\n✓ 过滤完成")
        print(f"  - 总项目数: {len(filtered_data)}")
        print(f"  - 被过滤的项目数: {filtered_count}")
        
        return filtered_data
    
    def save_filtered_data(self, task_id: str, filtered_data: List[Dict[str, Any]]):
        """
        保存过滤后的数据
        
        Args:
            task_id: 任务ID
            filtered_data: 过滤后的数据列表
        """
        filename = f"{task_id}.json"
        filepath = os.path.join(self.filtered_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ 过滤后的数据已保存到: {filepath}")
        print(f"  共保存 {len(filtered_data)} 个评论项目")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='过滤评论：如果二级评论超过指定数量，只保留upvote最多的',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基本用法（默认最多保留5个二级评论）
  python comment_filter.py --task-id task001
  
  # 指定最多保留的二级评论数
  python comment_filter.py --task-id task001 --max-second-level 10
        """
    )
    
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    parser.add_argument('--max-second-level', type=int, default=5,
                       help='最多保留的二级评论数（默认5）')
    
    args = parser.parse_args()
    
    try:
        filter = CommentFilter()
        filtered_data = filter.filter_task(args.task_id, max_second_level=args.max_second_level)
        
        if filtered_data:
            filter.save_filtered_data(args.task_id, filtered_data)
        else:
            print("\n没有数据需要保存")
            
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


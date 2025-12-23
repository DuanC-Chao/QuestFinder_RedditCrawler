#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
准备数据库入库数据脚本
功能：将comment_filtered_raw中的文件转换为可入库Supabase的JSON格式
- 一级评论转换为crawled_posts
- 二级评论转换为crawled_comments
"""

import os
import json
import argparse
import hashlib
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, parse_qs
from datetime import datetime


class DBDataPreparer:
    """数据库数据准备器"""
    
    def __init__(self):
        """初始化准备器"""
        self.data_dir = "Data"
        self.filtered_dir = os.path.join(self.data_dir, "comment_filtered_raw")
        self.posts_dir = os.path.join(self.data_dir, "ready_for_DB_posts")
        self.comments_dir = os.path.join(self.data_dir, "ready_for_DB_comments")
        
        # 确保输出目录存在
        os.makedirs(self.posts_dir, exist_ok=True)
        os.makedirs(self.comments_dir, exist_ok=True)
    
    def load_filtered_data(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载过滤后的数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            数据列表
        """
        filename = f"{task_id}.json"
        filepath = os.path.join(self.filtered_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"过滤后的数据文件不存在: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _calculate_content_hash(self, content: str) -> str:
        """
        计算内容的hash
        
        Args:
            content: 内容字符串
            
        Returns:
            hash字符串
        """
        if not content:
            return hashlib.md5(b'').hexdigest()
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _extract_query_seed(self, source_url: str) -> Optional[str]:
        """
        从source_url提取query_seed
        
        Args:
            source_url: 源URL
            
        Returns:
            query_seed或None
        """
        try:
            parsed = urlparse(source_url)
            query_params = parse_qs(parsed.query)
            if 'q' in query_params:
                return query_params['q'][0]
        except Exception:
            pass
        return None
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[str]:
        """
        解析时间戳字符串为ISO格式
        
        Args:
            timestamp_str: 时间戳字符串（ISO格式或Unix时间戳）
            
        Returns:
            ISO格式时间字符串或None
        """
        if not timestamp_str:
            return None
        
        try:
            # 如果已经是ISO格式，直接返回
            if 'T' in timestamp_str:
                return timestamp_str
            # 如果是Unix时间戳
            if timestamp_str.isdigit():
                return datetime.fromtimestamp(int(timestamp_str)).isoformat()
        except Exception:
            pass
        
        return timestamp_str
    
    def _count_replies(self, comment: Dict[str, Any]) -> int:
        """
        递归计算评论的回复数（包括所有子评论）
        
        Args:
            comment: 评论字典
            
        Returns:
            回复总数
        """
        replies = comment.get('replies', [])
        if not replies:
            return 0
        
        count = len(replies)
        for reply in replies:
            count += self._count_replies(reply)
        return count
    
    def convert_first_level_to_post(self, item: Dict[str, Any], classifier_results: Dict[str, Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        将一级评论转换为post格式
        
        Args:
            item: 一级评论项目
            classifier_results: 分类结果字典 {comment_id: result}
            
        Returns:
            post格式的字典
        """
        comments_tree = item.get('comments_tree', [])
        if not comments_tree:
            raise ValueError("comments_tree为空")
        
        first_level_comment = comments_tree[0]
        
        # 获取一级评论的permalink作为source_url
        source_url = first_level_comment.get('permalink', item.get('source_url', ''))
        if not source_url:
            source_url = item.get('source_url', '')
        
        # 获取一级评论的内容
        content_text = first_level_comment.get('body', '')
        
        # 计算hash
        content_hash = self._calculate_content_hash(content_text)
        
        # 获取标题（使用post的标题）
        title = item.get('title', '')
        if not title:
            post_info = item.get('post_info', {})
            title = post_info.get('post_title', '')
        
        # 获取query_seed（优先从item中获取，否则从source_url提取）
        query_seed = item.get('query_seed', None)
        if not query_seed:
            query_seed = self._extract_query_seed(item.get('source_url', ''))
        
        # 转换lang
        lang = item.get('lang', 'english')
        if lang == 'english':
            lang = 'en'
        
        # 统计二级评论数
        second_level_count = len(first_level_comment.get('replies', []))
        
        # 从classifier结果中获取分类信息
        comment_id = first_level_comment.get('id', '')
        classifier_result = classifier_results.get(comment_id, {}) if classifier_results else {}
        
        # 构建post记录
        post_record = {
            "platform": "reddit",
            "source_url": source_url,
            "source_platform_id": first_level_comment.get('id', ''),
            "content_hash": content_hash,
            "title": title,
            "content_text": content_text,
            "lang": lang,
            "media_urls": item.get('media_urls', []),
            "author_name": first_level_comment.get('author', '[deleted]'),
            "author_handle": first_level_comment.get('author', '[deleted]'),
            "author_followers": None,
            "author_profile": first_level_comment.get('author_profile', None),  # 从一级评论中获取author_profile
            "likes": first_level_comment.get('score', 0),
            "comments_count": second_level_count,
            "saves": None,
            "views": None,
            "scene": classifier_result.get('scene'),
            "sub_scene": None,
            "post_type": classifier_result.get('post_type'),
            "base_quality_score": float(classifier_result.get('base_quality_score', 0.0)),
            "is_source_available": True,
            "last_checked_at": None,
            "processed": False,
            "fetched_at": self._parse_timestamp(item.get('fetched_at', '')),
            "subtitle_text": None,
            "query_seed": query_seed,
            "content_type": None
        }
        
        return post_record
    
    def extract_all_second_level_comments(self, first_level_comment: Dict[str, Any], 
                                         post_source_platform_id: str,
                                         fetched_at: str) -> List[Dict[str, Any]]:
        """
        递归提取所有二级及更深层级的评论
        
        Args:
            first_level_comment: 一级评论字典
            post_source_platform_id: 一级评论的ID（作为post的source_platform_id）
            fetched_at: 抓取时间
            
        Returns:
            所有二级及更深层级评论的列表
        """
        comments = []
        second_level_replies = first_level_comment.get('replies', [])
        
        for second_level_comment in second_level_replies:
            comment_record = self._convert_comment_to_db_format(
                second_level_comment,
                post_source_platform_id=post_source_platform_id,
                parent_comment_id=None,  # 二级评论的parent是None（因为一级评论是post）
                fetched_at=fetched_at
            )
            comments.append(comment_record)
            
            # 递归处理更深层级的评论
            deeper_comments = self._extract_deeper_comments(
                second_level_comment,
                post_source_platform_id=post_source_platform_id,
                parent_comment_id=second_level_comment.get('id', ''),
                fetched_at=fetched_at
            )
            comments.extend(deeper_comments)
        
        return comments
    
    def _extract_deeper_comments(self, parent_comment: Dict[str, Any],
                                post_source_platform_id: str,
                                parent_comment_id: str,
                                fetched_at: str) -> List[Dict[str, Any]]:
        """
        递归提取更深层级的评论（三级、四级等）
        
        Args:
            parent_comment: 父评论字典
            post_source_platform_id: 一级评论的ID（作为post的source_platform_id）
            parent_comment_id: 父评论的ID
            fetched_at: 抓取时间
            
        Returns:
            更深层级评论的列表
        """
        comments = []
        replies = parent_comment.get('replies', [])
        
        for reply in replies:
            comment_record = self._convert_comment_to_db_format(
                reply,
                post_source_platform_id=post_source_platform_id,
                parent_comment_id=parent_comment_id,
                fetched_at=fetched_at
            )
            comments.append(comment_record)
            
            # 递归处理更深层级
            deeper_comments = self._extract_deeper_comments(
                reply,
                post_source_platform_id=post_source_platform_id,
                parent_comment_id=reply.get('id', ''),
                fetched_at=fetched_at
            )
            comments.extend(deeper_comments)
        
        return comments
    
    def _convert_comment_to_db_format(self, comment: Dict[str, Any],
                                     post_source_platform_id: str,
                                     parent_comment_id: Optional[str],
                                     fetched_at: str) -> Dict[str, Any]:
        """
        将评论转换为数据库格式
        
        Args:
            comment: 评论字典
            post_source_platform_id: 一级评论的ID（作为post的source_platform_id）
            parent_comment_id: 父评论的ID（如果是二级评论则为None）
            fetched_at: 抓取时间
            
        Returns:
            数据库格式的评论字典
        """
        replies_count = self._count_replies(comment)
        
        comment_record = {
            "post_id": None,  # 入库时关联
            "parent_comment_id": None,  # 入库时关联，这里先设为None
            "platform": "reddit",
            "source_comment_id": comment.get('id', ''),
            "content_text": comment.get('body', ''),
            "author_name": comment.get('author', '[deleted]'),
            "author_handle": comment.get('author', '[deleted]'),
            "likes": comment.get('score', 0),
            "replies_count": replies_count,
            "published_at": self._parse_timestamp(comment.get('created_utc', '')),
            "fetched_at": self._parse_timestamp(fetched_at)
        }
        
        # 保存parent_comment_id的原始值（source_comment_id），入库时用于关联
        if parent_comment_id:
            comment_record["_parent_source_comment_id"] = parent_comment_id
        
        # 保存post_source_platform_id，入库时用于关联
        comment_record["_post_source_platform_id"] = post_source_platform_id
        
        return comment_record
    
    def load_classifier_results(self, task_id: str) -> Dict[str, Dict[str, Any]]:
        """
        加载分类结果
        
        Args:
            task_id: 任务ID
            
        Returns:
            分类结果字典 {comment_id: result}
        """
        classifier_dir = os.path.join(self.data_dir, "classifier_output")
        filename = f"{task_id}_classifier.json"
        filepath = os.path.join(classifier_dir, filename)
        
        if not os.path.exists(filepath):
            print(f"  ⚠️  分类结果文件不存在: {filepath}，将使用默认值")
            return {}
        
        with open(filepath, 'r', encoding='utf-8') as f:
            classifier_list = json.load(f)
        
        # 转换为字典格式
        classifier_dict = {}
        for item in classifier_list:
            comment_id = item.get('id', '')
            if comment_id:
                classifier_dict[comment_id] = item
        
        return classifier_dict
    
    def prepare_task_data(self, task_id: str, use_classifier: bool = True) -> tuple:
        """
        准备任务数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            (posts列表, comments列表)
        """
        print(f"\n开始准备数据库数据: {task_id}")
        print("=" * 80)
        
        # 加载过滤后的数据
        try:
            filtered_data = self.load_filtered_data(task_id)
            print(f"✓ 加载了 {len(filtered_data)} 个一级评论项目")
        except FileNotFoundError as e:
            print(f"✗ 错误: {e}")
            return [], []
        
        if not filtered_data:
            print("数据为空")
            return [], []
        
        # 加载分类结果
        classifier_results = {}
        if use_classifier:
            classifier_results = self.load_classifier_results(task_id)
            if classifier_results:
                print(f"✓ 加载了 {len(classifier_results)} 条分类结果")
            else:
                print(f"  ⚠️  未找到分类结果，将使用默认值")
        
        posts = []
        comments = []
        
        for i, item in enumerate(filtered_data, 1):
            try:
                # 转换为post（传入classifier结果）
                post_record = self.convert_first_level_to_post(item, classifier_results)
                posts.append(post_record)
                
                # 提取所有二级及更深层级的评论
                comments_tree = item.get('comments_tree', [])
                if comments_tree:
                    first_level_comment = comments_tree[0]
                    post_source_platform_id = first_level_comment.get('id', '')
                    fetched_at = item.get('fetched_at', '')
                    
                    second_level_comments = self.extract_all_second_level_comments(
                        first_level_comment,
                        post_source_platform_id,
                        fetched_at
                    )
                    comments.extend(second_level_comments)
                
                if (i + 1) % 100 == 0:
                    print(f"  处理进度: {i + 1}/{len(filtered_data)}")
                    
            except Exception as e:
                print(f"  ⚠️  处理项目 {i} 时出错: {e}")
                continue
        
        print(f"\n✓ 数据准备完成")
        print(f"  - Posts: {len(posts)}")
        print(f"  - Comments: {len(comments)}")
        
        return posts, comments
    
    def save_posts(self, task_id: str, posts: List[Dict[str, Any]]):
        """
        保存posts数据
        
        Args:
            task_id: 任务ID
            posts: posts列表
        """
        filename = f"{task_id}_posts.json"
        filepath = os.path.join(self.posts_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Posts数据已保存到: {filepath}")
        print(f"  共 {len(posts)} 条记录")
    
    def save_comments(self, task_id: str, comments: List[Dict[str, Any]]):
        """
        保存comments数据
        
        Args:
            task_id: 任务ID
            comments: comments列表
        """
        filename = f"{task_id}_comments.json"
        filepath = os.path.join(self.comments_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(comments, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ Comments数据已保存到: {filepath}")
        print(f"  共 {len(comments)} 条记录")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='准备数据库入库数据：将comment_filtered_raw转换为可入库Supabase的格式',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基本用法
  python prepare_for_db.py --task-id task001
        """
    )
    
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    
    args = parser.parse_args()
    
    try:
        preparer = DBDataPreparer()
        posts, comments = preparer.prepare_task_data(args.task_id)
        
        if posts:
            preparer.save_posts(args.task_id, posts)
        
        if comments:
            preparer.save_comments(args.task_id, comments)
        
        if not posts and not comments:
            print("\n没有数据需要保存")
            
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


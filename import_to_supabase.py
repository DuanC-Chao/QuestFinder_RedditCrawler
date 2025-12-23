#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase入库脚本（新版本）
功能：将ready_for_DB_posts和ready_for_DB_comments中的数据导入到Supabase
- 一级评论导入到crawled_posts表
- 二级及更深层级评论导入到crawled_comments表
"""

import os
import json
import argparse
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from datetime import datetime


class SupabaseImporter:
    """Supabase数据导入器（新版本）"""
    
    def __init__(self):
        """初始化导入器"""
        self.data_dir = "Data"
        self.posts_dir = os.path.join(self.data_dir, "ready_for_DB_posts")
        self.comments_dir = os.path.join(self.data_dir, "ready_for_DB_comments")
        
        # 加载环境变量
        load_dotenv()
        
        # 获取Supabase配置
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_service_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        self.supabase_anon_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not self.supabase_url:
            raise ValueError("未找到 SUPABASE_URL 环境变量，请在 .env 文件中配置")
        
        # 优先使用service role key
        self.supabase_key = self.supabase_service_key or self.supabase_anon_key
        if not self.supabase_key:
            raise ValueError("未找到 SUPABASE_SERVICE_ROLE_KEY 或 SUPABASE_ANON_KEY 环境变量，请在 .env 文件中配置")
        
        # 初始化Supabase客户端
        try:
            from supabase import create_client, Client
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
        except ImportError:
            raise ImportError("supabase库未安装，请运行: pip install supabase")
        except Exception as e:
            raise Exception(f"创建Supabase客户端失败: {e}")
    
    def test_connection(self) -> bool:
        """
        测试Supabase连接和表是否存在
        
        Returns:
            是否连接成功
        """
        try:
            # 测试posts表
            response = self.supabase.table('crawled_posts').select('id').limit(1).execute()
            print("  ✓ crawled_posts表连接成功")
            
            # 测试comments表
            response = self.supabase.table('crawled_comments').select('id').limit(1).execute()
            print("  ✓ crawled_comments表连接成功")
            
            return True
        except Exception as e:
            error_msg = str(e)
            print(f"  ✗ 连接测试失败: {error_msg[:500]}")
            return False
    
    def load_posts_data(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载posts数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            posts数据列表
        """
        filename = f"{task_id}_posts.json"
        filepath = os.path.join(self.posts_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Posts文件不存在: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def load_comments_data(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载comments数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            comments数据列表
        """
        filename = f"{task_id}_comments.json"
        filepath = os.path.join(self.comments_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Comments文件不存在: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def format_post_for_db(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化post数据以匹配crawled_posts表结构
        
        Args:
            post: post数据字典
            
        Returns:
            格式化后的字典
        """
        db_record = {
            "platform": post.get('platform', 'reddit'),
            "source_url": post.get('source_url', ''),
            "source_platform_id": post.get('source_platform_id', ''),
            "content_hash": post.get('content_hash', ''),
            "title": post.get('title'),
            "content_text": post.get('content_text'),
            "lang": post.get('lang', 'en'),
            "media_urls": post.get('media_urls') if post.get('media_urls') else None,  # 空列表转为None
            "author_name": post.get('author_name'),
            "author_handle": post.get('author_handle'),
            "author_followers": post.get('author_followers') or 0,
            "author_profile": post.get('author_profile'),
            "likes": post.get('likes', 0),
            "comments_count": post.get('comments_count', 0),
            "saves": post.get('saves'),
            "views": post.get('views'),
            "scene": post.get('scene'),
            "sub_scene": post.get('sub_scene'),
            "post_type": post.get('post_type'),
            "base_quality_score": float(post.get('base_quality_score', 0.0)) if post.get('base_quality_score') is not None else 0.0,
            "is_source_available": post.get('is_source_available', True),
            "last_checked_at": post.get('last_checked_at'),
            "processed": post.get('processed', False),
            "fetched_at": post.get('fetched_at'),
            "subtitle_text": post.get('subtitle_text'),
            "query_seed": post.get('query_seed'),
            "content_type": "answer"  # 固定设置为"answer"
        }
        
        return db_record
    
    def format_comment_for_db(self, comment: Dict[str, Any], post_id: Optional[str] = None, parent_comment_id: Optional[str] = None) -> Dict[str, Any]:
        """
        格式化comment数据以匹配crawled_comments表结构
        
        Args:
            comment: comment数据字典
            post_id: 关联的post ID（UUID字符串）
            parent_comment_id: 父评论ID（UUID字符串）
            
        Returns:
            格式化后的字典
        """
        db_record = {
            "post_id": post_id,
            "parent_comment_id": parent_comment_id,
            "platform": comment.get('platform', 'reddit'),
            "source_comment_id": comment.get('source_comment_id', ''),
            "content_text": comment.get('content_text'),
            "author_name": comment.get('author_name'),
            "author_handle": comment.get('author_handle'),
            "likes": comment.get('likes', 0),
            "replies_count": comment.get('replies_count', 0),
            "published_at": comment.get('published_at'),
            "fetched_at": comment.get('fetched_at')
        }
        
        return db_record
    
    def import_posts(self, posts: List[Dict[str, Any]], skip_existing: bool = True, batch_size: int = 50) -> Dict[str, str]:
        """
        导入posts到crawled_posts表
        
        Args:
            posts: posts数据列表
            skip_existing: 是否跳过已存在的记录（基于source_url唯一约束）
            batch_size: 批次大小
            
        Returns:
            source_platform_id到post_id的映射字典
        """
        print(f"\n开始导入 {len(posts)} 条posts...")
        
        # source_platform_id -> post_id (UUID) 映射
        post_id_mapping = {}
        
        # 分批导入
        for i in range(0, len(posts), batch_size):
            batch = posts[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(posts) + batch_size - 1) // batch_size
            
            print(f"\n处理批次 {batch_num}/{total_batches} ({len(batch)} 条记录)...")
            
            # 格式化批次数据
            formatted_batch = []
            for post in batch:
                formatted_post = self.format_post_for_db(post)
                formatted_batch.append(formatted_post)
            
            # 批量插入
            try:
                if skip_existing:
                    # 使用upsert，但只插入新记录（不更新已存在的）
                    response = self.supabase.table('crawled_posts').upsert(
                        formatted_batch,
                        on_conflict='source_url'
                    ).execute()
                else:
                    # 直接插入
                    response = self.supabase.table('crawled_posts').insert(
                        formatted_batch
                    ).execute()
                
                # 从响应中提取插入的记录
                if hasattr(response, 'data') and response.data:
                    for inserted_post in response.data:
                        source_platform_id = inserted_post.get('source_platform_id', '')
                        post_id = inserted_post.get('id', '')
                        if source_platform_id and post_id:
                            post_id_mapping[source_platform_id] = post_id
                    
                    print(f"  ✓ 成功导入 {len(response.data)} 条记录")
                    
                    # 如果使用upsert，需要确保所有记录的映射都已建立
                    # 对于响应中没有返回的记录（可能已存在），需要查询数据库获取id
                    if skip_existing:
                        missing_mappings = []
                        for post in formatted_batch:
                            source_platform_id = post.get('source_platform_id', '')
                            if source_platform_id and source_platform_id not in post_id_mapping:
                                missing_mappings.append(post.get('source_url', ''))
                        
                        if missing_mappings:
                            # 批量查询缺失的映射
                            try:
                                source_urls = [post.get('source_url', '') for post in formatted_batch 
                                             if post.get('source_url') and post.get('source_platform_id', '') not in post_id_mapping]
                                if source_urls:
                                    # 分批查询（Supabase可能有URL长度限制）
                                    query_batch_size = 100
                                    for url_batch_start in range(0, len(source_urls), query_batch_size):
                                        url_batch = source_urls[url_batch_start:url_batch_start + query_batch_size]
                                        try:
                                            query_response = self.supabase.table('crawled_posts').select('id, source_platform_id').in_('source_url', url_batch).execute()
                                            if hasattr(query_response, 'data') and query_response.data:
                                                for existing_post in query_response.data:
                                                    source_platform_id = existing_post.get('source_platform_id', '')
                                                    post_id = existing_post.get('id', '')
                                                    if source_platform_id and post_id:
                                                        post_id_mapping[source_platform_id] = post_id
                                        except Exception as query_e:
                                            print(f"  ⚠️  批量查询失败（批次 {url_batch_start // query_batch_size + 1}）: {query_e}")
                                    print(f"  ✓ 补充了已存在记录的映射")
                            except Exception as e:
                                print(f"  ⚠️  查询已存在记录失败: {e}")
                else:
                    print(f"  ⚠️  响应中没有数据")
                    # 如果响应为空，尝试查询已存在的记录
                    if skip_existing:
                        try:
                            source_urls = [post.get('source_url', '') for post in formatted_batch if post.get('source_url')]
                            if source_urls:
                                # 分批查询（Supabase可能有URL长度限制）
                                query_batch_size = 100
                                total_found = 0
                                for url_batch_start in range(0, len(source_urls), query_batch_size):
                                    url_batch = source_urls[url_batch_start:url_batch_start + query_batch_size]
                                    try:
                                        query_response = self.supabase.table('crawled_posts').select('id, source_platform_id').in_('source_url', url_batch).execute()
                                        if hasattr(query_response, 'data') and query_response.data:
                                            for existing_post in query_response.data:
                                                source_platform_id = existing_post.get('source_platform_id', '')
                                                post_id = existing_post.get('id', '')
                                                if source_platform_id and post_id:
                                                    post_id_mapping[source_platform_id] = post_id
                                            total_found += len(query_response.data)
                                    except Exception as query_e:
                                        print(f"  ⚠️  批量查询失败（批次 {url_batch_start // query_batch_size + 1}）: {query_e}")
                                if total_found > 0:
                                    print(f"  ✓ 查询到 {total_found} 个已存在的记录")
                        except Exception as e:
                            print(f"  ⚠️  查询已存在记录失败: {e}")
                    
            except Exception as e:
                error_msg = str(e)
                # 如果是唯一约束冲突，尝试逐个插入
                if 'duplicate' in error_msg.lower() or 'unique' in error_msg.lower():
                    print(f"  ⚠️  批次插入遇到唯一约束冲突，改为逐个插入...")
                    success_count = 0
                    skip_count = 0
                    
                    for post in formatted_batch:
                        try:
                            if skip_existing:
                                response = self.supabase.table('crawled_posts').upsert(
                                    [post],
                                    on_conflict='source_url'
                                ).execute()
                            else:
                                response = self.supabase.table('crawled_posts').insert(
                                    [post]
                                ).execute()
                            
                            if hasattr(response, 'data') and response.data:
                                inserted_post = response.data[0]
                                source_platform_id = inserted_post.get('source_platform_id', '')
                                post_id = inserted_post.get('id', '')
                                if source_platform_id and post_id:
                                    post_id_mapping[source_platform_id] = post_id
                                success_count += 1
                            else:
                                # 如果响应为空，可能是记录已存在，尝试查询
                                if skip_existing:
                                    try:
                                        source_url = post.get('source_url', '')
                                        if source_url:
                                            query_response = self.supabase.table('crawled_posts').select('id, source_platform_id').eq('source_url', source_url).limit(1).execute()
                                            if hasattr(query_response, 'data') and query_response.data:
                                                existing_post = query_response.data[0]
                                                source_platform_id = existing_post.get('source_platform_id', '')
                                                post_id = existing_post.get('id', '')
                                                if source_platform_id and post_id:
                                                    post_id_mapping[source_platform_id] = post_id
                                                success_count += 1
                                            else:
                                                skip_count += 1
                                        else:
                                            skip_count += 1
                                    except Exception as query_e:
                                        skip_count += 1
                                else:
                                    skip_count += 1
                                
                        except Exception as e2:
                            if 'duplicate' in str(e2).lower() or 'unique' in str(e2).lower():
                                # 记录已存在，尝试查询获取id
                                if skip_existing:
                                    try:
                                        source_url = post.get('source_url', '')
                                        if source_url:
                                            query_response = self.supabase.table('crawled_posts').select('id, source_platform_id').eq('source_url', source_url).limit(1).execute()
                                            if hasattr(query_response, 'data') and query_response.data:
                                                existing_post = query_response.data[0]
                                                source_platform_id = existing_post.get('source_platform_id', '')
                                                post_id = existing_post.get('id', '')
                                                if source_platform_id and post_id:
                                                    post_id_mapping[source_platform_id] = post_id
                                                success_count += 1
                                            else:
                                                skip_count += 1
                                        else:
                                            skip_count += 1
                                    except Exception as query_e:
                                        skip_count += 1
                                else:
                                    skip_count += 1
                            else:
                                print(f"  ✗ 插入失败: {post.get('source_url', '')[:50]}... - {e2}")
                    
                    print(f"  ✓ 成功: {success_count} 条，跳过: {skip_count} 条")
                else:
                    print(f"  ✗ 批次插入失败: {error_msg[:500]}")
                    # 尝试逐个插入
                    print(f"  尝试逐个插入...")
                    for post in formatted_batch:
                        try:
                            response = self.supabase.table('crawled_posts').insert([post]).execute()
                            if hasattr(response, 'data') and response.data:
                                inserted_post = response.data[0]
                                source_platform_id = inserted_post.get('source_platform_id', '')
                                post_id = inserted_post.get('id', '')
                                if source_platform_id and post_id:
                                    post_id_mapping[source_platform_id] = post_id
                        except Exception as e2:
                            print(f"  ✗ 插入失败: {post.get('source_url', '')[:50]}... - {e2}")
        
        print(f"\n✓ Posts导入完成，共建立 {len(post_id_mapping)} 个映射关系")
        return post_id_mapping
    
    def import_comments(self, comments: List[Dict[str, Any]], 
                       post_id_mapping: Dict[str, str],
                       skip_existing: bool = True,
                       batch_size: int = 50) -> Dict[str, str]:
        """
        导入comments到crawled_comments表
        
        Args:
            comments: comments数据列表
            post_id_mapping: source_platform_id到post_id的映射
            skip_existing: 是否跳过已存在的记录（基于source_comment_id唯一约束）
            batch_size: 批次大小
            
        Returns:
            source_comment_id到comment_id的映射字典（用于建立父子关系）
        """
        print(f"\n开始导入 {len(comments)} 条comments...")
        
        # source_comment_id -> comment_id (UUID) 映射
        comment_id_mapping = {}
        
        # 先导入所有二级评论（parent_comment_id为None）
        # 然后导入更深层级的评论（需要parent_comment_id）
        
        # 分离二级评论和更深层级评论
        second_level_comments = []
        deeper_comments = []
        
        for comment in comments:
            parent_source_id = comment.get('_parent_source_comment_id')
            if parent_source_id:
                deeper_comments.append(comment)
            else:
                second_level_comments.append(comment)
        
        print(f"  - 二级评论: {len(second_level_comments)} 条")
        print(f"  - 更深层级评论: {len(deeper_comments)} 条")
        
        # 导入二级评论
        if second_level_comments:
            print(f"\n导入二级评论...")
            new_mapping = self._import_comment_batch(
                second_level_comments,
                post_id_mapping,
                {},  # 二级评论没有父评论
                skip_existing,
                batch_size
            )
            comment_id_mapping.update(new_mapping)
        
        # 导入更深层级评论（需要递归处理，因为可能有多层嵌套）
        if deeper_comments:
            print(f"\n导入更深层级评论...")
            # 按层级分组导入（需要确保父评论先导入）
            # 简化处理：多次遍历，每次导入能找到父评论的评论
            max_iterations = 10  # 最多10层嵌套
            remaining_comments = deeper_comments.copy()
            
            for iteration in range(max_iterations):
                if not remaining_comments:
                    break
                
                comments_to_import = []
                still_remaining = []
                
                for comment in remaining_comments:
                    parent_source_id = comment.get('_parent_source_comment_id', '')
                    if parent_source_id in comment_id_mapping:
                        # 找到了父评论，可以导入
                        comments_to_import.append(comment)
                    else:
                        # 父评论还没导入，等待下一轮
                        still_remaining.append(comment)
                
                if comments_to_import:
                    print(f"  第 {iteration + 1} 轮: 导入 {len(comments_to_import)} 条评论...")
                    new_mapping = self._import_comment_batch(
                        comments_to_import,
                        post_id_mapping,
                        comment_id_mapping,  # 传入已有的映射，用于查找父评论
                        skip_existing,
                        batch_size
                    )
                    comment_id_mapping.update(new_mapping)
                
                remaining_comments = still_remaining
                
                if not comments_to_import:
                    # 如果这一轮没有导入任何评论，说明有循环依赖或缺失的父评论
                    print(f"  ⚠️  第 {iteration + 1} 轮没有导入任何评论，可能存在问题")
                    break
            
            if remaining_comments:
                print(f"  ⚠️  仍有 {len(remaining_comments)} 条评论未能导入（可能缺少父评论）")
        
        print(f"\n✓ Comments导入完成，共建立 {len(comment_id_mapping)} 个映射关系")
        return comment_id_mapping
    
    def _import_comment_batch(self, comments: List[Dict[str, Any]],
                              post_id_mapping: Dict[str, str],
                              comment_id_mapping: Dict[str, str],
                              skip_existing: bool,
                              batch_size: int) -> Dict[str, str]:
        """
        导入一批comments
        
        Args:
            comments: comments数据列表
            post_id_mapping: source_platform_id到post_id的映射
            comment_id_mapping: 已有的source_comment_id到comment_id的映射
            skip_existing: 是否跳过已存在的记录
            batch_size: 批次大小
            
        Returns:
            新建立的source_comment_id到comment_id的映射
        """
        new_mapping = {}
        
        # 分批导入
        for i in range(0, len(comments), batch_size):
            batch = comments[i:i + batch_size]
            
            # 格式化批次数据
            formatted_batch = []
            skipped_count = 0
            
            for comment in batch:
                # 获取post_id
                post_source_id = comment.get('_post_source_platform_id', '')
                post_id = post_id_mapping.get(post_source_id)
                
                if not post_id:
                    # 如果找不到对应的post_id，跳过这条评论
                    skipped_count += 1
                    continue
                
                # 获取parent_comment_id
                parent_source_id = comment.get('_parent_source_comment_id')
                parent_comment_id = comment_id_mapping.get(parent_source_id) if parent_source_id else None
                
                formatted_comment = self.format_comment_for_db(
                    comment,
                    post_id=post_id,
                    parent_comment_id=parent_comment_id
                )
                formatted_batch.append(formatted_comment)
            
            if skipped_count > 0:
                print(f"  ⚠️  跳过了 {skipped_count} 条评论（找不到对应的post_id）")
            
            if not formatted_batch:
                continue
            
            # 批量插入
            try:
                if skip_existing:
                    response = self.supabase.table('crawled_comments').upsert(
                        formatted_batch,
                        on_conflict='source_comment_id'
                    ).execute()
                else:
                    response = self.supabase.table('crawled_comments').insert(
                        formatted_batch
                    ).execute()
                
                # 从响应中提取插入的记录
                if hasattr(response, 'data') and response.data:
                    for inserted_comment in response.data:
                        source_comment_id = inserted_comment.get('source_comment_id', '')
                        comment_id = inserted_comment.get('id', '')
                        if source_comment_id and comment_id:
                            new_mapping[source_comment_id] = comment_id
                    
                    print(f"  ✓ 成功导入 {len(response.data)} 条记录")
                    
            except Exception as e:
                error_msg = str(e)
                # 如果是唯一约束冲突，尝试逐个插入
                if 'duplicate' in error_msg.lower() or 'unique' in error_msg.lower():
                    print(f"  ⚠️  批次插入遇到唯一约束冲突，改为逐个插入...")
                    success_count = 0
                    skip_count = 0
                    
                    for comment in formatted_batch:
                        try:
                            if skip_existing:
                                response = self.supabase.table('crawled_comments').upsert(
                                    [comment],
                                    on_conflict='source_comment_id'
                                ).execute()
                            else:
                                response = self.supabase.table('crawled_comments').insert(
                                    [comment]
                                ).execute()
                            
                            if hasattr(response, 'data') and response.data:
                                inserted_comment = response.data[0]
                                source_comment_id = inserted_comment.get('source_comment_id', '')
                                comment_id = inserted_comment.get('id', '')
                                if source_comment_id and comment_id:
                                    new_mapping[source_comment_id] = comment_id
                                success_count += 1
                            else:
                                skip_count += 1
                                
                        except Exception as e2:
                            if 'duplicate' in str(e2).lower() or 'unique' in str(e2).lower():
                                skip_count += 1
                            else:
                                print(f"  ✗ 插入失败: {comment.get('source_comment_id', '')[:50]}... - {e2}")
                    
                    print(f"  ✓ 成功: {success_count} 条，跳过: {skip_count} 条")
                else:
                    print(f"  ✗ 批次插入失败: {error_msg[:500]}")
        
        return new_mapping
    
    def import_task(self, task_id: str, skip_existing: bool = True, batch_size: int = 50, update_existing: bool = False):
        """
        导入任务数据到Supabase
        
        Args:
            task_id: 任务ID
            skip_existing: 是否跳过已存在的记录（默认True）
            batch_size: 批次大小（默认50）
            update_existing: 是否更新已存在的记录（默认False）
        """
        print(f"\n开始导入任务: {task_id}")
        print("=" * 80)
        
        # 测试连接
        print("\n1. 测试Supabase连接...")
        if not self.test_connection():
            raise Exception("Supabase连接失败")
        
        # 加载数据
        print("\n2. 加载数据文件...")
        try:
            posts = self.load_posts_data(task_id)
            print(f"  ✓ 加载了 {len(posts)} 条posts")
        except FileNotFoundError as e:
            print(f"  ✗ {e}")
            raise
        
        try:
            comments = self.load_comments_data(task_id)
            print(f"  ✓ 加载了 {len(comments)} 条comments")
        except FileNotFoundError as e:
            print(f"  ⚠️  {e}，将只导入posts")
            comments = []
        
        # 数据验证
        print("\n3. 验证数据...")
        # 检查posts数据
        invalid_posts = []
        for i, post in enumerate(posts):
            if not post.get('source_url'):
                invalid_posts.append(i)
            if not post.get('platform'):
                invalid_posts.append(i)
        
        if invalid_posts:
            print(f"  ⚠️  发现 {len(invalid_posts)} 条无效的post记录（缺少source_url或platform）")
        
        # 检查comments数据
        invalid_comments = []
        comments_without_post_ref = []
        for i, comment in enumerate(comments):
            if not comment.get('source_comment_id'):
                invalid_comments.append(i)
            if not comment.get('platform'):
                invalid_comments.append(i)
            # 检查是否有_post_source_platform_id字段（用于关联post）
            if not comment.get('_post_source_platform_id'):
                comments_without_post_ref.append(i)
        
        if invalid_comments:
            print(f"  ⚠️  发现 {len(invalid_comments)} 条无效的comment记录（缺少source_comment_id或platform）")
        
        if comments_without_post_ref:
            print(f"  ⚠️  发现 {len(comments_without_post_ref)} 条comment记录缺少_post_source_platform_id字段（无法关联到post）")
        
        # 导入posts
        print("\n4. 导入posts到crawled_posts表...")
        post_id_mapping = self.import_posts(posts, skip_existing=skip_existing, batch_size=batch_size)
        
        if not post_id_mapping:
            print("  ⚠️  没有成功导入任何post，无法导入comments")
            return
        
        # 验证comments中的_post_source_platform_id是否都能在post_id_mapping中找到
        if comments:
            missing_post_refs = set()
            for comment in comments:
                post_source_id = comment.get('_post_source_platform_id', '')
                if post_source_id and post_source_id not in post_id_mapping:
                    missing_post_refs.add(post_source_id)
            
            if missing_post_refs:
                print(f"  ⚠️  警告: 发现 {len(missing_post_refs)} 个不同的_post_source_platform_id在posts中找不到对应的记录")
                print(f"  ⚠️  这些comments将被跳过。示例: {list(missing_post_refs)[:5]}")
            else:
                print(f"  ✓ 所有comments的_post_source_platform_id都能在posts中找到对应记录")
        
        # 导入comments
        if comments:
            print("\n5. 导入comments到crawled_comments表...")
            comment_id_mapping = self.import_comments(
                comments,
                post_id_mapping,
                skip_existing=skip_existing,
                batch_size=batch_size
            )
        else:
            print("\n5. 跳过comments导入（没有comments数据）")
        
        print("\n" + "=" * 80)
        print("✓ 导入完成！")
        print(f"  - Posts: {len(post_id_mapping)} 条")
        if comments:
            print(f"  - Comments: {len(comment_id_mapping)} 条")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='导入数据到Supabase（新版本：一级评论->posts，二级评论->comments）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基本用法（增量导入，跳过已存在的记录）
  python import_to_supabase.py --task-id task001
  
  # 更新已存在的记录（谨慎使用）
  python import_to_supabase.py --task-id task001 --update-existing
  
  # 自定义批次大小
  python import_to_supabase.py --task-id task001 --batch-size 100
        """
    )
    
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    parser.add_argument('--skip-existing', action='store_true', default=True,
                       help='跳过已存在的记录（默认True）')
    parser.add_argument('--no-skip-existing', dest='skip_existing', action='store_false',
                       help='不跳过已存在的记录（会报错如果记录已存在）')
    parser.add_argument('--update-existing', action='store_true', default=False,
                       help='更新已存在的记录（默认False）')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='批次大小（默认50）')
    
    args = parser.parse_args()
    
    try:
        importer = SupabaseImporter()
        importer.import_task(
            args.task_id,
            skip_existing=args.skip_existing,
            batch_size=args.batch_size,
            update_existing=args.update_existing
        )
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()


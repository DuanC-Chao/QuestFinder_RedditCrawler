#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase入库脚本
功能：将ready_for_DB中的数据导入到Supabase的crawled_posts表
"""

import os
import json
import argparse
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote_plus
import time


class SupabaseImporter:
    """Supabase数据导入器"""
    
    def __init__(self):
        """初始化导入器"""
        self.data_dir = "Data"
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.mask_dir = os.path.join(self.data_dir, "mask")
        self.classifier_output_dir = os.path.join(self.data_dir, "classifier_output")
        self.ready_dir = os.path.join(self.data_dir, "ready_for_DB")
        
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
            # 尝试查询表（即使为空也应该能查询）
            response = self.supabase.table('crawled_posts').select('id').limit(1).execute()
            print("  ✓ Supabase连接成功，表存在")
            return True
        except Exception as e:
            error_msg = str(e)
            error_details = error_msg
            # 尝试提取更详细的错误信息
            if hasattr(e, 'message'):
                error_details = e.message
            elif hasattr(e, 'args') and e.args:
                error_details = str(e.args[0])
            
            if 'relation' in error_msg.lower() or 'does not exist' in error_msg.lower() or '404' in error_msg or 'not found' in error_msg.lower():
                print(f"  ✗ 表 'crawled_posts' 不存在或无法访问")
                print(f"  错误详情: {error_details[:500]}")
                print(f"  请检查:")
                print(f"    1. 表名是否正确（应该是 'crawled_posts'）")
                print(f"    2. Supabase项目中的表是否存在")
                print(f"    3. 使用的Key是否有足够的权限（建议使用SUPABASE_SERVICE_ROLE_KEY）")
                print(f"    4. Supabase URL是否正确: {self.supabase_url}")
            else:
                print(f"  ✗ 连接测试失败: {error_details[:500]}")
            return False
    
    def check_task_files(self, task_id: str) -> Dict[str, bool]:
        """
        检查1: 确保task_id在所有目录中都有且仅有一个对应文件
        
        Args:
            task_id: 任务ID
            
        Returns:
            检查结果字典
        """
        checks = {
            'raw': False,
            'mask': False,
            'classifier_output': False,
            'ready_for_DB': False
        }
        
        # 检查raw文件
        raw_file = os.path.join(self.raw_dir, f"{task_id}.json")
        if os.path.exists(raw_file):
            checks['raw'] = True
        else:
            print(f"  ✗ Raw文件不存在: {raw_file}")
        
        # 检查mask文件
        mask_file = os.path.join(self.mask_dir, f"{task_id}_mask.json")
        if os.path.exists(mask_file):
            checks['mask'] = True
        else:
            print(f"  ✗ Mask文件不存在: {mask_file}")
        
        # 检查classifier_output文件
        classifier_file = os.path.join(self.classifier_output_dir, f"{task_id}_classifier.json")
        if os.path.exists(classifier_file):
            checks['classifier_output'] = True
        else:
            print(f"  ✗ Classifier输出文件不存在: {classifier_file}")
        
        # 检查ready_for_DB文件
        ready_file = os.path.join(self.ready_dir, f"{task_id}_ready.json")
        if os.path.exists(ready_file):
            checks['ready_for_DB'] = True
        else:
            print(f"  ✗ Ready文件不存在: {ready_file}")
        
        return checks
    
    def check_ready_data_validity(self, ready_data: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
        """
        检查2: 确保ready_for_DB文件中scene, post_type, base_quality_score都不为null
        
        Args:
            ready_data: ready_for_DB数据列表
            
        Returns:
            (是否有效, 错误列表)
        """
        errors = []
        
        for i, record in enumerate(ready_data):
            post_id = record.get('source_platform_id', f'index_{i}')
            
            # 检查scene
            if record.get('scene') is None:
                errors.append(f"记录 {post_id}: scene字段为null")
            
            # 检查post_type
            if record.get('post_type') is None:
                errors.append(f"记录 {post_id}: post_type字段为null")
            
            # 检查base_quality_score
            if record.get('base_quality_score') is None:
                errors.append(f"记录 {post_id}: base_quality_score字段为null")
        
        return len(errors) == 0, errors
    
    def extract_query_seed(self, source_url: str) -> Optional[str]:
        """
        从source_url中提取query_seed
        
        Args:
            source_url: 来源URL
            
        Returns:
            query_seed或None
        """
        try:
            parsed = urlparse(source_url)
            params = parse_qs(parsed.query)
            query = params.get('q', [''])[0]
            if query:
                # URL解码
                query = unquote_plus(query)
                return query
        except Exception:
            pass
        return None
    
    def format_record_for_db(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化记录以匹配数据库schema
        
        Args:
            record: ready_for_DB中的记录
            
        Returns:
            格式化后的记录
        """
        # 提取query_seed（优先使用原始URL，如果已修复）
        original_url = record.get('_original_source_url', '')
        source_url = record.get('source_url', '')
        query_seed = self.extract_query_seed(original_url if original_url else source_url)
        
        # 格式化时间戳（Supabase需要ISO格式字符串）
        fetched_at = record.get('fetched_at')
        if fetched_at:
            try:
                # 尝试解析ISO格式时间
                if isinstance(fetched_at, str):
                    # 移除时区信息（如果有）
                    if '+' in fetched_at:
                        fetched_at = fetched_at.split('+')[0]
                    if 'Z' in fetched_at:
                        fetched_at = fetched_at.replace('Z', '')
                    # 确保格式正确（Supabase需要 'YYYY-MM-DDTHH:MM:SS' 格式）
                    if 'T' in fetched_at:
                        # 验证格式
                        datetime.fromisoformat(fetched_at)
                    else:
                        # 如果没有时间部分，添加默认时间
                        fetched_at = f"{fetched_at}T00:00:00"
            except Exception as e:
                print(f"  警告: 时间戳格式错误: {fetched_at}, 错误: {e}")
                fetched_at = None
        
        # 构建数据库记录
        db_record = {
            "platform": record.get('platform', 'reddit'),
            "source_url": record.get('source_url', ''),
            "source_platform_id": record.get('source_platform_id'),
            "content_hash": record.get('content_hash'),
            "title": record.get('title'),
            "content_text": record.get('content_text'),
            "lang": record.get('lang', 'en'),
            "media_urls": record.get('media_urls', []),
            "author_name": record.get('author_name'),
            "author_handle": record.get('author_handle'),
            "author_followers": record.get('author_followers', 0) if record.get('author_followers') is not None else 0,
            "author_profile": record.get('author_profile'),
            "likes": record.get('likes', 0) if record.get('likes') is not None else 0,
            "comments_count": record.get('comments_count', 0) if record.get('comments_count') is not None else 0,
            "saves": record.get('saves', 0) if record.get('saves') is not None else 0,
            "views": record.get('views', 0) if record.get('views') is not None else 0,
            "scene": record.get('scene'),
            "subtag": record.get('subtag'),
            "post_type": record.get('post_type'),
            "base_quality_score": float(record.get('base_quality_score', 0)) if record.get('base_quality_score') is not None else 0.0,
            "is_source_available": record.get('is_source_available', True) if record.get('is_source_available') is not None else True,
            "last_checked_at": record.get('last_checked_at'),
            "processed": record.get('processed', False) if record.get('processed') is not None else False,
            "fetched_at": fetched_at,
            "subtitle_text": record.get('subtitle_text'),  # 如果ready_for_DB中没有，则为None
            "query_seed": query_seed
        }
        
        return db_record
    
    def extract_post_url_from_raw(self, raw_data: List[Dict[str, Any]], source_platform_id: str) -> Optional[str]:
        """
        从raw数据中提取帖子的实际URL
        
        Args:
            raw_data: raw数据列表
            source_platform_id: 帖子的platform ID
            
        Returns:
            帖子的实际URL，如果找不到则返回None
        """
        for post in raw_data:
            if post.get('source_platform_id') == source_platform_id:
                # 尝试从评论树中提取帖子URL
                comments_tree = post.get('comments_tree', [])
                if comments_tree and len(comments_tree) > 0:
                    first_comment = comments_tree[0]
                    permalink = first_comment.get('permalink', '')
                    if permalink:
                        # 从评论permalink中提取帖子URL（去掉评论ID部分）
                        # 格式: https://reddit.com/r/subreddit/comments/post_id/title/comment_id/
                        # 帖子URL: https://reddit.com/r/subreddit/comments/post_id/title/
                        parts = permalink.rstrip('/').split('/')
                        if len(parts) >= 6:
                            # 取前6部分（去掉评论ID）
                            post_url = '/'.join(parts[:6]) + '/'
                            return post_url
                
                # 如果从评论中提取失败，尝试根据subreddit和post_id构建
                subreddit = post.get('subreddit', '')
                if subreddit and source_platform_id:
                    # Reddit帖子URL格式: https://reddit.com/r/{subreddit}/comments/{post_id}/{title}/
                    # 但我们没有title的slug，所以使用简化版本
                    return f"https://reddit.com/r/{subreddit}/comments/{source_platform_id}/"
        
        return None
    
    def load_raw_data(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载raw数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            raw数据列表
        """
        filename = f"{task_id}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Raw文件不存在: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
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
    
    def import_to_supabase(self, task_id: str, batch_size: int = 100, skip_existing: bool = True, update_existing: bool = False):
        """
        将数据导入到Supabase
        
        Args:
            task_id: 任务ID
            batch_size: 批次大小
            skip_existing: 是否跳过已存在的记录（基于source_url），默认True（增量导入，不覆盖）
            update_existing: 是否更新已存在的记录，默认False（不覆盖现有数据）
        """
        print(f"\n开始导入任务: {task_id}")
        print("=" * 80)
        
        # 测试连接
        print("\n[连接测试] 测试Supabase连接...")
        if not self.test_connection():
            raise ValueError("Supabase连接失败，请检查配置和表是否存在")
        
        # 检查1: 文件存在性检查
        print("\n[检查1] 检查任务文件...")
        checks = self.check_task_files(task_id)
        
        if not all(checks.values()):
            missing = [k for k, v in checks.items() if not v]
            raise ValueError(f"缺少必需文件: {', '.join(missing)}")
        
        print("  ✓ 所有必需文件都存在")
        
        # 加载ready_for_DB数据
        print("\n加载ready_for_DB数据...")
        ready_data = self.load_ready_data(task_id)
        print(f"  ✓ 加载了 {len(ready_data)} 条记录")
        
        # 修复source_url：如果source_url是搜索页面URL，从raw数据中提取正确的帖子URL
        print("\n修复source_url字段...")
        try:
            raw_data = self.load_raw_data(task_id)
            fixed_count = 0
            for record in ready_data:
                source_url = record.get('source_url', '')
                # 检查是否是搜索页面URL
                if '/search/' in source_url:
                    source_platform_id = record.get('source_platform_id')
                    if source_platform_id:
                        post_url = self.extract_post_url_from_raw(raw_data, source_platform_id)
                        if post_url:
                            record['source_url'] = post_url
                            fixed_count += 1
            
            if fixed_count > 0:
                print(f"  ✓ 修复了 {fixed_count} 条记录的source_url")
            else:
                print(f"  ✓ 无需修复（所有source_url都是正确的）")
        except Exception as e:
            print(f"  警告: 修复source_url时出错: {e}")
            print("  将继续使用原始source_url")
        
        # 修复source_url：如果source_url是搜索页面URL，从raw数据中提取正确的帖子URL
        print("\n修复source_url字段...")
        try:
            raw_data = self.load_raw_data(task_id)
            fixed_count = 0
            for record in ready_data:
                source_url = record.get('source_url', '')
                # 检查是否是搜索页面URL
                if '/search/' in source_url:
                    source_platform_id = record.get('source_platform_id')
                    if source_platform_id:
                        post_url = self.extract_post_url_from_raw(raw_data, source_platform_id)
                        if post_url:
                            record['source_url'] = post_url
                            fixed_count += 1
            
            if fixed_count > 0:
                print(f"  ✓ 修复了 {fixed_count} 条记录的source_url")
            else:
                print(f"  ✓ 无需修复（所有source_url都是正确的）")
        except Exception as e:
            print(f"  警告: 修复source_url时出错: {e}")
            print("  将继续使用原始source_url")
        
        # 测试插入（使用第一条记录）
        if ready_data:
            print("\n[插入测试] 测试数据插入...")
            try:
                test_record = self.format_record_for_db(ready_data[0])
                # 创建唯一的测试URL
                test_url = test_record.get('source_url', '') + "_test_" + str(int(time.time()))
                # 只测试必需字段
                test_minimal = {
                    "platform": test_record.get('platform'),
                    "source_url": test_url,
                    "title": "Test Record - Please Delete",
                    "scene": test_record.get('scene'),
                    "post_type": test_record.get('post_type'),
                    "base_quality_score": test_record.get('base_quality_score')
                }
                test_response = self.supabase.table('crawled_posts').insert(test_minimal).execute()
                if hasattr(test_response, 'data') and test_response.data:
                    print("  ✓ 测试插入成功")
                    # 删除测试记录
                    try:
                        if test_response.data and len(test_response.data) > 0:
                            test_id = test_response.data[0].get('id')
                            if test_id:
                                self.supabase.table('crawled_posts').delete().eq('id', test_id).execute()
                                print("  ✓ 测试记录已删除")
                    except Exception as del_e:
                        print(f"  警告: 删除测试记录失败: {del_e}")
                        print(f"  请手动删除source_url包含'_test_'的记录")
                else:
                    print("  ⚠ 测试插入无返回数据（可能成功）")
            except Exception as e:
                error_msg = str(e)
                error_details = error_msg
                if hasattr(e, 'message'):
                    error_details = e.message
                elif hasattr(e, 'args') and e.args:
                    error_details = str(e.args[0])
                
                print(f"  ✗ 测试插入失败: {error_details[:500]}")
                print(f"  这可能是字段映射或数据类型问题")
                print(f"  请检查:")
                print(f"    1. 数据库表字段名是否与代码中的字段名匹配")
                print(f"    2. 数据类型是否正确（特别是base_quality_score应该是numeric(5,2)）")
                print(f"    3. 是否有必填字段缺失")
                print(f"    4. 测试记录字段:")
                print(f"       platform={test_minimal.get('platform')}")
                print(f"       scene={test_minimal.get('scene')}")
                print(f"       post_type={test_minimal.get('post_type')}")
                print(f"       base_quality_score={test_minimal.get('base_quality_score')} (类型: {type(test_minimal.get('base_quality_score'))})")
                raise ValueError(f"测试插入失败，请检查错误信息")
        
        # 检查2: 数据有效性检查
        print("\n[检查2] 检查数据有效性...")
        is_valid, errors = self.check_ready_data_validity(ready_data)
        
        if not is_valid:
            print(f"  ✗ 发现 {len(errors)} 个错误:")
            for error in errors[:10]:  # 只显示前10个错误
                print(f"    - {error}")
            if len(errors) > 10:
                print(f"    ... 还有 {len(errors) - 10} 个错误")
            raise ValueError(f"数据有效性检查失败，共 {len(errors)} 个错误")
        
        print("  ✓ 所有记录的scene, post_type, base_quality_score都不为null")
        
        # 格式化数据
        print("\n格式化数据...")
        db_records = []
        format_errors = []
        for idx, record in enumerate(ready_data):
            try:
                db_record = self.format_record_for_db(record)
                # 验证必需字段
                if not db_record.get('platform'):
                    format_errors.append(f"记录 {idx+1}: platform字段为空")
                if not db_record.get('source_url'):
                    format_errors.append(f"记录 {idx+1}: source_url字段为空")
                db_records.append(db_record)
            except Exception as e:
                format_errors.append(f"记录 {idx+1} ({record.get('source_platform_id', 'unknown')}): {e}")
        
        if format_errors:
            print(f"  ✗ 格式化时发现 {len(format_errors)} 个错误:")
            for error in format_errors[:5]:
                print(f"    - {error}")
            if len(format_errors) > 5:
                print(f"    ... 还有 {len(format_errors) - 5} 个错误")
            raise ValueError(f"数据格式化失败，共 {len(format_errors)} 个错误")
        
        print(f"  ✓ 格式化了 {len(db_records)} 条记录")
        
        # 打印第一条记录作为示例（用于调试）
        if db_records:
            print(f"\n  示例记录（第一条）:")
            sample = db_records[0]
            print(f"    platform: {sample.get('platform')}")
            print(f"    source_url: {sample.get('source_url')[:60]}...")
            print(f"    scene: {sample.get('scene')}")
            print(f"    post_type: {sample.get('post_type')}")
            print(f"    base_quality_score: {sample.get('base_quality_score')}")
            print(f"    fetched_at: {sample.get('fetched_at')}")
        
        # 如果skip_existing，先查询已存在的source_url
        existing_urls = set()
        if skip_existing:
            print("\n检查已存在的记录...")
            try:
                # 批量查询已存在的source_url
                source_urls = [r['source_url'] for r in db_records]
                
                # 逐个查询（Supabase的PostgREST不支持复杂的in查询）
                # 但我们可以使用or查询（如果URL数量不多）
                # 如果URL数量很多，逐个查询会比较慢，但更可靠
                checked_count = 0
                for url in source_urls:
                    try:
                        response = self.supabase.table('crawled_posts').select('source_url').eq('source_url', url).limit(1).execute()
                        if response.data:
                            existing_urls.add(url)
                        checked_count += 1
                        if checked_count % 50 == 0:
                            print(f"  已检查 {checked_count}/{len(source_urls)} 条...")
                    except Exception:
                        pass
                
                print(f"  ✓ 检查完成，找到 {len(existing_urls)} 条已存在的记录")
            except Exception as e:
                print(f"  警告: 检查已存在记录失败: {e}")
                print("  将继续导入所有记录（可能会因为唯一约束失败）")
        
        # 过滤掉已存在的记录
        if skip_existing and existing_urls:
            original_count = len(db_records)
            db_records = [r for r in db_records if r['source_url'] not in existing_urls]
            skipped_count = original_count - len(db_records)
            print(f"\n跳过 {skipped_count} 条已存在的记录")
            print(f"将导入 {len(db_records)} 条新记录")
        
        if not db_records:
            print("\n没有需要导入的记录")
            return
        
        # 分批导入
        print(f"\n开始导入数据（批次大小: {batch_size}）...")
        success_count = 0
        fail_count = 0
        fail_records = []
        
        for i in range(0, len(db_records), batch_size):
            batch = db_records[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(db_records) + batch_size - 1) // batch_size
            
            try:
                print(f"  导入批次 {batch_num}/{total_batches} ({len(batch)} 条记录)...")
                
                # 根据update_existing参数决定使用insert还是upsert
                if update_existing:
                    # 使用upsert更新已存在的记录
                    response = self.supabase.table('crawled_posts').upsert(
                        batch,
                        on_conflict='source_url'
                    ).execute()
                else:
                    # 使用insert（如果记录已存在会失败，但我们已经过滤掉了）
                    response = self.supabase.table('crawled_posts').insert(batch).execute()
                
                # Supabase客户端可能返回不同的响应格式
                if hasattr(response, 'data') and response.data:
                    success_count += len(batch)
                    print(f"    ✓ 成功导入 {len(batch)} 条记录")
                elif hasattr(response, 'data') and response.data is None:
                    # 某些情况下，即使成功也可能返回None
                    # 检查是否有错误
                    if hasattr(response, 'error') and response.error:
                        raise Exception(f"Supabase错误: {response.error}")
                    else:
                        # 假设成功（Supabase有时不返回数据）
                        success_count += len(batch)
                        print(f"    ✓ 成功导入 {len(batch)} 条记录（无返回数据，假设成功）")
                else:
                    fail_count += len(batch)
                    fail_records.extend([r.get('source_url', 'unknown') for r in batch])
                    print(f"    ✗ 导入失败: 无返回数据")
                    # 尝试逐个导入以获取更详细的错误信息
                    print(f"    尝试逐个导入以获取详细错误...")
                    for record in batch:
                        try:
                            # 根据update_existing参数决定使用insert还是upsert
                            if update_existing:
                                single_response = self.supabase.table('crawled_posts').upsert(
                                    record,
                                    on_conflict='source_url'
                                ).execute()
                            else:
                                single_response = self.supabase.table('crawled_posts').insert(record).execute()
                            if hasattr(single_response, 'data') and single_response.data:
                                success_count += 1
                                fail_count -= 1
                                if record.get('source_url') in fail_records:
                                    fail_records.remove(record.get('source_url'))
                        except Exception as single_e:
                            print(f"      记录失败: {record.get('source_url', 'unknown')[:60]}...")
                            print(f"        错误: {str(single_e)[:200]}")
                    
            except Exception as e:
                fail_count += len(batch)
                fail_records.extend([r.get('source_url', 'unknown') for r in batch])
                error_msg = str(e)
                
                # 提取更详细的错误信息
                error_details = error_msg
                if hasattr(e, 'message'):
                    error_details = e.message
                elif hasattr(e, 'args') and e.args:
                    error_details = str(e.args[0])
                
                print(f"    ✗ 批量导入失败: {error_details[:300]}")
                
                # 尝试逐个导入以找出问题
                print(f"    尝试逐个导入以找出问题记录...")
                for idx, record in enumerate(batch):
                    try:
                        # 根据update_existing参数决定使用insert还是upsert
                        if update_existing:
                            single_response = self.supabase.table('crawled_posts').upsert(
                                record,
                                on_conflict='source_url'
                            ).execute()
                        else:
                            single_response = self.supabase.table('crawled_posts').insert(record).execute()
                        if hasattr(single_response, 'data') and single_response.data:
                            success_count += 1
                            fail_count -= 1
                            if record.get('source_url') in fail_records:
                                fail_records.remove(record.get('source_url'))
                            print(f"      [{idx+1}/{len(batch)}] ✓ 成功")
                        else:
                            print(f"      [{idx+1}/{len(batch)}] ⚠ 无返回数据（可能成功）")
                    except Exception as single_e:
                        single_error = str(single_e)
                        print(f"      [{idx+1}/{len(batch)}] ✗ 失败: {record.get('source_url', 'unknown')[:60]}...")
                        print(f"        错误: {single_error[:200]}")
                        
                        # 如果是唯一约束错误，说明记录已存在，这不应该算作失败
                        if 'unique' in single_error.lower() or 'duplicate' in single_error.lower() or '23505' in single_error:
                            print(f"        注意: 记录已存在（这是正常的，不会覆盖）")
                            success_count += 1
                            fail_count -= 1
                            if record.get('source_url') in fail_records:
                                fail_records.remove(record.get('source_url'))
                        # 如果是字段错误，打印记录的关键字段
                        elif 'column' in single_error.lower() or 'field' in single_error.lower():
                            print(f"        记录字段: platform={record.get('platform')}, scene={record.get('scene')}, post_type={record.get('post_type')}")
        
        # 输出结果
        print("\n" + "=" * 80)
        print("导入完成")
        print("=" * 80)
        print(f"成功导入: {success_count} 条")
        print(f"导入失败: {fail_count} 条")
        
        if fail_records:
            print(f"\n失败的记录（前10条）:")
            for url in fail_records[:10]:
                print(f"  - {url}")
            if len(fail_records) > 10:
                print(f"  ... 还有 {len(fail_records) - 10} 条失败记录")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Supabase入库脚本')
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    parser.add_argument('--batch-size', '-b', type=int, default=100,
                       help='批次大小（默认100）')
    parser.add_argument('--no-skip-existing', action='store_true',
                       help='不跳过已存在的记录（默认跳过，增量导入）')
    parser.add_argument('--update-existing', action='store_true',
                       help='更新已存在的记录（默认不更新，只导入新记录）')
    
    args = parser.parse_args()
    
    try:
        importer = SupabaseImporter()
        importer.import_to_supabase(
            args.task_id,
            batch_size=args.batch_size,
            skip_existing=not args.no_skip_existing,
            update_existing=args.update_existing
        )
    except ValueError as e:
        print(f"\n错误: {e}")
        return 1
    except FileNotFoundError as e:
        print(f"\n错误: {e}")
        return 1
    except Exception as e:
        print(f"\n导入失败: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())


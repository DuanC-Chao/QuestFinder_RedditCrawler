#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Post过滤脚本（基于规则版本）
功能：使用关键词规则对Reddit帖子进行过滤，更新mask文件
"""

import os
import json
import argparse
import re
from typing import List, Dict, Any, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


class PostFilterRuleBased:
    """基于规则的Post过滤类"""
    
    def __init__(self, keywords_file: str = 'manual_filter_keywords.json'):
        """
        初始化过滤器
        
        Args:
            keywords_file: 关键词配置文件路径
        """
        self.data_dir = "Data"
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.mask_dir = os.path.join(self.data_dir, "mask")
        self.keywords_file = keywords_file
        self.print_lock = Lock()  # 用于线程安全的打印
        
        # 加载关键词
        self.ai_keywords = []
        self.recipe_keywords = []
        self._load_keywords()
    
    def _load_keywords(self):
        """从配置文件加载关键词"""
        if not os.path.exists(self.keywords_file):
            raise FileNotFoundError(f"关键词配置文件不存在: {self.keywords_file}")
        
        with open(self.keywords_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 提取ai_signal_block和recipe_signal_block
        global_constraints = config.get('global_constraints', {})
        
        # 处理ai_signal_block
        ai_signal_block = global_constraints.get('ai_signal_block', [])
        for keyword in ai_signal_block:
            # 移除引号（如果有）
            keyword = keyword.strip('"\'')
            if keyword:
                self.ai_keywords.append(keyword)
        
        # 处理recipe_signal_block
        recipe_signal_block = global_constraints.get('recipe_signal_block', [])
        for keyword in recipe_signal_block:
            # 移除引号（如果有）
            keyword = keyword.strip('"\'')
            if keyword:
                self.recipe_keywords.append(keyword)
        
        print(f"加载关键词:")
        print(f"  - AI信号关键词: {len(self.ai_keywords)} 个")
        print(f"  - Recipe信号关键词: {len(self.recipe_keywords)} 个")
    
    def _normalize_text(self, text: str) -> str:
        """
        标准化文本：移除标点符号，转换为小写
        
        Args:
            text: 原始文本
            
        Returns:
            标准化后的文本
        """
        if not text:
            return ""
        # 转换为小写
        text = text.lower()
        # 移除标点符号，只保留字母、数字和空格
        text = re.sub(r'[^\w\s]', ' ', text)
        # 将多个空格合并为一个
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def _normalize_keyword(self, keyword: str) -> str:
        """
        标准化关键词：移除引号，转换为小写，处理标点符号
        
        Args:
            keyword: 原始关键词
            
        Returns:
            标准化后的关键词
        """
        # 移除引号
        keyword = keyword.strip('"\'')
        # 转换为小写
        keyword = keyword.lower()
        # 移除标点符号，只保留字母、数字和空格
        keyword = re.sub(r'[^\w\s]', ' ', keyword)
        # 将多个空格合并为一个
        keyword = re.sub(r'\s+', ' ', keyword)
        return keyword.strip()
    
    def _check_keyword_match(self, text: str, keyword: str) -> bool:
        """
        检查文本是否包含关键词（忽略大小写和标点符号）
        
        Args:
            text: 要检查的文本
            keyword: 关键词（可能是短语，如"large language model"）
            
        Returns:
            是否匹配
        """
        if not text or not keyword:
            return False
        
        # 标准化文本和关键词（已转换为小写并移除标点）
        normalized_text = self._normalize_text(text)
        normalized_keyword = self._normalize_keyword(keyword)
        
        # 如果关键词是单个词，直接检查是否包含
        if ' ' not in normalized_keyword:
            # 单个词：检查是否作为完整单词出现（避免部分匹配）
            # 使用单词边界匹配
            pattern = r'\b' + re.escape(normalized_keyword) + r'\b'
            return bool(re.search(pattern, normalized_text))
        else:
            # 短语：检查是否包含整个短语（单词顺序必须一致）
            # 将短语中的单词用\s+连接，允许任意数量的空格
            words = normalized_keyword.split()
            pattern = r'\b' + r'\s+'.join([re.escape(word) for word in words]) + r'\b'
            return bool(re.search(pattern, normalized_text))
    
    def _extract_all_text(self, post: Dict[str, Any]) -> str:
        """
        提取Post的所有文本内容（标题、内容、所有评论）
        
        Args:
            post: 帖子数据
            
        Returns:
            所有文本内容的拼接字符串
        """
        text_parts = []
        
        # 标题
        title = post.get('title', '')
        if title:
            text_parts.append(title)
        
        # 内容
        content = post.get('content_text', post.get('selftext', ''))
        if content:
            text_parts.append(content)
        
        # 所有评论（递归提取）
        comments_tree = post.get('comments_tree', [])
        for comment in comments_tree:
            comment_text = self._extract_comment_text(comment)
            if comment_text:
                text_parts.append(comment_text)
        
        return ' '.join(text_parts)
    
    def _extract_comment_text(self, comment: Dict[str, Any]) -> str:
        """
        递归提取评论的所有文本内容
        
        Args:
            comment: 评论字典
            
        Returns:
            评论文本内容
        """
        text_parts = []
        
        # 评论内容
        body = comment.get('body', '')
        if body and body not in ['[deleted]', '[removed]']:
            text_parts.append(body)
        
        # 递归处理子评论
        replies = comment.get('replies', [])
        for reply in replies:
            reply_text = self._extract_comment_text(reply)
            if reply_text:
                text_parts.append(reply_text)
        
        return ' '.join(text_parts)
    
    def _check_post_valid(self, post: Dict[str, Any]) -> tuple[bool, Optional[str], Optional[str]]:
        """
        检查Post是否valid（同时包含AI信号和Recipe信号）
        
        Args:
            post: 帖子数据
            
        Returns:
            (is_valid, matched_ai_keyword, matched_recipe_keyword) 元组
        """
        # 提取所有文本
        all_text = self._extract_all_text(post)
        
        # 检查是否包含AI信号关键词
        has_ai_signal = False
        matched_ai_keyword = None
        for keyword in self.ai_keywords:
            if self._check_keyword_match(all_text, keyword):
                has_ai_signal = True
                matched_ai_keyword = keyword
                break
        
        # 检查是否包含Recipe信号关键词
        has_recipe_signal = False
        matched_recipe_keyword = None
        for keyword in self.recipe_keywords:
            if self._check_keyword_match(all_text, keyword):
                has_recipe_signal = True
                matched_recipe_keyword = keyword
                break
        
        # 必须同时包含AI信号和Recipe信号
        is_valid = has_ai_signal and has_recipe_signal
        
        return (is_valid, matched_ai_keyword, matched_recipe_keyword)
    
    def load_task_data(self, task_id: str) -> List[Dict[str, Any]]:
        """
        从Data/raw加载任务数据
        
        Args:
            task_id: 任务ID
            
        Returns:
            帖子数据列表
        """
        filename = f"{task_id}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"任务数据文件不存在: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def create_mask_file_from_raw(self, task_id: str) -> List[Dict[str, Any]]:
        """
        根据raw文件创建mask文件
        
        Args:
            task_id: 任务ID
            
        Returns:
            mask数据列表
        """
        # 加载raw数据
        raw_data = self.load_task_data(task_id)
        
        # 创建mask数据
        mask_data = []
        for post in raw_data:
            # 获取post_id（优先使用source_platform_id）
            post_id = post.get('source_platform_id', post.get('id', ''))
            if post_id:
                mask_entry = {
                    "id": post_id,
                    "contains_valid_ai_tool_recipe": True  # 默认值为True，后续会更新
                }
                mask_data.append(mask_entry)
        
        # 保存mask文件
        mask_filename = f"{task_id}_mask.json"
        mask_filepath = os.path.join(self.mask_dir, mask_filename)
        
        # 确保mask目录存在
        os.makedirs(self.mask_dir, exist_ok=True)
        
        with open(mask_filepath, 'w', encoding='utf-8') as f:
            json.dump(mask_data, f, ensure_ascii=False, indent=2)
        
        print(f"已创建mask文件: {mask_filepath}")
        print(f"共创建 {len(mask_data)} 个mask条目")
        
        return mask_data
    
    def load_mask_data(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载mask数据（如果不存在则自动创建）
        
        Args:
            task_id: 任务ID
            
        Returns:
            mask数据列表
        """
        mask_filename = f"{task_id}_mask.json"
        mask_filepath = os.path.join(self.mask_dir, mask_filename)
        
        if not os.path.exists(mask_filepath):
            print(f"Mask文件不存在，根据raw文件自动创建...")
            return self.create_mask_file_from_raw(task_id)
        
        with open(mask_filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def save_mask_data(self, task_id: str, mask_data: List[Dict[str, Any]]):
        """
        保存mask数据
        
        Args:
            task_id: 任务ID
            mask_data: mask数据列表
        """
        mask_filename = f"{task_id}_mask.json"
        mask_filepath = os.path.join(self.mask_dir, mask_filename)
        
        with open(mask_filepath, 'w', encoding='utf-8') as f:
            json.dump(mask_data, f, ensure_ascii=False, indent=2)
        
        print(f"Mask文件已更新: {mask_filepath}")
    
    def process_post(self, post: Dict[str, Any]) -> tuple[str, bool, Optional[str], Optional[str]]:
        """
        处理单个Post（线程安全版本）
        
        Args:
            post: 帖子数据
            
        Returns:
            (post_id, is_valid, matched_ai_keyword, matched_recipe_keyword) 元组
        """
        # 优先使用source_platform_id，如果没有则使用id
        post_id = post.get('source_platform_id', post.get('id', ''))
        
        # 检查是否valid
        is_valid, matched_ai_keyword, matched_recipe_keyword = self._check_post_valid(post)
        
        return (post_id, is_valid, matched_ai_keyword, matched_recipe_keyword)
    
    def filter_task(self, task_id: str, num_threads: int = 16):
        """
        过滤任务的所有Post（多线程版本）
        
        Args:
            task_id: 任务ID
            num_threads: 并发线程数（默认16）
        """
        print(f"\n开始处理任务: {task_id}")
        print(f"使用 {num_threads} 个线程并发处理")
        print(f"过滤规则: 必须同时包含AI信号关键词和Recipe信号关键词")
        
        # 加载数据
        print("加载数据...")
        posts = self.load_task_data(task_id)
        mask_data = self.load_mask_data(task_id)
        
        # 创建Post ID到mask条目的映射
        mask_dict = {item['id']: item for item in mask_data}
        
        # 过滤出需要处理的Post（只处理mask中存在的）
        posts_to_process = []
        for post in posts:
            # 优先使用source_platform_id，如果没有则使用id
            post_id = post.get('source_platform_id', post.get('id', ''))
            if post_id in mask_dict:
                posts_to_process.append(post)
            else:
                with self.print_lock:
                    print(f"警告: Post {post_id} 不在mask文件中，跳过")
        
        print(f"共 {len(posts_to_process)} 个帖子需要处理")
        
        # 使用线程池并发处理
        success_count = 0
        fail_count = 0
        valid_count = 0
        invalid_count = 0
        processed_count = 0
        total_count = len(posts_to_process)
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # 提交所有任务
            future_to_post = {
                executor.submit(self.process_post, post): post 
                for post in posts_to_process
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_post):
                post = future_to_post[future]
                # 优先使用source_platform_id，如果没有则使用id
                post_id = post.get('source_platform_id', post.get('id', ''))
                title = post.get('title', '')[:60]
                
                try:
                    result_post_id, is_valid, matched_ai_keyword, matched_recipe_keyword = future.result()
                    processed_count += 1
                    
                    with self.print_lock:
                        print(f"[{processed_count}/{total_count}] Post: {post_id}")
                        print(f"  标题: {title}...")
                    
                    # 更新mask
                    mask_dict[result_post_id]['contains_valid_ai_tool_recipe'] = is_valid
                    
                    if is_valid:
                        valid_count += 1
                        with self.print_lock:
                            print(f"  结果: ✓ 有效")
                            if matched_ai_keyword:
                                print(f"    匹配的AI关键词: {matched_ai_keyword}")
                            if matched_recipe_keyword:
                                print(f"    匹配的Recipe关键词: {matched_recipe_keyword}")
                    else:
                        invalid_count += 1
                        with self.print_lock:
                            print(f"  结果: ✗ 无效")
                            if not matched_ai_keyword:
                                print(f"    未找到AI信号关键词")
                            if not matched_recipe_keyword:
                                print(f"    未找到Recipe信号关键词")
                    
                    success_count += 1
                
                except Exception as e:
                    processed_count += 1
                    fail_count += 1
                    with self.print_lock:
                        print(f"[{processed_count}/{total_count}] Post: {post_id}")
                        print(f"  错误: {e}")
        
        # 保存更新后的mask数据
        updated_mask_data = list(mask_dict.values())
        self.save_mask_data(task_id, updated_mask_data)
        
        print(f"\n处理完成:")
        print(f"  - 成功处理: {success_count} 个")
        print(f"  - 处理失败: {fail_count} 个")
        print(f"  - 标记为有效: {valid_count} 个")
        print(f"  - 标记为无效: {invalid_count} 个")
        print(f"  - 总计: {len(posts_to_process)} 个")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Post过滤脚本（基于规则版本，多线程）')
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    parser.add_argument('--threads', '-n', type=int, default=16,
                       help='并发线程数（默认16）')
    parser.add_argument('--keywords-file', '-k', default='manual_filter_keywords.json',
                       help='关键词配置文件路径（默认: manual_filter_keywords.json）')
    
    args = parser.parse_args()
    
    # 创建过滤器实例
    try:
        post_filter = PostFilterRuleBased(keywords_file=args.keywords_file)
    except FileNotFoundError as e:
        print(f"错误: {e}")
        return
    
    # 处理任务
    try:
        post_filter.filter_task(args.task_id, num_threads=args.threads)
    except FileNotFoundError as e:
        print(f"错误: {e}")
    except Exception as e:
        print(f"处理失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


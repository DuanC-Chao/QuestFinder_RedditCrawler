#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Post分类脚本
功能：使用DeepSeek API对Reddit帖子进行分类，生成分类结果和数据库就绪文件
"""

import os
import json
import argparse
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime
from format_content_tree import format_post_content_tree


class PostClassifier:
    """Post分类类"""
    
    def __init__(self, api_key: str):
        """
        初始化分类器
        
        Args:
            api_key: DeepSeek API密钥
        """
        self.api_key = api_key
        self.api_url = "https://api.deepseek.com/v1/chat/completions"
        self.data_dir = "Data"
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.mask_dir = os.path.join(self.data_dir, "mask")
        self.classifier_output_dir = os.path.join(self.data_dir, "classifier_output")
        self.ready_dir = os.path.join(self.data_dir, "ready_for_DB")
        self.prompt_template_path = "classifier_prompt.txt"
        self.print_lock = Lock()
        
        # 确保目录存在
        os.makedirs(self.classifier_output_dir, exist_ok=True)
        os.makedirs(self.ready_dir, exist_ok=True)
    
    def load_prompt_template(self) -> str:
        """加载prompt模板"""
        if not os.path.exists(self.prompt_template_path):
            raise FileNotFoundError(f"Prompt模板文件不存在: {self.prompt_template_path}")
        
        with open(self.prompt_template_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def load_task_data(self, task_id: str) -> List[Dict[str, Any]]:
        """从Data/raw加载任务数据"""
        filename = f"{task_id}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"任务数据文件不存在: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def load_mask_data(self, task_id: str) -> Dict[str, bool]:
        """加载mask数据并转换为字典"""
        mask_filename = f"{task_id}_mask.json"
        mask_filepath = os.path.join(self.mask_dir, mask_filename)
        
        if not os.path.exists(mask_filepath):
            raise FileNotFoundError(f"Mask文件不存在: {mask_filepath}")
        
        with open(mask_filepath, 'r', encoding='utf-8') as f:
            mask_list = json.load(f)
        
        # 转换为字典：{post_id: is_valid}
        mask_dict = {}
        for item in mask_list:
            post_id = item.get('id', '')
            is_valid = item.get('contains_valid_ai_tool_recipe', False)
            mask_dict[post_id] = is_valid
        
        return mask_dict
    
    def format_comments_tree(self, comments_tree: List[Dict[str, Any]], depth: int = 0, max_chars: Optional[int] = None, current_chars: int = 0) -> Tuple[str, int]:
        """
        格式化评论树为字符串（保留树结构）
        
        Args:
            comments_tree: 评论树列表
            depth: 当前深度
            max_chars: 最大字符数限制
            current_chars: 当前已使用的字符数
            
        Returns:
            (格式化后的字符串, 使用的字符数)
        """
        if not comments_tree:
            return "", current_chars
        
        result_parts = []
        indent = "  " * depth
        
        for comment in comments_tree:
            if max_chars and current_chars >= max_chars:
                result_parts.append(f"\n{indent}[评论内容已截断...]")
                break
            
            author = comment.get('author', 'Unknown')
            body = comment.get('body', '')
            score = comment.get('score', 0)
            created = comment.get('created_utc', '')
            
            comment_str = f"\n{indent}--- 评论 ---"
            comment_str += f"\n{indent}作者: {author}"
            comment_str += f"\n{indent}点赞数: {score}"
            comment_str += f"\n{indent}时间: {created}"
            comment_str += f"\n{indent}内容: {body}"
            
            comment_len = len(comment_str)
            if max_chars and current_chars + comment_len > max_chars:
                # 截断当前评论
                remaining = max_chars - current_chars
                if remaining > 50:  # 至少保留50个字符
                    comment_str = comment_str[:remaining] + "\n[内容已截断...]"
                    result_parts.append(comment_str)
                    current_chars = max_chars
                    break
                else:
                    result_parts.append(f"\n{indent}[评论内容已截断...]")
                    current_chars = max_chars
                    break
            
            result_parts.append(comment_str)
            current_chars += comment_len
            
            # 处理回复
            replies = comment.get('replies', [])
            if replies:
                replies_str, current_chars = self.format_comments_tree(
                    replies, depth + 1, max_chars, current_chars
                )
                if replies_str:
                    result_parts.append(replies_str)
                    if max_chars and current_chars >= max_chars:
                        break
        
        return "".join(result_parts), current_chars
    
    def format_post_for_prompt(self, post: Dict[str, Any], max_chars: Optional[int] = None) -> str:
        """
        格式化帖子为LLM可读的字符串
        
        Args:
            post: 帖子数据
            max_chars: 最大字符数限制
            
        Returns:
            格式化后的字符串
        """
        parts = []
        
        # 标题
        title = post.get('title', '')
        parts.append(f"标题: {title}\n")
        
        # 内容
        content = post.get('content_text', '')
        parts.append(f"内容:\n{content}\n")
        
        # 评论
        comments_tree = post.get('comments_tree', [])
        if comments_tree:
            parts.append("\n=== 评论 ===")
            comments_str, _ = self.format_comments_tree(comments_tree, max_chars=max_chars)
            parts.append(comments_str)
        else:
            parts.append("\n=== 无评论 ===")
        
        result = "\n".join(parts)
        
        # 如果超过限制，截断
        if max_chars and len(result) > max_chars:
            result = result[:max_chars] + "\n\n[内容已截断...]"
        
        return result
    
    def build_prompt(self, post_content: str) -> str:
        """
        构建完整的prompt
        
        Args:
            post_content: 格式化后的帖子内容
            
        Returns:
            完整的prompt
        """
        template = self.load_prompt_template()
        prompt = template.replace('[INPUT]', post_content)
        return prompt
    
    def call_deepseek_api(self, prompt: str, max_retries: int = 3) -> Optional[str]:
        """
        调用DeepSeek API
        
        Args:
            prompt: 完整的prompt
            max_retries: 最大重试次数
            
        Returns:
            API响应文本或None
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.3
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.post(self.api_url, json=payload, headers=headers, timeout=60)
                response.raise_for_status()
                
                data = response.json()
                content = data.get('choices', [{}])[0].get('message', {}).get('content', '')
                
                if content:
                    return content.strip()
                else:
                    with self.print_lock:
                        print(f"  - API返回空内容，响应: {data}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 * (attempt + 1)
                    with self.print_lock:
                        print(f"  - API调用失败，等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    with self.print_lock:
                        print(f"  - API调用失败: {e}")
                    return None
        
        return None
    
    def parse_classifier_response(self, response: str) -> Optional[Dict[str, Any]]:
        """
        解析分类器响应
        
        Args:
            response: API响应文本
            
        Returns:
            解析后的分类结果或None
        """
        if not response:
            return None
        
        # 检查是否是错误响应
        if "ERROR_INPUT_UNRECOGNIZABLE" in response:
            return {
                "base_quality_score": None,
                "scene": None,
                "post_type": None,
                "error": "ERROR_INPUT_UNRECOGNIZABLE"
            }
        
        # 尝试提取JSON
        try:
            # 查找JSON部分
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            
            if start_idx == -1 or end_idx == 0:
                return None
            
            json_str = response[start_idx:end_idx]
            result = json.loads(json_str)
            
            # 验证字段
            if 'base_quality_score' not in result:
                result['base_quality_score'] = None
            if 'scene' not in result:
                result['scene'] = None
            if 'post_type' not in result:
                result['post_type'] = None
            
            return result
            
        except json.JSONDecodeError as e:
            with self.print_lock:
                print(f"  - JSON解析失败: {e}")
                print(f"  - 响应内容: {response[:200]}")
            return None
    
    def process_post(self, post: Dict[str, Any], mask_dict: Dict[str, bool], max_chars: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        处理单个帖子
        
        Args:
            post: 帖子数据
            mask_dict: mask字典
            max_chars: 最大字符数限制
            
        Returns:
            分类结果或None
        """
        post_id = post.get('source_platform_id', post.get('id', ''))
        
        # 检查是否valid
        if not mask_dict.get(post_id, False):
            return None
        
        # 格式化帖子内容
        post_content = self.format_post_for_prompt(post, max_chars=max_chars)
        
        # 构建prompt
        prompt = self.build_prompt(post_content)
        
        # 调用API
        response = self.call_deepseek_api(prompt)
        if not response:
            return None
        
        # 解析响应
        classifier_result = self.parse_classifier_response(response)
        if not classifier_result:
            return None
        
        # 添加post_id
        classifier_result['post_id'] = post_id
        
        return classifier_result
    
    def save_classifier_output(self, task_id: str, classifier_results: Dict[str, Dict[str, Any]]):
        """
        保存分类结果
        
        Args:
            task_id: 任务ID
            classifier_results: 分类结果字典 {post_id: result}
        """
        filename = f"{task_id}_classifier.json"
        filepath = os.path.join(self.classifier_output_dir, filename)
        
        # 转换为列表格式
        output_list = []
        for post_id, result in classifier_results.items():
            output_list.append(result)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_list, f, ensure_ascii=False, indent=2)
        
        print(f"\n分类结果已保存到: {filepath}")
        print(f"共保存 {len(output_list)} 条分类结果")
    
    def build_ready_data(self, raw_data: List[Dict[str, Any]], classifier_results: Dict[str, Dict[str, Any]], mask_dict: Dict[str, bool]) -> List[Dict[str, Any]]:
        """
        构建数据库就绪的数据（只包含mask中标记为valid的帖子）
        
        Args:
            raw_data: 原始数据列表
            classifier_results: 分类结果字典 {post_id: result}
            mask_dict: mask字典 {post_id: is_valid}
            
        Returns:
            数据库就绪的数据列表
        """
        ready_data = []
        matched_count = 0
        unmatched_count = 0
        skipped_count = 0
        
        for post in raw_data:
            post_id = post.get('source_platform_id', post.get('id', ''))
            
            # 只处理mask中标记为valid的帖子
            if not mask_dict.get(post_id, False):
                skipped_count += 1
                continue
            
            # 获取分类结果
            classifier_result = classifier_results.get(post_id, {})
            
            # 统计匹配情况
            if classifier_result:
                matched_count += 1
            else:
                unmatched_count += 1
            
            # 处理lang字段：将"english"转换为"en"
            lang = post.get('lang', 'en')
            if lang and lang.lower() == 'english':
                lang = 'en'
            elif not lang:
                lang = 'en'
            
            # 使用格式化函数生成完整的content_text（包含标题、内容、评论树）
            formatted_content = format_post_content_tree(post)
            
            # 构建数据库记录
            record = {
                "platform": post.get('platform', 'reddit'),
                "source_url": post.get('source_url', ''),
                "source_platform_id": post_id,
                "content_hash": post.get('hash_content', ''),
                "title": post.get('title', ''),
                "content_text": formatted_content,  # 使用格式化后的完整内容树
                "lang": lang,
                "media_urls": post.get('media_urls', []),
                "author_name": post.get('author_name'),
                "author_handle": post.get('author_handle'),
                "author_followers": post.get('author_followers') if post.get('author_followers') is not None else 0,
                "author_profile": post.get('author_profile'),
                "likes": post.get('likes', 0) if post.get('likes') is not None else 0,
                "comments_count": post.get('comments', 0) if post.get('comments') is not None else 0,
                "saves": post.get('saves') if post.get('saves') is not None else 0,
                "views": post.get('views') if post.get('views') is not None else 0,
                "scene": classifier_result.get('scene'),
                "subtag": None,  # 暂时为空
                "post_type": classifier_result.get('post_type'),
                "base_quality_score": classifier_result.get('base_quality_score'),
                "is_source_available": True,
                "last_checked_at": None,
                "fetched_at": post.get('fetched_at'),
                "processed": True
            }
            
            ready_data.append(record)
        
        print(f"  - Valid帖子总数: {len(ready_data)} 条")
        print(f"  - 匹配到分类结果的帖子: {matched_count} 条")
        print(f"  - 未匹配到分类结果的帖子: {unmatched_count} 条")
        print(f"  - 跳过（非valid）的帖子: {skipped_count} 条")
        
        return ready_data
    
    def save_ready_data(self, task_id: str, ready_data: List[Dict[str, Any]]):
        """
        保存数据库就绪的数据
        
        Args:
            task_id: 任务ID
            ready_data: 数据库就绪的数据列表
        """
        filename = f"{task_id}_ready.json"
        filepath = os.path.join(self.ready_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(ready_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n数据库就绪数据已保存到: {filepath}")
        print(f"共保存 {len(ready_data)} 条记录")
    
    def classify_task(self, task_id: str, max_chars: Optional[int] = None, num_threads: int = 16):
        """
        分类任务的主函数
        
        Args:
            task_id: 任务ID
            max_chars: 最大字符数限制
            num_threads: 并发线程数
        """
        print(f"开始处理任务: {task_id}")
        
        # 加载数据
        print("\n1. 加载数据...")
        raw_data = self.load_task_data(task_id)
        mask_dict = self.load_mask_data(task_id)
        
        print(f"  - 原始数据: {len(raw_data)} 条帖子")
        print(f"  - Mask数据: {len(mask_dict)} 条记录")
        
        # 过滤valid的帖子
        valid_posts = []
        for post in raw_data:
            post_id = post.get('source_platform_id', post.get('id', ''))
            if mask_dict.get(post_id, False):
                valid_posts.append(post)
        
        print(f"  - Valid帖子: {len(valid_posts)} 条")
        
        if not valid_posts:
            print("没有valid的帖子，退出")
            return
        
        # 多线程处理
        print(f"\n2. 使用 {num_threads} 个线程并行分类...")
        classifier_results = {}
        success_count = 0
        fail_count = 0
        
        def process_single_post(post: Dict[str, Any]) -> tuple[str, Optional[Dict[str, Any]]]:
            """处理单个帖子"""
            post_id = post.get('source_platform_id', post.get('id', ''))
            result = self.process_post(post, mask_dict, max_chars=max_chars)
            return post_id, result
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_post = {
                executor.submit(process_single_post, post): post
                for post in valid_posts
            }
            
            completed_count = 0
            for future in as_completed(future_to_post):
                post = future_to_post[future]
                post_id = post.get('source_platform_id', post.get('id', ''))
                title = post.get('title', '')[:50]
                
                try:
                    result_post_id, result = future.result()
                    completed_count += 1
                    
                    with self.print_lock:
                        print(f"[{completed_count}/{len(valid_posts)}] Post: {post_id}")
                        print(f"  标题: {title}...")
                    
                    if result:
                        classifier_results[result_post_id] = result
                        success_count += 1
                        with self.print_lock:
                            print(f"  结果: 成功")
                    else:
                        fail_count += 1
                        with self.print_lock:
                            print(f"  结果: 失败")
                            
                except Exception as e:
                    completed_count += 1
                    fail_count += 1
                    with self.print_lock:
                        print(f"[{completed_count}/{len(valid_posts)}] Post: {post_id}")
                        print(f"  错误: {e}")
        
        print(f"\n分类完成:")
        print(f"  - 成功: {success_count} 条")
        print(f"  - 失败: {fail_count} 条")
        
        # 保存分类结果
        print("\n3. 保存分类结果...")
        self.save_classifier_output(task_id, classifier_results)
        
        # 构建并保存数据库就绪数据
        print("\n4. 构建数据库就绪数据...")
        print(f"  - 分类结果数量: {len(classifier_results)}")
        print(f"  - 原始数据数量: {len(raw_data)}")
        print(f"  - Valid帖子数量: {len(valid_posts)}")
        ready_data = self.build_ready_data(raw_data, classifier_results, mask_dict)
        self.save_ready_data(task_id, ready_data)
        
        print(f"\n任务 {task_id} 处理完成！")


def main():
    """主函数"""
    load_dotenv()
    
    parser = argparse.ArgumentParser(description='Post分类脚本')
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    parser.add_argument('--process-char-count', '-c', type=int, default=None,
                       help='处理时的最大字符数限制（可选）')
    parser.add_argument('--threads', '-n', type=int, default=16,
                       help='并发线程数（默认16）')
    
    args = parser.parse_args()
    
    # 获取API密钥
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        print("错误: 未找到DEEPSEEK_API_KEY环境变量")
        print("请在.env文件中设置DEEPSEEK_API_KEY")
        return
    
    # 创建分类器并处理
    classifier = PostClassifier(api_key)
    classifier.classify_task(args.task_id, max_chars=args.process_char_count, num_threads=args.threads)


if __name__ == '__main__':
    main()


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Post过滤脚本
功能：使用DeepSeek API对Reddit帖子进行过滤，更新mask文件
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


class PostFilter:
    """Post过滤类"""
    
    def __init__(self, api_key: str):
        """
        初始化过滤器
        
        Args:
            api_key: DeepSeek API密钥
        """
        self.api_key = api_key
        self.api_url = "https://api.deepseek.com/v1/chat/completions"
        self.data_dir = "Data"
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.mask_dir = os.path.join(self.data_dir, "mask")
        self.prompt_template_path = "filter_prompt.txt"
        self.print_lock = Lock()  # 用于线程安全的打印
    
    def load_prompt_template(self) -> str:
        """
        加载prompt模板
        
        Returns:
            prompt模板字符串
        """
        if not os.path.exists(self.prompt_template_path):
            raise FileNotFoundError(f"Prompt模板文件不存在: {self.prompt_template_path}")
        
        with open(self.prompt_template_path, 'r', encoding='utf-8') as f:
            return f.read()
    
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
                    "contains_valid_ai_tool_recipe": True  # 默认值为True
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
    
    def extract_post_info(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取Post的关键信息：标题、内容、前三条评论
        
        Args:
            post: 帖子数据
            
        Returns:
            提取的信息字典
        """
        title = post.get('title', '')
        # 优先使用content_text，如果没有则使用selftext
        content = post.get('content_text', post.get('selftext', ''))
        
        # 提取前三条顶级评论（从comments_tree中提取）
        comments_tree = post.get('comments_tree', [])
        top_comments = []
        for comment in comments_tree:
            if comment.get('depth', 0) == 0:
                comment_body = comment.get('body', '')
                # 跳过被删除或移除的评论
                if comment_body and comment_body not in ['[deleted]', '[removed]']:
                    top_comments.append(comment_body)
                if len(top_comments) >= 3:
                    break
        
        return {
            'title': title,
            'content': content,
            'comments': top_comments
        }
    
    def format_post_for_prompt(self, post_info: Dict[str, Any]) -> str:
        """
        格式化Post信息为prompt输入格式
        
        Args:
            post_info: Post信息字典
            
        Returns:
            格式化后的字符串
        """
        lines = []
        
        # 标题
        lines.append(f"标题: {post_info['title']}")
        lines.append("")
        
        # 内容
        if post_info['content']:
            lines.append(f"内容: {post_info['content']}")
        else:
            lines.append("内容: (无)")
        lines.append("")
        
        # 评论
        if post_info['comments']:
            lines.append("评论:")
            for i, comment in enumerate(post_info['comments'], 1):
                lines.append(f"{i}. {comment}")
        else:
            lines.append("评论: (无评论)")
        
        return "\n".join(lines)
    
    def build_prompt(self, post_info: Dict[str, Any]) -> str:
        """
        构建完整的prompt
        
        Args:
            post_info: Post信息字典
            
        Returns:
            完整的prompt字符串
        """
        template = self.load_prompt_template()
        post_text = self.format_post_for_prompt(post_info)
        
        # 替换[INPUT]标记
        prompt = template.replace('[INPUT]', post_text)
        
        return prompt
    
    def call_deepseek_api(self, prompt: str) -> Optional[Dict[str, Any]]:
        """
        调用DeepSeek API
        
        Args:
            prompt: 完整的prompt字符串
            
        Returns:
            API返回的JSON响应，如果失败返回None
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        
        data = {
            'model': 'deepseek-chat',
            'messages': [
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'temperature': 0.1  # 降低温度以获得更一致的输出
        }
        
        try:
            response = requests.post(self.api_url, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            return result
        except Exception as e:
            print(f"  - API调用失败: {e}")
            return None
    
    def parse_api_response(self, api_response: Dict[str, Any]) -> Optional[bool]:
        """
        解析API响应，提取is_valid值
        
        Args:
            api_response: API返回的响应
            
        Returns:
            is_valid值，如果解析失败返回None
        """
        try:
            # 提取content
            content = api_response.get('choices', [{}])[0].get('message', {}).get('content', '')
            
            # 尝试解析JSON
            # 可能包含markdown代码块
            content = content.strip()
            if content.startswith('```'):
                # 移除markdown代码块标记
                lines = content.split('\n')
                json_lines = []
                in_json = False
                for line in lines:
                    if line.strip().startswith('```'):
                        if in_json:
                            break
                        in_json = True
                        continue
                    if in_json:
                        json_lines.append(line)
                content = '\n'.join(json_lines)
            
            # 解析JSON
            result = json.loads(content.strip())
            return result.get('is_valid', None)
        except Exception as e:
            print(f"  - 解析API响应失败: {e}")
            print(f"  - 响应内容: {content[:200] if 'content' in locals() else 'N/A'}")
            return None
    
    def process_post(self, post: Dict[str, Any]) -> Tuple[str, Optional[bool]]:
        """
        处理单个Post（线程安全版本）
        
        Args:
            post: 帖子数据
            
        Returns:
            (post_id, is_valid) 元组，如果处理失败is_valid为None
        """
        # 优先使用source_platform_id，如果没有则使用id
        post_id = post.get('source_platform_id', post.get('id', ''))
        
        # 提取Post信息
        post_info = self.extract_post_info(post)
        
        # 构建prompt
        prompt = self.build_prompt(post_info)
        
        # 调用API
        api_response = self.call_deepseek_api(prompt)
        if not api_response:
            return (post_id, None)
        
        # 解析响应
        is_valid = self.parse_api_response(api_response)
        
        return (post_id, is_valid)
    
    def filter_task(self, task_id: str, num_threads: int = 16):
        """
        过滤任务的所有Post（多线程版本）
        
        Args:
            task_id: 任务ID
            num_threads: 并发线程数（默认16）
        """
        print(f"\n开始处理任务: {task_id}")
        print(f"使用 {num_threads} 个线程并发处理")
        
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
                    result_post_id, is_valid = future.result()
                    processed_count += 1
                    
                    with self.print_lock:
                        print(f"[{processed_count}/{total_count}] Post: {post_id}")
                        print(f"  标题: {title}...")
                    
                    if is_valid is not None:
                        # 更新mask
                        mask_dict[result_post_id]['contains_valid_ai_tool_recipe'] = is_valid
                        with self.print_lock:
                            print(f"  结果: {'有效' if is_valid else '无效'}")
                        success_count += 1
                    else:
                        with self.print_lock:
                            print(f"  结果: 处理失败，保持原值")
                        fail_count += 1
                
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
        print(f"  - 总计: {len(posts_to_process)} 个")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Post过滤脚本（多线程版本）')
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    parser.add_argument('--threads', '-n', type=int, default=16,
                       help='并发线程数（默认16）')
    parser.add_argument('--api-key', help='DeepSeek API密钥（可选，优先使用.env文件）')
    
    args = parser.parse_args()
    
    # 加载环境变量
    load_dotenv()
    
    # 获取API密钥
    api_key = args.api_key or os.getenv('DEEPSEEK_API_KEY')
    
    if not api_key:
        print("错误: 未找到DeepSeek API密钥")
        print("请设置环境变量 DEEPSEEK_API_KEY 或在 .env 文件中配置")
        print("或在命令行使用 --api-key 参数")
        return
    
    # 创建过滤器实例
    post_filter = PostFilter(api_key)
    
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


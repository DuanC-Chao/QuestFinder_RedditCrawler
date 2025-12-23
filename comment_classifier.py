#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
评论分类脚本（适配新数据结构）
功能：使用DeepSeek API对一级评论进行分类，生成分类结果
"""

import os
import json
import argparse
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from format_content_tree import format_post_content_tree


class CommentClassifier:
    """评论分类类（适配新数据结构：一级评论作为post）"""
    
    def __init__(self, api_key: str):
        """
        初始化分类器
        
        Args:
            api_key: DeepSeek API密钥
        """
        self.api_key = api_key
        self.api_url = "https://api.deepseek.com/v1/chat/completions"
        self.data_dir = "Data"
        self.filtered_dir = os.path.join(self.data_dir, "comment_filtered_raw")
        self.classifier_output_dir = os.path.join(self.data_dir, "classifier_output")
        self.prompt_template_path = "classifier_prompt.txt"
        self.print_lock = Lock()
        
        # 确保目录存在
        os.makedirs(self.classifier_output_dir, exist_ok=True)
    
    def load_prompt_template(self) -> str:
        """加载prompt模板"""
        if not os.path.exists(self.prompt_template_path):
            raise FileNotFoundError(f"Prompt模板文件不存在: {self.prompt_template_path}")
        
        with open(self.prompt_template_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def load_filtered_data(self, task_id: str) -> List[Dict[str, Any]]:
        """从Data/comment_filtered_raw加载任务数据"""
        filename = f"{task_id}.json"
        filepath = os.path.join(self.filtered_dir, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"过滤后的数据文件不存在: {filepath}")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def format_post_for_prompt(self, item: Dict[str, Any], max_chars: Optional[int] = None) -> str:
        """
        格式化一级评论项目为prompt输入
        
        Args:
            item: 一级评论项目字典
            max_chars: 最大字符数限制
            
        Returns:
            格式化后的字符串
        """
        # 使用format_content_tree格式化
        # 需要构建一个类似post的结构
        comments_tree = item.get('comments_tree', [])
        if not comments_tree:
            return ""
        
        first_level_comment = comments_tree[0]
        
        # 构建post结构用于格式化
        post_for_formatting = {
            "title": item.get('title', ''),
            "content_text": first_level_comment.get('body', ''),
            "author_name": first_level_comment.get('author', '[deleted]'),
            "author_handle": first_level_comment.get('author', '[deleted]'),
            "comments_tree": first_level_comment.get('replies', [])  # 只包含二级评论
        }
        
        formatted_content = format_post_content_tree(post_for_formatting)
        
        if max_chars and len(formatted_content) > max_chars:
            formatted_content = formatted_content[:max_chars] + "\n[内容已截断...]"
        
        return formatted_content
    
    def build_prompt(self, post_content: str) -> str:
        """
        构建完整的prompt
        
        Args:
            post_content: 格式化后的帖子内容
            
        Returns:
            完整的prompt
        """
        template = self.load_prompt_template()
        
        # 查找 [INPUT] 占位符并替换
        if '[INPUT]' in template:
            prompt = template.replace('[INPUT]', post_content)
        else:
            prompt = template + "\n\n" + post_content
        
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
                        print(f"  ⚠️  API返回空内容，响应: {data}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2 * (attempt + 1)
                    with self.print_lock:
                        print(f"  ⚠️  API调用失败，等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    with self.print_lock:
                        print(f"  ✗ API调用失败: {e}")
                    return None
        
        return None
    
    def parse_classifier_response(self, response_text: str) -> Optional[Dict[str, Any]]:
        """
        解析分类器响应
        
        Args:
            response_text: API返回的文本
            
        Returns:
            解析后的分类结果字典或None
        """
        if not response_text:
            return None
        
        # 检查是否是错误响应
        if "ERROR_INPUT_UNRECOGNIZABLE" in response_text:
            return None
        
        # 尝试提取JSON部分
        import re
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if not json_match:
            return None
        
        try:
            result = json.loads(json_match.group(0))
            
            # 验证必需字段
            if 'base_quality_score' not in result or 'scene' not in result or 'post_type' not in result:
                return None
            
            # 确保类型正确
            if isinstance(result.get('base_quality_score'), (int, float)):
                result['base_quality_score'] = float(result['base_quality_score'])
            else:
                return None
            
            return result
        except json.JSONDecodeError:
            return None
    
    def process_comment(self, item: Dict[str, Any], max_chars: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """
        处理单个一级评论项目
        
        Args:
            item: 一级评论项目字典
            max_chars: 最大字符数限制
            
        Returns:
            分类结果字典或None
        """
        comment_id = item.get('source_platform_id', '')
        
        # 格式化内容
        formatted_content = self.format_post_for_prompt(item, max_chars=max_chars)
        if not formatted_content:
            return None
        
        # 构建prompt
        prompt = self.build_prompt(formatted_content)
        
        # 调用API
        response_text = self.call_deepseek_api(prompt)
        if not response_text:
            return None
        
        # 解析响应
        result = self.parse_classifier_response(response_text)
        return result
    
    def classify_task(self, task_id: str, max_chars: Optional[int] = None, num_threads: int = 16):
        """
        分类任务的主函数
        
        Args:
            task_id: 任务ID
            max_chars: 最大字符数限制
            num_threads: 并发线程数
        """
        print(f"\n开始分类任务: {task_id}")
        print("=" * 80)
        
        # 加载数据
        try:
            filtered_data = self.load_filtered_data(task_id)
            print(f"✓ 加载了 {len(filtered_data)} 个一级评论项目")
        except FileNotFoundError as e:
            print(f"✗ 错误: {e}")
            return
        
        if not filtered_data:
            print("数据为空")
            return
        
        # 多线程处理
        print(f"\n使用 {num_threads} 个线程并行分类...")
        classifier_results = {}
        success_count = 0
        fail_count = 0
        
        def process_single_comment(item: Dict[str, Any]) -> tuple[str, Optional[Dict[str, Any]]]:
            """处理单个一级评论"""
            comment_id = item.get('source_platform_id', '')
            result = self.process_comment(item, max_chars=max_chars)
            return comment_id, result
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_item = {
                executor.submit(process_single_comment, item): item
                for item in filtered_data
            }
            
            completed_count = 0
            for future in as_completed(future_to_item):
                item = future_to_item[future]
                comment_id = item.get('source_platform_id', '')
                title = item.get('title', '')[:50]
                
                try:
                    result_comment_id, result = future.result()
                    completed_count += 1
                    
                    with self.print_lock:
                        print(f"[{completed_count}/{len(filtered_data)}] Comment: {comment_id}")
                        print(f"  标题: {title}...")
                    
                    if result:
                        classifier_results[result_comment_id] = result
                        success_count += 1
                        with self.print_lock:
                            print(f"  结果: 成功 - scene={result.get('scene')}, type={result.get('post_type')}, score={result.get('base_quality_score')}")
                    else:
                        fail_count += 1
                        with self.print_lock:
                            print(f"  结果: 失败")
                            
                except Exception as e:
                    completed_count += 1
                    fail_count += 1
                    with self.print_lock:
                        print(f"[{completed_count}/{len(filtered_data)}] Comment: {comment_id}")
                        print(f"  错误: {e}")
        
        print(f"\n分类完成:")
        print(f"  - 成功: {success_count} 条")
        print(f"  - 失败: {fail_count} 条")
        
        # 保存分类结果
        print(f"\n保存分类结果...")
        self.save_classifier_output(task_id, classifier_results)
        
        print(f"\n任务 {task_id} 分类完成！")
    
    def save_classifier_output(self, task_id: str, classifier_results: Dict[str, Dict[str, Any]]):
        """
        保存分类结果
        
        Args:
            task_id: 任务ID
            classifier_results: 分类结果字典 {comment_id: result}
        """
        filename = f"{task_id}_classifier.json"
        filepath = os.path.join(self.classifier_output_dir, filename)
        
        # 转换为列表格式
        output_list = []
        for comment_id, result in classifier_results.items():
            output_item = {
                "id": comment_id,
                **result
            }
            output_list.append(output_item)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_list, f, ensure_ascii=False, indent=2)
        
        print(f"✓ 分类结果已保存到: {filepath}")


def main():
    """主函数"""
    # 加载环境变量
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description='对一级评论进行分类（适配新数据结构）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基本用法
  python comment_classifier.py --task-id task001
  
  # 指定线程数和字符限制
  python comment_classifier.py --task-id task001 --threads 16 --max-chars 50000
        """
    )
    
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    parser.add_argument('--threads', type=int, default=16,
                       help='并发线程数（默认16）')
    parser.add_argument('--max-chars', '-c', type=int, default=None,
                       help='最大字符数限制（可选）')
    
    args = parser.parse_args()
    
    # 获取API密钥
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        print("错误: 未找到 DEEPSEEK_API_KEY 环境变量")
        print("请在 .env 文件中配置 DEEPSEEK_API_KEY")
        return
    
    try:
        classifier = CommentClassifier(api_key)
        classifier.classify_task(args.task_id, max_chars=args.max_chars, num_threads=args.threads)
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


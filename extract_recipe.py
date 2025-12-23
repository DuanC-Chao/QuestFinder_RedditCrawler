#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recipe提取脚本
功能：从ready_for_DB提取Post内容，发送给DeepSeek API提取Recipe
"""

import os
import json
import argparse
import re
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from parse_content_tree import ContentTreeParser
from format_content_tree import format_post_content_tree


class RecipeExtractor:
    """Recipe提取器"""
    
    def __init__(self, api_key: str):
        """
        初始化提取器
        
        Args:
            api_key: DeepSeek API密钥
        """
        self.api_key = api_key
        self.api_url = "https://api.deepseek.com/v1/chat/completions"
        self.data_dir = "Data"
        self.ready_dir = os.path.join(self.data_dir, "ready_for_DB")
        self.recipe_dir = os.path.join(self.data_dir, "recipe")
        self.prompt_template_path = "to_recipe_prompt.txt"
        self.parser = ContentTreeParser()
        self.print_lock = Lock()
        
        # 确保recipe目录存在
        os.makedirs(self.recipe_dir, exist_ok=True)
    
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
    
    def count_comments(self, comments: List[Dict[str, Any]]) -> int:
        """
        递归计算评论总数（包括子评论）
        
        Args:
            comments: 评论列表
            
        Returns:
            评论总数
        """
        count = len(comments)
        for comment in comments:
            replies = comment.get('replies', [])
            if replies:
                count += self.count_comments(replies)
        return count
    
    def format_comment_tree(self, comment: Dict[str, Any], depth: int = 0) -> str:
        """
        格式化单个评论及其所有子评论为字符串
        
        Args:
            comment: 评论字典
            depth: 当前深度（用于缩进）
            
        Returns:
            格式化后的字符串
        """
        indent = "  " * depth
        parts = []
        
        # 评论头部信息
        author_id = comment.get('author_id', '[deleted]')
        is_submitter = comment.get('is_submitter', False)
        score = comment.get('score', 0)
        created_utc = comment.get('created_utc', '')
        body = comment.get('body', '[deleted]')
        
        submitter_mark = " [发帖者]" if is_submitter else ""
        
        parts.append(f"{indent}评论:")
        parts.append(f"{indent}  作者: {author_id}{submitter_mark}")
        if score:
            parts.append(f"{indent}  点赞: {score}")
        if created_utc:
            parts.append(f"{indent}  时间: {created_utc}")
        parts.append(f"{indent}  内容: {body}")
        
        # 递归处理子评论
        replies = comment.get('replies', [])
        if replies:
            parts.append(f"{indent}  回复 ({len(replies)} 条):")
            for reply in replies:
                parts.append(self.format_comment_tree(reply, depth + 1))
        
        return "\n".join(parts)
    
    def extract_content_for_prompt(self, post: Dict[str, Any]) -> List[str]:
        """
        根据评论数提取内容用于prompt
        
        Args:
            post: Post数据字典
            
        Returns:
            内容字符串列表。如果评论数<10，返回一个元素；如果>=10，每个一级评论返回一个元素
        """
        # 解析content_text
        content_text = post.get('content_text', '')
        if not content_text:
            return []
        
        try:
            parsed = self.parser.parse(content_text)
        except Exception as e:
            with self.print_lock:
                print(f"  ⚠️  解析content_text失败: {e}")
            return [content_text]  # 如果解析失败，返回原始内容
        
        # 获取评论数
        comments = parsed.get('comments', [])
        comment_count = self.count_comments(comments)
        
        # 基础信息（标题、作者、内容）
        base_parts = []
        title = parsed.get('title', '')
        if title:
            base_parts.append(f"标题: {title}")
        
        author = parsed.get('author', {})
        author_name = author.get('name', '')
        if author_name:
            base_parts.append(f"发帖者: {author_name}")
        
        content = parsed.get('content', '')
        if content and content.strip():
            base_parts.append(f"内容:\n{content}")
        
        base_info = "\n".join(base_parts)
        
        # 根据评论数决定处理方式
        if comment_count < 10:
            # 评论数<10：整个Post作为一个内容块
            parts = base_parts.copy()
            if comments:
                parts.append(f"\n评论 ({comment_count} 条):")
                for i, comment in enumerate(comments, 1):
                    parts.append(f"\n--- 评论 {i} ---")
                    parts.append(self.format_comment_tree(comment, depth=0))
            return ["\n\n".join(parts)]
        else:
            # 评论数>=10：每个一级评论单独作为一个内容块
            result_list = []
            if comments:
                for i, comment in enumerate(comments, 1):
                    # 每个一级评论单独构建一个内容块
                    parts = base_parts.copy()
                    parts.append(f"\n一级评论 {i}/{len(comments)}:")
                    parts.append(self.format_comment_tree(comment, depth=0))
                    result_list.append("\n\n".join(parts))
            else:
                # 如果没有评论，只返回基础信息
                result_list.append(base_info)
            
            return result_list
    
    def build_prompt(self, content: str) -> str:
        """
        构建完整的prompt
        
        Args:
            content: 提取的内容字符串
            
        Returns:
            完整的prompt
        """
        template = self.load_prompt_template()
        
        # 提取RECIPE_EXTRACTION_PROMPT部分
        # 模板格式可能是：
        # 1. RECIPE_EXTRACTION_PROMPT = '''...''' 然后是 [INPUT]
        # 2. 直接包含 [INPUT] 占位符
        
        # 如果包含RECIPE_EXTRACTION_PROMPT定义，提取其中的内容
        if 'RECIPE_EXTRACTION_PROMPT' in template:
            # 提取三引号之间的内容
            match = re.search(r"RECIPE_EXTRACTION_PROMPT\s*=\s*'''(.+?)'''", template, re.DOTALL)
            if match:
                prompt_content = match.group(1).strip()
            else:
                # 尝试单引号
                match = re.search(r"RECIPE_EXTRACTION_PROMPT\s*=\s*\"\"\"(.+?)\"\"\"", template, re.DOTALL)
                if match:
                    prompt_content = match.group(1).strip()
                else:
                    prompt_content = template
        else:
            prompt_content = template
        
        # 查找 [INPUT] 占位符并替换
        if '[INPUT]' in prompt_content:
            prompt = prompt_content.replace('[INPUT]', content)
        else:
            # 如果没有找到占位符，在末尾添加内容
            prompt = prompt_content + "\n\n" + content
        
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
                response = requests.post(self.api_url, json=payload, headers=headers, timeout=120)
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
    
    def parse_api_response(self, response_text: str) -> Optional[Dict[str, Any]]:
        """
        解析API响应，提取JSON
        
        Args:
            response_text: API返回的文本
            
        Returns:
            解析后的JSON字典或None
        """
        if not response_text:
            return None
        
        # 尝试提取JSON部分
        # 可能包含在代码块中 ```json ... ``` 或直接是JSON
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # 尝试直接查找JSON对象
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
            else:
                json_str = response_text
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            with self.print_lock:
                print(f"  ⚠️  JSON解析失败: {e}")
                print(f"  响应文本前500字符: {response_text[:500]}")
            return None
    
    def process_post(self, post: Dict[str, Any], index: int, total: int) -> List[Dict[str, Any]]:
        """
        处理单个Post
        
        Args:
            post: Post数据字典
            index: 当前索引（从1开始）
            total: 总数
            
        Returns:
            处理结果字典列表（可能包含多个结果，如果评论数>=10）
        """
        post_id = post.get('source_platform_id', post.get('id', ''))
        comments_count = post.get('comments_count', 0)
        
        with self.print_lock:
            print(f"\n处理 Post {index}/{total} (ID: {post_id}, 评论数: {comments_count})")
        
        # 提取内容（可能返回多个内容块）
        content_list = self.extract_content_for_prompt(post)
        if not content_list:
            with self.print_lock:
                print(f"  ⚠️  内容为空，跳过")
            return []
        
        results = []
        
        # 对于每个内容块，单独调用API
        for chunk_idx, content in enumerate(content_list):
            # 如果是多个内容块，显示进度
            if len(content_list) > 1:
                with self.print_lock:
                    print(f"  → 处理内容块 {chunk_idx + 1}/{len(content_list)}...")
            
            # 构建prompt
            prompt = self.build_prompt(content)
            
            # 调用API
            with self.print_lock:
                if len(content_list) == 1:
                    print(f"  → 发送到DeepSeek API...")
                else:
                    print(f"    → 发送到DeepSeek API...")
            
            response_text = self.call_deepseek_api(prompt)
            if not response_text:
                with self.print_lock:
                    print(f"  ✗ API调用失败")
                continue
            
            # 解析响应
            parsed_response = self.parse_api_response(response_text)
            if not parsed_response:
                with self.print_lock:
                    print(f"  ✗ 响应解析失败")
                continue
            
            # 构建结果（所有字段平级，不嵌套）
            result = {
                "post_id": post_id,
                "source_url": post.get('source_url', ''),
                "post_title": post.get('title', ''),  # 重命名为post_title避免与recipe.title冲突
                "comments_count": comments_count,
                "chunk_index": chunk_idx + 1 if len(content_list) > 1 else None,  # 标记是第几个内容块
                "total_chunks": len(content_list) if len(content_list) > 1 else None,
                "raw_response": response_text  # 保留原始响应用于调试
            }
            
            # 将api_response中的所有字段展开到顶层
            if isinstance(parsed_response, dict):
                for key, value in parsed_response.items():
                    if key == "recipe" and isinstance(value, dict):
                        # recipe字段也展开到顶层
                        for recipe_key, recipe_value in value.items():
                            result[recipe_key] = recipe_value
                    else:
                        result[key] = value
            
            with self.print_lock:
                is_valid = parsed_response.get('is_valid', False)
                status = "✓ 有效" if is_valid else "✗ 无效"
                if len(content_list) > 1:
                    print(f"    {status} (内容块 {chunk_idx + 1}/{len(content_list)})")
                else:
                    print(f"  {status}")
            
            results.append(result)
        
        return results
    
    def extract_recipes(self, task_id: str, max_threads: int = 16) -> List[Dict[str, Any]]:
        """
        提取所有Post的Recipe
        
        Args:
            task_id: 任务ID
            max_threads: 最大并发线程数
            
        Returns:
            结果列表
        """
        print(f"\n开始提取Recipe: {task_id}")
        print("=" * 80)
        
        # 加载数据
        try:
            ready_data = self.load_ready_data(task_id)
            print(f"✓ 加载了 {len(ready_data)} 条记录")
        except FileNotFoundError as e:
            print(f"✗ 错误: {e}")
            return []
        
        if not ready_data:
            print("数据为空")
            return []
        
        # 多线程处理
        results = []
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {
                executor.submit(self.process_post, post, i + 1, len(ready_data)): (i, post)
                for i, post in enumerate(ready_data)
            }
            
            for future in as_completed(futures):
                try:
                    post_results = future.result()  # 可能返回多个结果
                    if post_results:
                        results.extend(post_results)  # 扩展列表而不是追加
                except Exception as e:
                    i, post = futures[future]
                    with self.print_lock:
                        print(f"\n✗ Post {i+1} 处理异常: {e}")
        
        print(f"\n完成！共处理 {len(results)} 条记录")
        return results
    
    def save_results(self, task_id: str, results: List[Dict[str, Any]]):
        """
        保存结果到文件
        
        Args:
            task_id: 任务ID
            results: 结果列表
        """
        filename = f"{task_id}_recipe.json"
        filepath = os.path.join(self.recipe_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ 结果已保存到: {filepath}")
        
        # 打印统计信息
        valid_count = sum(1 for r in results if r.get('is_valid', False))
        print(f"  有效Recipe: {valid_count}/{len(results)}")


def main():
    """主函数"""
    # 加载环境变量
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description='从ready_for_DB提取Recipe',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基本用法
  python extract_recipe.py --task-id task001
  
  # 指定线程数
  python extract_recipe.py --task-id task001 --threads 16
        """
    )
    
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    parser.add_argument('--threads', type=int, default=16,
                       help='并发线程数（默认16）')
    
    args = parser.parse_args()
    
    # 获取API密钥
    api_key = os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        print("错误: 未找到 DEEPSEEK_API_KEY 环境变量")
        print("请在 .env 文件中配置 DEEPSEEK_API_KEY")
        return
    
    try:
        extractor = RecipeExtractor(api_key)
        results = extractor.extract_recipes(args.task_id, max_threads=args.threads)
        
        if results:
            extractor.save_results(args.task_id, results)
        else:
            print("\n没有提取到任何结果")
            
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


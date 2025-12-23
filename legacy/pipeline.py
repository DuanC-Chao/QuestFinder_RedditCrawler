#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reddit数据爬取和处理管线脚本
功能：一键完成爬取、过滤、分类三个步骤
如果中途任何阶段失败，将清理所有已创建的文件并退出
"""

import os
import sys
import json
import argparse
import shutil
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# 导入三个模块的类
from reddit_html_crawler import RedditHTMLCrawler
from post_filter import PostFilter
from post_filter_rule_based import PostFilterRuleBased
from post_classifier import PostClassifier


class Pipeline:
    """数据管线类"""
    
    def __init__(self, task_id: str):
        """
        初始化管线
        
        Args:
            task_id: 任务ID
        """
        self.task_id = task_id
        self.data_dir = "Data"
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.mask_dir = os.path.join(self.data_dir, "mask")
        self.classifier_output_dir = os.path.join(self.data_dir, "classifier_output")
        self.ready_dir = os.path.join(self.data_dir, "ready_for_DB")
        
        # 记录已创建的文件，用于失败时清理
        self.created_files = []
        
        # 加载环境变量
        load_dotenv()
    
    def _mark_file_created(self, filepath: str):
        """标记文件已创建"""
        if os.path.exists(filepath):
            self.created_files.append(filepath)
    
    def _cleanup_on_failure(self):
        """失败时清理所有已创建的文件"""
        if not self.created_files:
            return
        
        print("\n" + "=" * 80)
        print("管线失败，开始清理已创建的文件...")
        print("=" * 80)
        
        for filepath in self.created_files:
            try:
                if os.path.exists(filepath):
                    if os.path.isfile(filepath):
                        os.remove(filepath)
                        print(f"  ✓ 已删除文件: {filepath}")
                    elif os.path.isdir(filepath):
                        shutil.rmtree(filepath)
                        print(f"  ✓ 已删除目录: {filepath}")
            except Exception as e:
                print(f"  ✗ 删除失败 {filepath}: {e}")
        
        print("\n清理完成")
    
    def step1_crawl(self, query_seeds_file: str = 'to_craw_query_seeds.txt',
                    keywords_file: str = 'filter_keywords.txt',
                    delay: float = 3.0,
                    max_posts: Optional[int] = None,
                    threads: int = 8,
                    user_agent: Optional[str] = None) -> bool:
        """
        步骤1: 爬取Reddit数据
        
        Returns:
            是否成功
        """
        print("\n" + "=" * 80)
        print("步骤 1/3: 爬取Reddit数据")
        print("=" * 80)
        
        try:
            # 创建爬虫实例
            crawler = RedditHTMLCrawler(user_agent=user_agent, delay=delay)
            
            # 检查任务ID是否已存在
            if crawler.check_task_id_exists(self.task_id):
                raise ValueError(f"任务ID '{self.task_id}' 已存在，不允许使用同名ID")
            
            # 加载搜索关键词和过滤关键词
            query_seeds = crawler.load_query_seeds_from_file(query_seeds_file)
            filter_keywords = crawler.load_keywords_from_file(keywords_file)
            
            if not query_seeds:
                raise ValueError("没有找到要爬取的搜索关键词")
            
            print(f"\n任务ID: {self.task_id}")
            if max_posts:
                print(f"最大爬取数量: {max_posts} 个帖子")
                print(f"  - 将平均分配给 {len(query_seeds)} 个搜索关键词")
            else:
                print("爬取数量: 不限制（将翻页爬取所有匹配的帖子）")
            print(f"并发线程数: {threads}")
            
            if filter_keywords:
                print(f"标题过滤关键词: {filter_keywords}")
            else:
                print("未设置标题过滤关键词，将爬取所有帖子")
            
            # 批量爬取
            data = crawler.crawl_batch(query_seeds, filter_keywords, max_posts=max_posts, num_threads=threads)
            
            if not data:
                raise ValueError("未找到匹配的帖子")
            
            # 保存数据
            crawler.save_to_json(data, self.task_id)
            
            # 标记文件已创建
            raw_file = os.path.join(self.raw_dir, f"{self.task_id}.json")
            self._mark_file_created(raw_file)
            
            # 统计信息
            def count_comments(comments_tree):
                if not comments_tree:
                    return 0
                count = len(comments_tree)
                for comment in comments_tree:
                    if isinstance(comment, dict) and 'replies' in comment:
                        count += count_comments(comment.get('replies', []))
                return count
            
            total_comments = sum(count_comments(post.get('comments_tree', [])) for post in data)
            print(f"\n统计信息:")
            print(f"  - 帖子数: {len(data)}")
            print(f"  - 总评论数: {total_comments}")
            
            print("\n✓ 步骤1完成")
            return True
            
        except Exception as e:
            print(f"\n✗ 步骤1失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def step2_filter(self, threads: int = 16, use_rule_based: bool = False, keywords_file: str = 'manual_filter_keywords.json') -> bool:
        """
        步骤2: 过滤帖子
        
        Args:
            threads: 并发线程数
            use_rule_based: 是否使用基于规则的过滤（默认False，使用LLM）
            keywords_file: 关键词配置文件路径（仅在使用rule_based时有效）
            
        Returns:
            是否成功
        """
        print("\n" + "=" * 80)
        print("步骤 2/3: 过滤帖子")
        print("=" * 80)
        
        try:
            if use_rule_based:
                print("使用基于规则的过滤模式")
                # 创建基于规则的过滤器实例
                post_filter = PostFilterRuleBased(keywords_file=keywords_file)
            else:
                print("使用LLM驱动的过滤模式")
                # 获取API密钥
                api_key = os.getenv('DEEPSEEK_API_KEY')
                if not api_key:
                    raise ValueError("未找到DEEPSEEK_API_KEY环境变量，请在.env文件中配置")
                
                # 创建LLM过滤器实例
                post_filter = PostFilter(api_key)
            
            # 处理任务
            post_filter.filter_task(self.task_id, num_threads=threads)
            
            # 标记文件已创建
            mask_file = os.path.join(self.mask_dir, f"{self.task_id}_mask.json")
            self._mark_file_created(mask_file)
            
            print("\n✓ 步骤2完成")
            return True
            
        except Exception as e:
            print(f"\n✗ 步骤2失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def step3_classify(self, max_chars: Optional[int] = None, threads: int = 16) -> bool:
        """
        步骤3: 分类帖子
        
        Args:
            max_chars: 最大字符数限制
            threads: 并发线程数
            
        Returns:
            是否成功
        """
        print("\n" + "=" * 80)
        print("步骤 3/3: 分类帖子")
        print("=" * 80)
        
        try:
            # 获取API密钥
            api_key = os.getenv('DEEPSEEK_API_KEY')
            if not api_key:
                raise ValueError("未找到DEEPSEEK_API_KEY环境变量，请在.env文件中配置")
            
            # 创建分类器实例
            classifier = PostClassifier(api_key)
            
            # 处理任务
            classifier.classify_task(self.task_id, max_chars=max_chars, num_threads=threads)
            
            # 标记文件已创建
            classifier_file = os.path.join(self.classifier_output_dir, f"{self.task_id}_classifier.json")
            ready_file = os.path.join(self.ready_dir, f"{self.task_id}_ready.json")
            self._mark_file_created(classifier_file)
            self._mark_file_created(ready_file)
            
            print("\n✓ 步骤3完成")
            return True
            
        except Exception as e:
            print(f"\n✗ 步骤3失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def run(self, query_seeds_file: str = 'to_craw_query_seeds.txt',
            keywords_file: str = 'filter_keywords.txt',
            delay: float = 3.0,
            max_posts: Optional[int] = None,
            crawl_threads: int = 8,
            filter_threads: int = 16,
            classify_threads: int = 16,
            max_chars: Optional[int] = None,
            user_agent: Optional[str] = None,
            use_rule_based_filter: bool = False,
            filter_keywords_file: str = 'manual_filter_keywords.json') -> bool:
        """
        运行完整管线
        
        Args:
            query_seeds_file: 搜索关键词文件路径
            keywords_file: 过滤关键词文件路径
            delay: 请求延迟（秒）
            max_posts: 最大爬取帖子数量
            crawl_threads: 爬取阶段的并发线程数
            filter_threads: 过滤阶段的并发线程数
            classify_threads: 分类阶段的并发线程数
            max_chars: 分类时的最大字符数限制
            user_agent: 自定义User-Agent
            use_rule_based_filter: 是否使用基于规则的过滤（默认False，使用LLM）
            filter_keywords_file: 规则过滤的关键词配置文件路径
            
        Returns:
            是否成功
        """
        print("\n" + "=" * 80)
        print(f"开始运行管线: {self.task_id}")
        print("=" * 80)
        print("\n注意: 如果中途任何阶段失败，将自动清理所有已创建的文件")
        
        if use_rule_based_filter:
            print(f"\n过滤模式: 基于规则（关键词文件: {filter_keywords_file}）")
        else:
            print(f"\n过滤模式: LLM驱动")
        
        # 步骤1: 爬取
        if not self.step1_crawl(query_seeds_file, keywords_file, delay, max_posts, crawl_threads, user_agent):
            self._cleanup_on_failure()
            return False
        
        # 步骤2: 过滤
        if not self.step2_filter(filter_threads, use_rule_based_filter, filter_keywords_file):
            self._cleanup_on_failure()
            return False
        
        # 步骤3: 分类
        if not self.step3_classify(max_chars, classify_threads):
            self._cleanup_on_failure()
            return False
        
        print("\n" + "=" * 80)
        print("管线执行成功！")
        print("=" * 80)
        print(f"\n生成的文件:")
        print(f"  - Raw数据: {os.path.join(self.raw_dir, f'{self.task_id}.json')}")
        print(f"  - Mask文件: {os.path.join(self.mask_dir, f'{self.task_id}_mask.json')}")
        print(f"  - 分类结果: {os.path.join(self.classifier_output_dir, f'{self.task_id}_classifier.json')}")
        print(f"  - 数据库就绪数据: {os.path.join(self.ready_dir, f'{self.task_id}_ready.json')}")
        print("\n可以使用以下命令导入到Supabase:")
        print(f"  python import_to_supabase.py --task-id {self.task_id}")
        
        return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Reddit数据爬取和处理管线脚本（一键完成爬取、过滤、分类）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基本用法（使用默认参数，LLM驱动过滤）
  python pipeline.py --task-id task001
  
  # 使用基于规则的过滤（不需要API密钥）
  python pipeline.py --task-id task001 --use-rule-based-filter
  
  # 指定爬取数量
  python pipeline.py --task-id task001 --max-posts 100
  
  # 自定义所有参数（使用规则过滤）
  python pipeline.py --task-id task001 \\
    --query-seeds-file my_seeds.txt \\
    --keywords-file my_keywords.txt \\
    --max-posts 200 \\
    --delay 5.0 \\
    --crawl-threads 8 \\
    --filter-threads 16 \\
    --classify-threads 16 \\
    --max-chars 50000 \\
    --use-rule-based-filter \\
    --filter-keywords-file manual_filter_keywords.json

注意:
  - 如果中途任何阶段失败，将自动清理所有已创建的文件
  - LLM驱动过滤需要配置 .env 文件中的 DEEPSEEK_API_KEY
  - 基于规则过滤不需要API密钥，使用关键词匹配
  - 建议使用较大的 delay 值（3-5秒）以避免Reddit限流
        """
    )
    
    # 必需参数
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需，用于命名输出文件）')
    
    # 文件路径参数
    parser.add_argument('--query-seeds-file', '-q', default='to_craw_query_seeds.txt',
                       help='搜索关键词列表文件路径（默认: to_craw_query_seeds.txt）')
    parser.add_argument('--keywords-file', '-k', default='filter_keywords.txt',
                       help='标题过滤关键词列表文件路径（默认: filter_keywords.txt）')
    
    # 爬取参数
    parser.add_argument('--delay', '-d', type=float, default=3.0,
                       help='请求之间的延迟（秒，默认3.0，建议3-5秒以避免限流）')
    parser.add_argument('--max-posts', '-m', type=int, default=None,
                       help='最大爬取帖子数量（默认不限制）')
    parser.add_argument('--crawl-threads', type=int, default=8,
                       help='爬取阶段的并发线程数（默认8）')
    
    # 过滤参数
    parser.add_argument('--filter-threads', type=int, default=16,
                       help='过滤阶段的并发线程数（默认16）')
    
    # 分类参数
    parser.add_argument('--classify-threads', type=int, default=16,
                       help='分类阶段的并发线程数（默认16）')
    parser.add_argument('--max-chars', '-c', type=int, default=None,
                       help='分类时的最大字符数限制（可选）')
    
    # 过滤模式参数
    parser.add_argument('--use-rule-based-filter', action='store_true',
                       help='使用基于规则的过滤（默认使用LLM驱动过滤）')
    parser.add_argument('--filter-keywords-file', default='manual_filter_keywords.json',
                       help='规则过滤的关键词配置文件路径（默认: manual_filter_keywords.json）')
    
    # 其他参数
    parser.add_argument('--user-agent', help='自定义User-Agent')
    
    args = parser.parse_args()
    
    # 创建管线实例
    pipeline = Pipeline(args.task_id)
    
    # 运行管线
    success = pipeline.run(
        query_seeds_file=args.query_seeds_file,
        keywords_file=args.keywords_file,
        delay=args.delay,
        max_posts=args.max_posts,
        crawl_threads=args.crawl_threads,
        filter_threads=args.filter_threads,
        classify_threads=args.classify_threads,
        max_chars=args.max_chars,
        user_agent=args.user_agent,
        use_rule_based_filter=args.use_rule_based_filter,
        filter_keywords_file=args.filter_keywords_file
    )
    
    if success:
        sys.exit(0)
    else:
        print("\n管线执行失败，已清理所有临时文件")
        sys.exit(1)


if __name__ == "__main__":
    main()


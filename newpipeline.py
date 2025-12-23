#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新数据管线脚本
功能：整合爬虫、评论过滤、数据库准备三个步骤
流程：reddit_html_crawler -> comment_filter -> prepare_for_db
"""

import os
import argparse
from typing import Optional
from reddit_html_crawler import RedditHTMLCrawler
from comment_filter import CommentFilter
from comment_classifier import CommentClassifier
from prepare_for_db import DBDataPreparer
from dotenv import load_dotenv
import os


class NewPipeline:
    """新数据管线类"""
    
    def __init__(self, task_id: str):
        """
        初始化管线
        
        Args:
            task_id: 任务ID
        """
        self.task_id = task_id
        self.data_dir = "Data"
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.filtered_dir = os.path.join(self.data_dir, "comment_filtered_raw")
        self.posts_dir = os.path.join(self.data_dir, "ready_for_DB_posts")
        self.comments_dir = os.path.join(self.data_dir, "ready_for_DB_comments")
        self.classifier_output_dir = os.path.join(self.data_dir, "classifier_output")
        self.created_files = []  # 记录已创建的文件，用于失败时清理
        
        # 加载环境变量
        load_dotenv()
    
    def _mark_file_created(self, filepath: str):
        """标记文件已创建"""
        if filepath not in self.created_files:
            self.created_files.append(filepath)
    
    def _cleanup_on_failure(self):
        """失败时清理所有已创建的文件"""
        print("\n" + "=" * 80)
        print("管线执行失败，开始清理已创建的文件...")
        
        for filepath in self.created_files:
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
                    print(f"  ✓ 已删除: {filepath}")
            except Exception as e:
                print(f"  ✗ 删除失败 {filepath}: {e}")
        
        print("清理完成")
        print("=" * 80)
    
    def step1_crawl(self, query_seeds_file: str = 'to_craw_query_seeds.txt',
                    keywords_file: str = 'filter_keywords.txt',
                    delay: float = 0.5,
                    max_first_level_comments: Optional[int] = None,
                    threads: int = 8,
                    user_agent: Optional[str] = None) -> bool:
        """
        步骤1：爬取数据
        
        Args:
            query_seeds_file: 搜索关键词文件路径
            keywords_file: 标题过滤关键词文件路径
            delay: 请求延迟（秒）
            max_posts: 最大爬取数量
            threads: 并发线程数
            user_agent: 自定义User-Agent
            
        Returns:
            是否成功
        """
        try:
            print("\n" + "=" * 80)
            print("步骤1: 爬取Reddit数据")
            print("=" * 80)
            
            # 从环境变量获取PRAW配置
            crawler = RedditHTMLCrawler(delay=delay)
            
            # 加载搜索关键词
            query_seeds = crawler.load_query_seeds_from_file(query_seeds_file)
            if not query_seeds:
                raise ValueError(f"未找到搜索关键词文件或文件为空: {query_seeds_file}")
            
            # 加载标题过滤关键词
            filter_keywords = crawler.load_keywords_from_file(keywords_file)
            
            if filter_keywords:
                print(f"标题过滤关键词: {filter_keywords}")
            else:
                print("未设置标题过滤关键词，将爬取所有帖子")
            
            print(f"\n任务ID: {self.task_id}")
            if max_first_level_comments:
                print(f"最大一级评论数量: {max_first_level_comments}")
                print(f"  - 将完全均分给 {len(query_seeds)} 个搜索关键词")
            else:
                print("爬取数量: 不限制（将翻页爬取所有匹配的帖子）")
            print(f"并发线程数: {threads}")
            
            # 批量爬取
            data = crawler.crawl_batch(query_seeds, filter_keywords, max_first_level_comments=max_first_level_comments, num_threads=threads)
            
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
            first_level_comments = sum(len(post.get('comments_tree', [])) for post in data)
            print(f"\n统计信息:")
            print(f"  - 原始帖子数: {len(data)}")
            print(f"  - 第一层评论数: {first_level_comments}")
            print(f"  - 总评论数（包括子评论）: {total_comments}")
            print(f"\n注意: 实际保存时，每个第一层评论会作为独立项目保存")
            
            print("\n✓ 步骤1完成")
            return True
            
        except Exception as e:
            print(f"\n✗ 步骤1失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def step2_filter(self, max_second_level: int = 5) -> bool:
        """
        步骤2：过滤评论
        
        Args:
            max_second_level: 最多保留的二级评论数
            
        Returns:
            是否成功
        """
        try:
            print("\n" + "=" * 80)
            print("步骤2: 过滤评论（保留upvote最多的二级评论）")
            print("=" * 80)
            
            filter = CommentFilter()
            filtered_data = filter.filter_task(self.task_id, max_second_level=max_second_level)
            
            if not filtered_data:
                raise ValueError("过滤后数据为空")
            
            # 保存过滤后的数据
            filter.save_filtered_data(self.task_id, filtered_data)
            
            # 标记文件已创建
            filtered_file = os.path.join(self.filtered_dir, f"{self.task_id}.json")
            self._mark_file_created(filtered_file)
            
            print("\n✓ 步骤2完成")
            return True
            
        except Exception as e:
            print(f"\n✗ 步骤2失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def step3_classify(self, max_chars: Optional[int] = None, num_threads: int = 16) -> bool:
        """
        步骤3：分类一级评论
        
        Args:
            max_chars: 最大字符数限制
            num_threads: 并发线程数
            
        Returns:
            是否成功
        """
        try:
            print("\n" + "=" * 80)
            print("步骤3: 分类一级评论")
            print("=" * 80)
            
            # 获取API密钥
            api_key = os.getenv('DEEPSEEK_API_KEY')
            if not api_key:
                raise ValueError("未找到 DEEPSEEK_API_KEY 环境变量，请在 .env 文件中配置")
            
            classifier = CommentClassifier(api_key)
            classifier.classify_task(self.task_id, max_chars=max_chars, num_threads=num_threads)
            
            # 标记文件已创建
            classifier_file = os.path.join(self.classifier_output_dir, f"{self.task_id}_classifier.json")
            self._mark_file_created(classifier_file)
            
            print("\n✓ 步骤3完成")
            return True
            
        except Exception as e:
            print(f"\n✗ 步骤3失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def step4_prepare_db(self, use_classifier: bool = True) -> bool:
        """
        步骤3：准备数据库数据
        
        Returns:
            是否成功
        """
        try:
            print("\n" + "=" * 80)
            print("步骤3: 准备数据库入库数据")
            print("=" * 80)
            
            preparer = DBDataPreparer()
            posts, comments = preparer.prepare_task_data(self.task_id, use_classifier=use_classifier)
            
            if not posts and not comments:
                raise ValueError("没有数据需要保存")
            
            # 保存posts数据
            if posts:
                preparer.save_posts(self.task_id, posts)
                posts_file = os.path.join(self.posts_dir, f"{self.task_id}_posts.json")
                self._mark_file_created(posts_file)
            
            # 保存comments数据
            if comments:
                preparer.save_comments(self.task_id, comments)
                comments_file = os.path.join(self.comments_dir, f"{self.task_id}_comments.json")
                self._mark_file_created(comments_file)
            
            print("\n✓ 步骤3完成")
            return True
            
        except Exception as e:
            print(f"\n✗ 步骤3失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def run(self, query_seeds_file: str = 'to_craw_query_seeds.txt',
            keywords_file: str = 'filter_keywords.txt',
            delay: float = 0.5,
            max_first_level_comments: Optional[int] = None,
            crawl_threads: int = 8,
            max_second_level: int = 5,
            classify_threads: int = 16,
            max_chars: Optional[int] = None,
            user_agent: Optional[str] = None) -> bool:
        """
        运行完整管线
        
        Args:
            query_seeds_file: 搜索关键词文件路径
            keywords_file: 标题过滤关键词文件路径
            delay: 请求延迟（秒）
            max_posts: 最大爬取数量
            crawl_threads: 爬取阶段的并发线程数
            max_second_level: 最多保留的二级评论数
            user_agent: 自定义User-Agent
            
        Returns:
            是否成功
        """
        print("\n" + "=" * 80)
        print(f"新数据管线开始执行 - 任务ID: {self.task_id}")
        print("=" * 80)
        print("\n流程:")
        print("  1. 爬取Reddit数据 (reddit_html_crawler)")
        print("  2. 过滤评论 (comment_filter)")
        print("  3. 分类一级评论 (comment_classifier)")
        print("  4. 准备数据库数据 (prepare_for_db)")
        print("=" * 80)
        
        # 步骤1：爬取
        if not self.step1_crawl(query_seeds_file, keywords_file, delay, max_first_level_comments, crawl_threads, user_agent):
            self._cleanup_on_failure()
            return False
        
        # 步骤2：过滤
        if not self.step2_filter(max_second_level):
            self._cleanup_on_failure()
            return False
        
        # 步骤3：分类
        if not self.step3_classify(max_chars=max_chars, num_threads=classify_threads):
            self._cleanup_on_failure()
            return False
        
        # 步骤4：准备数据库数据
        if not self.step4_prepare_db(use_classifier=True):
            self._cleanup_on_failure()
            return False
        
        print("\n" + "=" * 80)
        print("✓ 新数据管线执行成功！")
        print("=" * 80)
        print(f"\n输出文件:")
        print(f"  - Posts: Data/ready_for_DB_posts/{self.task_id}_posts.json")
        print(f"  - Comments: Data/ready_for_DB_comments/{self.task_id}_comments.json")
        print("\n可以开始导入到Supabase数据库了")
        
        return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='新数据管线：爬取 -> 过滤 -> 准备数据库数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基本用法
  python newpipeline.py --task-id task001
  
  # 指定一级评论数量
  python newpipeline.py --task-id task001 --max-first-level-comments 100
  
  # 完整参数
  python newpipeline.py --task-id task001 \\
    --max-first-level-comments 200 \\
    --delay 5.0 \\
    --crawl-threads 8 \\
    --max-second-level 5
        """
    )
    
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    
    # 文件路径参数
    parser.add_argument('--query-seeds-file', '-q', default='to_craw_query_seeds.txt',
                       help='搜索关键词列表文件路径（默认: to_craw_query_seeds.txt）')
    parser.add_argument('--keywords-file', '-k', default='filter_keywords.txt',
                       help='标题过滤关键词列表文件路径（默认: filter_keywords.txt）')
    
    # 爬取参数
    parser.add_argument('--delay', '-d', type=float, default=0.5,
                       help='请求之间的延迟（秒，默认0.5）')
    parser.add_argument('--max-first-level-comments', '-m', type=int, default=None,
                       help='最大一级评论数量（默认不限制，会完全均分给每个query seed）')
    parser.add_argument('--crawl-threads', type=int, default=8,
                       help='爬取阶段的并发线程数（默认8）')
    
    # 过滤参数
    parser.add_argument('--max-second-level', type=int, default=5,
                       help='最多保留的二级评论数（默认5）')
    
    # 分类参数
    parser.add_argument('--classify-threads', type=int, default=16,
                       help='分类阶段的并发线程数（默认16）')
    parser.add_argument('--max-chars', '-c', type=int, default=None,
                       help='分类时的最大字符数限制（可选）')
    
    # 其他参数
    parser.add_argument('--user-agent', help='自定义User-Agent')
    
    args = parser.parse_args()
    
    try:
        pipeline = NewPipeline(args.task_id)
        success = pipeline.run(
            query_seeds_file=args.query_seeds_file,
            keywords_file=args.keywords_file,
            delay=args.delay,
            max_first_level_comments=args.max_first_level_comments,
            crawl_threads=args.crawl_threads,
            max_second_level=args.max_second_level,
            classify_threads=args.classify_threads,
            max_chars=args.max_chars,
            user_agent=args.user_agent
        )
        
        if not success:
            exit(1)
            
    except KeyboardInterrupt:
        print("\n\n管线被用户中断")
        exit(1)
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()


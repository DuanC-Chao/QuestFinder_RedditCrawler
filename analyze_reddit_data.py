#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reddit数据聚类分析脚本
功能：分析Reddit爬取的数据，生成统计报告
"""

import os
import json
import argparse
from collections import Counter, defaultdict
from typing import List, Dict, Any
import re


class RedditDataAnalyzer:
    """Reddit数据分析器"""
    
    def __init__(self):
        """初始化分析器"""
        self.data_dir = "Data"
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.mask_dir = os.path.join(self.data_dir, "mask")
        
        # AI工具关键词列表（常见工具，包含变体）
        self.ai_tools_map = {
            'chatgpt': ['chatgpt', 'chat gpt', 'gpt-3', 'gpt-3.5'],
            'gpt-4': ['gpt-4', 'gpt4', 'gpt 4'],
            'claude': ['claude', 'claude ai', 'anthropic'],
            'gemini': ['gemini', 'google gemini', 'bard'],
            'deepseek': ['deepseek', 'deep seek'],
            'kimi': ['kimi', 'kimi ai'],
            'gamma': ['gamma', 'gamma.app'],
            'notebooklm': ['notebooklm', 'notebook lm', 'notebooklm ai'],
            'canva': ['canva', 'canva ai'],
            'perplexity': ['perplexity', 'perplexity ai'],
            'cursor': ['cursor', 'cursor ai'],
            'midjourney': ['midjourney', 'mid journey'],
            'copilot': ['copilot', 'github copilot', 'microsoft copilot'],
            'notion': ['notion', 'notion ai'],
            'tome': ['tome', 'tome.ai'],
            '豆包': ['豆包', 'doubao'],
            '文心': ['文心', 'wenxin', 'ernie'],
            '通义': ['通义', 'tongyi', 'qwen'],
            '夸克': ['夸克', 'quark'],
            'chatppt': ['chatppt', 'chat ppt'],
            'beautiful.ai': ['beautiful.ai', 'beautiful ai'],
            'slidesgo': ['slidesgo'],
            'genspark': ['genspark'],
            'gensmo': ['gensmo'],
            'justdone': ['justdone', 'just done'],
            'granola': ['granola'],
            'lovable': ['lovable'],
            'kilo code': ['kilo code', 'kilocode'],
            'vomo': ['vomo', 'vomo ai']
        }
        
        # 扁平化工具列表用于匹配
        self.ai_tools = []
        for tool, variants in self.ai_tools_map.items():
            self.ai_tools.extend([tool] + variants)
        
        # 使用场景关键词
        self.scenarios = {
            'ppt': ['ppt', 'powerpoint', '演示文稿', '幻灯片', 'presentation', 'slides'],
            'prompt': ['prompt', '提示词', '指令', '指令词'],
            '文档': ['文档', 'document', 'doc', 'word', 'wps'],
            '自动化': ['自动化', 'automation', '自动', 'workflow'],
            '论文': ['论文', 'paper', 'research', '学术'],
            '竞品分析': ['竞品', '竞品分析', 'competitor', 'competitive'],
            '数据分析': ['数据分析', 'data analysis', 'excel', 'spreadsheet'],
            '研报': ['研报', 'research report', '报告', 'report'],
            '年终总结': ['年终', '年终总结', 'year-end', 'summary'],
            '会议纪要': ['会议', '会议纪要', 'meeting', 'minutes'],
            '周报': ['周报', 'weekly', 'week report'],
            '简历': ['简历', 'resume', 'cv', '求职']
        }
    
    def load_task_data(self, task_id: str) -> List[Dict[str, Any]]:
        """
        加载任务数据
        
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
    
    def load_mask_data(self, task_id: str) -> Dict[str, bool]:
        """
        加载mask数据并转换为字典
        
        Args:
            task_id: 任务ID
            
        Returns:
            mask字典 {post_id: is_valid}
        """
        mask_filename = f"{task_id}_mask.json"
        mask_filepath = os.path.join(self.mask_dir, mask_filename)
        
        if not os.path.exists(mask_filepath):
            print(f"警告: Mask文件不存在: {mask_filepath}")
            print("将分析所有帖子")
            return {}
        
        with open(mask_filepath, 'r', encoding='utf-8') as f:
            mask_list = json.load(f)
        
        # 转换为字典：{post_id: is_valid}
        mask_dict = {}
        for item in mask_list:
            post_id = item.get('id', '')
            is_valid = item.get('contains_valid_ai_tool_recipe', False)
            mask_dict[post_id] = is_valid
        
        return mask_dict
    
    def extract_text_from_post(self, post: Dict[str, Any]) -> str:
        """
        从帖子中提取所有文本（标题、内容、评论）
        
        Args:
            post: 帖子数据
            
        Returns:
            合并后的文本
        """
        texts = []
        
        # 标题
        title = post.get('title', '')
        if title:
            texts.append(title)
        
        # 内容
        content = post.get('content_text', '')
        if content:
            texts.append(content)
        
        # 评论
        comments_tree = post.get('comments_tree', [])
        self._extract_comments_text(comments_tree, texts)
        
        return ' '.join(texts)
    
    def _extract_comments_text(self, comments: List[Dict], texts: List[str]):
        """
        递归提取评论文本
        
        Args:
            comments: 评论列表
            texts: 文本列表（用于收集）
        """
        for comment in comments:
            if isinstance(comment, dict):
                body = comment.get('body', '')
                if body and body not in ['[deleted]', '[removed]']:
                    texts.append(body)
                
                # 递归处理回复
                replies = comment.get('replies', [])
                if replies:
                    self._extract_comments_text(replies, texts)
    
    def extract_hashtags(self, text: str) -> List[str]:
        """
        提取Hashtag（#标签）
        
        Args:
            text: 文本内容
            
        Returns:
            Hashtag列表
        """
        # 匹配 #标签 格式
        hashtags = re.findall(r'#[\w\u4e00-\u9fff]+', text, re.IGNORECASE)
        # 转换为小写并去重
        return [tag.lower() for tag in hashtags]
    
    def extract_ai_tools(self, text: str) -> List[str]:
        """
        提取提及的AI工具
        
        Args:
            text: 文本内容
            
        Returns:
            AI工具列表（标准化后的工具名）
        """
        text_lower = text.lower()
        found_tools = []
        found_tool_set = set()
        
        # 遍历工具映射，找到匹配的工具
        for tool_name, variants in self.ai_tools_map.items():
            for variant in variants:
                if variant.lower() in text_lower and tool_name not in found_tool_set:
                    found_tools.append(tool_name)
                    found_tool_set.add(tool_name)
                    break  # 每个工具只计数一次
        
        return found_tools
    
    def extract_scenarios(self, text: str) -> List[str]:
        """
        提取使用场景
        
        Args:
            text: 文本内容
            
        Returns:
            场景列表
        """
        text_lower = text.lower()
        found_scenarios = []
        
        for scenario, keywords in self.scenarios.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    found_scenarios.append(scenario)
                    break  # 每个场景只计数一次
        
        return found_scenarios
    
    def analyze(self, task_id: str):
        """
        分析数据并生成报告（只分析mask中标记为valid的帖子）
        
        Args:
            task_id: 任务ID
        """
        print(f"正在分析任务: {task_id}")
        
        # 加载数据
        all_posts = self.load_task_data(task_id)
        print(f"加载了 {len(all_posts)} 个帖子")
        
        # 加载mask数据
        mask_dict = self.load_mask_data(task_id)
        
        # 过滤出valid的帖子
        valid_posts = []
        for post in all_posts:
            post_id = post.get('source_platform_id', post.get('id', ''))
            if mask_dict.get(post_id, False):
                valid_posts.append(post)
        
        print(f"Mask中标记为valid的帖子: {len(valid_posts)} 个")
        print(f"将只分析这 {len(valid_posts)} 个valid帖子")
        
        if not valid_posts:
            print("警告: 没有valid的帖子，无法进行分析")
            return
        
        posts = valid_posts
        
        # 统计变量
        subreddit_counter = Counter()
        hashtag_counter = Counter()
        ai_tool_counter = Counter()
        scenario_counter = Counter()
        query_seed_counter = Counter()  # 从source_url提取
        platform_tools = defaultdict(Counter)  # 各平台的工具分布
        high_engagement_posts = []  # 高互动帖子
        
        # 遍历所有帖子
        for post in posts:
            # Subreddit分布
            subreddit = post.get('subreddit', 'unknown')
            if subreddit:
                subreddit_counter[subreddit] += 1
            
            # 提取所有文本
            all_text = self.extract_text_from_post(post)
            
            # Hashtags
            hashtags = self.extract_hashtags(all_text)
            hashtag_counter.update(hashtags)
            
            # AI工具
            ai_tools = self.extract_ai_tools(all_text)
            ai_tool_counter.update(ai_tools)
            
            # 使用场景
            scenarios = self.extract_scenarios(all_text)
            scenario_counter.update(scenarios)
            
            # Query Seed分布（从source_url提取）
            source_url = post.get('source_url', '')
            if source_url:
                # 提取查询参数
                try:
                    from urllib.parse import urlparse, parse_qs, unquote_plus
                    parsed = urlparse(source_url)
                    params = parse_qs(parsed.query)
                    query = params.get('q', [''])[0]
                    if query:
                        # URL解码
                        query = unquote_plus(query)
                        query_seed_counter[query] += 1
                except:
                    pass
            
            # 各Subreddit的工具分布
            if ai_tools:
                platform_tools[subreddit].update(ai_tools)
            
            # 高互动帖子（按likes排序）
            likes = post.get('likes', 0)
            comments_count = post.get('comments', 0)
            engagement_score = likes + comments_count * 2  # 评论权重更高
            
            if engagement_score > 0:
                high_engagement_posts.append({
                    'subreddit': subreddit,
                    'title': post.get('title', '')[:60],
                    'likes': likes,
                    'comments': comments_count,
                    'score': engagement_score
                })
        
        # 生成报告
        self._generate_report(
            subreddit_counter,
            hashtag_counter,
            ai_tool_counter,
            scenario_counter,
            query_seed_counter,
            platform_tools,
            high_engagement_posts
        )
    
    def _generate_report(
        self,
        subreddit_counter: Counter,
        hashtag_counter: Counter,
        ai_tool_counter: Counter,
        scenario_counter: Counter,
        query_seed_counter: Counter,
        platform_tools: Dict[str, Counter],
        high_engagement_posts: List[Dict]
    ):
        """生成报告"""
        
        print("\n" + "=" * 80)
        print("Reddit数据聚类分析报告")
        print("=" * 80)
        
        # Subreddit分布
        print("\n## Subreddit分布")
        total_posts = sum(subreddit_counter.values())
        for subreddit, count in subreddit_counter.most_common():
            percentage = (count / total_posts * 100) if total_posts > 0 else 0
            print(f"   r/{subreddit}: {count} ({percentage:.1f}%)")
        
        # 高频Hashtags
        print("\n## 高频 Hashtags (Top 30)")
        for hashtag, count in hashtag_counter.most_common(30):
            print(f"   {hashtag}: {count}")
        
        # 提及的AI工具
        print("\n## 提及的 AI 工具 (Top 20)")
        for tool, count in ai_tool_counter.most_common(20):
            print(f"   {tool}: {count}")
        
        # 使用场景分布
        print("\n## 使用场景分布")
        for scenario, count in scenario_counter.most_common():
            print(f"   {scenario}: {count}")
        
        # Query Seed分布
        if query_seed_counter:
            print("\n## Query Seed 分布 (Top 20)")
            for query, count in query_seed_counter.most_common(20):
                print(f"   {query}: {count}")
        
        # 各Subreddit Top工具
        print("\n## 各Subreddit Top 工具")
        for subreddit, tools_counter in platform_tools.items():
            total_posts = subreddit_counter.get(subreddit, 0)
            if total_posts > 0:
                print(f"\n   [r/{subreddit}] (共 {total_posts} 条)")
                for tool, count in tools_counter.most_common(5):
                    print(f"      {tool}: {count}")
        
        # 高互动帖子
        print("\n## 高互动帖子 (Top 10)")
        high_engagement_posts.sort(key=lambda x: x['score'], reverse=True)
        for i, post in enumerate(high_engagement_posts[:10], 1):
            title = post['title']
            if len(title) > 60:
                title = title[:57] + '...'
            print(f"   {i}. [r/{post['subreddit']}] {title} ({post['likes']:,} likes, {post['comments']} comments)")
        
        print("\n" + "=" * 80)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Reddit数据聚类分析脚本')
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需）')
    
    args = parser.parse_args()
    
    analyzer = RedditDataAnalyzer()
    
    try:
        analyzer.analyze(args.task_id)
    except FileNotFoundError as e:
        print(f"错误: {e}")
    except Exception as e:
        print(f"分析失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()


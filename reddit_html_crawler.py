#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reddit HTML爬虫脚本
功能：通过HTML爬取Reddit帖子内容和评论
支持批量URL和关键词过滤
"""

import os
import json
import time
import hashlib
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs, quote_plus
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore


class RedditHTMLCrawler:
    """Reddit HTML爬虫类"""
    
    def __init__(self, user_agent: str = None, delay: float = 0.5):
        """
        初始化爬虫
        
        Args:
            user_agent: 用户代理字符串
            delay: 请求之间的延迟（秒，默认0.5）
        """
        self.session = requests.Session()
        # 改进User-Agent格式，符合Reddit建议
        self.user_agent = user_agent or "python:QuestFinderCrawler:v1.0.0 (by u/QuestFinder)"
        
        # 准备多个不同的请求头配置（用于反限流）
        self.header_configs = [
            {
                'User-Agent': "python:QuestFinderCrawler:v1.0.0 (by u/QuestFinder)",
                'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            },
            {
                'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            },
            {
                'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            },
            {
                'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            },
            {
                'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
                'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            },
        ]
        
        self.current_header_index = 0
        self.session.headers.update(self.header_configs[self.current_header_index])
        
        self.delay = delay
        self.data_dir = "Data"
        self.raw_dir = os.path.join(self.data_dir, "raw")
        self.mask_dir = os.path.join(self.data_dir, "mask")
        self._ensure_data_dir()
        self._post_counter = 0  # 用于生成自增ID
        self.print_lock = Lock()  # 用于线程安全的打印
        self.rate_limit_reset_time = None  # 限流重置时间
        # 使用信号量限制并发请求数（Reddit限流：每分钟最多100次请求）
        # 设置最大并发请求数为3，降低并发度以避免限流
        self.request_semaphore = Semaphore(3)
        # 全局请求时间戳队列，用于控制请求频率
        self.request_timestamps = []
        self.request_timestamps_lock = Lock()  # 保护请求时间戳队列
        # 最小请求间隔（秒），确保请求不会过于频繁
        self.min_request_interval = delay  # 使用设置的delay值
    
    def _calculate_content_hash(self, content: str) -> str:
        """
        计算正文内容的hash
        
        Args:
            content: 正文内容
            
        Returns:
            hash字符串
        """
        if not content:
            return hashlib.md5(b'').hexdigest()
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def _extract_media_urls(self, url: str, selftext: str) -> List[str]:
        """
        提取媒体URL（图片/视频）
        
        Args:
            url: 帖子URL
            selftext: 帖子正文
            
        Returns:
            媒体URL列表
        """
        media_urls = []
        
        # Reddit的图片/视频URL模式
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.webp']
        video_extensions = ['.mp4', '.webm', '.gifv']
        
        # 检查url是否是媒体文件
        url_lower = url.lower()
        if any(url_lower.endswith(ext) for ext in image_extensions + video_extensions):
            media_urls.append(url)
        
        # 检查selftext中的链接（Reddit markdown格式）
        # 简单的URL提取（不使用正则表达式）
        urls_in_text = []
        text = selftext
        while 'http://' in text or 'https://' in text:
            # 找到http://或https://的位置
            http_pos = text.find('http://')
            https_pos = text.find('https://')
            
            if http_pos == -1:
                start_pos = https_pos
            elif https_pos == -1:
                start_pos = http_pos
            else:
                start_pos = min(http_pos, https_pos)
            
            # 从起始位置开始提取URL
            url_start = start_pos
            url_end = len(text)
            
            # 找到URL的结束位置（空格、换行、括号等）
            for end_char in [' ', '\n', '\r', '\t', ')', ']', '}', '>']:
                end_pos = text.find(end_char, url_start)
                if end_pos != -1 and end_pos < url_end:
                    url_end = end_pos
            
            found_url = text[url_start:url_end]
            if found_url:
                urls_in_text.append(found_url)
            
            # 继续查找下一个URL
            text = text[url_end:]
        
        for found_url in urls_in_text:
            found_url_lower = found_url.lower()
            if any(found_url_lower.endswith(ext) for ext in image_extensions + video_extensions):
                # 移除可能的markdown格式
                clean_url = found_url.rstrip(')').rstrip(']')
                if clean_url not in media_urls:
                    media_urls.append(clean_url)
        
        return media_urls if media_urls else []
    
    def _format_post_to_standard(self, post_data: Dict[str, Any], source_url: str = "") -> Dict[str, Any]:
        """
        将Reddit post数据格式化为标准格式
        
        Args:
            post_data: Reddit原始post数据
            source_url: 来源URL
            
        Returns:
            格式化后的post字典
        """
        selftext = post_data.get('selftext', '')
        url = post_data.get('url', '')
        
        # 计算hash
        hash_content = self._calculate_content_hash(selftext)
        
        # 提取媒体URL
        media_urls = self._extract_media_urls(url, selftext)
        
        # 格式化时间
        created_utc = post_data.get('created_utc', 0)
        if created_utc:
            fetched_at = datetime.fromtimestamp(created_utc).isoformat()
        else:
            fetched_at = datetime.now().isoformat()
        
        # 处理permalink（帖子的实际URL）
        permalink = post_data.get('permalink', '')
        if permalink and not permalink.startswith('http'):
            permalink = f"https://reddit.com{permalink}"
        
        # 构建标准格式
        # source_url应该始终是帖子的实际URL（permalink），而不是搜索页面URL
        # 如果permalink存在，优先使用它；否则使用传入的source_url或post的url
        post_url = permalink or post_data.get('url', '') or source_url
        
        formatted_post = {
            "post_id": None,  # 将在保存时填充
            "platform": "reddit",
            "source_url": post_url,
            "source_platform_id": post_data.get('id', ''),
            "hash_content": hash_content,
            "fetched_at": fetched_at,
            "title": post_data.get('title', ''),
            "content_text": selftext,
            "lang": "english",
            "media_urls": media_urls if media_urls else [],
            "author_name": post_data.get('author', '[deleted]'),
            "author_handle": post_data.get('author', '[deleted]'),
            "author_followers": None,  # Reddit没有粉丝量
            "author_profile": None,  # Reddit没有简介/认证信息
            "likes": post_data.get('score', 0),
            "comments": post_data.get('num_comments', 0),
            "saves": None,  # Reddit没有收藏数
            "views": None,  # Reddit没有观看数量
            "comments_tree": []  # 评论树（递归结构）
        }
        
        return formatted_post
    
    def _ensure_data_dir(self):
        """确保Data目录及其子目录存在"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            print(f"已创建目录: {self.data_dir}")
        if not os.path.exists(self.raw_dir):
            os.makedirs(self.raw_dir)
            print(f"已创建目录: {self.raw_dir}")
        if not os.path.exists(self.mask_dir):
            os.makedirs(self.mask_dir)
            print(f"已创建目录: {self.mask_dir}")
    
    def _normalize_url(self, url: str) -> str:
        """
        标准化Reddit URL
        
        Args:
            url: 原始URL
            
        Returns:
            标准化后的URL
        """
        # 移除尾部斜杠
        url = url.rstrip('/')
        
        # 如果已经是old.reddit.com，直接返回
        if 'old.reddit.com' in url:
            return url
        
        # 转换为old.reddit.com格式（用于显示）
        # 但JSON API会使用www.reddit.com
        url = url.replace('www.reddit.com', 'old.reddit.com')
        if 'old.reddit.com' not in url and 'reddit.com' in url:
            url = url.replace('reddit.com', 'old.reddit.com')
        
        return url
    
    def _rotate_headers(self):
        """
        轮换请求头（用于反限流）
        
        Returns:
            是否成功轮换（如果所有请求头都试过了，返回False）
        """
        self.current_header_index = (self.current_header_index + 1) % len(self.header_configs)
        self.session.headers.update(self.header_configs[self.current_header_index])
        return True
    
    def _get_json_data(self, url: str, max_retries: int = 3) -> Optional[Dict]:
        """
        尝试从Reddit的JSON API获取数据（带重试机制）
        
        Args:
            url: Reddit URL
            max_retries: 最大重试次数
            
        Returns:
            JSON数据或None
        """
        # 先处理URL，移除尾部斜杠
        clean_url = url.rstrip('/')
        
        # 分离基础URL和参数
        if '?' in clean_url:
            base_url, params_str = clean_url.split('?', 1)
        else:
            base_url = clean_url
            params_str = ''
        
        # 将old.reddit.com或reddit.com替换为www.reddit.com（JSON API使用www域名）
        if 'old.reddit.com' in base_url:
            base_url = base_url.replace('old.reddit.com', 'www.reddit.com')
        elif 'reddit.com' in base_url and 'www.reddit.com' not in base_url:
            base_url = base_url.replace('reddit.com', 'www.reddit.com')
        
        # 清理参数：移除Reddit前端使用的参数（如cId, iId等），只保留标准参数
        if params_str:
            param_dict = {}
            for param in params_str.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    # 只保留Reddit JSON API支持的标准参数
                    if key in ['q', 'restrict_sr', 'sort', 't', 'limit', 'after', 'before']:
                        param_dict[key] = value
            
            if param_dict:
                param_str = '&'.join([f"{k}={v}" for k, v in param_dict.items()])
                # .json应该在查询参数之前
                json_url = f"{base_url}.json?{param_str}"
            else:
                json_url = base_url + '.json'
        else:
            json_url = base_url + '.json'
        
        # 为JSON API请求添加Referer头
        headers = {
            'Referer': base_url.replace('www.reddit.com', 'old.reddit.com'),
        }
        
        # 检查是否需要等待限流重置
        if self.rate_limit_reset_time and time.time() < self.rate_limit_reset_time:
            wait_time = int(self.rate_limit_reset_time - time.time()) + 1
            if wait_time > 0:
                with self.print_lock:
                    print(f"  - 等待限流重置，还需等待 {wait_time} 秒...")
                time.sleep(wait_time)
        
        # 使用信号量限制并发请求数
        with self.request_semaphore:
            # 智能延迟控制：确保请求间隔足够
            with self.request_timestamps_lock:
                current_time = time.time()
                # 清理超过1分钟的旧时间戳
                self.request_timestamps = [t for t in self.request_timestamps if current_time - t < 60]
                
                # 如果最近有请求，计算需要等待的时间
                if self.request_timestamps:
                    last_request_time = max(self.request_timestamps)
                    time_since_last = current_time - last_request_time
                    if time_since_last < self.min_request_interval:
                        wait_time = self.min_request_interval - time_since_last
                        if wait_time > 0:
                            time.sleep(wait_time)
                            current_time = time.time()
                
                # 记录本次请求时间
                self.request_timestamps.append(current_time)
            
            # 重试机制
            for attempt in range(max_retries):
                try:
                    response = self.session.get(json_url, timeout=30, verify=True, headers=headers)
                    
                    # 监控限流响应头（即使成功也要检查）
                    rate_limit_remaining = response.headers.get('X-Ratelimit-Remaining')
                    rate_limit_used = response.headers.get('X-Ratelimit-Used')
                    rate_limit_reset = response.headers.get('X-Ratelimit-Reset')
                    
                    if rate_limit_remaining:
                        try:
                            remaining = int(rate_limit_remaining)
                            # 如果剩余请求数很少，主动增加延迟
                            if remaining < 10:
                                extra_delay = (10 - remaining) * 0.5
                                with self.print_lock:
                                    print(f"  - 限流警告: 剩余 {remaining} 次请求，增加延迟 {extra_delay:.1f} 秒")
                                time.sleep(extra_delay)
                        except ValueError:
                            pass
                    
                    if rate_limit_reset:
                        try:
                            # X-Ratelimit-Reset是Unix时间戳
                            reset_time = int(rate_limit_reset)
                            self.rate_limit_reset_time = reset_time
                        except ValueError:
                            pass
                    
                    # 处理429限流错误
                    if response.status_code == 429:
                        # 先尝试更换请求头（如果还没试过所有请求头）
                        if attempt == 0 and len(self.header_configs) > 1:
                            with self.print_lock:
                                print(f"  - 429限流错误，尝试更换请求头...")
                            self._rotate_headers()
                            
                            # 立即用新请求头重试一次（不等待）
                            try:
                                retry_response = self.session.get(json_url, timeout=30, verify=True, headers={
                                    'Referer': base_url.replace('www.reddit.com', 'old.reddit.com'),
                                })
                                
                                # 如果更换请求头后成功，直接返回
                                if retry_response.status_code == 200:
                                    with self.print_lock:
                                        print(f"  - ✓ 更换请求头后成功，继续使用新请求头")
                                    
                                    # 更新限流信息
                                    retry_rate_limit_remaining = retry_response.headers.get('X-Ratelimit-Remaining')
                                    retry_rate_limit_reset = retry_response.headers.get('X-Ratelimit-Reset')
                                    if retry_rate_limit_reset:
                                        try:
                                            reset_time = int(retry_rate_limit_reset)
                                            self.rate_limit_reset_time = reset_time
                                        except ValueError:
                                            pass
                                    
                                    # 解析JSON
                                    try:
                                        return retry_response.json()
                                    except json.JSONDecodeError:
                                        with self.print_lock:
                                            print(f"  - JSON解析失败: {json_url}")
                                        return None
                                
                                # 如果更换请求头后仍然是429，继续使用等待逻辑
                                if retry_response.status_code == 429:
                                    with self.print_lock:
                                        print(f"  - ⚠️  更换请求头后仍然限流，进入等待逻辑...")
                            except Exception as e:
                                with self.print_lock:
                                    print(f"  - ⚠️  更换请求头后重试失败: {e}，进入等待逻辑...")
                        
                        # 等待逻辑（如果更换请求头无效或已经试过）
                        # 优先使用Retry-After头
                        retry_after = response.headers.get('Retry-After')
                        if retry_after:
                            try:
                                wait_time = int(retry_after)
                            except ValueError:
                                wait_time = None
                        else:
                            wait_time = None
                        
                        # 如果没有Retry-After，使用X-Ratelimit-Reset计算等待时间
                        if not wait_time and rate_limit_reset:
                            try:
                                reset_time = int(rate_limit_reset)
                                current_time = int(time.time())
                                wait_time = max(reset_time - current_time, 60)  # 至少等待60秒
                            except ValueError:
                                wait_time = None
                        
                        # 如果都没有，使用更长的指数退避（Reddit限流通常需要等待更长时间）
                        if not wait_time:
                            # 指数退避：第1次等60秒，第2次等120秒，第3次等240秒
                            wait_time = 60 * (2 ** attempt)
                        
                        if attempt < max_retries - 1:
                            with self.print_lock:
                                print(f"  - 429限流错误，等待 {wait_time} 秒后重试 (尝试 {attempt + 1}/{max_retries})...")
                                if rate_limit_remaining:
                                    print(f"  - 限流状态: 剩余 {rate_limit_remaining} 次请求")
                                if rate_limit_reset:
                                    reset_seconds = int(rate_limit_reset) - int(time.time())
                                    print(f"  - 限流将在 {reset_seconds} 秒后重置")
                            time.sleep(wait_time)
                            continue
                        else:
                            with self.print_lock:
                                print(f"  - 429限流错误，已达到最大重试次数")
                                print(f"  - 建议: 增加 --delay 参数值（当前: {self.delay}秒）或等待更长时间")
                            return None
                    
                    # 处理403错误
                    if response.status_code == 403:
                        with self.print_lock:
                            print(f"  - 403错误: Reddit可能阻止了请求")
                            print(f"  - 尝试访问的URL: {json_url}")
                            print(f"  - 提示: 可能需要增加延迟时间或使用VPN")
                        return None
                    
                    response.raise_for_status()
                    
                    # 检查响应内容类型
                    content_type = response.headers.get('Content-Type', '')
                    if 'application/json' not in content_type:
                        with self.print_lock:
                            print(f"  - 警告: 响应不是JSON格式，Content-Type: {content_type}")
                            print(f"  - 尝试访问的URL: {json_url}")
                    
                    data = response.json()
                    
                    # 根据限流状态动态调整延迟
                    if rate_limit_remaining:
                        try:
                            remaining = int(rate_limit_remaining)
                            # 如果剩余请求数很少，增加延迟
                            if remaining < 20:
                                adjusted_delay = self.delay * (1 + (20 - remaining) * 0.1)
                            else:
                                adjusted_delay = self.delay
                        except ValueError:
                            adjusted_delay = self.delay
                    else:
                        adjusted_delay = self.delay
                    
                    time.sleep(adjusted_delay)
                    return data
                
                except requests.exceptions.Timeout:
                    if attempt < max_retries - 1:
                        wait_time = 5 * (2 ** attempt)
                        print(f"  - 请求超时，等待 {wait_time} 秒后重试 (尝试 {attempt + 1}/{max_retries})...")
                        time.sleep(wait_time)
                    else:
                        print(f"  - 请求超时，已达到最大重试次数")
                        return None
                        
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:
                        # 429错误在上面已经处理
                        continue
                    else:
                        print(f"  - HTTP错误 {e.response.status_code}: {e}")
                        if 'json_url' in locals():
                            print(f"  - 尝试访问的URL: {json_url}")
                        return None
                        
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = 2 * (attempt + 1)
                        print(f"  - 请求失败: {e}，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                    else:
                        print(f"  - JSON API获取失败: {e}")
                        if 'json_url' in locals():
                            print(f"  - 尝试访问的URL: {json_url}")
                        return None
        
        return None
    
    def _parse_post_from_json(self, post_data: Dict) -> Optional[Dict[str, Any]]:
        """
        从JSON数据解析帖子信息
        
        Args:
            post_data: Reddit JSON数据
            
        Returns:
            帖子字典
        """
        try:
            if 'data' not in post_data:
                return None
            
            data = post_data['data']
            if 'children' not in data or len(data['children']) == 0:
                return None
            
            post = data['children'][0]['data']
            
            # 使用标准格式
            permalink = post.get('permalink', '')
            if permalink and not permalink.startswith('http'):
                permalink = f"https://reddit.com{permalink}"
            
            post_dict = self._format_post_to_standard(post, source_url=permalink)
            # 保留一些额外信息（如果需要）
            post_dict['subreddit'] = post.get('subreddit', '')
            # 保存permalink用于获取评论
            post_dict['_permalink'] = permalink
            
            return post_dict
        except Exception as e:
            print(f"  - 解析帖子JSON失败: {e}")
            return None
    
    def _parse_comment_from_json(self, comment_data: Dict, depth: int = 0) -> Optional[Dict[str, Any]]:
        """
        从JSON数据递归解析评论
        
        Args:
            comment_data: 评论JSON数据
            depth: 当前深度
            
        Returns:
            评论字典
        """
        try:
            if comment_data.get('kind') != 't1':  # t1是评论类型
                return None
            
            data = comment_data.get('data', {})
            
            # 跳过被删除的评论
            if data.get('body') == '[deleted]' and not data.get('replies'):
                return None
            
            # 构建author个人主页URL
            author = data.get('author', '[deleted]')
            author_profile_url = None
            if author and author != '[deleted]':
                author_profile_url = f"https://reddit.com/user/{author}"
            
            comment_dict = {
                "id": data.get('id', ''),
                "author": author,
                "body": data.get('body', ''),
                "score": data.get('score', 0),
                "created_utc": datetime.fromtimestamp(data.get('created_utc', 0)).isoformat() if data.get('created_utc') else '',
                "is_submitter": data.get('is_submitter', False),
                "permalink": f"https://reddit.com{data.get('permalink', '')}",
                "author_profile": author_profile_url,  # 添加author个人主页URL
                "depth": depth,
                "replies": []
            }
            
            # 递归处理回复
            replies = data.get('replies', {})
            if replies and isinstance(replies, dict) and 'data' in replies:
                for child in replies['data'].get('children', []):
                    reply = self._parse_comment_from_json(child, depth + 1)
                    if reply:
                        comment_dict["replies"].append(reply)
            
            return comment_dict
        except Exception as e:
            return None
    
    def _extract_posts_from_listing_json(self, listing_data: Dict) -> Tuple[List[Dict], Optional[str]]:
        """
        从列表页JSON数据提取帖子列表和翻页token
        
        Args:
            listing_data: Reddit列表页JSON数据
            
        Returns:
            (帖子数据列表, after_token用于翻页)
        """
        posts = []
        after_token = None
        try:
            if 'data' not in listing_data:
                return posts, after_token
            
            data = listing_data['data']
            
            # 提取after token用于翻页
            after_token = data.get('after')
            
            for child in data.get('children', []):
                if child.get('kind') == 't3':  # t3是帖子类型
                    post_data = child.get('data', {})
                    # 处理permalink
                    permalink = post_data.get('permalink', '')
                    if permalink and not permalink.startswith('http'):
                        permalink = f"https://reddit.com{permalink}"
                    # 使用标准格式
                    post_dict = self._format_post_to_standard(post_data, source_url=permalink)
                    # 保留一些额外信息（如果需要）
                    post_dict['subreddit'] = post_data.get('subreddit', '')
                    # 保存permalink用于获取评论
                    post_dict['_permalink'] = permalink
                    posts.append(post_dict)
        except Exception as e:
            print(f"  - 提取帖子列表失败: {e}")
        
        return posts, after_token
    
    def _crawl_post_comments(self, post_url: str) -> List[Dict[str, Any]]:
        """
        爬取单个帖子的所有评论
        
        Args:
            post_url: 帖子URL
            
        Returns:
            评论列表
        """
        comments = []
        try:
            json_data = self._get_json_data(post_url)
            if not json_data or len(json_data) < 2:
                return comments
            
            # 第二个元素包含评论
            comments_data = json_data[1]
            if 'data' not in comments_data:
                return comments
            
            for child in comments_data['data'].get('children', []):
                comment = self._parse_comment_from_json(child)
                if comment:
                    comments.append(comment)
        
        except Exception as e:
            print(f"  - 爬取评论失败: {e}")
        
        return comments
    
    def _filter_by_keywords(self, title: str, keywords: List[str]) -> bool:
        """
        检查标题是否包含关键词（简单字符串匹配）
        
        Args:
            title: 帖子标题
            keywords: 关键词列表（*表示不过滤）
            
        Returns:
            是否匹配
        """
        if not keywords:
            return True
        
        # 如果关键词列表包含*，表示不过滤，返回True
        if '*' in keywords:
            return True
        
        title_lower = title.lower()
        
        # 简单的字符串匹配
        for keyword in keywords:
            keyword = keyword.strip().lower()
            if not keyword:
                continue
            
            if keyword in title_lower:
                return True
        
        return False
    
    def _build_search_url(self, query: str) -> str:
        """
        构建Reddit搜索URL
        
        Args:
            query: 搜索关键词
            
        Returns:
            Reddit搜索URL
        """
        # URL编码查询字符串
        encoded_query = quote_plus(query)
        # 构建Reddit搜索URL（全站搜索）
        search_url = f"https://old.reddit.com/search/?q={encoded_query}&restrict_sr=0&sort=relevance&t=all"
        return search_url
    
    def _is_single_post_url(self, url: str) -> bool:
        """
        判断URL是否是单个帖子URL
        
        Args:
            url: Reddit URL
            
        Returns:
            是否是单个帖子
        """
        # 单个帖子URL通常包含 /comments/
        return '/comments/' in url and url.count('/comments/') == 1
    
    def _fetch_post_comments_worker(self, post: Dict[str, Any], permalink: str) -> List[Dict[str, Any]]:
        """
        工作线程：获取单个帖子的评论
        
        Args:
            post: 帖子数据
            permalink: 帖子permalink
            
        Returns:
            评论列表
        """
        if not permalink:
            with self.print_lock:
                print(f"  - 跳过（无permalink）: {post.get('title', '')[:30]}...")
            return []
        
        try:
            comments = self._crawl_post_comments(permalink)
            return comments
        except Exception as e:
            with self.print_lock:
                print(f"  - 获取评论失败 ({post.get('title', '')[:30]}...): {e}")
            return []
    
    def crawl_url(self, url: str, keywords: List[str] = None, fetch_comments: bool = True, max_posts: int = None, max_first_level_comments: int = None, num_threads: int = 16, query_seed: str = None) -> List[Dict[str, Any]]:
        """
        爬取指定URL的帖子（支持翻页和多线程）
        
        Args:
            url: Reddit URL（可以是Sub页面、搜索结果页面或单个帖子）
            keywords: 关键词过滤列表
            fetch_comments: 是否获取评论（列表页中的帖子需要单独访问）
            max_posts: 最大爬取帖子数量（已废弃，保留兼容性）
            max_first_level_comments: 最大一级评论数量（优先使用）
            num_threads: 并发线程数（默认16，用于并行获取评论）
            query_seed: 查询种子（用于标记来源）
            
        Returns:
            帖子数据列表
        """
        results = []
        normalized_url = self._normalize_url(url)
        is_single_post = self._is_single_post_url(normalized_url)
        
        print(f"\n正在爬取: {normalized_url}")
        if is_single_post:
            print("  - 检测到单个帖子URL")
        else:
            print("  - 检测到列表页URL")
            if max_first_level_comments:
                print(f"  - 目标一级评论数量: {max_first_level_comments}")
            elif max_posts:
                print(f"  - 目标爬取数量: {max_posts} 个帖子（已废弃，建议使用max_first_level_comments）")
        
        try:
            # 如果是单个帖子，直接处理
            if is_single_post:
                json_data = self._get_json_data(normalized_url)
                if json_data:
                    post = self._parse_post_from_json(json_data[0])
                    if post:
                        if self._filter_by_keywords(post['title'], keywords or []):
                            if fetch_comments:
                                # 单个帖子也可以使用多线程（虽然只有一个，但保持一致性）
                                permalink = post.get('_permalink', post.get('source_url', normalized_url))
                                if permalink:
                                    post['comments_tree'] = self._crawl_post_comments(permalink)
                                else:
                                    post['comments_tree'] = []
                            else:
                                post['comments_tree'] = []
                            # 清理临时字段
                            post.pop('_permalink', None)
                            results.append(post)
                return results
            
            # 列表页：支持翻页
            current_url = normalized_url
            after_token = None
            page_num = 0
            total_crawled = 0
            
            while True:
                page_num += 1
                print(f"\n  - 第 {page_num} 页...")
                
                # 构建带翻页参数的URL
                if after_token:
                    # 添加after参数用于翻页
                    if '?' in current_url:
                        page_url = f"{current_url}&after={after_token}&limit=100"
                    else:
                        page_url = f"{current_url}?after={after_token}&limit=100"
                else:
                    # 第一页，添加limit参数
                    if '?' in current_url:
                        page_url = f"{current_url}&limit=100"
                    else:
                        page_url = f"{current_url}?limit=100"
                
                # 获取JSON数据
                json_data = self._get_json_data(page_url)
                
                if not json_data:
                    print("  - 无法获取JSON数据，停止翻页")
                    break
                
                # 提取帖子列表和after token
                posts = []
                if isinstance(json_data, list) and len(json_data) > 0:
                    if json_data[0].get('kind') == 'Listing':
                        posts, after_token = self._extract_posts_from_listing_json(json_data[0])
                elif isinstance(json_data, dict):
                    if json_data.get('kind') == 'Listing':
                        posts, after_token = self._extract_posts_from_listing_json(json_data)
                
                if not posts:
                    print("  - 本页没有更多帖子，停止翻页")
                    break
                
                print(f"  - 本页提取了 {len(posts)} 个帖子")
                
                # 处理本页的帖子（先过滤和准备）
                posts_to_process = []
                for i, post in enumerate(posts):
                    # 计算当前已爬取的一级评论数
                    current_first_level_count = sum(len(p.get('comments_tree', [])) for p in results)
                    
                    # 检查是否达到一级评论数限制（优先检查）
                    if max_first_level_comments and current_first_level_count >= max_first_level_comments:
                        print(f"  - 已达到目标一级评论数 {max_first_level_comments}（当前: {current_first_level_count}），停止爬取")
                        break
                    
                    # 兼容性检查：如果使用旧的max_posts参数
                    if max_posts and not max_first_level_comments and len(results) >= max_posts:
                        print(f"  - 已达到最大爬取数量 {max_posts}，停止爬取")
                        break
                    
                    post['source_url'] = normalized_url
                    post['_index'] = len(results) + len(posts_to_process)  # 用于排序
                    post['_query_seed'] = query_seed  # 记录query_seed
                    
                    # 关键词过滤
                    if not self._filter_by_keywords(post['title'], keywords or []):
                        continue
                    
                    posts_to_process.append(post)
                    total_crawled += 1
                
                # 如果有一级评论数限制，在获取评论前先检查是否还需要
                if max_first_level_comments:
                    current_first_level_count = sum(len(p.get('comments_tree', [])) for p in results)
                    remaining_needed = max_first_level_comments - current_first_level_count
                    if remaining_needed <= 0:
                        print(f"  - 已达到目标一级评论数 {max_first_level_comments}，跳过本页所有帖子")
                        break
                
                # 如果需要获取评论，使用多线程并行获取
                if fetch_comments and posts_to_process:
                    posts_with_comments = [p for p in posts_to_process if p.get('comments', 0) > 0]
                    posts_without_comments = [p for p in posts_to_process if p.get('comments', 0) == 0]
                    
                    # 为没有评论的帖子设置空评论树
                    for post in posts_without_comments:
                        post['comments_tree'] = []
                    
                    # 如果有一级评论数限制，只获取需要的帖子数量（估算）
                    # 假设每个帖子平均有5个一级评论，那么需要获取 ceil(remaining_needed / 5) 个帖子
                    if max_first_level_comments and posts_with_comments:
                        current_first_level_count = sum(len(p.get('comments_tree', [])) for p in results)
                        remaining_needed = max_first_level_comments - current_first_level_count
                        # 保守估计：假设每个帖子平均有3-5个一级评论
                        estimated_posts_needed = min(len(posts_with_comments), max(1, (remaining_needed + 4) // 3))
                        posts_to_fetch = posts_with_comments[:estimated_posts_needed]
                        if len(posts_to_fetch) < len(posts_with_comments):
                            print(f"  - 目标一级评论数: {max_first_level_comments}，当前: {current_first_level_count}，还需: {remaining_needed}")
                            print(f"  - 本页有 {len(posts_with_comments)} 个有评论的帖子，先获取前 {len(posts_to_fetch)} 个帖子的评论")
                    else:
                        posts_to_fetch = posts_with_comments
                    
                    # 多线程获取评论
                    if posts_to_fetch:
                        print(f"  - 使用 {num_threads} 个线程并行获取 {len(posts_to_fetch)} 个帖子的评论...")
                        
                        with ThreadPoolExecutor(max_workers=num_threads) as executor:
                            future_to_post = {
                                executor.submit(self._fetch_post_comments_worker, post, post.get('_permalink', post.get('source_url', ''))): post
                                for post in posts_to_fetch
                                if post.get('_permalink') or post.get('source_url')
                            }
                            
                            completed_count = 0
                            for future in as_completed(future_to_post):
                                post = future_to_post[future]
                                try:
                                    comments = future.result()
                                    post['comments_tree'] = comments
                                    completed_count += 1
                                    with self.print_lock:
                                        print(f"    [{completed_count}/{len(posts_to_fetch)}] 完成: {post['title'][:50]}...")
                                except Exception as e:
                                    post['comments_tree'] = []
                                    with self.print_lock:
                                        print(f"    - 获取评论失败: {post['title'][:50]}... - {e}")
                        
                        # 如果获取的评论还不够，继续获取剩余的帖子
                        if max_first_level_comments:
                            # 计算当前已获取的一级评论数（包括results和已获取评论的posts_to_fetch）
                            current_first_level_count = sum(len(p.get('comments_tree', [])) for p in results)
                            current_first_level_count += sum(len(p.get('comments_tree', [])) for p in posts_to_fetch)
                            remaining_needed = max_first_level_comments - current_first_level_count
                            
                            if remaining_needed > 0 and len(posts_to_fetch) < len(posts_with_comments):
                                remaining_posts = posts_with_comments[len(posts_to_fetch):]
                                # 继续获取剩余帖子，但分批获取
                                batch_size = min(10, len(remaining_posts), (remaining_needed + 4) // 3)
                                for batch_start in range(0, len(remaining_posts), batch_size):
                                    # 重新计算当前已获取的评论数
                                    current_first_level_count = sum(len(p.get('comments_tree', [])) for p in results)
                                    current_first_level_count += sum(len(p.get('comments_tree', [])) for p in posts_to_fetch)
                                    current_first_level_count += sum(len(p.get('comments_tree', [])) for p in remaining_posts[:batch_start])
                                    remaining_needed = max_first_level_comments - current_first_level_count
                                    
                                    if remaining_needed <= 0:
                                        break
                                    
                                    batch = remaining_posts[batch_start:batch_start + batch_size]
                                    print(f"  - 继续获取 {len(batch)} 个帖子的评论（还需 {remaining_needed} 个一级评论）...")
                                    
                                    with ThreadPoolExecutor(max_workers=num_threads) as executor:
                                        future_to_post = {
                                            executor.submit(self._fetch_post_comments_worker, post, post.get('_permalink', post.get('source_url', ''))): post
                                            for post in batch
                                            if post.get('_permalink') or post.get('source_url')
                                        }
                                        
                                        for future in as_completed(future_to_post):
                                            post = future_to_post[future]
                                            try:
                                                comments = future.result()
                                                post['comments_tree'] = comments
                                            except Exception as e:
                                                post['comments_tree'] = []
                                    
                                    # 检查是否达到目标（包括刚获取的batch）
                                    current_first_level_count = sum(len(p.get('comments_tree', [])) for p in results)
                                    current_first_level_count += sum(len(p.get('comments_tree', [])) for p in posts_to_fetch)
                                    current_first_level_count += sum(len(p.get('comments_tree', [])) for p in batch)
                                    remaining_needed = max_first_level_comments - current_first_level_count
                                    if remaining_needed <= 0:
                                        print(f"  - 已达到目标一级评论数 {max_first_level_comments}，停止获取更多评论")
                                        break
                    
                    # 按索引排序并添加到结果
                    posts_to_process.sort(key=lambda x: x.get('_index', 0))
                    
                    # 如果有一级评论数限制，精确控制
                    if max_first_level_comments:
                        # 逐个添加帖子，直到达到目标一级评论数
                        posts_to_add = []
                        current_count = sum(len(p.get('comments_tree', [])) for p in results)
                        
                        for post in posts_to_process:
                            first_level_count = len(post.get('comments_tree', []))
                            
                            # 如果加上这个帖子会超过限制
                            if current_count + first_level_count > max_first_level_comments:
                                # 检查是否还有剩余空间
                                remaining = max_first_level_comments - current_count
                                if remaining > 0:
                                    # 只保留前remaining个一级评论
                                    truncated_post = post.copy()
                                    truncated_post['comments_tree'] = post.get('comments_tree', [])[:remaining]
                                    posts_to_add.append(truncated_post)
                                    current_count += remaining
                                # 无论是否截断，都停止添加（因为已经达到或超过限制）
                                break
                            else:
                                # 可以完整添加这个帖子
                                posts_to_add.append(post)
                                current_count += first_level_count
                        
                        # 添加帖子到results
                        if posts_to_add:
                            results.extend(posts_to_add)
                            final_count = sum(len(p.get('comments_tree', [])) for p in results)
                            print(f"  - 已添加 {len(posts_to_add)} 个帖子到结果，当前一级评论数: {final_count}")
                            
                            if final_count >= max_first_level_comments:
                                print(f"  - 已达到目标一级评论数 {max_first_level_comments}，停止添加")
                                break
                        else:
                            # 如果没有帖子可以添加，说明已经达到限制
                            current_count = sum(len(p.get('comments_tree', [])) for p in results)
                            if current_count >= max_first_level_comments:
                                print(f"  - 已达到目标一级评论数 {max_first_level_comments}，跳过本页剩余帖子")
                                break
                    elif max_posts:
                        # 兼容性处理：使用旧的max_posts逻辑
                        remaining_slots = max_posts - len(results)
                        if remaining_slots <= 0:
                            print(f"  - 已达到最大爬取数量 {max_posts}，跳过本页剩余帖子")
                            break
                        posts_to_add = posts_to_process[:remaining_slots]
                        results.extend(posts_to_add)
                        if len(posts_to_process) > remaining_slots:
                            print(f"  - 本页有 {len(posts_to_process)} 个帖子，但只能添加 {remaining_slots} 个（已达到限制 {max_posts}）")
                    else:
                        results.extend(posts_to_process)
                    
                    # 清理临时字段
                    for post in posts_to_process:
                        post.pop('_index', None)
                        post.pop('_permalink', None)
                else:
                    # 不需要获取评论，直接添加
                    for post in posts_to_process:
                        post['comments_tree'] = []
                    
                    # 如果有限制，确保不超过限制
                    if max_posts:
                        remaining_slots = max_posts - len(results)
                        if remaining_slots <= 0:
                            print(f"  - 已达到最大爬取数量 {max_posts}，跳过本页剩余帖子")
                            break
                        # 只添加不超过限制的数量
                        posts_to_add = posts_to_process[:remaining_slots]
                        results.extend(posts_to_add)
                        if len(posts_to_process) > remaining_slots:
                            print(f"  - 本页有 {len(posts_to_process)} 个帖子，但只能添加 {remaining_slots} 个（已达到限制 {max_posts}）")
                    else:
                        results.extend(posts_to_process)
                
                # 检查是否达到一级评论数限制
                current_first_level_count = sum(len(p.get('comments_tree', [])) for p in results)
                if max_first_level_comments and current_first_level_count >= max_first_level_comments:
                    print(f"  - 已达到目标一级评论数 {max_first_level_comments}（当前: {current_first_level_count}），停止翻页")
                    break
                
                # 兼容性检查：如果使用旧的max_posts参数
                if max_posts and not max_first_level_comments and len(results) >= max_posts:
                    break
                
                # 检查是否还有下一页
                if not after_token:
                    print("  - 没有更多页面，停止翻页")
                    break
                
                # 延迟避免请求过快
                time.sleep(self.delay)
            
            if not results:
                print(f"  - 未找到匹配的帖子（关键词: {keywords}）")
            else:
                # 计算总评论数（使用comments_tree）
                def count_comments(comments_tree):
                    if not comments_tree:
                        return 0
                    count = len(comments_tree)
                    for comment in comments_tree:
                        if isinstance(comment, dict) and 'replies' in comment:
                            count += count_comments(comment.get('replies', []))
                    return count
                
                total_comments = sum(count_comments(p.get('comments_tree', [])) for p in results)
                print(f"\n  - 爬取完成: 共找到 {len(results)} 个匹配的帖子，共 {total_comments} 条评论")
                print(f"  - 共爬取了 {page_num} 页，处理了 {total_crawled} 个帖子")
        
        except Exception as e:
            print(f"  - 爬取失败: {e}")
            import traceback
            traceback.print_exc()
        
        return results
    
    def crawl_batch(self, query_seeds: List[str], filter_keywords: List[str] = None, max_first_level_comments: int = None, num_threads: int = 16) -> List[Dict[str, Any]]:
        """
        批量爬取多个搜索关键词（并行版本）
        
        Args:
            query_seeds: 搜索关键词列表
            filter_keywords: 标题过滤关键词列表（用于二次过滤）
            max_first_level_comments: 总最大一级评论数量（None表示不限制，会平均分配给每个关键词）
            num_threads: 并发线程数（默认16，用于并行获取评论）
            
        Returns:
            所有帖子数据列表
        """
        all_results = []
        seen_post_ids = set()
        
        # 提前计算每个关键词的一级评论配额（完全均分）
        query_quotas = {}
        if max_first_level_comments and len(query_seeds) > 0:
            comments_per_query = max_first_level_comments // len(query_seeds)
            remainder = max_first_level_comments % len(query_seeds)
            print(f"\n一级评论配额分配:")
            print(f"  - 总一级评论数量: {max_first_level_comments}")
            print(f"  - 关键词数量: {len(query_seeds)}")
            print(f"  - 每个关键词基础配额: {comments_per_query} 个一级评论")
            if remainder > 0:
                print(f"  - 前 {remainder} 个关键词额外分配 1 个一级评论")
            
            for i, query in enumerate(query_seeds, 1):
                if remainder > 0 and i <= remainder:
                    query_quotas[query] = comments_per_query + 1
                else:
                    query_quotas[query] = comments_per_query
                print(f"  - [{i}] {query}: {query_quotas[query]} 个一级评论")
        else:
            for query in query_seeds:
                query_quotas[query] = None
        
        # 定义单个关键词的爬取函数
        def crawl_single_query(query: str, query_index: int) -> List[Dict[str, Any]]:
            """爬取单个关键词的帖子，直到达到一级评论配额"""
            try:
                print(f"\n[{query_index}/{len(query_seeds)}] 处理搜索关键词: {query}")
                
                # 构建搜索URL
                search_url = self._build_search_url(query)
                print(f"  - 搜索URL: {search_url}")
                
                # 获取该关键词的一级评论配额
                target_first_level_comments = query_quotas.get(query)
                if target_first_level_comments:
                    print(f"  - 目标一级评论数量: {target_first_level_comments}")
                
                # 爬取搜索结果（传递query_seed和目标一级评论数）
                results = self.crawl_url(
                    search_url, 
                    filter_keywords, 
                    fetch_comments=True, 
                    max_first_level_comments=target_first_level_comments,
                    num_threads=num_threads, 
                    query_seed=query
                )
                
                # 统计实际爬取的一级评论数
                total_first_level = sum(len(post.get('comments_tree', [])) for post in results)
                print(f"  - [{query_index}/{len(query_seeds)}] {query} 完成: 爬取了 {len(results)} 个帖子，共 {total_first_level} 个一级评论")
                return results
            except Exception as e:
                with self.print_lock:
                    print(f"  - [{query_index}/{len(query_seeds)}] {query} 爬取失败: {e}")
                return []
        
        # 串行爬取所有关键词（避免并发访问新URL触发限流）
        # 如果关键词数量较少（<=3），可以并行；否则串行
        max_parallel_queries = min(3, len(query_seeds))
        print(f"\n开始爬取 {len(query_seeds)} 个关键词（最多 {max_parallel_queries} 个并行）...")
        with ThreadPoolExecutor(max_workers=max_parallel_queries) as executor:
            # 提交所有任务
            future_to_query = {
                executor.submit(crawl_single_query, query, i+1): query
                for i, query in enumerate(query_seeds)
            }
            
            # 收集结果
            for future in as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    results = future.result()
                    for post in results:
                        # 去重（使用source_platform_id）
                        post_id = post.get('source_platform_id', post.get('id', ''))
                        if post_id and post_id not in seen_post_ids:
                            seen_post_ids.add(post_id)
                            all_results.append(post)
                    
                    # 统计当前总一级评论数
                    current_total_first_level = sum(len(post.get('comments_tree', [])) for post in all_results)
                    
                    # 检查是否达到总一级评论数限制
                    if max_first_level_comments and current_total_first_level >= max_first_level_comments:
                        print(f"\n已达到总一级评论数限制 {max_first_level_comments}，当前共 {current_total_first_level} 个，停止收集结果")
                        # 取消其他未完成的任务
                        for f in future_to_query:
                            if not f.done():
                                f.cancel()
                        break
                except Exception as e:
                    with self.print_lock:
                        print(f"  - 处理 {query} 的结果时出错: {e}")
        
        # 如果超过限制，精确截断到目标一级评论数
        if max_first_level_comments:
            total_first_level = sum(len(post.get('comments_tree', [])) for post in all_results)
            if total_first_level > max_first_level_comments:
                # 需要精确截断：保留帖子直到达到目标一级评论数
                truncated_results = []
                current_count = 0
                for post in all_results:
                    first_level_count = len(post.get('comments_tree', []))
                    if current_count + first_level_count <= max_first_level_comments:
                        truncated_results.append(post)
                        current_count += first_level_count
                    else:
                        # 如果加上这个帖子会超过，需要截断这个帖子的评论
                        remaining_needed = max_first_level_comments - current_count
                        if remaining_needed > 0:
                            # 只保留前remaining_needed个一级评论
                            truncated_post = post.copy()
                            truncated_post['comments_tree'] = post.get('comments_tree', [])[:remaining_needed]
                            truncated_results.append(truncated_post)
                        break
                all_results = truncated_results
                final_count = sum(len(post.get('comments_tree', [])) for post in all_results)
                print(f"\n精确截断完成: 从 {total_first_level} 个一级评论截断到 {final_count} 个（目标: {max_first_level_comments}）")
        
        return all_results
    
    def load_query_seeds_from_file(self, filepath: str = "to_craw_query_seeds.txt") -> List[str]:
        """
        从文件加载搜索关键词列表
        
        Args:
            filepath: 文件路径
            
        Returns:
            关键词列表
        """
        queries = []
        if not os.path.exists(filepath):
            print(f"警告: 文件 {filepath} 不存在")
            return queries
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        queries.append(line)
            print(f"从 {filepath} 加载了 {len(queries)} 个搜索关键词")
        except Exception as e:
            print(f"读取关键词文件失败: {e}")
        
        return queries
    
    def load_keywords_from_file(self, filepath: str = "filter_keywords.txt") -> List[str]:
        """
        从文件加载关键词列表
        
        Args:
            filepath: 文件路径
            
        Returns:
            关键词列表
        """
        keywords = []
        if not os.path.exists(filepath):
            print(f"警告: 文件 {filepath} 不存在，将不过滤关键词")
            return keywords
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        keywords.append(line)
            print(f"从 {filepath} 加载了 {len(keywords)} 个关键词")
        except Exception as e:
            print(f"读取关键词文件失败: {e}")
        
        return keywords
    
    def check_task_id_exists(self, task_id: str) -> bool:
        """
        检查任务ID是否已存在
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否存在
        """
        filename = f"{task_id}.json"
        filepath = os.path.join(self.raw_dir, filename)
        return os.path.exists(filepath)
    
    def save_to_json(self, data: List[Dict[str, Any]], task_id: str):
        """
        将数据保存为JSON文件到Data/raw/目录
        注意：不是保存整个Post，而是将每个第一层评论提取出来作为独立的项目
        
        Args:
            data: 要保存的数据（Post列表）
            task_id: 任务ID（必需）
        """
        if not task_id:
            raise ValueError("任务ID不能为空")
        
        # 检查任务ID是否已存在
        if self.check_task_id_exists(task_id):
            raise ValueError(f"任务ID '{task_id}' 已存在，不允许使用同名ID")
        
        # 提取每个Post的第一层评论作为独立项目
        formatted_data = []
        item_counter = 0
        
        for post in data:
            # 确保comments_tree字段存在
            comments_tree = post.get('comments_tree', [])
            
            if not comments_tree:
                # 如果没有评论，跳过该Post（因为现在只保存评论作为项目）
                continue
            
            # 为每个第一层评论创建一个独立的项目
            for first_level_comment in comments_tree:
                item_counter += 1
                
                # 构建新项目，以第一层评论为主体
                comment_item = {
                    "post_id": item_counter,  # 自增ID
                    "platform": post.get('platform', 'reddit'),
                    "source_url": post.get('source_url', ''),  # Post的URL
                    "source_platform_id": first_level_comment.get('id', ''),  # 第一层评论的ID
                    "hash_content": self._calculate_content_hash(first_level_comment.get('body', '')),
                    "fetched_at": post.get('fetched_at', datetime.now().isoformat()),
                    "title": post.get('title', ''),  # Post的标题
                    "content_text": first_level_comment.get('body', ''),  # 第一层评论的内容
                    "lang": post.get('lang', 'english'),
                    "media_urls": post.get('media_urls', []),
                    "author_name": first_level_comment.get('author', '[deleted]'),  # 第一层评论的作者
                    "author_handle": first_level_comment.get('author', '[deleted]'),
                    "author_followers": None,  # Reddit没有粉丝量
                    "author_profile": first_level_comment.get('author_profile', None),  # 第一层评论的author个人主页URL
                    "likes": first_level_comment.get('score', 0),  # 第一层评论的upvote数
                    "comments": self._count_comments_in_tree([first_level_comment]),  # 该评论及其子评论的总数
                    "saves": None,
                    "views": None,
                    # Post相关信息（保留用于上下文）
                    "post_info": {
                        "post_id": post.get('source_platform_id', ''),
                        "post_title": post.get('title', ''),
                        "post_author": post.get('author_name', ''),
                        "post_likes": post.get('likes', 0),
                        "post_comments_count": post.get('comments', 0)
                    },
                    # 记录query_seed
                    "query_seed": post.get('_query_seed', None),
                    # 评论树（包含该第一层评论及其所有子评论）
                    "comments_tree": [first_level_comment]  # 只包含这一个第一层评论及其子评论
                }
                
                formatted_data.append(comment_item)
        
        filename = f"{task_id}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(formatted_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n数据已保存到: {filepath}")
        print(f"共保存 {len(formatted_data)} 个评论项目（来自 {len(data)} 个帖子）")
    
    def _count_comments_in_tree(self, comments_tree: List[Dict[str, Any]]) -> int:
        """
        递归计算评论树中的评论总数（包括所有层级的子评论）
        
        Args:
            comments_tree: 评论树列表
            
        Returns:
            评论总数
        """
        if not comments_tree:
            return 0
        count = len(comments_tree)
        for comment in comments_tree:
            replies = comment.get('replies', [])
            if replies:
                count += self._count_comments_in_tree(replies)
        return count
    


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Reddit HTML爬虫脚本')
    parser.add_argument('--task-id', '-t', required=True,
                       help='任务ID（必需，用于命名输出文件）')
    parser.add_argument('--query-seeds-file', '-q', default='to_craw_query_seeds.txt', 
                       help='搜索关键词列表文件路径（默认: to_craw_query_seeds.txt）')
    parser.add_argument('--keywords-file', '-k', default='filter_keywords.txt',
                       help='标题过滤关键词列表文件路径（默认: filter_keywords.txt，用于二次过滤）')
    parser.add_argument('--delay', '-d', type=float, default=0.5,
                       help='请求之间的延迟（秒，默认0.5）')
    parser.add_argument('--max-posts', '-m', type=int, default=None,
                       help='最大爬取帖子数量（默认不限制，会翻页爬取所有匹配的帖子）')
    parser.add_argument('--threads', '-n', type=int, default=8,
                       help='并发线程数（默认8，用于并行获取评论。Reddit限流严格，建议不超过8）')
    parser.add_argument('--user-agent', help='自定义User-Agent')
    
    args = parser.parse_args()
    
    # 创建爬虫实例
    crawler = RedditHTMLCrawler(
        user_agent=args.user_agent,
        delay=args.delay
    )
    
    # 检查任务ID是否已存在
    if crawler.check_task_id_exists(args.task_id):
        print(f"错误: 任务ID '{args.task_id}' 已存在，不允许使用同名ID")
        print(f"请使用不同的任务ID或删除已存在的文件: {os.path.join(crawler.raw_dir, args.task_id + '.json')}")
        return
    
    # 加载搜索关键词和过滤关键词
    query_seeds = crawler.load_query_seeds_from_file(args.query_seeds_file)
    filter_keywords = crawler.load_keywords_from_file(args.keywords_file)
    
    if not query_seeds:
        print("错误: 没有找到要爬取的搜索关键词")
        return
    
    if filter_keywords:
        print(f"标题过滤关键词: {filter_keywords}")
    else:
        print("未设置标题过滤关键词，将爬取所有帖子")
    
    print(f"\n任务ID: {args.task_id}")
    if args.max_posts:
        print(f"最大爬取数量: {args.max_posts} 个帖子")
        print(f"  - 将平均分配给 {len(query_seeds)} 个搜索关键词")
    else:
        print("爬取数量: 不限制（将翻页爬取所有匹配的帖子）")
    print(f"并发线程数: {args.threads}")
    
    # 批量爬取
    data = crawler.crawl_batch(query_seeds, filter_keywords, max_posts=args.max_posts, num_threads=args.threads)
    
    # 保存数据
    if data:
        try:
            crawler.save_to_json(data, args.task_id)
            
            # 统计信息（注意：save_to_json已经打印了保存后的统计信息）
            # 这里只打印原始Post的统计
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
            print(f"\n原始数据统计:")
            print(f"  - 帖子数: {len(data)}")
            print(f"  - 第一层评论数: {first_level_comments}")
            print(f"  - 总评论数（包括子评论）: {total_comments}")
            print(f"\n注意: 实际保存时，每个第一层评论会作为独立项目保存")
        except ValueError as e:
            print(f"错误: {e}")
            return
    else:
        print("\n未找到匹配的帖子")


if __name__ == "__main__":
    main()


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase表检查脚本
功能：连接Supabase并列出所有表
"""

import os
from dotenv import load_dotenv
import requests
import json


def check_supabase_tables():
    """检查Supabase项目中的表"""
    
    # 加载环境变量
    load_dotenv()
    
    # 获取Supabase配置
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_service_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
    supabase_anon_key = os.getenv('SUPABASE_ANON_KEY')
    
    if not supabase_url:
        print("错误: 未找到 SUPABASE_URL 环境变量")
        print("请在 .env 文件中配置 SUPABASE_URL")
        return
    
    # 优先使用service role key，因为它有更多权限
    supabase_key = supabase_service_key or supabase_anon_key
    
    if not supabase_key:
        print("错误: 未找到 SUPABASE_SERVICE_ROLE_KEY 或 SUPABASE_ANON_KEY 环境变量")
        print("请在 .env 文件中配置 SUPABASE_SERVICE_ROLE_KEY 或 SUPABASE_ANON_KEY")
        return
    
    print("=" * 80)
    print("Supabase 连接信息")
    print("=" * 80)
    print(f"URL: {supabase_url}")
    print(f"使用Key: {'Service Role Key' if supabase_service_key else 'Anon Key'}")
    print()
    
    # 方法1: 使用PostgreSQL直接连接（最可靠的方法）
    print("方法1: 尝试直接连接PostgreSQL数据库...")
    try:
        import psycopg2
        from urllib.parse import urlparse
        
        # 从Supabase URL提取项目引用
        # Supabase URL格式: https://xxxxx.supabase.co
        # 数据库连接需要从Supabase Dashboard获取连接字符串
        # 或者使用连接池URL
        
        # 尝试从环境变量获取数据库连接字符串
        db_url = os.getenv('DATABASE_URL')
        
        if db_url:
            print("  使用 DATABASE_URL 连接...")
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            
            # 查询所有表
            cur.execute("""
                SELECT 
                    table_schema,
                    table_name,
                    table_type
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                AND table_schema NOT LIKE 'pg_temp_%'
                ORDER BY table_schema, table_name;
            """)
            
            tables = cur.fetchall()
            
            if tables:
                print(f"\n✓ 找到 {len(tables)} 个表:\n")
                print("=" * 80)
                for schema, table_name, table_type in tables:
                    print(f"Schema: {schema}")
                    print(f"表名: {table_name}")
                    print(f"类型: {table_type}")
                    
                    # 查询表的列信息
                    cur.execute("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                        ORDER BY ordinal_position;
                    """, (schema, table_name))
                    
                    columns = cur.fetchall()
                    if columns:
                        print("  列:")
                        for col_name, col_type, is_nullable in columns:
                            nullable = "NULL" if is_nullable == 'YES' else "NOT NULL"
                            print(f"    - {col_name}: {col_type} ({nullable})")
                    
                    print("-" * 80)
            else:
                print("  未找到任何表")
            
            cur.close()
            conn.close()
            return
            
        else:
            print("  未找到 DATABASE_URL，跳过直接PostgreSQL连接")
            print("  提示: 可以在Supabase Dashboard的Settings > Database中获取连接字符串")
            
    except ImportError:
        print("  psycopg2未安装，跳过直接PostgreSQL连接")
        print("  提示: 安装psycopg2可以获得更详细的表信息: pip install psycopg2-binary")
    except Exception as e:
        print(f"  直接连接失败: {e}")
    
    # 方法2: 使用Supabase REST API + RPC函数
    print("\n方法2: 尝试使用Supabase REST API...")
    try:
        headers = {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json'
        }
        
        # 测试连接
        test_url = f"{supabase_url}/rest/v1/"
        response = requests.get(test_url, headers=headers)
        
        if response.status_code == 200:
            print("  ✓ 成功连接到Supabase REST API")
        else:
            print(f"  API连接失败: {response.status_code}")
            print(f"  响应: {response.text[:200]}")
        
        # 尝试使用RPC函数查询表（如果存在）
        print("\n  尝试查询表信息...")
        
        # 由于PostgREST的限制，我们需要知道表名才能查询
        # 但我们可以尝试一些常见的方法
        
        # 方法2.1: 尝试查询pg_catalog（需要特殊权限）
        try:
            # 创建一个RPC函数来查询表（如果不存在）
            print("  提示: PostgREST API无法直接列出所有表")
            print("  需要:")
            print("    1. 在Supabase Dashboard中查看表")
            print("    2. 或者提供DATABASE_URL使用PostgreSQL直接连接")
            print("    3. 或者创建一个RPC函数来查询information_schema")
            
        except Exception as e:
            print(f"  RPC查询失败: {e}")
        
    except Exception as e:
        print(f"  REST API查询失败: {e}")
    
    # 方法3: 使用Supabase Python客户端
    print("\n方法3: 尝试使用Supabase Python客户端...")
    try:
        from supabase import create_client, Client
        
        supabase: Client = create_client(supabase_url, supabase_key)
        print("  ✓ Supabase客户端创建成功")
        
        # 测试连接
        try:
            # 尝试查询一个不存在的表来测试连接
            test_response = supabase.table('_test_connection_').select('*').limit(1).execute()
        except Exception as e:
            error_msg = str(e)
            if 'relation' in error_msg.lower() or 'does not exist' in error_msg.lower():
                print("  ✓ 连接正常（预期的错误：表不存在）")
            else:
                print(f"  连接测试: {error_msg[:100]}")
        
    except ImportError:
        print("  supabase-py未安装")
        print("  提示: 安装supabase库: pip install supabase")
    except Exception as e:
        print(f"  客户端创建失败: {e}")
    
    print("\n" + "=" * 80)
    print("建议:")
    print("=" * 80)
    print("1. 在Supabase Dashboard (https://app.supabase.com) 中查看表列表")
    print("2. 在Settings > Database中获取DATABASE_URL，然后使用psycopg2直接连接")
    print("3. 或者提供已知的表名，我可以帮你测试连接")


if __name__ == "__main__":
    check_supabase_tables()


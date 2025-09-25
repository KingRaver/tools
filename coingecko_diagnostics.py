#!/usr/bin/env python3
"""
CoinGecko Data Flow Diagnostic Script
=====================================

This script will test your exact CoinGecko setup and identify why
data isn't being stored in the coingecko_market_data table.

Usage: python coingecko_diagnostic.py
"""

import sqlite3
import requests
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

def test_database_connection():
    """Test database connection and table structure"""
    print("🔍 TESTING DATABASE CONNECTION")
    print("-" * 50)
    
    try:
        conn = sqlite3.connect('data/crypto_history.db')
        cursor = conn.cursor()
        
        # Check if coingecko_market_data table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='coingecko_market_data'
        """)
        
        table_exists = cursor.fetchone() is not None
        print(f"✅ Database connection: OK")
        print(f"✅ coingecko_market_data table exists: {table_exists}")
        
        if table_exists:
            # Check table schema
            cursor.execute("PRAGMA table_info(coingecko_market_data)")
            columns = cursor.fetchall()
            print(f"✅ Table has {len(columns)} columns")
            
            # Check record count
            cursor.execute("SELECT COUNT(*) FROM coingecko_market_data")
            total_records = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM coingecko_market_data 
                WHERE timestamp > datetime('now', '-24 hours')
            """)
            recent_records = cursor.fetchone()[0]
            
            print(f"📊 Total records: {total_records}")
            print(f"📊 Recent records (24h): {recent_records}")
            
            # Check latest record
            cursor.execute("""
                SELECT coin_id, symbol, timestamp, current_price 
                FROM coingecko_market_data 
                ORDER BY timestamp DESC LIMIT 1
            """)
            latest = cursor.fetchone()
            if latest:
                print(f"📅 Latest record: {latest[1]} ({latest[0]}) at {latest[2]} - ${latest[3]}")
            else:
                print("⚠️  No records found in table")
        
        conn.close()
        return table_exists
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False

def test_coingecko_api_direct():
    """Test CoinGecko API directly without any handlers"""
    print("\n🌐 TESTING COINGECKO API DIRECTLY")
    print("-" * 50)
    
    try:
        # Test basic connection
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 5,
            'page': 1,
            'sparkline': False
        }
        
        print(f"🔗 Testing: {url}")
        print(f"📋 Params: {params}")
        
        response = requests.get(url, params=params, timeout=10)
        
        print(f"📊 Status Code: {response.status_code}")
        print(f"📊 Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ API Response: SUCCESS")
            print(f"📊 Data type: {type(data)}")
            print(f"📊 Records returned: {len(data) if isinstance(data, list) else 'N/A'}")
            
            if isinstance(data, list) and len(data) > 0:
                first_coin = data[0]
                print(f"📋 Sample coin: {first_coin.get('symbol', 'N/A')} - ${first_coin.get('current_price', 'N/A')}")
                print(f"📋 Required fields present:")
                required_fields = ['id', 'symbol', 'name', 'current_price', 'market_cap']
                for field in required_fields:
                    present = field in first_coin
                    print(f"   {field}: {'✅' if present else '❌'}")
            
            return True, data
            
        elif response.status_code == 429:
            print(f"🚫 RATE LIMITED: {response.status_code}")
            print("⚠️  This might be why CoinGecko data collection is failing!")
            return False, None
            
        else:
            print(f"❌ API Error: {response.status_code}")
            print(f"❌ Error response: {response.text[:200]}")
            return False, None
            
    except Exception as e:
        print(f"❌ API request failed: {e}")
        return False, None

def test_storage_simulation(api_data):
    """Simulate storing CoinGecko data to see where it might fail"""
    print("\n💾 TESTING DATA STORAGE SIMULATION")
    print("-" * 50)
    
    if not api_data:
        print("❌ No API data to test storage")
        return False
    
    conn = None  # Initialize conn to None
    try:
        conn = sqlite3.connect('data/crypto_history.db')
        cursor = conn.cursor()
        
        # Try to insert one test record
        test_coin = api_data[0] if isinstance(api_data, list) and len(api_data) > 0 else None
        
        if not test_coin:
            print("❌ No test coin data available")
            return False
        
        print(f"🧪 Testing storage for: {test_coin.get('symbol', 'UNKNOWN')}")
        
        # Simulate the exact store_coingecko_data process
        current_time = datetime.now()
        
        coin_id = test_coin.get('id', '')
        symbol = test_coin.get('symbol', '')
        name = test_coin.get('name', '')
        current_price = test_coin.get('current_price', 0.0)
        market_cap = test_coin.get('market_cap', 0.0)
        
        print(f"📋 Extracted data:")
        print(f"   coin_id: {coin_id}")
        print(f"   symbol: {symbol}")
        print(f"   name: {name}")
        print(f"   current_price: {current_price}")
        print(f"   market_cap: {market_cap}")
        
        # Try the actual INSERT (but rollback so we don't affect real data)
        cursor.execute("""
            INSERT INTO coingecko_market_data (
                timestamp, coin_id, symbol, name, current_price, market_cap
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (current_time, coin_id, symbol, name, current_price, market_cap))
        
        print("✅ Storage simulation: SUCCESS")
        print("✅ INSERT statement works correctly")
        
        # Rollback so we don't actually store test data
        conn.rollback()
        return True
        
    except Exception as insert_error:
        print(f"❌ Storage simulation failed: {insert_error}")
        print("🔍 This is likely why CoinGecko data isn't being stored!")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False
        
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def check_bot_integration():
    """Check how bot.py integrates with CoinGecko"""
    print("\n🤖 CHECKING BOT.PY INTEGRATION")
    print("-" * 50)
    
    try:
        # Try to import the CoinGecko handler
        import sys
        sys.path.append('src')
        
        from coingecko_handler import CoinGeckoHandler
        print("✅ CoinGecko handler import: SUCCESS")
        
        # Initialize with the required base_url parameter
        handler = CoinGeckoHandler(base_url="https://api.coingecko.com/api/v3")
        print("✅ CoinGecko handler creation: SUCCESS")
        
        # Check quota status
        quota_status = handler.quota_tracker.get_quota_status()
        print(f"📊 Daily requests remaining: {quota_status.get('daily_remaining', 'Unknown')}")
        print(f"📊 Success rate (1h): {quota_status.get('success_rate_1h', 'Unknown')}")
        
        # Try a small API call
        print("🧪 Testing handler API call...")
        test_data = handler.get_market_data(params={'per_page': 2})
        
        if test_data:
            print("✅ Handler API call: SUCCESS")
            print(f"📊 Data returned: {len(test_data)} items")
            return True
        else:
            print("❌ Handler API call: FAILED")
            print("🔍 This confirms the issue is in the handler!")
            return False
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Handler test error: {e}")
        return False

def find_the_gap():
    """Try to identify exactly where the data flow breaks"""
    print("\n🔍 FINDING THE DATA FLOW GAP")
    print("-" * 50)
    
    # Check if data is being collected but not stored in the right table
    try:
        conn = sqlite3.connect('data/crypto_history.db')
        cursor = conn.cursor()
        
        # Check all tables for recent CoinGecko data
        tables_to_check = ['market_data', 'coingecko_market_data', 'coinmarketcap_market_data']
        
        for table in tables_to_check:
            try:
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {table} 
                    WHERE timestamp > datetime('now', '-24 hours')
                """)
                recent_count = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total_count = cursor.fetchone()[0]
                
                print(f"📊 {table}: {recent_count} recent / {total_count} total")
                
                # Check for any records that might be CoinGecko sourced
                if table == 'market_data':
                    cursor.execute(f"""
                        SELECT DISTINCT data_source FROM {table} 
                        WHERE timestamp > datetime('now', '-24 hours')
                        LIMIT 5
                    """)
                    sources = cursor.fetchall()
                    if sources:
                        print(f"   📋 Data sources: {[s[0] for s in sources]}")
                
            except Exception as table_error:
                print(f"❌ {table}: Error - {table_error}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Gap analysis error: {e}")

def main():
    """Run complete diagnostic"""
    print("🚀 COINGECKO DATA FLOW DIAGNOSTIC")
    print("=" * 60)
    print(f"🕐 Started at: {datetime.now()}")
    print()
    
    # Step 1: Test database
    db_ok = test_database_connection()
    
    # Step 2: Test API directly
    api_ok, api_data = test_coingecko_api_direct()
    
    # Step 3: Test storage simulation
    storage_ok = False
    if api_data:
        storage_ok = test_storage_simulation(api_data)
    
    # Step 4: Test bot integration
    bot_ok = check_bot_integration()
    
    # Step 5: Find the gap
    find_the_gap()
    
    # Summary
    print("\n📋 DIAGNOSTIC SUMMARY")
    print("=" * 60)
    print(f"✅ Database connection: {'PASS' if db_ok else 'FAIL'}")
    print(f"✅ CoinGecko API: {'PASS' if api_ok else 'FAIL'}")
    print(f"✅ Data storage: {'PASS' if storage_ok else 'FAIL'}")
    print(f"✅ Bot integration: {'PASS' if bot_ok else 'FAIL'}")
    
    print("\n🎯 LIKELY ISSUE:")
    if not api_ok:
        print("❌ CoinGecko API is failing (rate limits or network issues)")
    elif not storage_ok:
        print("❌ Data storage process is broken")
    elif not bot_ok:
        print("❌ Bot.py integration has issues")
    else:
        print("🤔 All components work individually - timing or coordination issue")
    
    print("\n🔧 RECOMMENDED NEXT STEPS:")
    if not api_ok:
        print("1. Check if you have a CoinGecko API key")
        print("2. Wait for rate limits to reset")
        print("3. Consider using CoinMarketCap only temporarily")
    elif not storage_ok:
        print("1. Check database table schema")
        print("2. Verify store_coingecko_data function")
        print("3. Check for missing columns or data type mismatches")
    elif not bot_ok:
        print("1. Check bot.py CoinGecko handler initialization")
        print("2. Verify API call flow in bot.py")
        print("3. Check for silent exception handling")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
🔍 FOCUSED OHLC TEST - Method + Endpoint 🔍

Tests:
1. Your existing get_coin_ohlc method in coingecko_handler.py
2. The exact endpoint from CoinGecko docs: /coins-id-ohlc

Usage:
    python focused_ohlc_test.py
"""

import sys
import os
import requests
import json
from typing import Optional, List

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

try:
    from coingecko_handler import CoinGeckoHandler
except ImportError as e:
    print(f"❌ Import Error: {e}")
    sys.exit(1)


def test_existing_method():
    """Test your existing get_coin_ohlc method"""
    print("🔧 TESTING EXISTING get_coin_ohlc METHOD")
    print("=" * 50)
    
    try:
        handler = CoinGeckoHandler(
            base_url="https://api.coingecko.com/api/v3",
            cache_duration=60
        )
        
        test_cases = [
            ("bitcoin", 1),
            ("ethereum", 7),
            ("polygon-pos", 1),  # POL token that was failing
        ]
        
        for coin_id, days in test_cases:
            print(f"\n📊 Testing: {coin_id} for {days} days")
            
            try:
                result = handler.get_coin_ohlc(coin_id, days)
                
                if result:
                    print(f"  ✅ SUCCESS: Got {len(result)} OHLC candles")
                    print(f"  📊 Sample data: {result[0] if len(result) > 0 else 'No data'}")
                    print(f"  📊 Data structure: {type(result)} containing {type(result[0]) if result else 'nothing'}")
                    
                    if len(result) > 0 and len(result[0]) == 5:
                        print(f"  ✅ Correct OHLC format: [timestamp, open, high, low, close]")
                    else:
                        print(f"  ⚠️  Unexpected format: {len(result[0]) if result else 0} fields")
                        
                else:
                    print(f"  ❌ FAILED: Method returned None")
                    
            except Exception as e:
                print(f"  ❌ EXCEPTION: {e}")
    
    except Exception as e:
        print(f"❌ Failed to initialize handler: {e}")


def test_docs_endpoint():
    """Test the exact endpoint from CoinGecko docs: /coins-id-ohlc"""
    print("\n\n📖 TESTING DOCS ENDPOINT: /coins-id-ohlc")
    print("=" * 50)
    
    base_url = "https://api.coingecko.com/api/v3"
    
    test_cases = [
        ("bitcoin", 1),
        ("ethereum", 7), 
        ("polygon-pos", 1),  # POL token
    ]
    
    for coin_id, days in test_cases:
        print(f"\n📊 Testing: {coin_id} for {days} days")
        
        # Test the exact format from docs: /coins-id-ohlc
        url = f"{base_url}/coins-{coin_id}-ohlc"
        params = {
            "vs_currency": "usd",
            "days": days
        }
        
        print(f"  🌐 URL: {url}")
        print(f"  📋 Params: {params}")
        
        try:
            response = requests.get(url, params=params, timeout=10)
            
            print(f"  📊 Status: {response.status_code}")
            print(f"  📊 Response length: {len(response.text)}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"  ✅ SUCCESS: {type(data)} with {len(data) if isinstance(data, list) else 'N/A'} items")
                    
                    if isinstance(data, list) and len(data) > 0:
                        print(f"  📊 Sample: {data[0]}")
                        print(f"  📊 Fields per candle: {len(data[0]) if isinstance(data[0], list) else 'Not a list'}")
                        
                except json.JSONDecodeError:
                    print(f"  ⚠️  Non-JSON response: {response.text[:100]}...")
                    
            else:
                print(f"  ❌ HTTP {response.status_code}")
                print(f"  📋 Error: {response.text[:200]}...")
                
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request failed: {e}")


def test_alternative_formats():
    """Test alternative URL formats"""
    print("\n\n🔄 TESTING ALTERNATIVE FORMATS")
    print("=" * 50)
    
    base_url = "https://api.coingecko.com/api/v3"
    coin_id = "bitcoin"
    days = 1
    
    formats = [
        # Your current format
        f"{base_url}/coins/{coin_id}/ohlc",
        
        # Docs format 
        f"{base_url}/coins-{coin_id}-ohlc",
        
        # Alternative interpretations
        f"{base_url}/coins/{coin_id}-ohlc",
        f"{base_url}/coins/ohlc/{coin_id}",
    ]
    
    params = {"vs_currency": "usd", "days": days}
    
    for i, url in enumerate(formats, 1):
        print(f"\n📊 Format {i}: {url}")
        
        try:
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    print(f"  ✅ WORKS: {len(data) if isinstance(data, list) else 'dict'} items")
                except:
                    print(f"  ⚠️  Works but non-JSON")
            else:
                print(f"  ❌ HTTP {response.status_code}")
                
        except Exception as e:
            print(f"  ❌ Failed: {e}")


def generate_corrected_method(working_url: str, sample_data):
    """Generate corrected method based on working endpoint"""
    print(f"\n\n🔧 CORRECTED METHOD IMPLEMENTATION")
    print("=" * 50)
    
    # Extract endpoint from URL
    endpoint = working_url.replace("https://api.coingecko.com/api/v3/", "")
    
    print(f"def get_coin_ohlc(self, coin_id: str, days: int = 1) -> Optional[List[List[float]]]:")
    print(f'    """Get OHLC data for a specific coin with validation"""')
    print(f"    if days not in [1, 7, 14, 30, 90, 180, 365]:")
    print(f"        days = 1")
    print(f"        ")
    print(f'    endpoint = "{endpoint}"')
    print(f"    params = {{")
    print(f'        "vs_currency": "usd",')
    print(f'        "days": days')
    print(f"    }}")
    print(f"    ")
    print(f"    result = self.get_with_cache(endpoint, params)")
    print(f"    ")
    print(f"    # Validate OHLC data")
    print(f"    if result and isinstance(result, list):")
    print(f"        validated_ohlc = []")
    print(f"        for candle in result:")
    print(f"            if len(candle) == 5:  # [timestamp, open, high, low, close]")
    print(f"                try:")
    print(f"                    timestamp = float(candle[0])")
    print(f"                    open_price = float(candle[1])")
    print(f"                    high_price = float(candle[2])")
    print(f"                    low_price = float(candle[3])")
    print(f"                    close_price = float(candle[4])")
    print(f"                    ")
    print(f"                    # Validate OHLC logic")
    print(f"                    if (open_price > 0 and high_price > 0 and low_price > 0 and close_price > 0 and")
    print(f"                        high_price >= max(open_price, close_price) and")
    print(f"                        low_price <= min(open_price, close_price)):")
    print(f"                        validated_ohlc.append([timestamp, open_price, high_price, low_price, close_price])")
    print(f"                except (ValueError, TypeError):")
    print(f"                    continue")
    print(f"        ")
    print(f"        if len(validated_ohlc) >= len(result) * 0.8:  # Require 80% valid candles")
    print(f"            return validated_ohlc")
    print(f"    ")
    print(f"    logger.logger.error(f'❌ OHLC data validation failed for {{coin_id}}')")
    print(f"    return None")


def main():
    """Run all OHLC tests"""
    print("🎯 FOCUSED OHLC ENDPOINT AND METHOD TEST")
    print("=" * 60)
    
    # Test 1: Your existing method
    test_existing_method()
    
    # Test 2: Docs endpoint format
    test_docs_endpoint()
    
    # Test 3: Alternative formats
    test_alternative_formats()
    
    # Summary
    print(f"\n\n📋 SUMMARY & NEXT STEPS")
    print("=" * 50)
    print("1. Check which format(s) returned ✅ SUCCESS")
    print("2. Use the working format in your corrected method")
    print("3. Update your coingecko_handler.py with the working implementation")
    print("4. Test with your validation method that was failing")


if __name__ == "__main__":
    main()

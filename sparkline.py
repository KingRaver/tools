#!/usr/bin/env python3
"""
🔍 SPARKLINE DATA EXTRACTION DEBUG SCRIPT 🔍

This script investigates what sparkline data CoinGecko actually provides
for UNI and other tokens to identify the root cause of extraction failures.
"""

import json
import requests
import time
from typing import Dict, Any, List, Optional

def debug_coingecko_sparkline_data():
    """
    Debug CoinGecko sparkline data for specific tokens
    """
    print("🔍 SPARKLINE DATA EXTRACTION DEBUG")
    print("=" * 60)
    
    # Test tokens - focusing on UNI since it's failing
    test_tokens = {
        'UNI': 'uniswap',
        'BTC': 'bitcoin', 
        'ETH': 'ethereum'
    }
    
    base_url = "https://api.coingecko.com/api/v3"
    
    for symbol, coingecko_id in test_tokens.items():
        print(f"\n🎯 TESTING: {symbol} ({coingecko_id})")
        print("-" * 40)
        
        # Test different API endpoints and parameters
        test_configs = [
            {
                'name': 'Market Data with Sparkline',
                'endpoint': 'coins/markets',
                'params': {
                    'vs_currency': 'usd',
                    'ids': coingecko_id,
                    'sparkline': True,
                    'price_change_percentage': '1h,24h,7d'
                }
            },
            {
                'name': 'Individual Coin Data',
                'endpoint': f'coins/{coingecko_id}',
                'params': {
                    'localization': 'false',
                    'tickers': 'false',
                    'market_data': 'true',
                    'sparkline': 'true'
                }
            },
            {
                'name': 'Historical Data (1 day)',
                'endpoint': f'coins/{coingecko_id}/market_chart',
                'params': {
                    'vs_currency': 'usd',
                    'days': 1,
                    'interval': 'hourly'
                }
            }
        ]
        
        for config in test_configs:
            try:
                print(f"\n📡 {config['name']}:")
                
                url = f"{base_url}/{config['endpoint']}"
                response = requests.get(url, params=config['params'], timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Analyze the response structure
                    if config['endpoint'] == 'coins/markets':
                        analyze_market_data_sparkline(data, symbol)
                    elif 'market_chart' in config['endpoint']:
                        analyze_historical_data(data, symbol)
                    else:
                        analyze_individual_coin_data(data, symbol)
                        
                else:
                    print(f"   ❌ API Error: {response.status_code}")
                    
            except Exception as e:
                print(f"   ❌ Request failed: {str(e)}")
            
            # Rate limiting
            time.sleep(1)

def analyze_market_data_sparkline(data: List[Dict], symbol: str):
    """Analyze sparkline data from markets endpoint"""
    if not data or not isinstance(data, list):
        print(f"   ❌ No market data returned")
        return
    
    coin_data = data[0] if data else {}
    
    print(f"   ✅ Market data received")
    print(f"   📊 Current price: ${coin_data.get('current_price', 'N/A')}")
    
    # Check sparkline structure
    sparkline = coin_data.get('sparkline_in_7d')
    if sparkline:
        print(f"   📈 Sparkline structure type: {type(sparkline)}")
        print(f"   📈 Sparkline keys: {list(sparkline.keys()) if isinstance(sparkline, dict) else 'Not a dict'}")
        
        if isinstance(sparkline, dict) and 'price' in sparkline:
            prices = sparkline['price']
            print(f"   💰 Price array type: {type(prices)}")
            print(f"   💰 Price array length: {len(prices) if isinstance(prices, list) else 'Not a list'}")
            
            if isinstance(prices, list) and len(prices) > 0:
                print(f"   💰 First 3 prices: {prices[:3]}")
                print(f"   💰 Last 3 prices: {prices[-3:]}")
                print(f"   💰 Price range: ${min(prices):.6f} - ${max(prices):.6f}")
                
                # Check for valid data
                valid_prices = [p for p in prices if p is not None and isinstance(p, (int, float)) and p > 0]
                print(f"   ✅ Valid prices: {len(valid_prices)}/{len(prices)}")
                
                if len(valid_prices) < len(prices):
                    invalid_prices = [p for p in prices if not (p is not None and isinstance(p, (int, float)) and p > 0)]
                    print(f"   ⚠️  Invalid price examples: {invalid_prices[:5]}")
            else:
                print(f"   ❌ Price array is empty or invalid")
        else:
            print(f"   ❌ No 'price' field in sparkline")
    else:
        print(f"   ❌ No sparkline_in_7d data")
    
    # Show raw sparkline structure for debugging
    print(f"   🔍 Raw sparkline (first 200 chars): {str(sparkline)[:200]}...")

def analyze_historical_data(data: Dict, symbol: str):
    """Analyze historical market chart data"""
    if not isinstance(data, dict):
        print(f"   ❌ Invalid historical data format")
        return
    
    prices = data.get('prices', [])
    print(f"   📊 Historical prices count: {len(prices)}")
    
    if prices and len(prices) > 0:
        print(f"   💰 First price entry: {prices[0]}")
        print(f"   💰 Last price entry: {prices[-1]}")
        
        # Extract just the price values (second element of each [timestamp, price] pair)
        price_values = [entry[1] for entry in prices if len(entry) >= 2]
        if price_values:
            print(f"   💰 Price range: ${min(price_values):.6f} - ${max(price_values):.6f}")

def analyze_individual_coin_data(data: Dict, symbol: str):
    """Analyze individual coin endpoint data"""
    if not isinstance(data, dict):
        print(f"   ❌ Invalid coin data format")
        return
    
    print(f"   📊 Coin ID: {data.get('id', 'N/A')}")
    print(f"   📊 Coin symbol: {data.get('symbol', 'N/A')}")
    
    market_data = data.get('market_data', {})
    if market_data:
        current_price = market_data.get('current_price', {})
        if isinstance(current_price, dict):
            usd_price = current_price.get('usd')
            print(f"   💰 Current USD price: ${usd_price}")
        
        # Check for sparkline in market_data
        sparkline = market_data.get('sparkline_7d')
        if sparkline:
            print(f"   📈 Market data sparkline type: {type(sparkline)}")
            if isinstance(sparkline, dict) and 'price' in sparkline:
                prices = sparkline['price']
                print(f"   📈 Market data sparkline prices: {len(prices) if isinstance(prices, list) else 'Invalid'}")

def test_polars_extraction():
    """Test Polars extraction logic on sample data"""
    print(f"\n🧪 TESTING POLARS EXTRACTION LOGIC")
    print("=" * 60)
    
    # Sample sparkline data structure (like what CoinGecko returns)
    sample_data = [
        {
            'id': 'uniswap',
            'symbol': 'uni',
            'current_price': 8.50,
            'sparkline_in_7d': {
                'price': [8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7]
            }
        }
    ]
    
    try:
        import polars as pl
        
        df = pl.DataFrame(sample_data)
        print(f"   ✅ Polars DataFrame created")
        print(f"   📊 Columns: {df.columns}")
        
        # Test sparkline extraction (like in our processing method)
        df_with_sparkline = df.with_columns([
            pl.when(pl.col('sparkline_in_7d').is_not_null())
            .then(pl.col('sparkline_in_7d').struct.field('price'))
            .otherwise(pl.lit([]))
            .alias('sparkline')
        ])
        
        for row in df_with_sparkline.iter_rows(named=True):
            sparkline = row.get('sparkline', [])
            print(f"   ✅ Extracted sparkline: {sparkline}")
            print(f"   ✅ Sparkline length: {len(sparkline) if isinstance(sparkline, list) else 'Not a list'}")
            
    except ImportError:
        print(f"   ❌ Polars not available for testing")
    except Exception as e:
        print(f"   ❌ Polars extraction test failed: {str(e)}")

def main():
    """Main debug function"""
    print("🚀 Starting Sparkline Data Extraction Debug")
    print("This script will investigate CoinGecko sparkline data issues")
    print()
    
    # Debug actual API responses
    debug_coingecko_sparkline_data()
    
    # Test our extraction logic
    test_polars_extraction()
    
    print(f"\n🎯 DEBUG COMPLETE")
    print("Check the output above to identify sparkline data issues")

if __name__ == "__main__":
    main()

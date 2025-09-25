#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Multi-Chain Manager Test Script
------------------------------
This script tests the Multi-Chain Manager's price data retrieval functionality
to identify why it's returning $0.0000 for the NEAR token.
"""

import asyncio
import os
import time
import json
from typing import Dict, List, Optional, Any
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('multi_chain_manager_test')

# Import required modules
try:
    # Try to import from the current directory first
    from multi_chain_manager import MultiChainManager, PriceDataManager
    logger.info("✅ Successfully imported MultiChainManager from current directory")
except ImportError:
    # If not found, add parent directory to path and try again
    import sys
    sys.path.append('..')
    try:
        from multi_chain_manager import MultiChainManager, PriceDataManager
        logger.info("✅ Successfully imported MultiChainManager from parent directory")
    except ImportError:
        logger.error("❌ Failed to import MultiChainManager - make sure the file is accessible")
        sys.exit(1)

def print_divider(title: str):
    """Print a section divider with title"""
    print("\n" + "=" * 80)
    print(f" 🔍 {title}")
    print("=" * 80 + "\n")

def test_price_data_manager_initialization():
    """Test PriceDataManager initialization"""
    print_divider("TESTING PRICE DATA MANAGER INITIALIZATION")
    
    try:
        price_manager = PriceDataManager()
        logger.info(f"✅ PriceDataManager initialized successfully")
        
        # Check if API Manager is available
        if price_manager.api_manager:
            logger.info("✅ API Manager is available")
            
            # Print available providers
            if hasattr(price_manager.api_manager, 'providers'):
                provider_count = len(price_manager.api_manager.providers)
                logger.info(f"✅ {provider_count} providers available")
                
                for name in price_manager.api_manager.providers.keys():
                    logger.info(f"  - {name}")
        else:
            logger.warning("⚠️ API Manager is not available")
        
        # Check if database is available
        if price_manager.db:
            logger.info("✅ Database connection is available")
        else:
            logger.warning("⚠️ Database connection is not available")
        
        # Check token mapping
        if hasattr(price_manager, 'token_mapping'):
            logger.info(f"✅ Token mapping contains {len(price_manager.token_mapping)} entries")
            
            # Check for NEAR specifically
            if 'NEAR' in price_manager.token_mapping:
                near_mapping = price_manager.token_mapping['NEAR']
                logger.info(f"✅ NEAR is mapped to '{near_mapping}'")
            else:
                logger.warning("⚠️ NEAR is not in the token mapping")
        else:
            logger.warning("⚠️ Token mapping is not available")
        
        return price_manager
    except Exception as e:
        logger.error(f"❌ PriceDataManager initialization failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

async def test_token_price_retrieval(price_manager, tokens_to_test=None):
    """Test token price retrieval for specific tokens"""
    if tokens_to_test is None:
        tokens_to_test = ["BTC", "ETH", "NEAR"]  # Default tokens to test
    
    print_divider(f"TESTING TOKEN PRICE RETRIEVAL FOR {', '.join(tokens_to_test)}")
    
    if not price_manager:
        logger.error("❌ PriceDataManager not initialized")
        return
    
    results = {}
    
    for token in tokens_to_test:
        try:
            logger.info(f"🔍 Retrieving price for {token}...")
            
            # Get token price
            start_time = time.time()
            price = await price_manager.get_token_price(token)
            elapsed_time = time.time() - start_time
            
            results[token] = price
            
            if price > 0:
                logger.info(f"✅ Valid price for {token}: ${price:.4f} (retrieved in {elapsed_time:.2f}s)")
            else:
                logger.warning(f"⚠️ Zero or invalid price for {token}: ${price} (retrieved in {elapsed_time:.2f}s)")
                
                # Print cache state for this token
                if token in price_manager.token_price_cache:
                    cache_entry = price_manager.token_price_cache[token]
                    cache_age = time.time() - cache_entry['timestamp']
                    logger.info(f"📊 Cache for {token}: price=${cache_entry['price']}, age={cache_age:.2f}s")
                else:
                    logger.info(f"📊 No cache entry for {token}")
        except Exception as e:
            logger.error(f"❌ Error retrieving price for {token}: {str(e)}")
            results[token] = None
    
    return results

async def test_price_retrieval_flow(price_manager, token="NEAR"):
    """Test the complete price retrieval flow and trace execution path"""
    print_divider(f"TESTING PRICE RETRIEVAL FLOW FOR {token}")
    
    if not price_manager:
        logger.error("❌ PriceDataManager not initialized")
        return
    
    try:
        # Test each strategy individually with tracing
        
        # STRATEGY 1: Check cache first
        logger.info("🔍 STRATEGY 1: Testing cache retrieval...")
        cache_key = token.upper()
        
        if cache_key in price_manager.token_price_cache:
            cached_data = price_manager.token_price_cache[cache_key]
            cache_age = time.time() - cached_data['timestamp']
            
            if cache_age < price_manager.cache_duration:
                logger.info(f"✅ Cache hit for {cache_key}: ${cached_data['price']}, age={cache_age:.2f}s")
            else:
                logger.info(f"📊 Cache expired for {cache_key}: ${cached_data['price']}, age={cache_age:.2f}s")
        else:
            logger.info(f"📊 No cache entry for {cache_key}")
        
        # STRATEGY 2: Use API Manager
        if price_manager.api_manager:
            logger.info("🔍 STRATEGY 2: Testing API Manager retrieval...")
            
            try:
                token_id = price_manager.token_mapping.get(cache_key, cache_key.lower())
                logger.info(f"📡 Token {cache_key} mapped to '{token_id}' for API")
                
                # Test API Manager directly
                market_data = price_manager.api_manager.get_market_data(
                    params={"ids": token_id, "vs_currency": "usd"}
                )
                
                if market_data:
                    logger.info(f"✅ API Manager returned data with {len(market_data)} entries")
                    
                    # Check if our token is in the response
                    if cache_key in market_data:
                        price = market_data[cache_key].get('current_price', 0)
                        logger.info(f"✅ Found {cache_key} in response: ${price}")
                    else:
                        logger.warning(f"⚠️ {cache_key} not found in API response")
                        
                        # Check for case-insensitive match
                        token_lower = cache_key.lower()
                        case_matches = [t for t in market_data.keys() if t.lower() == token_lower]
                        if case_matches:
                            logger.info(f"🔍 Found case-insensitive matches: {case_matches}")
                            
                            # Check first match
                            first_match = case_matches[0]
                            price = market_data[first_match].get('current_price', 0)
                            logger.info(f"📊 Price for {first_match}: ${price}")
                else:
                    logger.warning("⚠️ API Manager returned no data")
            except Exception as e:
                logger.error(f"❌ API Manager retrieval failed: {str(e)}")
        else:
            logger.warning("⚠️ API Manager is not available")
        
        # STRATEGY 3: Try database fallback
        if price_manager.db:
            logger.info("🔍 STRATEGY 3: Testing database fallback...")
            
            try:
                # List available methods in the database object
                db_methods = [m for m in dir(price_manager.db) if 'get' in m.lower() and 'data' in m.lower()]
                logger.info(f"📊 Available database methods: {', '.join(db_methods)}")
                
                # Try each potential method
                for method_name in db_methods:
                    try:
                        method = getattr(price_manager.db, method_name)
                        if callable(method):
                            logger.info(f"📡 Trying database method: {method_name}")
                            
                            # Try with and without parameters
                            try:
                                data = method([cache_key])
                                
                                if data:
                                    logger.info(f"✅ Method {method_name} returned data with parameter")
                                    
                                    # Check if our token exists in the response
                                    if isinstance(data, dict) and cache_key in data:
                                        price_data = data[cache_key]
                                        if isinstance(price_data, dict) and 'current_price' in price_data:
                                            price = price_data['current_price']
                                            logger.info(f"✅ Found price in database: ${price}")
                            except:
                                try:
                                    data = method()
                                    
                                    if data:
                                        logger.info(f"✅ Method {method_name} returned data without parameter")
                                        
                                        # Handle both dictionary and list formats
                                        if isinstance(data, dict) and cache_key in data:
                                            price_data = data[cache_key]
                                            if isinstance(price_data, dict) and 'current_price' in price_data:
                                                price = price_data['current_price']
                                                logger.info(f"✅ Found price in database: ${price}")
                                        # Handle list format
                                        elif isinstance(data, list):
                                            for item in data:
                                                if isinstance(item, dict) and item.get('symbol', '').upper() == cache_key:
                                                    price = item.get('current_price', 0)
                                                    if price > 0:
                                                        logger.info(f"✅ Found price in database list: ${price}")
                                except:
                                    logger.debug(f"Method {method_name} failed without parameter")
                    except Exception as method_error:
                        logger.debug(f"Method {method_name} failed: {method_error}")
            except Exception as db_error:
                logger.error(f"❌ Database fallback failed: {str(db_error)}")
        else:
            logger.warning("⚠️ Database is not available")
        
        # STRATEGY 4: Check for stale cache
        logger.info("🔍 STRATEGY 4: Checking for stale cache...")
        if cache_key in price_manager.token_price_cache:
            price = price_manager.token_price_cache[cache_key]['price']
            age = (time.time() - price_manager.token_price_cache[cache_key]['timestamp']) / 60  # age in minutes
            logger.info(f"📊 Stale cache for {cache_key}: ${price}, age={age:.1f} minutes")
        else:
            logger.info(f"📊 No stale cache available for {cache_key}")
        
        # STRATEGY 5: Final fallback (this is where 0 is returned)
        logger.info("🔍 STRATEGY 5: Testing final fallback...")
        logger.info(f"⚠️ If all strategies fail, this is where $0.0000 would be returned")
        
        # Now test the actual method
        logger.info(f"🔍 Now testing the actual get_token_price method for {token}...")
        price = await price_manager.get_token_price(token)
        
        if price > 0:
            logger.info(f"✅ Final price for {token}: ${price:.4f}")
        else:
            logger.warning(f"⚠️ get_token_price returned zero for {token}: ${price}")
            
        return price
    except Exception as e:
        logger.error(f"❌ Price retrieval flow test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def test_multi_chain_manager_initialization():
    """Test MultiChainManager initialization"""
    print_divider("TESTING MULTI-CHAIN MANAGER INITIALIZATION")
    
    try:
        # Initialize MultiChainManager
        manager = MultiChainManager()
        logger.info("✅ MultiChainManager initialized successfully")
        
        # Check if price_manager is available
        if hasattr(manager, 'price_manager'):
            logger.info("✅ PriceDataManager is available within MultiChainManager")
            return manager
        else:
            logger.error("❌ PriceDataManager not found in MultiChainManager")
            return None
    except Exception as e:
        logger.error(f"❌ MultiChainManager initialization failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

async def test_direct_vs_manager(price_manager, token="NEAR"):
    """Compare direct API calls vs. going through the manager"""
    print_divider(f"COMPARING DIRECT API VS MANAGER FOR {token}")
    
    if not price_manager or not price_manager.api_manager:
        logger.error("❌ PriceDataManager or API Manager not initialized")
        return
    
    results = {}
    
    try:
        # Test 1: Direct API call
        logger.info("🔍 TEST 1: Direct API call")
        token_id = price_manager.token_mapping.get(token.upper(), token.lower())
        
        try:
            params = {"ids": token_id, "vs_currency": "usd"}
            direct_data = price_manager.api_manager.get_market_data(params=params)
            
            if direct_data:
                # Check if our token is in the response (try different cases)
                found = False
                for key, value in direct_data.items():
                    if key.upper() == token.upper():
                        price = value.get('current_price', 0)
                        logger.info(f"✅ Direct API: Found {key} with price ${price}")
                        results['direct_api'] = price
                        found = True
                        break
                
                if not found:
                    logger.warning(f"⚠️ Direct API: Token {token} not found in response")
                    results['direct_api'] = 0
            else:
                logger.warning("⚠️ Direct API: No data returned")
                results['direct_api'] = None
        except Exception as e:
            logger.error(f"❌ Direct API call failed: {str(e)}")
            results['direct_api'] = None
        
        # Test 2: Through PriceDataManager
        logger.info("🔍 TEST 2: Through PriceDataManager")
        try:
            manager_price = await price_manager.get_token_price(token)
            logger.info(f"📊 Manager: Price for {token}: ${manager_price}")
            results['manager'] = manager_price
        except Exception as e:
            logger.error(f"❌ Manager price retrieval failed: {str(e)}")
            results['manager'] = None
        
        # Compare results
        if results.get('direct_api') is not None and results.get('manager') is not None:
            if results['direct_api'] == results['manager']:
                logger.info("✅ Results match: Direct API and Manager returned the same price")
            else:
                logger.warning(f"⚠️ Results differ: Direct API=${results['direct_api']}, Manager=${results['manager']}")
        
        return results
    except Exception as e:
        logger.error(f"❌ Comparison test failed: {str(e)}")
        return None

async def main():
    """Main async test function"""
    print_divider("MULTI-CHAIN MANAGER TEST")
    print("This script tests the Multi-Chain Manager's price data retrieval functionality.")
    
    # Initialize MultiChainManager if needed
    multi_chain = test_multi_chain_manager_initialization()
    
    # Initialize PriceDataManager directly
    price_manager = test_price_data_manager_initialization()
    
    if not price_manager:
        if multi_chain and hasattr(multi_chain, 'price_manager'):
            price_manager = multi_chain.price_manager
            logger.info("✅ Using PriceDataManager from MultiChainManager")
        else:
            logger.error("❌ Cannot continue without a working PriceDataManager")
            return
    
    # Test token price retrieval for multiple tokens
    results = await test_token_price_retrieval(price_manager, ["BTC", "ETH", "NEAR"])
    
    # Test the complete price retrieval flow for NEAR specifically
    near_price = await test_price_retrieval_flow(price_manager, "NEAR")
    
    # Compare direct API vs. manager for NEAR
    comparison = await test_direct_vs_manager(price_manager, "NEAR")
    
    print_divider("TEST COMPLETED")
    
    # Summarize results
    print("\n📊 SUMMARY OF RESULTS:")
    if results:
        for token, price in results.items():
            status = "✅" if price and price > 0 else "❌"
            print(f"{status} {token}: ${price if price is not None else 'Error'}")
    
    # Show NEAR specific results
    if near_price is not None:
        print(f"\nNEAR specific flow test: {'✅' if near_price > 0 else '❌'} ${near_price}")
    
    # Show comparison results
    if comparison:
        direct_api = comparison.get('direct_api')
        manager = comparison.get('manager')
        print(f"\nComparison - Direct API: ${direct_api if direct_api is not None else 'Error'}")
        print(f"Comparison - Manager: ${manager if manager is not None else 'Error'}")
        
        if direct_api is not None and manager is not None:
            match_status = "✅ Match" if direct_api == manager else "❌ Mismatch"
            print(f"Result: {match_status}")
    
    # Final conclusion
    if results and "NEAR" in results and results["NEAR"] and results["NEAR"] > 0:
        print("\n✅ NEAR price retrieval working correctly")
    else:
        print("\n❌ NEAR price retrieval failing - verify fixes needed")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())

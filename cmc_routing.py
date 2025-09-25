#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🎯 ADVANCED COINMARKETCAP ENDPOINT ROUTING DIAGNOSTIC 🎯

Since your API works but endpoints aren't being called, this diagnostic will:
- Analyze your actual project configuration and routing logic
- Test API Manager provider initialization and selection
- Trace request flow through your centralized routing system
- Identify exactly why CoinMarketCap isn't being called
- Provide actionable fixes for routing and configuration issues
"""

import os
import sys
import time
import inspect
import traceback
import threading
from pathlib import Path
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv

# Add project root to path for imports
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

def advanced_endpoint_diagnostic():
    """Next-level diagnostic that analyzes your actual project architecture"""
    
    print("🎯 ADVANCED COINMARKETCAP ENDPOINT ROUTING DIAGNOSTIC")
    print("=" * 70)
    print("Focus: Analyzing why CoinMarketCap endpoints aren't being called")
    print("despite working API key\n")
    
    # ================================================================
    # STEP 1: PROJECT ARCHITECTURE ANALYSIS
    # ================================================================
    
    print("🏗️  STEP 1: Project Architecture Analysis")
    print("-" * 50)
    
    # Analyze project structure
    project_files = {
        'api_manager.py': 'Centralized routing logic',
        'coinmarketcap_handler.py': 'CoinMarketCap implementation', 
        'coingecko_handler.py': 'CoinGecko implementation',
        'config.py': 'Configuration and token mapping',
        'database.py': 'Data storage and processing',
        '.env': 'Environment variables'
    }
    
    found_files = {}
    for file_name, description in project_files.items():
        if os.path.exists(file_name):
            size_kb = os.path.getsize(file_name) / 1024
            print(f"  ✅ {file_name}: {description} ({size_kb:.1f}KB)")
            found_files[file_name] = description
        else:
            print(f"  ❌ {file_name}: Missing - {description}")
    
    # ================================================================
    # STEP 2: API MANAGER INITIALIZATION DEEP DIVE
    # ================================================================
    
    print(f"\n🔧 STEP 2: API Manager Initialization Analysis")
    print("-" * 50)
    
    try:
        # Import your actual API manager
        from api_manager import create_api_manager, get_api_manager_diagnostics, CryptoAPIManager
        
        print("✅ Successfully imported API manager modules")
        
        # Test environment diagnostics first
        print("\n🔍 Running environment diagnostics...")
        env_diagnostics = get_api_manager_diagnostics()
        
        print("\n📊 Environment Variable Status:")
        for var_name, status in env_diagnostics['environment_variables'].items():
            if status['present']:
                print(f"  ✅ {var_name}: Found (length: {status['length']})")
            else:
                print(f"  ❌ {var_name}: Missing")
        
        # Test API manager creation
        print("\n🚀 Testing API Manager Creation...")
        api_manager = create_api_manager()
        
        print("✅ API Manager created successfully!")
        
        # Analyze provider status in detail
        print(f"\n📋 Provider Status Analysis:")
        for provider_name, status in api_manager.provider_status.items():
            print(f"\n  🔸 {provider_name.upper()}:")
            print(f"     Available: {status['available']}")
            print(f"     Last Check: {time.strftime('%H:%M:%S', time.localtime(status['last_check']))}")
            
            if status['available']:
                print(f"     ✅ Specializations: {', '.join(status.get('specializations', []))}")
                print(f"     ✅ Capabilities: {len(status.get('method_capabilities', {}))} methods")
                print(f"     ✅ Cache Enabled: {status.get('cache_enabled', False)}")
                print(f"     ✅ Quota Tracking: {status.get('quota_tracking', False)}")
            else:
                error = status.get('initialization_error', 'Unknown error')
                print(f"     ❌ Error: {error}")
        
    except ImportError as e:
        print(f"❌ Failed to import API manager: {e}")
        print("   This indicates a module import issue")
        return False
    except Exception as e:
        print(f"❌ API Manager creation failed: {e}")
        print(f"   Traceback: {traceback.format_exc()}")
        return False
    
    # ================================================================
    # STEP 3: REQUEST ROUTING FLOW ANALYSIS
    # ================================================================
    
    print(f"\n🔀 STEP 3: Request Routing Flow Analysis")
    print("-" * 50)
    
    try:
        # Test different request types to see routing behavior
        test_scenarios = [
            {
                'name': 'Bulk Market Data Request',
                'method': 'get_market_data',
                'params': {},
                'expected_provider': 'coinmarketcap',
                'reason': 'CoinMarketCap specializes in bulk operations'
            },
            {
                'name': 'Token by Market Cap Request', 
                'method': 'get_tokens_by_market_cap',
                'params': {'hours': 24},
                'expected_provider': 'mixed',
                'reason': 'May use multiple providers for comprehensive data'
            },
            {
                'name': 'Provider Statistics Request',
                'method': 'get_provider_statistics',
                'params': {},
                'expected_provider': 'internal',
                'reason': 'Internal API manager method'
            }
        ]
        
        routing_analysis = {
            'total_tests': len(test_scenarios),
            'correct_routing': 0,
            'routing_decisions': {},
            'provider_call_counts': {'coingecko': 0, 'coinmarketcap': 0}
        }
        
        for scenario in test_scenarios:
            print(f"\n  🧪 Testing: {scenario['name']}")
            print(f"      Expected Provider: {scenario['expected_provider']}")
            print(f"      Reason: {scenario['reason']}")
            
            # Capture which provider gets called
            original_providers = {}
            call_tracker = {'calls': []}
            
            # Monkey patch to track calls
            for provider_name in ['coingecko', 'coinmarketcap']:
                if provider_name in api_manager.providers:
                    provider = api_manager.providers[provider_name]
                    original_method = getattr(provider, scenario['method'], None)
                    
                    if original_method:
                        def create_tracked_method(orig_method, prov_name):
                            def tracked_method(*args, **kwargs):
                                call_tracker['calls'].append(prov_name)
                                print(f"      🎯 {prov_name.upper()} method called!")
                                # Return mock data to avoid actual API calls
                                return {'mock': True, 'provider': prov_name}
                            return tracked_method
                        
                        setattr(provider, scenario['method'], create_tracked_method(original_method, provider_name))
                        original_providers[provider_name] = original_method
            
            try:
                # Make the test request
                if hasattr(api_manager, scenario['method']):
                    result = getattr(api_manager, scenario['method'])(**scenario['params'])
                    
                    if call_tracker['calls']:
                        actual_provider = call_tracker['calls'][0]
                        routing_analysis['provider_call_counts'][actual_provider] += 1
                        
                        if actual_provider == scenario['expected_provider']:
                            routing_analysis['correct_routing'] += 1
                            print(f"      ✅ Correct routing to {actual_provider}")
                        else:
                            print(f"      ⚠️  Incorrect routing: expected {scenario['expected_provider']}, got {actual_provider}")
                    else:
                        print(f"      ❌ No provider was called!")
                else:
                    print(f"      ❌ Method {scenario['method']} not found in API manager")
            
            except Exception as e:
                print(f"      ❌ Test failed: {e}")
            
            # Restore original methods
            for provider_name, original_method in original_providers.items():
                setattr(api_manager.providers[provider_name], scenario['method'], original_method)
        
        # ================================================================
        # STEP 4: CONFIGURATION ANALYSIS
        # ================================================================
        
        print(f"\n⚙️  STEP 4: Configuration Analysis")
        print("-" * 50)
        
        # Analyze provider specialization config
        print("📊 Provider Specialization Analysis:")
        
        for provider_name, status in api_manager.provider_status.items():
            if status['available']:
                specializations = status.get('specializations', [])
                print(f"\n  🔸 {provider_name.upper()}:")
                if specializations:
                    print(f"      ✅ Specializations: {', '.join(specializations)}")
                    
                    # Check if specializations match expected use cases
                    if provider_name == 'coinmarketcap':
                        expected_specs = ['bulk_data', '7d_historical_data', 'real_time', 'market_overview']
                        missing_specs = [spec for spec in expected_specs if spec not in specializations]
                        if missing_specs:
                            print(f"      ⚠️  Missing expected specializations: {', '.join(missing_specs)}")
                        else:
                            print(f"      ✅ All expected specializations present")
                else:
                    print(f"      ❌ No specializations configured!")
        
        # ================================================================
        # STEP 5: CALL FLOW DEBUGGING
        # ================================================================
        
        print(f"\n🐛 STEP 5: Call Flow Debugging")
        print("-" * 50)
        
        # Test a simple market data request with full debugging
        print("Testing actual market data request with debug tracing...")
        
        # Enable debug mode
        import logging
        logging.basicConfig(level=logging.DEBUG)
        
        try:
            # Make a real request and trace the flow
            print("\n🔍 Making test request: get_market_data() with default params")
            
            # Capture thread info for debugging
            thread_id = threading.current_thread().ident
            process_id = os.getpid()
            print(f"📍 Request context: Thread {thread_id}, Process {process_id}")
            
            # Use proper method signature from your API manager
            result = api_manager.get_market_data()
            
            if result:
                print(f"✅ Request successful! Got {len(result) if isinstance(result, list) else 1} items")
                
                # Check which provider was actually used based on data format
                if isinstance(result, list) and result:
                    sample_item = result[0]
                    if 'quote' in sample_item:
                        print("🎯 Data format suggests CoinMarketCap was used")
                    elif 'current_price' in sample_item:
                        print("🎯 Data format suggests CoinGecko was used")
                    else:
                        print("🤔 Data format is unclear")
            else:
                print("❌ Request failed or returned None")
                
        except Exception as e:
            print(f"❌ Debug request failed: {e}")
            print(f"   Traceback: {traceback.format_exc()}")
        
        # ================================================================
        # STEP 6: RECOMMENDATIONS & FIXES
        # ================================================================
        
        print(f"\n💡 STEP 6: Recommendations & Fixes")
        print("-" * 50)
        
        recommendations = []
        
        # Analyze routing effectiveness
        if routing_analysis['provider_call_counts']['coinmarketcap'] == 0:
            recommendations.append({
                'issue': 'CoinMarketCap never called',
                'severity': 'HIGH',
                'fix': 'Check routing logic in api_manager.py - CoinMarketCap specializations may not be triggering',
                'code_location': 'api_manager.py line ~200-300 (provider selection logic)'
            })
        
        if routing_analysis['correct_routing'] < routing_analysis['total_tests'] * 0.8:
            recommendations.append({
                'issue': 'Poor routing accuracy',
                'severity': 'MEDIUM', 
                'fix': 'Review specialization matching logic and request type detection',
                'code_location': 'api_manager.py CryptoAPIManager._select_provider method'
            })
        
        # Check for configuration issues
        cmc_status = api_manager.provider_status.get('coinmarketcap', {})
        if cmc_status.get('available') and not cmc_status.get('specializations'):
            recommendations.append({
                'issue': 'CoinMarketCap has no specializations configured',
                'severity': 'HIGH',
                'fix': 'Ensure specializations list is properly set during initialization',
                'code_location': 'api_manager.py coinmarketcap initialization section'
            })
        
        print("🎯 Identified Issues & Fixes:")
        for i, rec in enumerate(recommendations, 1):
            print(f"\n  {i}. {rec['issue']} [{rec['severity']}]")
            print(f"     Fix: {rec['fix']}")
            print(f"     Location: {rec['code_location']}")
        
        if not recommendations:
            print("🎉 No critical issues found! CoinMarketCap should be working.")
            print("    If it's still not being called, check your application logs")
            print("    for routing decisions during actual usage.")
        
        # ================================================================
        # FINAL SUMMARY
        # ================================================================
        
        print(f"\n📈 DIAGNOSTIC SUMMARY")
        print("-" * 50)
        print(f"✅ API Manager: Successfully initialized")
        print(f"📊 Provider Status:")
        for name, status in api_manager.provider_status.items():
            symbol = "✅" if status['available'] else "❌"
            print(f"   {symbol} {name}: {status['available']}")
        print(f"🎯 Routing Tests: {routing_analysis['correct_routing']}/{routing_analysis['total_tests']} correct")
        print(f"🔧 Issues Found: {len(recommendations)}")
        
        return len(recommendations) == 0
        
    except Exception as e:
        print(f"❌ Diagnostic failed: {e}")
        print(f"   Full traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Add timing
    start_time = time.time()
    success = advanced_endpoint_diagnostic()
    duration = time.time() - start_time
    
    print(f"\n⏱️  Diagnostic completed in {duration:.2f} seconds")
    if success:
        print("🎉 All systems operational!")
    else:
        print("⚠️  Issues found - see recommendations above")

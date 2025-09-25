#!/usr/bin/env python3
"""
Token Intersection Diagnostic Script
=====================================

This script diagnoses the 4 missing tokens in the intersection between 
reference_tokens and market_data keys by using the existing TokenMappingManager 
analysis methods.

Usage: python token_diagnostic.py
"""

import sys
import json
from pathlib import Path

# Add src directory to Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

def run_token_intersection_diagnostics():
    """
    Run comprehensive diagnostics to identify the 4 missing tokens
    """
    print("🔬 TOKEN INTERSECTION DIAGNOSTIC ANALYSIS")
    print("=" * 60)
    
    try:
        # Import the enhanced classes
        from config import TokenMappingManager
        from api_manager import CryptoAPIManager
        from database import CryptoDatabase
        
        # Initialize components
        print("📋 Initializing components...")
        token_mapper = TokenMappingManager()
        api_manager = CryptoAPIManager()
        db = CryptoDatabase()
        
        print("✅ Components initialized successfully")
        print()
        
        # STEP 1: Get current reference tokens (simulated from bot.py logic)
        print("🎯 STEP 1: ANALYZING REFERENCE TOKENS")
        print("-" * 40)
        
        try:
            # Get reference tokens using the same method as bot.py
            reference_tokens = api_manager.get_tokens_with_recent_data_by_market_cap(hours=24, limit=17)
            print(f"📊 Reference tokens ({len(reference_tokens)}): {reference_tokens}")
        except Exception as ref_error:
            print(f"❌ Error getting reference tokens: {ref_error}")
            # Fallback to hardcoded list for testing
            reference_tokens = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'AVAX', 'DOT', 'UNI', 'NEAR', 'AAVE', 'FIL', 'POL', 'TRUMP', 'KAITO', 'ADA', 'LINK', 'MATIC']
            print(f"📊 Using fallback reference tokens ({len(reference_tokens)}): {reference_tokens}")
        
        print()
        
        # STEP 2: Get market data keys (simulated)
        print("🎯 STEP 2: ANALYZING MARKET DATA KEYS")
        print("-" * 40)
        
        # Simulate market data keys - mix of symbols and IDs as mentioned in the project knowledge
        market_data_keys = [
            'BTC', 'bitcoin',           # BTC has both symbol and ID
            'ETH', 'ethereum',          # ETH has both symbol and ID  
            'SOL', 'XRP', 'BNB',        # These match directly
            'AVAX', 'DOT', 'UNI',       # These match directly
            'NEAR', 'AAVE', 'FIL',      # These match directly
            'POL',                      # This matches directly
            # Missing: TRUMP, KAITO, ADA, LINK - these might be the 4 missing
            'official-trump',           # TRUMP as CoinGecko ID
            'kaito',                    # KAITO as CoinGecko ID
            'cardano',                  # ADA as CoinGecko ID
            'chainlink',                # LINK as CoinGecko ID
        ]
        
        print(f"📊 Market data keys ({len(market_data_keys)}): {market_data_keys}")
        print()
        
        # STEP 3: Enhanced intersection analysis
        print("🎯 STEP 3: ENHANCED INTERSECTION ANALYSIS")
        print("-" * 40)
        
        common_tokens = []
        missing_tokens = []
        conversion_attempts = []
        
        for ref_token in reference_tokens:
            found = False
            conversion_log = {"token": ref_token, "attempts": [], "result": "NOT_FOUND"}
            
            # 1. Direct match
            if ref_token in market_data_keys:
                common_tokens.append(ref_token)
                conversion_log["result"] = "DIRECT_MATCH"
                found = True
            else:
                conversion_log["attempts"].append({"method": "direct_match", "result": "failed"})
            
            if not found:
                # 2. TokenMappingManager symbol-to-coingecko conversion
                try:
                    coingecko_id = token_mapper.symbol_to_coingecko_id(ref_token)
                    conversion_log["attempts"].append({"method": "symbol_to_coingecko_id", "result": coingecko_id})
                    
                    if coingecko_id and coingecko_id in market_data_keys:
                        common_tokens.append(ref_token)
                        conversion_log["result"] = f"COINGECKO_MATCH ({coingecko_id})"
                        found = True
                except Exception as conv_error:
                    conversion_log["attempts"].append({"method": "symbol_to_coingecko_id", "error": str(conv_error)})
            
            if not found:
                # 3. Enhanced database-aware conversion using get_token_from_all_sources
                try:
                    token_info = token_mapper.get_token_from_all_sources(ref_token, 'symbol')
                    conversion_log["attempts"].append({"method": "get_token_from_all_sources", "result": token_info})
                    
                    # Check all possible formats
                    for format_type in ['coingecko_id', 'cmc_slug', 'database_name']:
                        alt_format = token_info.get(format_type)
                        if alt_format and alt_format in market_data_keys:
                            common_tokens.append(ref_token)
                            conversion_log["result"] = f"ENHANCED_MATCH ({format_type}: {alt_format})"
                            found = True
                            break
                            
                except Exception as enhanced_error:
                    conversion_log["attempts"].append({"method": "get_token_from_all_sources", "error": str(enhanced_error)})
            
            if not found:
                # 4. Case-insensitive fallback
                ref_token_upper = ref_token.upper()
                for market_key in market_data_keys:
                    if ref_token_upper == market_key.upper():
                        common_tokens.append(ref_token)
                        conversion_log["result"] = f"CASE_INSENSITIVE_MATCH ({market_key})"
                        found = True
                        break
                
                if not found:
                    conversion_log["attempts"].append({"method": "case_insensitive", "result": "failed"})
            
            if not found:
                missing_tokens.append(ref_token)
            
            conversion_attempts.append(conversion_log)
        
        # STEP 4: Results analysis
        print("🎯 STEP 4: RESULTS ANALYSIS")
        print("-" * 40)
        
        print(f"✅ Common tokens found: {len(common_tokens)}")
        print(f"❌ Missing tokens: {len(missing_tokens)}")
        print(f"📊 Success rate: {len(common_tokens)}/{len(reference_tokens)} ({len(common_tokens)/len(reference_tokens)*100:.1f}%)")
        print()
        
        print("🔍 MISSING TOKENS DETAILED ANALYSIS:")
        for token in missing_tokens:
            print(f"  ❌ {token}")
            # Find the conversion log for this token
            for log in conversion_attempts:
                if log["token"] == token:
                    print(f"     Attempts: {len(log['attempts'])}")
                    for attempt in log["attempts"]:
                        if "error" in attempt:
                            print(f"       - {attempt['method']}: ERROR - {attempt['error']}")
                        else:
                            print(f"       - {attempt['method']}: {attempt.get('result', 'failed')}")
                    break
        print()
        
        # STEP 5: Token coverage analysis using existing methods
        print("🎯 STEP 5: TOKEN COVERAGE ANALYSIS")
        print("-" * 40)
        
        try:
            coverage_analysis = token_mapper.analyze_token_coverage()
            print("📊 Token Coverage Summary:")
            print(f"   Total unique tokens: {coverage_analysis.get('total_unique_tokens', 'N/A')}")
            print(f"   Hardcoded mappings: {coverage_analysis.get('hardcoded_count', 'N/A')}")
            print(f"   CoinGecko database: {coverage_analysis.get('coingecko_db_count', 'N/A')}")
            print(f"   CoinMarketCap database: {coverage_analysis.get('coinmarketcap_db_count', 'N/A')}")
            print()
            
            if 'coverage_percentages' in coverage_analysis:
                print("📊 Coverage Percentages:")
                for source, percentage in coverage_analysis['coverage_percentages'].items():
                    print(f"   {source}: {percentage}%")
            print()
            
            # Check for gaps that might explain missing tokens
            gaps = coverage_analysis.get('coverage_gaps', {})
            print("🔍 Coverage Gaps:")
            for gap_type, tokens in gaps.items():
                if tokens and len(tokens) > 0:
                    print(f"   {gap_type}: {len(tokens)} tokens")
                    # Show missing tokens that are in our reference list
                    missing_in_ref = [t for t in tokens if t in missing_tokens]
                    if missing_in_ref:
                        print(f"     Related to our missing tokens: {missing_in_ref}")
            
        except Exception as coverage_error:
            print(f"❌ Error in coverage analysis: {coverage_error}")
        
        print()
        
        # STEP 6: Recommendations
        print("🎯 STEP 6: RECOMMENDATIONS")
        print("-" * 40)
        
        print("💡 Based on the analysis, here are the recommended fixes:")
        
        if missing_tokens:
            print(f"1. Add missing tokens to hardcoded mappings: {missing_tokens}")
            print("2. Ensure database tables have entries for these tokens")
            print("3. Update market data standardization to handle these specific tokens")
            
        print("4. Consider using the new database-aware TokenMappingManager methods:")
        print("   - database_lookup_symbol_to_coingecko_id()")
        print("   - get_token_from_all_sources()")
        
        return {
            "reference_tokens": reference_tokens,
            "market_data_keys": market_data_keys,
            "common_tokens": common_tokens,
            "missing_tokens": missing_tokens,
            "conversion_attempts": conversion_attempts,
            "success_rate": len(common_tokens)/len(reference_tokens)*100
        }
        
    except Exception as main_error:
        print(f"❌ CRITICAL ERROR: {main_error}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

if __name__ == "__main__":
    print("Starting token intersection diagnostic...")
    print()
    
    results = run_token_intersection_diagnostics()
    
    if results:
        print()
        print("🎯 DIAGNOSTIC COMPLETE")
        print("=" * 60)
        print(f"Success rate: {results['success_rate']:.1f}%")
        print(f"Missing tokens: {results['missing_tokens']}")
        print()
        print("💡 Run this script to identify specific issues with token matching.")
        print("   The output will help pinpoint exactly why certain tokens aren't matching.")
    else:
        print("❌ Diagnostic failed - check error messages above")

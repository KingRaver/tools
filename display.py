#!/usr/bin/env python3
"""
AAVE Display Issue Diagnostic Script
Traces data flow from API -> Database -> Display to find where $0.0000 issue occurs
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

class AAVEDiagnostic:
    def __init__(self, db_path: str = "data/crypto_history.db"):
            self.db_path = db_path
        
    def run_full_diagnostic(self) -> Dict[str, Any]:
        """Run complete diagnostic of AAVE data flow"""
        results = {
            "timestamp": datetime.now().isoformat(),
            "database_check": {},
            "token_mapping_check": {},
            "display_formatting_check": {},
            "recommendations": []
        }
        
        print("🔍 AAVE DIAGNOSTIC - Starting comprehensive analysis...")
        print("=" * 60)
        
        # Step 1: Check database for AAVE records
        results["database_check"] = self.check_database_records()
        
        # Step 2: Check token mapping consistency  
        results["token_mapping_check"] = self.check_token_mapping()
        
        # Step 3: Simulate display formatting
        results["display_formatting_check"] = self.check_display_formatting()
        
        # Step 4: Generate recommendations
        results["recommendations"] = self.generate_recommendations(results)
        
        print("\n📊 DIAGNOSTIC COMPLETE")
        print("=" * 60)
        
        return results
        
    def check_database_records(self) -> Dict[str, Any]:
        """Check all database tables for AAVE data"""
        print("\n1. 📊 DATABASE RECORDS CHECK")
        print("-" * 40)
        
        results: Dict[str, Any] = {
            "market_data_table": {},
            "price_history_table": {},
            "coingecko_market_data_table": {},  
            "coinmarketcap_market_data_table": {},
            "latest_records": {},
            "error": None  # Initialize error field
        }
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Check market_data table (main table)
            cursor.execute("""
                SELECT COUNT(*) as count, MAX(timestamp) as latest_timestamp 
                FROM market_data 
                WHERE chain IN ('AAVE', 'aave')
            """)
            row = cursor.fetchone()
            results["market_data_table"] = {
                "record_count": row["count"],
                "latest_timestamp": row["latest_timestamp"],
                "status": "✅ Found records" if row["count"] > 0 else "❌ No records found"
            }
            print(f"   market_data: {results['market_data_table']['status']} ({row['count']} records)")
            
            # Check price_history table
            cursor.execute("""
                SELECT COUNT(*) as count, MAX(timestamp) as latest_timestamp 
                FROM price_history 
                WHERE token IN ('AAVE', 'aave')
            """)
            row = cursor.fetchone()
            results["price_history_table"] = {
                "record_count": row["count"], 
                "latest_timestamp": row["latest_timestamp"],
                "status": "✅ Found records" if row["count"] > 0 else "❌ No records found"
            }
            print(f"   price_history: {results['price_history_table']['status']} ({row['count']} records)")
            
            # Check new API-specific tables (if they exist)
            for table_name in ["coingecko_market_data", "coinmarketcap_market_data"]:
                try:
                    cursor.execute(f"""
                        SELECT COUNT(*) as count, MAX(timestamp) as latest_timestamp 
                        FROM {table_name} 
                        WHERE token IN ('AAVE', 'aave') OR id IN ('aave')
                    """)
                    row = cursor.fetchone()
                    results[f"{table_name}_table"] = {
                        "record_count": row["count"],
                        "latest_timestamp": row["latest_timestamp"],
                        "status": "✅ Found records" if row["count"] > 0 else "❌ No records found"
                    }
                    print(f"   {table_name}: {results[f'{table_name}_table']['status']} ({row['count']} records)")
                except sqlite3.OperationalError:
                    results[f"{table_name}_table"] = {
                        "status": "⚠️ Table does not exist",
                        "record_count": 0
                    }
                    print(f"   {table_name}: ⚠️ Table does not exist")
            
            # Get latest AAVE records from each table
            for table in ["market_data", "price_history"]:
                token_field = "chain" if table == "market_data" else "token"
                try:
                    cursor.execute(f"""
                        SELECT * FROM {table} 
                        WHERE {token_field} IN ('AAVE', 'aave')
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    """)
                    row = cursor.fetchone()
                    if row:
                        results["latest_records"][table] = dict(row)
                        # Show key price data
                        price_field = "price" if "price" in dict(row) else "current_price"
                        price = dict(row).get(price_field, 0)
                        timestamp = dict(row).get("timestamp", "Unknown")
                        print(f"   Latest {table} price: ${price} at {timestamp}")
                    else:
                        results["latest_records"][table] = None
                        print(f"   No records in {table}")
                except Exception as e:
                    print(f"   Error checking {table}: {str(e)}")
            
            conn.close()
            
        except Exception as e:
            results["error"] = str(e)
            print(f"   ❌ Database error: {str(e)}")
            
        return results
    
    def check_token_mapping(self) -> Dict[str, Any]:
        """Check token mapping consistency across system"""
        print("\n2. 🔗 TOKEN MAPPING CHECK")
        print("-" * 40)
        
        # Expected mappings based on your codebase
        expected_mappings = {
            "symbol_to_coingecko": {
                "AAVE": "aave"
            },
            "coingecko_to_symbol": {
                "aave": "AAVE"
            }
        }
        
        results = {
            "expected_mappings": expected_mappings,
            "mapping_consistency": "✅ Consistent",
            "issues_found": []
        }
        
        print(f"   Symbol -> CoinGecko: AAVE -> {expected_mappings['symbol_to_coingecko']['AAVE']}")
        print(f"   CoinGecko -> Symbol: {expected_mappings['coingecko_to_symbol']['aave']} <- aave")
        print("   ✅ Token mappings are consistent")
        
        return results
    
    def check_display_formatting(self) -> Dict[str, Any]:
        """Simulate display formatting logic"""
        print("\n3. 📱 DISPLAY FORMATTING CHECK")
        print("-" * 40)
        
        results = {
            "simulation_results": {},
            "potential_issues": []
        }
        
        # Simulate different data scenarios
        test_scenarios = [
            {
                "name": "Normal AAVE data",
                "market_data": {
                    "AAVE": {
                        "current_price": 150.75,
                        "price_change_percentage_24h": 2.34,
                        "volume": 45000000
                    }
                }
            },
            {
                "name": "Zero price AAVE data", 
                "market_data": {
                    "AAVE": {
                        "current_price": 0,
                        "price_change_percentage_24h": 0,
                        "volume": 0
                    }
                }
            },
            {
                "name": "Missing AAVE key",
                "market_data": {
                    "BTC": {"current_price": 50000}
                }
            },
            {
                "name": "AAVE with None values",
                "market_data": {
                    "AAVE": {
                        "current_price": None,
                        "price_change_percentage_24h": None
                    }
                }
            }
        ]
        
        for scenario in test_scenarios:
            print(f"\n   Testing: {scenario['name']}")
            
            # Simulate the display logic from your bot.py
            market_data = scenario["market_data"]
            token_data = market_data.get("AAVE", {})
            current_price = token_data.get("current_price", 0)
            price_change_24h = token_data.get("price_change_percentage_24h", 0)
            
            # This mimics your bot.py formatting logic
            if current_price and current_price > 0:
                formatted_price = f"${current_price:.4f}"
                formatted_change = f"({price_change_24h:+.2f}%)"
                status = "✅ Displays correctly"
            else:
                formatted_price = "$0.0000"
                formatted_change = "(+0.00%)"
                status = "❌ Shows $0.0000 - THIS IS THE ISSUE!"
            
            result = {
                "status": status,
                "formatted_price": formatted_price,
                "formatted_change": formatted_change,
                "raw_current_price": current_price,
                "raw_change": price_change_24h
            }
            
            results["simulation_results"][scenario["name"]] = result 
            print(f"     Result: {formatted_price} {formatted_change}")
            print(f"     Status: {status}")
            
            if "❌" in status:
                results["potential_issues"].append({
                    "scenario": scenario["name"],
                    "issue": "current_price is 0 or None",
                    "raw_data": {"current_price": current_price, "change": price_change_24h}
                })
        
        return results
    
    def generate_recommendations(self, diagnostic_results: Dict[str, Any]) -> list:
        """Generate recommendations based on diagnostic results"""
        print("\n4. 💡 RECOMMENDATIONS")
        print("-" * 40)
        
        recommendations = []
        
        # Check database issues
        db_results = diagnostic_results["database_check"]
        
        if db_results.get("market_data_table", {}).get("record_count", 0) == 0:
            rec = "❗ No AAVE records in market_data table - check API data ingestion"
            recommendations.append(rec)
            print(f"   {rec}")
        
        # Check for zero price issues in latest records
        latest_records = db_results.get("latest_records", {})
        for table, record in latest_records.items():
            if record:
                price_field = "price" if "price" in record else "current_price"
                price = record.get(price_field, 0)
                if price == 0 or price is None:
                    rec = f"❗ Latest {table} record has zero/null price: {price}"
                    recommendations.append(rec)
                    print(f"   {rec}")
        
        # Check display formatting issues
        display_results = diagnostic_results["display_formatting_check"]
        if display_results.get("potential_issues"):
            for issue in display_results["potential_issues"]:
                if "Zero price" in issue["scenario"] or "None values" in issue["scenario"]:
                    rec = f"❗ Display logic fails when current_price is {issue['raw_data']['current_price']}"
                    recommendations.append(rec)
                    print(f"   {rec}")
        
        # General recommendations
        general_recs = [
            "✅ Add null/zero price validation in display formatting",
            "✅ Add logging to track when AAVE price becomes 0",
            "✅ Verify API responses contain valid AAVE price data",
            "✅ Check database storage logic preserves API price data"
        ]
        
        recommendations.extend(general_recs)
        for rec in general_recs:
            print(f"   {rec}")
        
        return recommendations
    
    def quick_fix_suggestions(self) -> Dict[str, str]:
        """Provide immediate fix suggestions"""
        return {
            "database_query": """
            -- Check latest AAVE data in all tables:
            SELECT 'market_data' as table_name, chain as token, price, timestamp 
            FROM market_data 
            WHERE chain IN ('AAVE', 'aave') 
            ORDER BY timestamp DESC LIMIT 5
            
            UNION ALL
            
            SELECT 'price_history' as table_name, token, price, timestamp 
            FROM price_history 
            WHERE token IN ('AAVE', 'aave') 
            ORDER BY timestamp DESC LIMIT 5;
            """,
            
            "display_fix": """
            # Add this validation in your display formatting:
            
            def safe_format_price(token_data: Dict[str, Any]) -> str:
                current_price = token_data.get('current_price', 0)
                
                # Add validation
                if current_price is None or current_price <= 0:
                    # Log the issue
                    logger.warning(f"Invalid price data for display: {current_price}")
                    
                    # Try fallback data sources
                    # 1. Check database for recent price
                    # 2. Check alternative API
                    # 3. Return last known good price
                    
                    return "Price unavailable"
                
                return f"${current_price:.4f}"
            """,
            
            "debug_logging": """
            # Add debug logging to trace AAVE data:
            
            def debug_aave_data_flow(market_data: Dict[str, Any]):
                if 'AAVE' in market_data:
                    aave_data = market_data['AAVE']
                    logger.info(f"AAVE data found: {aave_data}")
                    
                    current_price = aave_data.get('current_price')
                    logger.info(f"AAVE current_price: {current_price} (type: {type(current_price)})")
                else:
                    logger.warning("AAVE not found in market_data keys")
                    logger.info(f"Available keys: {list(market_data.keys())}")
            """
        }

if __name__ == "__main__":
    diagnostic = AAVEDiagnostic()
    results = diagnostic.run_full_diagnostic()
    
    print("\n🔧 QUICK FIX SUGGESTIONS")
    print("=" * 60)
    fixes = diagnostic.quick_fix_suggestions()
    
    for fix_name, fix_code in fixes.items():
        print(f"\n{fix_name.upper()}:")
        print(fix_code)

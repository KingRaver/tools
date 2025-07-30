#!/usr/bin/env python3
"""
Market Data Terminology & Mapping Test Script
Tests database field names vs dictionary keys to find mapping issues
"""

import sqlite3
import sys
import os
from typing import Dict, Any, List
from datetime import datetime

class MarketDataTerminologyTester:
    def __init__(self, db_path: str = "data/crypto_history.db"):
        self.db_path = db_path
        
    def run_terminology_tests(self) -> Dict[str, Any]:
        """Run comprehensive terminology and mapping tests"""
        print("🔍 MARKET DATA TERMINOLOGY & MAPPING ANALYSIS")
        print("=" * 80)
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "database_schema": {},
            "field_name_analysis": {},
            "dictionary_mapping_test": {},
            "aave_data_comparison": {},
            "recommendations": []
        }
        
        # Test 1: Database Schema Analysis
        print("\n📊 TEST 1: DATABASE SCHEMA ANALYSIS")
        print("-" * 50)
        results["database_schema"] = self.analyze_database_schema()
        
        # Test 2: Field Name Mismatch Analysis
        print("\n🔗 TEST 2: FIELD NAME MISMATCH ANALYSIS")
        print("-" * 50)
        results["field_name_analysis"] = self.analyze_field_name_mismatches()
        
        # Test 3: Dictionary Mapping Test
        print("\n📱 TEST 3: DICTIONARY MAPPING TEST")
        print("-" * 50)
        results["dictionary_mapping_test"] = self.test_dictionary_mappings()
        
        # Test 4: AAVE Data Comparison
        print("\n🎯 TEST 4: AAVE DATA COMPARISON")
        print("-" * 50)
        results["aave_data_comparison"] = self.compare_aave_data_formats()
        
        # Test 5: Generate Recommendations
        print("\n💡 TEST 5: RECOMMENDATIONS")
        print("-" * 50)
        results["recommendations"] = self.generate_terminology_recommendations(results)
        
        return results
    
    def analyze_database_schema(self) -> Dict[str, Any]:
        """Analyze database table schemas to identify field names"""
        schema_analysis = {}
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get relevant tables
            relevant_tables = ['market_data', 'price_history', 'coingecko_market_data', 'coinmarketcap_market_data']
            
            for table in relevant_tables:
                try:
                    # Get table schema
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = cursor.fetchall()
                    
                    if columns:
                        table_info = {
                            "columns": [{"name": col["name"], "type": col["type"]} for col in columns],
                            "token_column": None,
                            "price_column": None,
                            "timestamp_column": None
                        }
                        
                        # Identify key columns
                        for col in columns:
                            col_name = col["name"].lower()
                            if col_name in ['token', 'chain', 'symbol', 'id']:
                                table_info["token_column"] = col["name"]
                            elif col_name in ['price', 'current_price', 'value']:
                                table_info["price_column"] = col["name"]
                            elif col_name in ['timestamp', 'created_at', 'date']:
                                table_info["timestamp_column"] = col["name"]
                        
                        schema_analysis[table] = table_info
                        
                        print(f"📋 {table}:")
                        print(f"   Token column: {table_info['token_column']}")
                        print(f"   Price column: {table_info['price_column']}")
                        print(f"   Timestamp column: {table_info['timestamp_column']}")
                        print(f"   Total columns: {len(columns)}")
                        
                except sqlite3.OperationalError:
                    schema_analysis[table] = {"status": "table_not_found"}
                    print(f"❌ {table}: Table not found")
            
            conn.close()
            
        except Exception as e:
            schema_analysis["error"] = str(e)
            print(f"❌ Database error: {str(e)}")
        
        return schema_analysis
    
    def analyze_field_name_mismatches(self) -> Dict[str, Any]:
        """Analyze potential field name mismatches"""
        
        # Expected field names in code vs database
        field_mappings = {
            "token_field": {
                "code_expects": "token",  # Dictionary key expected by code
                "database_has": ["chain", "token", "symbol", "id"],  # Possible DB columns
                "mismatch_risk": "HIGH"
            },
            "price_field": {
                "code_expects": "current_price",  # Dictionary key expected by code  
                "database_has": ["price", "current_price", "value"],  # Possible DB columns
                "mismatch_risk": "HIGH"
            },
            "change_field": {
                "code_expects": "price_change_percentage_24h",
                "database_has": ["price_change_24h", "price_change_percentage_24h"],
                "mismatch_risk": "MEDIUM"
            },
            "volume_field": {
                "code_expects": "volume",
                "database_has": ["volume", "total_volume"],
                "mismatch_risk": "LOW"
            }
        }
        
        analysis = {
            "field_mappings": field_mappings,
            "potential_issues": []
        }
        
        print("🔍 Field Name Mismatch Analysis:")
        for field_type, mapping in field_mappings.items():
            print(f"\n   {field_type}:")
            print(f"     Code expects: '{mapping['code_expects']}'")
            print(f"     Database has: {mapping['database_has']}")
            print(f"     Risk level: {mapping['mismatch_risk']}")
            
            if mapping["mismatch_risk"] == "HIGH":
                analysis["potential_issues"].append({
                    "field": field_type,
                    "issue": f"Code expects '{mapping['code_expects']}' but database might use different column names",
                    "risk": mapping["mismatch_risk"]
                })
        
        return analysis
    
    def test_dictionary_mappings(self) -> Dict[str, Any]:
        """Test how database data should be mapped to dictionary format"""
        
        test_results = {
            "database_format": {},
            "expected_dictionary_format": {},
            "conversion_needed": {}
        }
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get sample AAVE data from database
            cursor.execute("""
                SELECT * FROM market_data 
                WHERE chain = 'AAVE' 
                ORDER BY timestamp DESC 
                LIMIT 1
            """)
            
            row = cursor.fetchone()
            if row:
                # Database format (what we actually have)
                db_record = dict(row)
                test_results["database_format"] = db_record
                
                print("📊 Database Record Format:")
                for key, value in db_record.items():
                    print(f"   {key}: {value}")
                
                # Expected dictionary format (what code expects)
                expected_format = {
                    "AAVE": {  # Token as top-level key
                        "current_price": db_record.get("price"),  # Map 'price' -> 'current_price'
                        "price_change_percentage_24h": db_record.get("price_change_24h"),
                        "volume": db_record.get("volume"),
                        "market_cap": db_record.get("market_cap"),
                        "symbol": "AAVE"
                    }
                }
                
                test_results["expected_dictionary_format"] = expected_format
                
                print("\n📱 Expected Dictionary Format:")
                print(f"   {expected_format}")
                
                # Identify conversion needed
                conversions = {
                    "structure": "Flat DB record -> Nested dictionary with token as key",
                    "field_mappings": {
                        "chain": "token_key",  # 'chain' becomes the dictionary key
                        "price": "current_price",  # 'price' -> 'current_price' 
                        "price_change_24h": "price_change_percentage_24h"  # Potential mismatch
                    }
                }
                
                test_results["conversion_needed"] = conversions
                
                print("\n🔄 Conversion Needed:")
                print(f"   Structure: {conversions['structure']}")
                print("   Field mappings:")
                for db_field, dict_field in conversions["field_mappings"].items():
                    print(f"     '{db_field}' -> '{dict_field}'")
            
            conn.close()
            
        except Exception as e:
            test_results["error"] = {"error_message": str(e), "error_type": type(e).__name__}
            print(f"❌ Database query error: {str(e)}")
        
        return test_results
    
    def compare_aave_data_formats(self) -> Dict[str, Any]:
        """Compare AAVE data in different expected formats"""
        
        comparison = {
            "raw_database": {},
            "market_data_format": {},
            "prediction_input_format": {},
            "missing_conversions": []
        }
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get raw database format
            cursor.execute("""
                SELECT chain, price, volume, market_cap, price_change_24h, timestamp
                FROM market_data 
                WHERE chain = 'AAVE' 
                ORDER BY timestamp DESC 
                LIMIT 1
            """)
            
            row = cursor.fetchone()
            if row:
                raw_data = dict(row)
                comparison["raw_database"] = raw_data
                
                print("📊 Raw Database Format:")
                print(f"   {raw_data}")
                
                # How it should look for market_data parameter
                market_data_format = {
                    "AAVE": {
                        "current_price": raw_data["price"],
                        "price_change_percentage_24h": raw_data["price_change_24h"], 
                        "volume": raw_data["volume"],
                        "market_cap": raw_data["market_cap"],
                        "symbol": "AAVE"
                    }
                }
                comparison["market_data_format"] = market_data_format
                
                print("\n📈 Market Data Parameter Format:")
                print(f"   {market_data_format}")
                
                # How prediction function expects to access it
                prediction_access = {
                    "token_data_access": "market_data.get('AAVE', {})",
                    "price_access": "token_data.get('current_price', 0)",
                    "change_access": "token_data.get('price_change_percentage_24h', 0)"
                }
                comparison["prediction_input_format"] = prediction_access
                
                print("\n🎯 Prediction Function Access Pattern:")
                for access_type, pattern in prediction_access.items():
                    print(f"   {access_type}: {pattern}")
                
                # Check for missing conversions
                if raw_data["price"] != market_data_format["AAVE"]["current_price"]:
                    comparison["missing_conversions"].append("price -> current_price mapping")
                
                if not market_data_format["AAVE"]["current_price"]:
                    comparison["missing_conversions"].append("NULL price value in database")
            
            conn.close()
            
        except Exception as e:
            comparison["error"] = str(e)
            print(f"❌ Comparison error: {str(e)}")
        
        return comparison
    
    def generate_terminology_recommendations(self, test_results: Dict[str, Any]) -> List[str]:
        """Generate specific recommendations based on test results"""
        recommendations = []
        
        # Check database schema issues
        schema = test_results.get("database_schema", {})
        if "market_data" in schema:
            table_info = schema["market_data"]
            if table_info.get("token_column") == "chain":
                recommendations.append("🔧 Database uses 'chain' as token column, code expects 'token' key")
            if table_info.get("price_column") == "price":
                recommendations.append("🔧 Database uses 'price' column, code expects 'current_price' key")
        
        # Check field mapping issues
        field_analysis = test_results.get("field_name_analysis", {})
        for issue in field_analysis.get("potential_issues", []):
            if issue["risk"] == "HIGH":
                recommendations.append(f"❗ HIGH RISK: {issue['issue']}")
        
        # Check dictionary mapping
        dict_test = test_results.get("dictionary_mapping_test", {})
        if "conversion_needed" in dict_test:
            recommendations.append("🔄 Add conversion logic: Database records -> Dictionary format")
        
        # Check AAVE data comparison
        aave_comparison = test_results.get("aave_data_comparison", {})
        for missing in aave_comparison.get("missing_conversions", []):
            recommendations.append(f"⚠️ Missing conversion: {missing}")
        
        # General recommendations
        recommendations.extend([
            "✅ Create standard field mapping function",
            "✅ Add database-to-dictionary conversion layer", 
            "✅ Validate all field name assumptions",
            "✅ Test with actual market_data parameter"
        ])
        
        print("💡 Recommendations:")
        for i, rec in enumerate(recommendations, 1):
            print(f"   {i}. {rec}")
        
        return recommendations
    
    def generate_conversion_function(self) -> str:
        """Generate a suggested conversion function"""
        return """
# SUGGESTED CONVERSION FUNCTION:

def convert_database_to_market_data(db_records: List[Dict]) -> Dict[str, Any]:
    '''Convert database records to market_data dictionary format'''
    market_data = {}
    
    for record in db_records:
        # Get token symbol (handle both 'chain' and 'token' columns)
        token = record.get('chain') or record.get('token') or record.get('symbol')
        
        if token:
            market_data[token.upper()] = {
                'current_price': record.get('price', 0),  # Map 'price' -> 'current_price'
                'price_change_percentage_24h': record.get('price_change_24h', 0),
                'volume': record.get('volume', 0),
                'market_cap': record.get('market_cap', 0),
                'symbol': token.upper()
            }
    
    return market_data

# USAGE:
# db_records = fetch_from_database()
# market_data = convert_database_to_market_data(db_records)
# prediction = _generate_predictions('AAVE', market_data, '1h')
        """

if __name__ == "__main__":
    # Handle command line args
    db_path = "data/crypto_history.db"
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    if not os.path.exists(db_path):
        print(f"❌ Database file not found: {db_path}")
        print("💡 Usage: python3 terminology_test.py [database_path]")
        sys.exit(1)
    
    tester = MarketDataTerminologyTester(db_path)
    results = tester.run_terminology_tests()
    
    print("\n🔧 SUGGESTED CONVERSION FUNCTION")
    print("=" * 80)
    print(tester.generate_conversion_function())
    
    print(f"\n✅ ANALYSIS COMPLETE - Check results for field mapping issues!")

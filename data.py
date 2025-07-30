#!/usr/bin/env python3
"""
Database Structure Discovery Script
Analyzes all .db files to find AAVE data and correct table structure
"""

import sqlite3
import os
from typing import Dict, List, Any

class DatabaseDiscovery:
    def __init__(self):
        self.db_files = [
            "./nonexistent.db",
            "./crypto_history.db", 
            "./data/crypto_history.db",
            "./database.db",
            "./src/data/crypto_history.db"
        ]
    
    def discover_all_databases(self) -> Dict[str, Any]:
        """Discover structure of all database files"""
        print("🔍 DATABASE STRUCTURE DISCOVERY")
        print("=" * 60)
        
        results = {}
        
        for db_file in self.db_files:
            print(f"\n📊 Analyzing: {db_file}")
            print("-" * 40)
            
            if not os.path.exists(db_file):
                print(f"   ❌ File does not exist")
                results[db_file] = {"status": "does_not_exist"}
                continue
            
            try:
                results[db_file] = self.analyze_database(db_file)
            except Exception as e:
                print(f"   ❌ Error analyzing database: {str(e)}")
                results[db_file] = {"status": "error", "error": str(e)}
        
        return results
    
    def analyze_database(self, db_file: str) -> Dict[str, Any]:
        """Analyze a single database file"""
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row["name"] for row in cursor.fetchall()]
        
        print(f"   📋 Tables found: {tables}")
        
        analysis = {
            "status": "analyzed",
            "tables": tables,
            "table_details": {},
            "aave_data_found": False,
            "aave_locations": []
        }
        
        # Analyze each table
        for table in tables:
            try:
                table_info = self.analyze_table(cursor, table)
                analysis["table_details"][table] = table_info
                
                # Check for AAVE data
                if table_info["aave_records"] > 0:
                    analysis["aave_data_found"] = True
                    analysis["aave_locations"].append({
                        "table": table,
                        "records": table_info["aave_records"],
                        "latest_price": table_info["latest_aave_price"],
                        "latest_timestamp": table_info["latest_aave_timestamp"]
                    })
                    
                    print(f"   ✅ AAVE data found in {table}: {table_info['aave_records']} records")
                    print(f"      Latest price: ${table_info['latest_aave_price']}")
                    print(f"      Latest timestamp: {table_info['latest_aave_timestamp']}")
                
            except Exception as e:
                print(f"   ⚠️ Error analyzing table {table}: {str(e)}")
                analysis["table_details"][table] = {"error": str(e)}
        
        conn.close()
        return analysis
    
    def analyze_table(self, cursor, table: str) -> Dict[str, Any]:
        """Analyze a single table for structure and AAVE data"""
        
        # Get table schema
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [{"name": row["name"], "type": row["type"]} for row in cursor.fetchall()]
        
        # Get total record count
        cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
        total_records = cursor.fetchone()["count"]
        
        # Look for AAVE data - try different possible column combinations
        aave_records = 0
        latest_aave_price = None
        latest_aave_timestamp = None
        
        # Try different ways to find AAVE data
        possible_token_columns = ["token", "chain", "symbol", "id"]
        possible_price_columns = ["price", "current_price", "value"]
        possible_timestamp_columns = ["timestamp", "created_at", "date"]
        
        for token_col in possible_token_columns:
            if any(col["name"] == token_col for col in columns):
                try:
                    # Count AAVE records
                    cursor.execute(f"""
                        SELECT COUNT(*) as count 
                        FROM {table} 
                        WHERE {token_col} IN ('AAVE', 'aave')
                    """)
                    aave_records = cursor.fetchone()["count"]
                    
                    if aave_records > 0:
                        # Get latest AAVE data
                        price_col = None
                        timestamp_col = None
                        
                        # Find price column
                        for price_col_name in possible_price_columns:
                            if any(col["name"] == price_col_name for col in columns):
                                price_col = price_col_name
                                break
                        
                        # Find timestamp column  
                        for ts_col_name in possible_timestamp_columns:
                            if any(col["name"] == ts_col_name for col in columns):
                                timestamp_col = ts_col_name
                                break
                        
                        # Build query for latest AAVE data
                        select_fields = [token_col]
                        if price_col:
                            select_fields.append(price_col)
                        if timestamp_col:
                            select_fields.append(timestamp_col)
                        
                        order_by = f"ORDER BY {timestamp_col} DESC" if timestamp_col else ""
                        
                        query = f"""
                            SELECT {', '.join(select_fields)}
                            FROM {table} 
                            WHERE {token_col} IN ('AAVE', 'aave')
                            {order_by}
                            LIMIT 1
                        """
                        
                        cursor.execute(query)
                        latest_row = cursor.fetchone()
                        
                        if latest_row:
                            if price_col:
                                latest_aave_price = latest_row[price_col]
                            if timestamp_col:
                                latest_aave_timestamp = latest_row[timestamp_col]
                        
                        break
                        
                except Exception as e:
                    # Column doesn't exist or query failed, continue trying
                    continue
        
        return {
            "columns": columns,
            "total_records": total_records,
            "aave_records": aave_records,
            "latest_aave_price": latest_aave_price,
            "latest_aave_timestamp": latest_aave_timestamp
        }
    
    def generate_fix_recommendations(self, discovery_results: Dict[str, Any]) -> List[str]:
        """Generate specific fix recommendations based on discovery"""
        recommendations = []
        
        # Find databases with AAVE data
        databases_with_aave = []
        for db_file, analysis in discovery_results.items():
            if isinstance(analysis, dict) and analysis.get("aave_data_found"):
                databases_with_aave.append((db_file, analysis))
        
        if not databases_with_aave:
            recommendations.append("❌ NO AAVE DATA FOUND in any database!")
            recommendations.append("   → Check if your API ingestion is working")
            recommendations.append("   → Verify database connection in your bot code")
            return recommendations
        
        # Found AAVE data - provide specific fix
        print(f"\n🎯 AAVE DATA FOUND!")
        print("=" * 40)
        
        for db_file, analysis in databases_with_aave:
            print(f"\n📊 Database: {db_file}")
            for location in analysis["aave_locations"]:
                table = location["table"]
                records = location["records"]
                price = location["latest_price"]
                timestamp = location["latest_timestamp"]
                
                print(f"   Table: {table}")
                print(f"   Records: {records}")
                print(f"   Latest Price: ${price}")
                print(f"   Latest Timestamp: {timestamp}")
                
                recommendations.append(f"✅ Update your display code to read from: {db_file} -> {table}")
                
                # Generate specific code fix
                if price and price > 0:
                    recommendations.append(f"✅ Valid AAVE price found: ${price}")
                else:
                    recommendations.append(f"⚠️ AAVE price is {price} - check data quality")
        
        return recommendations
    
    def generate_code_fix(self, discovery_results: Dict[str, Any]) -> str:
        """Generate the actual code fix"""
        
        # Find the best database/table with AAVE data
        best_db = None
        best_table = None
        
        for db_file, analysis in discovery_results.items():
            if isinstance(analysis, dict) and analysis.get("aave_data_found"):
                for location in analysis["aave_locations"]:
                    if location["latest_price"] and location["latest_price"] > 0:
                        best_db = db_file
                        best_table = location["table"]
                        break
                if best_db:
                    break
        
        if not best_db:
            return "# No valid AAVE data found - fix API ingestion first"
        
        return f"""
# UPDATE YOUR DISPLAY CODE TO USE THE CORRECT DATABASE/TABLE:

# OLD (broken):
# conn = sqlite3.connect("database.db")  # Wrong DB or missing table
# cursor.execute("SELECT * FROM market_data WHERE chain = 'AAVE'")  # Wrong table

# NEW (working):
conn = sqlite3.connect("{best_db}")
cursor.execute("SELECT * FROM {best_table} WHERE token = 'AAVE' ORDER BY timestamp DESC LIMIT 1")

# This should return valid AAVE price data for your display
        """

if __name__ == "__main__":
    discovery = DatabaseDiscovery()
    results = discovery.discover_all_databases()
    
    print("\n🎯 SUMMARY & RECOMMENDATIONS")
    print("=" * 60)
    
    recommendations = discovery.generate_fix_recommendations(results)
    for rec in recommendations:
        print(rec)
    
    print("\n🔧 CODE FIX")
    print("=" * 60)
    code_fix = discovery.generate_code_fix(results)
    print(code_fix)

#!/usr/bin/env python3
"""
Comprehensive Token Data Analysis Script
- Verifies all 14 original tokens + 100 current tokens
- Checks database storage and pathways
- Shows latest prices and timestamps
- Identifies database connections across project files
"""

import sqlite3
import os
import glob
from typing import Dict, List, Any, Optional
from datetime import datetime

class ComprehensiveTokenAnalysis:
    def __init__(self):
        self.db_files = [
            "./nonexistent.db",
            "./crypto_history.db", 
            "./data/crypto_history.db",
            "./database.db",
            "./src/data/crypto_history.db"
        ]
        
        # Original 14 tokens that should be working
        self.original_14_tokens = [
            'BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'AVAX', 'DOT', 
            'UNI', 'NEAR', 'AAVE', 'FIL', 'POL', 'KAITO', 'TRUMP'
        ]
        
        # CoinGecko ID mappings
        self.token_to_coingecko = {
            'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana', 'XRP': 'ripple',
            'BNB': 'binancecoin', 'AVAX': 'avalanche-2', 'DOT': 'polkadot', 
            'UNI': 'uniswap', 'NEAR': 'near', 'AAVE': 'aave', 'FIL': 'filecoin', 
            'POL': 'matic-network', 'KAITO': 'kaito', 'TRUMP': 'official-trump'
        }
        
        # Initialize database analysis storage
        self.database_analysis: Dict[str, Any] = {}
    
    def run_comprehensive_analysis(self) -> Dict[str, Any]:
        """Run complete analysis of token data across all databases"""
        print("🔍 COMPREHENSIVE TOKEN DATA ANALYSIS")
        print("=" * 80)
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "database_analysis": {},
            "token_coverage": {},
            "database_connections_in_code": {},
            "recommendations": []
        }
        
        # Step 1: Analyze all databases
        print("\n📊 STEP 1: DATABASE ANALYSIS")
        print("-" * 50)
        self.database_analysis = self.analyze_all_databases()
        results["database_analysis"] = self.database_analysis
        
        # Step 2: Check token coverage across original 14
        print("\n🎯 STEP 2: TOKEN COVERAGE ANALYSIS")
        print("-" * 50)
        results["token_coverage"] = self.analyze_token_coverage()
        
        # Step 3: Find database connections in code
        print("\n🔗 STEP 3: DATABASE CONNECTIONS IN PROJECT FILES")
        print("-" * 50)
        results["database_connections_in_code"] = self.find_database_connections_in_code()
        
        # Step 4: Generate recommendations
        print("\n💡 STEP 4: ANALYSIS & RECOMMENDATIONS")
        print("-" * 50)
        results["recommendations"] = self.generate_comprehensive_recommendations(results)
        
        return results
    
    def analyze_all_databases(self) -> Dict[str, Any]:
        """Analyze all database files for token data"""
        analysis = {}
        
        for db_file in self.db_files:
            print(f"\n📋 Analyzing: {db_file}")
            
            if not os.path.exists(db_file):
                print(f"   ❌ File does not exist")
                analysis[db_file] = {"status": "does_not_exist"}
                continue
            
            try:
                analysis[db_file] = self.analyze_single_database(db_file)
            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
                analysis[db_file] = {"status": "error", "error": str(e)}
        
        return analysis
    
    def analyze_single_database(self, db_file: str) -> Dict[str, Any]:
        """Detailed analysis of a single database"""
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row["name"] for row in cursor.fetchall()]
        
        print(f"   📋 Tables: {len(tables)} found - {tables}")
        
        analysis = {
            "status": "analyzed",
            "tables": tables,
            "token_data": {},
            "total_tokens_found": 0,
            "original_14_coverage": {},
            "latest_data_timestamps": {}
        }
        
        # Check token data in relevant tables
        relevant_tables = ['market_data', 'price_history']
        
        for table in relevant_tables:
            if table in tables:
                token_data = self.get_token_data_from_table(cursor, table)
                analysis["token_data"][table] = token_data
                
                print(f"   📊 {table}:")
                print(f"      Total unique tokens: {len(token_data)}")
                
                # Check coverage of original 14 tokens
                original_14_found = []
                for token in self.original_14_tokens:
                    if token in token_data:
                        original_14_found.append(token)
                        latest_price = token_data[token].get('latest_price', 0)
                        latest_timestamp = token_data[token].get('latest_timestamp', 'Unknown')
                        print(f"      ✅ {token}: ${latest_price} at {latest_timestamp}")
                    else:
                        print(f"      ❌ {token}: NOT FOUND")
                
                analysis["original_14_coverage"][table] = {
                    "found": original_14_found,
                    "missing": [t for t in self.original_14_tokens if t not in original_14_found],
                    "coverage_percentage": len(original_14_found) / len(self.original_14_tokens) * 100
                }
                
                print(f"      📈 Original 14 coverage: {len(original_14_found)}/14 ({analysis['original_14_coverage'][table]['coverage_percentage']:.1f}%)")
        
        conn.close()
        return analysis
    
    def get_token_data_from_table(self, cursor, table: str) -> Dict[str, Any]:
        """Extract token data from a specific table"""
        token_data = {}
        
        # Determine column names for this table
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [row["name"] for row in cursor.fetchall()]
        
        # Find the right column names
        token_column = None
        price_column = None
        timestamp_column = None
        
        # Token column
        for col in ['token', 'chain', 'symbol', 'id']:
            if col in columns:
                token_column = col
                break
        
        # Price column  
        for col in ['price', 'current_price', 'value']:
            if col in columns:
                price_column = col
                break
        
        # Timestamp column
        for col in ['timestamp', 'created_at', 'date']:
            if col in columns:
                timestamp_column = col
                break
        
        if not token_column:
            return {}
        
        # Get all tokens in this table
        try:
            query = f"SELECT DISTINCT {token_column} FROM {table}"
            cursor.execute(query)
            all_tokens = [row[0] for row in cursor.fetchall()]
            
            # Get latest data for each token
            for token in all_tokens:
                if token and token.strip():
                    # Only call if we have valid column names
                    if token_column and price_column is not None and timestamp_column is not None:
                        latest_data = self.get_latest_token_data(cursor, table, token, token_column, price_column, timestamp_column)
                    elif token_column and price_column is not None:
                        latest_data = self.get_latest_token_data(cursor, table, token, token_column, price_column, "")
                    elif token_column and timestamp_column is not None:
                        latest_data = self.get_latest_token_data(cursor, table, token, token_column, "", timestamp_column)
                    else:
                        latest_data = self.get_latest_token_data(cursor, table, token, token_column, "", "")
                    
                    if latest_data:
                        token_data[token] = latest_data
            
        except Exception as e:
            print(f"      ⚠️ Error querying {table}: {str(e)}")
        
        return token_data
    
    def get_latest_token_data(self, cursor, table: str, token: str, token_col: str, price_col: Optional[str] = None, timestamp_col: Optional[str] = None) -> Dict[str, Any]:
        """Get latest data for a specific token"""
        try:
            select_fields = [token_col]
            if price_col:
                select_fields.append(price_col)
            if timestamp_col:
                select_fields.append(timestamp_col)
            
            order_by = f"ORDER BY {timestamp_col} DESC" if timestamp_col else ""
            
            query = f"""
                SELECT {', '.join(select_fields)}
                FROM {table} 
                WHERE {token_col} = ?
                {order_by}
                LIMIT 1
            """
            
            cursor.execute(query, (token,))
            row = cursor.fetchone()
            
            if row:
                data = {
                    'token': token,
                    'latest_price': row[price_col] if price_col and price_col in row.keys() else None,
                    'latest_timestamp': row[timestamp_col] if timestamp_col and timestamp_col in row.keys() else None,
                    'table': table
                }
                return data
            
        except Exception as e:
            print(f"         Error getting data for {token}: {str(e)}")
        
        return {}
    
    def analyze_token_coverage(self) -> Dict[str, Any]:
        """Analyze coverage of original 14 tokens across all databases"""
        coverage_summary = {
            "original_14_status": {},
            "best_database": None,
            "missing_tokens": [],
            "data_quality_issues": []
        }
        
        print("📊 Original 14 Token Coverage Summary:")
        print("-" * 40)
        
        # Find the database with the best coverage
        best_db = None
        best_coverage = 0
        
        for db_file, analysis in self.database_analysis.items():
            if analysis.get("status") == "analyzed":
                for table, table_coverage in analysis.get("original_14_coverage", {}).items():
                    coverage_pct = table_coverage.get("coverage_percentage", 0)
                    if coverage_pct > best_coverage:
                        best_coverage = coverage_pct
                        best_db = f"{db_file} -> {table}"
        
        coverage_summary["best_database"] = best_db
        coverage_summary["best_coverage_percentage"] = best_coverage
        
        print(f"🎯 Best coverage: {best_db} ({best_coverage:.1f}%)")
        
        # Analyze each token
        for token in self.original_14_tokens:
            token_status = {
                "found_in": [],
                "latest_prices": [],
                "latest_timestamps": [],
                "status": "missing"
            }
            
            # Check all databases for this token
            for db_file, analysis in self.database_analysis.items():
                if analysis.get("status") == "analyzed":
                    for table, token_data in analysis.get("token_data", {}).items():
                        if token in token_data:
                            token_status["found_in"].append(f"{db_file}->{table}")
                            token_status["latest_prices"].append(token_data[token].get("latest_price"))
                            token_status["latest_timestamps"].append(token_data[token].get("latest_timestamp"))
                            token_status["status"] = "found"
            
            coverage_summary["original_14_status"][token] = token_status
            
            # Print status
            if token_status["status"] == "found":
                latest_price = token_status["latest_prices"][0] if token_status["latest_prices"] else "None"
                latest_timestamp = token_status["latest_timestamps"][0] if token_status["latest_timestamps"] else "None"
                print(f"   ✅ {token}: ${latest_price} ({latest_timestamp})")
                
                # Check for data quality issues
                if latest_price is None or latest_price == 0:
                    coverage_summary["data_quality_issues"].append(f"{token}: Price is {latest_price}")
            else:
                print(f"   ❌ {token}: NOT FOUND")
                coverage_summary["missing_tokens"].append(token)
        
        return coverage_summary
    
    def find_database_connections_in_code(self) -> Dict[str, Any]:
        """Find all database connections in project files"""
        connections = {
            "files_with_db_connections": [],
            "database_paths_found": [],
            "connection_patterns": []
        }
        
        # Search patterns for database connections
        patterns = [
            "sqlite3.connect",
            ".db",
            "database.db",
            "crypto_history.db",
            "db_path",
            "Database("
        ]
        
        # Search in Python files
        python_files = glob.glob("**/*.py", recursive=True)
        
        for file_path in python_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    file_connections = []
                    for pattern in patterns:
                        if pattern in content:
                            # Find the specific lines
                            lines = content.split('\n')
                            for i, line in enumerate(lines):
                                if pattern in line and not line.strip().startswith('#'):
                                    file_connections.append({
                                        "line_number": i + 1,
                                        "line_content": line.strip(),
                                        "pattern": pattern
                                    })
                    
                    if file_connections:
                        connections["files_with_db_connections"].append({
                            "file": file_path,
                            "connections": file_connections
                        })
                        
                        print(f"🔗 {file_path}:")
                        for conn in file_connections:
                            print(f"   Line {conn['line_number']}: {conn['line_content']}")
            
            except Exception as e:
                print(f"   ⚠️ Error reading {file_path}: {str(e)}")
        
        return connections
    
    def generate_comprehensive_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate comprehensive recommendations based on full analysis"""
        recommendations = []
        
        # Database path recommendations
        db_analysis = results.get("database_analysis", {})
        best_db = None
        best_token_count = 0
        
        for db_file, analysis in db_analysis.items():
            if analysis.get("status") == "analyzed":
                token_data = analysis.get("token_data", {})
                for table, tokens in token_data.items():
                    if len(tokens) > best_token_count:
                        best_token_count = len(tokens)
                        best_db = f"{db_file} -> {table}"
        
        if best_db:
            recommendations.append(f"✅ PRIMARY DATABASE: Use {best_db} (has {best_token_count} tokens)")
        else:
            recommendations.append("❌ NO VALID DATABASE FOUND - Check API data ingestion")
        
        # Token coverage recommendations
        token_coverage = results.get("token_coverage", {})
        missing_tokens = token_coverage.get("missing_tokens", [])
        
        if missing_tokens:
            recommendations.append(f"❗ MISSING TOKENS: {', '.join(missing_tokens)} - Check API token list")
        
        data_issues = token_coverage.get("data_quality_issues", [])
        if data_issues:
            recommendations.append(f"⚠️ DATA QUALITY ISSUES: {'; '.join(data_issues)}")
        
        # Code connection recommendations
        db_connections = results.get("database_connections_in_code", {})
        files_with_connections = db_connections.get("files_with_db_connections", [])
        
        if files_with_connections:
            recommendations.append(f"🔧 UPDATE DATABASE PATHS in {len(files_with_connections)} files")
            for file_info in files_with_connections:
                recommendations.append(f"   → {file_info['file']}")
        
        return recommendations

if __name__ == "__main__":
    analyzer = ComprehensiveTokenAnalysis()
    
    # Run full analysis
    results = analyzer.run_comprehensive_analysis()
    
    print("\n🎯 FINAL SUMMARY")
    print("=" * 80)
    for rec in results["recommendations"]:
        print(rec)

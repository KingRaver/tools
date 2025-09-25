#!/usr/bin/env python3
"""
🔍 DATA FLOW DIAGNOSTIC TOOL
Comprehensive tracing of CoinGecko vs CoinMarketCap data attribution issue

This tool traces:
1. Raw data storage in dedicated tables
2. combine_market_data_sources processing
3. Analysis function data retrieval
4. Source attribution and logging
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


class DataFlowDiagnostic:
    def __init__(self, db_path: str = "data/crypto_history.db"):
        """Initialize diagnostic tool with database connection"""
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        
        # Connect to database
        try:
            self.conn = sqlite3.connect(db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            print(f"✅ Connected to database: {db_path}")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            raise

    def _ensure_connection(self) -> bool:
        """Ensure database connection is active"""
        if self.conn is None or self.cursor is None:
            print("❌ Database connection not available")
            return False
        return True

    def __del__(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    def run_comprehensive_diagnostic(self) -> Dict[str, Any]:
        """Run complete data flow diagnostic"""
        print("\n" + "="*80)
        print("🔍 COMPREHENSIVE DATA FLOW DIAGNOSTIC")
        print("="*80)
        
        diagnostic_results = {
            'database_tables': self.analyze_database_tables(),
            'raw_data_analysis': self.analyze_raw_data_storage(),
            'combination_process': self.analyze_combination_process(),
            'final_analysis_data': self.analyze_final_analysis_data(),
            'attribution_tracing': self.trace_attribution_flow(),
            'recommendations': []
        }
        
        # Generate recommendations
        diagnostic_results['recommendations'] = self.generate_recommendations(diagnostic_results)
        
        return diagnostic_results

    def analyze_database_tables(self) -> Dict[str, Any]:
        """Analyze database table structure and record counts"""
        print("\n📊 STEP 1: ANALYZING DATABASE TABLES")
        print("-" * 50)
        
        if not self._ensure_connection():
            return {'error': 'Database connection not available'}
        
        tables_info = {}
        
        # Check if tables exist and get record counts
        tables_to_check = [
            'coingecko_market_data',
            'coinmarketcap_market_data', 
            'market_data',
            'price_history'
        ]
        
        for table in tables_to_check:
            try:
                # Check if table exists
                assert self.cursor is not None  # Type safety assertion
                self.cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (table,))
                
                if self.cursor.fetchone():
                    # Get total record count
                    self.cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                    result = self.cursor.fetchone()
                    total_count = result['count'] if result else 0
                    
                    # Get recent record count (last 24 hours)
                    self.cursor.execute(f"""
                        SELECT COUNT(*) as count FROM {table}
                        WHERE timestamp >= datetime('now', '-24 hours')
                    """)
                    result = self.cursor.fetchone()
                    recent_count = result['count'] if result else 0
                    
                    # Get latest timestamp
                    self.cursor.execute(f"""
                        SELECT MAX(timestamp) as latest FROM {table}
                    """)
                    result = self.cursor.fetchone()
                    latest_timestamp = result['latest'] if result else None
                    
                    tables_info[table] = {
                        'exists': True,
                        'total_records': total_count,
                        'recent_records_24h': recent_count,
                        'latest_timestamp': latest_timestamp,
                        'status': '✅ Active' if recent_count > 0 else '⚠️ No recent data'
                    }
                    
                    print(f"  {table}:")
                    print(f"    Total records: {total_count:,}")
                    print(f"    Recent (24h): {recent_count:,}")
                    print(f"    Latest: {latest_timestamp}")
                    print(f"    Status: {tables_info[table]['status']}")
                else:
                    tables_info[table] = {
                        'exists': False,
                        'status': '❌ Missing'
                    }
                    print(f"  {table}: ❌ TABLE MISSING")
                    
            except Exception as e:
                tables_info[table] = {
                    'exists': False,
                    'error': str(e),
                    'status': f'❌ Error: {e}'
                }
                print(f"  {table}: ❌ Error - {e}")
        
        return tables_info

    def analyze_raw_data_storage(self) -> Dict[str, Any]:
        """Analyze raw data storage in dedicated tables"""
        print("\n📥 STEP 2: ANALYZING RAW DATA STORAGE")
        print("-" * 50)
        
        if not self._ensure_connection():
            return {'error': 'Database connection not available'}
        
        storage_analysis = {
            'coingecko_analysis': {},
            'coinmarketcap_analysis': {},
            'storage_comparison': {}
        }
        
        # Analyze CoinGecko data
        try:
            assert self.cursor is not None  # Type safety assertion
            
            # Get sample CoinGecko records
            self.cursor.execute("""
                SELECT coin_id, symbol, name, current_price, market_cap, timestamp
                FROM coingecko_market_data
                WHERE timestamp >= datetime('now', '-24 hours')
                ORDER BY timestamp DESC
                LIMIT 5
            """)
            coingecko_samples = [dict(row) for row in self.cursor.fetchall()]
            
            # Get unique tokens
            self.cursor.execute("""
                SELECT COUNT(DISTINCT symbol) as unique_tokens
                FROM coingecko_market_data
                WHERE timestamp >= datetime('now', '-24 hours')
            """)
            result = self.cursor.fetchone()
            coingecko_unique = result['unique_tokens'] if result else 0
            
            storage_analysis['coingecko_analysis'] = {
                'unique_tokens_24h': coingecko_unique,
                'sample_records': coingecko_samples,
                'status': '✅ Active' if coingecko_unique > 0 else '⚠️ No data'
            }
            
            print(f"  CoinGecko Table:")
            print(f"    Unique tokens (24h): {coingecko_unique}")
            print(f"    Sample records: {len(coingecko_samples)}")
            if coingecko_samples:
                sample = coingecko_samples[0]
                print(f"    Sample: {sample['symbol']} - ${sample['current_price']} - {sample['timestamp']}")
            
        except Exception as e:
            storage_analysis['coingecko_analysis'] = {'error': str(e)}
            print(f"  CoinGecko Analysis: ❌ Error - {e}")
        
        # Analyze CoinMarketCap data
        try:
            assert self.cursor is not None  # Type safety assertion
            
            # Get sample CoinMarketCap records
            self.cursor.execute("""
                SELECT symbol, name, quote_price, quote_market_cap, timestamp
                FROM coinmarketcap_market_data
                WHERE timestamp >= datetime('now', '-24 hours')
                ORDER BY timestamp DESC
                LIMIT 5
            """)
            coinmarketcap_samples = [dict(row) for row in self.cursor.fetchall()]
            
            # Get unique tokens
            self.cursor.execute("""
                SELECT COUNT(DISTINCT symbol) as unique_tokens
                FROM coinmarketcap_market_data
                WHERE timestamp >= datetime('now', '-24 hours')
            """)
            result = self.cursor.fetchone()
            coinmarketcap_unique = result['unique_tokens'] if result else 0
            
            storage_analysis['coinmarketcap_analysis'] = {
                'unique_tokens_24h': coinmarketcap_unique,
                'sample_records': coinmarketcap_samples,
                'status': '✅ Active' if coinmarketcap_unique > 0 else '⚠️ No data'
            }
            
            print(f"  CoinMarketCap Table:")
            print(f"    Unique tokens (24h): {coinmarketcap_unique}")
            print(f"    Sample records: {len(coinmarketcap_samples)}")
            if coinmarketcap_samples:
                sample = coinmarketcap_samples[0]
                print(f"    Sample: {sample['symbol']} - ${sample['quote_price']} - {sample['timestamp']}")
            
        except Exception as e:
            storage_analysis['coinmarketcap_analysis'] = {'error': str(e)}
            print(f"  CoinMarketCap Analysis: ❌ Error - {e}")
        
        # Compare storage
        try:
            coingecko_count = storage_analysis['coingecko_analysis'].get('unique_tokens_24h', 0)
            coinmarketcap_count = storage_analysis['coinmarketcap_analysis'].get('unique_tokens_24h', 0)
            
            storage_analysis['storage_comparison'] = {
                'coingecko_tokens': coingecko_count,
                'coinmarketcap_tokens': coinmarketcap_count,
                'total_unique_sources': 2 if coingecko_count > 0 and coinmarketcap_count > 0 else 1 if (coingecko_count > 0 or coinmarketcap_count > 0) else 0,
                'data_balance': 'balanced' if abs(coingecko_count - coinmarketcap_count) < (max(coingecko_count, coinmarketcap_count) * 0.3) else 'imbalanced'
            }
            
            print(f"  Storage Comparison:")
            print(f"    CoinGecko: {coingecko_count} tokens")
            print(f"    CoinMarketCap: {coinmarketcap_count} tokens")
            print(f"    Balance: {storage_analysis['storage_comparison']['data_balance']}")
            
        except Exception as e:
            storage_analysis['storage_comparison'] = {'error': str(e)}
            print(f"  Storage Comparison: ❌ Error - {e}")
        
        return storage_analysis

    def analyze_combination_process(self) -> Dict[str, Any]:
        """Analyze the combine_market_data_sources process"""
        print("\n🔄 STEP 3: ANALYZING COMBINATION PROCESS")
        print("-" * 50)
        
        if not self._ensure_connection():
            return {'error': 'Database connection not available'}
        
        combination_analysis = {
            'combined_data_analysis': {},
            'source_attribution': {},
            'processing_effectiveness': {}
        }
        
        # Analyze market_data table (where combined data goes)
        try:
            assert self.cursor is not None  # Type safety assertion
            
            # Get recent combined data
            self.cursor.execute("""
                SELECT chain, price, volume, market_cap, timestamp
                FROM market_data
                WHERE timestamp >= datetime('now', '-24 hours')
                ORDER BY timestamp DESC
                LIMIT 10
            """)
            combined_samples = [dict(row) for row in self.cursor.fetchall()]
            
            # Get unique tokens in combined data
            self.cursor.execute("""
                SELECT COUNT(DISTINCT chain) as unique_tokens
                FROM market_data
                WHERE timestamp >= datetime('now', '-24 hours')
            """)
            result = self.cursor.fetchone()
            combined_unique = result['unique_tokens'] if result else 0
            
            # Get timestamp of most recent combination
            self.cursor.execute("""
                SELECT MAX(timestamp) as latest_combination
                FROM market_data
                WHERE timestamp >= datetime('now', '-24 hours')
            """)
            result = self.cursor.fetchone()
            latest_combination = result['latest_combination'] if result else None
            
            combination_analysis['combined_data_analysis'] = {
                'unique_tokens_24h': combined_unique,
                'sample_records': combined_samples,
                'latest_combination': latest_combination,
                'status': '✅ Active' if combined_unique > 0 else '⚠️ No combined data'
            }
            
            print(f"  Combined Data (market_data table):")
            print(f"    Unique tokens (24h): {combined_unique}")
            print(f"    Latest combination: {latest_combination}")
            print(f"    Sample records: {len(combined_samples)}")
            
        except Exception as e:
            combination_analysis['combined_data_analysis'] = {'error': str(e)}
            print(f"  Combined Data Analysis: ❌ Error - {e}")
        
        # Analyze source attribution by comparing tokens
        try:
            assert self.cursor is not None  # Type safety assertion
            
            # Get tokens that exist in each source
            self.cursor.execute("""
                SELECT DISTINCT symbol as token FROM coingecko_market_data
                WHERE timestamp >= datetime('now', '-24 hours')
            """)
            coingecko_tokens = {row['token'] for row in self.cursor.fetchall()}
            
            self.cursor.execute("""
                SELECT DISTINCT symbol as token FROM coinmarketcap_market_data
                WHERE timestamp >= datetime('now', '-24 hours')
            """)
            coinmarketcap_tokens = {row['token'] for row in self.cursor.fetchall()}
            
            self.cursor.execute("""
                SELECT DISTINCT chain as token FROM market_data
                WHERE timestamp >= datetime('now', '-24 hours')
            """)
            combined_tokens = {row['token'] for row in self.cursor.fetchall()}
            
            # Calculate overlaps
            coingecko_only = coingecko_tokens - coinmarketcap_tokens
            coinmarketcap_only = coinmarketcap_tokens - coingecko_tokens
            both_sources = coingecko_tokens & coinmarketcap_tokens
            
            # Check which combined tokens came from which source
            combined_from_coingecko = combined_tokens & coingecko_tokens
            combined_from_coinmarketcap = combined_tokens & coinmarketcap_tokens
            combined_from_both = combined_tokens & both_sources
            
            combination_analysis['source_attribution'] = {
                'coingecko_only_tokens': len(coingecko_only),
                'coinmarketcap_only_tokens': len(coinmarketcap_only),
                'both_sources_tokens': len(both_sources),
                'combined_from_coingecko': len(combined_from_coingecko),
                'combined_from_coinmarketcap': len(combined_from_coinmarketcap),
                'combined_from_both': len(combined_from_both),
                'sample_both_sources': list(both_sources)[:5],
                'sample_coingecko_only': list(coingecko_only)[:5],
                'sample_coinmarketcap_only': list(coinmarketcap_only)[:5]
            }
            
            print(f"  Source Attribution:")
            print(f"    CoinGecko only: {len(coingecko_only)} tokens")
            print(f"    CoinMarketCap only: {len(coinmarketcap_only)} tokens")
            print(f"    Both sources: {len(both_sources)} tokens")
            print(f"    Combined includes CoinGecko: {len(combined_from_coingecko)} tokens")
            print(f"    Combined includes CoinMarketCap: {len(combined_from_coinmarketcap)} tokens")
            print(f"    Combined from both: {len(combined_from_both)} tokens")
            
        except Exception as e:
            combination_analysis['source_attribution'] = {'error': str(e)}
            print(f"  Source Attribution: ❌ Error - {e}")
        
        return combination_analysis

    def analyze_final_analysis_data(self) -> Dict[str, Any]:
        """Analyze what data the analysis functions actually use"""
        print("\n📈 STEP 4: ANALYZING FINAL ANALYSIS DATA")
        print("-" * 50)
        
        if not self._ensure_connection():
            return {'error': 'Database connection not available'}
        
        analysis_data = {
            'market_data_for_analysis': {},
            'analysis_effectiveness': {}
        }
        
        # Simulate what analysis functions see
        try:
            assert self.cursor is not None  # Type safety assertion
            
            # This mimics what get_tokens_with_recent_data_by_market_cap returns
            self.cursor.execute("""
                SELECT chain, price, volume, market_cap, timestamp,
                       'market_data' as source_table
                FROM market_data
                WHERE timestamp >= datetime('now', '-24 hours')
                ORDER BY market_cap DESC
                LIMIT 15
            """)
            analysis_ready_data = [dict(row) for row in self.cursor.fetchall()]
            
            analysis_data['market_data_for_analysis'] = {
                'tokens_for_analysis': len(analysis_ready_data),
                'sample_tokens': analysis_ready_data[:5],
                'top_token_by_market_cap': analysis_ready_data[0] if analysis_ready_data else None,
                'data_quality': 'good' if len(analysis_ready_data) >= 10 else 'insufficient'
            }
            
            print(f"  Analysis-Ready Data:")
            print(f"    Tokens available for analysis: {len(analysis_ready_data)}")
            print(f"    Data quality: {analysis_data['market_data_for_analysis']['data_quality']}")
            if analysis_ready_data:
                top_token = analysis_ready_data[0]
                print(f"    Top token: {top_token['chain']} (${top_token['price']}, MCap: ${top_token['market_cap']:,})")
            
        except Exception as e:
            analysis_data['market_data_for_analysis'] = {'error': str(e)}
            print(f"  Analysis Data: ❌ Error - {e}")
        
        return analysis_data

    def trace_attribution_flow(self) -> Dict[str, Any]:
        """Trace how source attribution flows through the system"""
        print("\n🔍 STEP 5: TRACING SOURCE ATTRIBUTION")
        print("-" * 50)
        
        if not self._ensure_connection():
            return {'error': 'Database connection not available'}
        
        attribution_trace = {
            'data_flow_analysis': {},
            'attribution_issues': [],
            'flow_integrity': {}
        }
        
        try:
            assert self.cursor is not None  # Type safety assertion
            
            # Check specific token flows
            test_tokens = ['BTC', 'ETH', 'SOL']  # Common tokens likely in both sources
            
            for token in test_tokens:
                token_trace = {
                    'token': token,
                    'coingecko_records': 0,
                    'coinmarketcap_records': 0,
                    'combined_records': 0,
                    'attribution_status': 'unknown'
                }
                
                # Check CoinGecko presence
                self.cursor.execute("""
                    SELECT COUNT(*) as count FROM coingecko_market_data
                    WHERE UPPER(symbol) = ? AND timestamp >= datetime('now', '-24 hours')
                """, (token.upper(),))
                result = self.cursor.fetchone()
                token_trace['coingecko_records'] = result['count'] if result else 0
                
                # Check CoinMarketCap presence
                self.cursor.execute("""
                    SELECT COUNT(*) as count FROM coinmarketcap_market_data
                    WHERE UPPER(symbol) = ? AND timestamp >= datetime('now', '-24 hours')
                """, (token.upper(),))
                result = self.cursor.fetchone()
                token_trace['coinmarketcap_records'] = result['count'] if result else 0
                
                # Check combined data presence
                self.cursor.execute("""
                    SELECT COUNT(*) as count FROM market_data
                    WHERE UPPER(chain) = ? AND timestamp >= datetime('now', '-24 hours')
                """, (token.upper(),))
                result = self.cursor.fetchone()
                token_trace['combined_records'] = result['count'] if result else 0
                
                # Determine attribution status
                if token_trace['coingecko_records'] > 0 and token_trace['coinmarketcap_records'] > 0:
                    if token_trace['combined_records'] > 0:
                        token_trace['attribution_status'] = 'both_sources_combined'
                    else:
                        token_trace['attribution_status'] = 'both_sources_not_combined'
                        attribution_trace['attribution_issues'].append(f"{token}: Data in both sources but not combined")
                elif token_trace['coingecko_records'] > 0:
                    token_trace['attribution_status'] = 'coingecko_only'
                elif token_trace['coinmarketcap_records'] > 0:
                    token_trace['attribution_status'] = 'coinmarketcap_only'
                else:
                    token_trace['attribution_status'] = 'no_data'
                    attribution_trace['attribution_issues'].append(f"{token}: No data in any source")
                
                attribution_trace['data_flow_analysis'][token] = token_trace
                
                print(f"  {token} Flow:")
                print(f"    CoinGecko: {token_trace['coingecko_records']} records")
                print(f"    CoinMarketCap: {token_trace['coinmarketcap_records']} records")
                print(f"    Combined: {token_trace['combined_records']} records")
                print(f"    Status: {token_trace['attribution_status']}")
            
            # Analyze overall flow integrity
            total_attribution_issues = len(attribution_trace['attribution_issues'])
            attribution_trace['flow_integrity'] = {
                'total_issues': total_attribution_issues,
                'flow_health': 'good' if total_attribution_issues == 0 else 'issues_detected'
            }
            
            print(f"  Flow Integrity:")
            print(f"    Issues detected: {total_attribution_issues}")
            print(f"    Overall health: {attribution_trace['flow_integrity']['flow_health']}")
            
        except Exception as e:
            attribution_trace = {'error': str(e)}
            print(f"  Attribution Tracing: ❌ Error - {e}")
        
        return attribution_trace

    def generate_recommendations(self, diagnostic_results: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on diagnostic results"""
        print("\n💡 GENERATING RECOMMENDATIONS")
        print("-" * 50)
        
        recommendations = []
        
        # Analyze results and generate recommendations
        try:
            # Check table status
            tables = diagnostic_results.get('database_tables', {})
            if not tables.get('coingecko_market_data', {}).get('exists', False):
                recommendations.append("CRITICAL: CoinGecko table missing - create coingecko_market_data table")
            
            if not tables.get('coinmarketcap_market_data', {}).get('exists', False):
                recommendations.append("CRITICAL: CoinMarketCap table missing - create coinmarketcap_market_data table")
            
            # Check data flow
            raw_analysis = diagnostic_results.get('raw_data_analysis', {})
            coingecko_tokens = raw_analysis.get('coingecko_analysis', {}).get('unique_tokens_24h', 0)
            coinmarketcap_tokens = raw_analysis.get('coinmarketcap_analysis', {}).get('unique_tokens_24h', 0)
            
            if coingecko_tokens == 0:
                recommendations.append("HIGH: No CoinGecko data in last 24h - check CoinGecko API handler and store_coingecko_data calls")
            
            if coinmarketcap_tokens == 0:
                recommendations.append("HIGH: No CoinMarketCap data in last 24h - check CoinMarketCap API handler")
            
            # Check combination process
            combination = diagnostic_results.get('combination_process', {})
            combined_tokens = combination.get('combined_data_analysis', {}).get('unique_tokens_24h', 0)
            
            if combined_tokens == 0:
                recommendations.append("HIGH: No combined data - combine_market_data_sources() may not be running")
            elif combined_tokens < max(coingecko_tokens, coinmarketcap_tokens):
                recommendations.append("MEDIUM: Combined data less than source data - check combine_market_data_sources() logic")
            
            # Check attribution issues
            attribution = diagnostic_results.get('attribution_tracing', {})
            issues = attribution.get('attribution_issues', [])
            
            if issues:
                recommendations.append(f"MEDIUM: {len(issues)} attribution issues detected - review token mapping in combine process")
            
            # Data balance check
            storage_comparison = raw_analysis.get('storage_comparison', {})
            if storage_comparison.get('data_balance') == 'imbalanced':
                recommendations.append("LOW: Data sources imbalanced - verify both APIs are collecting similar token sets")
            
        except Exception as e:
            recommendations.append(f"ERROR: Failed to generate recommendations - {e}")
        
        # Print recommendations
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
        
        return recommendations

    def export_diagnostic_report(self, results: Dict[str, Any], filename: Optional[str] = None) -> str:
        """Export diagnostic results to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_flow_diagnostic_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\n📋 Diagnostic report exported to: {filename}")
            return filename
        except Exception as e:
            print(f"❌ Export failed: {e}")
            return ""


def main():
    """Run the diagnostic tool"""
    try:
        # Initialize diagnostic tool
        diagnostic = DataFlowDiagnostic()
        
        # Run comprehensive diagnostic
        results = diagnostic.run_comprehensive_diagnostic()
        
        # Export results
        report_file = diagnostic.export_diagnostic_report(results)
        
        print("\n" + "="*80)
        print("🎯 DIAGNOSTIC COMPLETE")
        print("="*80)
        print(f"Report saved to: {report_file}")
        print("Review the recommendations above to fix CoinGecko attribution issues.")
        
    except Exception as e:
        print(f"❌ Diagnostic failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

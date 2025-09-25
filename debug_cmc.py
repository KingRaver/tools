# Quick debug script - save as debug_cmc.py
from database import CryptoDatabase

db = CryptoDatabase()
conn, cursor = db._get_connection()

# Check if table_source column exists
print("=== COINMARKETCAP TABLE SCHEMA ===")
cursor.execute("PRAGMA table_info(coinmarketcap_market_data)")
columns = cursor.fetchall()
for col in columns:
    print(f"Column: {col[1]}, Type: {col[2]}")

print(f"\n=== CHECKING FOR table_source COLUMN ===")
has_table_source = any(col[1] == 'table_source' for col in columns)
print(f"table_source column exists: {has_table_source}")

# Check total CMC records
cursor.execute("SELECT COUNT(*) FROM coinmarketcap_market_data")
total_records = cursor.fetchone()[0]
print(f"\n=== TOTAL CMC RECORDS ===")
print(f"Total CoinMarketCap records: {total_records}")

# Check recent CMC records
cursor.execute("SELECT COUNT(*) FROM coinmarketcap_market_data WHERE timestamp >= datetime('now', '-24 hours')")
recent_records = cursor.fetchone()[0]
print(f"Recent CoinMarketCap records (24h): {recent_records}")

# Check timestamps
cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM coinmarketcap_market_data")
timestamps = cursor.fetchone()
print(f"\n=== TIMESTAMP RANGE ===")
print(f"Oldest: {timestamps[0]}")
print(f"Newest: {timestamps[1]}")

# If table_source exists, check values
if has_table_source:
    cursor.execute("SELECT DISTINCT table_source FROM coinmarketcap_market_data")
    sources = cursor.fetchall()
    print(f"\n=== table_source VALUES ===")
    for source in sources:
        print(f"Source: {source[0]}")

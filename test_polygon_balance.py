#!/usr/bin/env python3
"""
Simple Polygon Balance Checker - Debug your POL balance detection
"""

import asyncio
from web3 import Web3
import requests

class PolygonBalanceChecker:
    def __init__(self):
        # Polygon RPC endpoints
        self.rpc_endpoints = [
            "https://polygon.llamarpc.com",
            "https://polygon-rpc.com", 
            "https://rpc.ankr.com/polygon",
            "https://polygon.blockpi.network/v1/rpc/public",
            "https://polygon-mainnet.public.blastapi.io"
        ]
        self.w3 = None
        
    def connect_to_polygon(self):
        """Connect to Polygon network"""
        print("🌐 Connecting to Polygon network...")
        
        for i, rpc_url in enumerate(self.rpc_endpoints):
            try:
                print(f"   Trying RPC {i+1}/{len(self.rpc_endpoints)}: {rpc_url}")
                
                self.w3 = Web3(Web3.HTTPProvider(
                    rpc_url,
                    request_kwargs={
                        'timeout': 10,
                        'headers': {'User-Agent': 'BalanceChecker/1.0'}
                    }
                ))
                
                if self.w3.is_connected():
                    chain_id = self.w3.eth.chain_id
                    if chain_id == 137:  # Polygon mainnet
                        print(f"✅ Connected to Polygon via {rpc_url}")
                        return True
                    else:
                        print(f"   ❌ Wrong chain ID: {chain_id}")
                else:
                    print(f"   ❌ Connection failed")
                    
            except Exception as e:
                print(f"   ❌ Error: {e}")
                continue
        
        print("❌ Failed to connect to Polygon")
        return False
    
    def check_balance(self, wallet_address):
        """Check POL/MATIC balance on Polygon"""
        if not self.w3:
            print("❌ No connection to Polygon")
            return None
            
        try:
            print(f"\n💰 Checking balance for: {wallet_address}")
            
            # Get balance in wei
            balance_wei = self.w3.eth.get_balance(wallet_address)
            print(f"   Raw balance (wei): {balance_wei}")
            
            # Convert to POL/MATIC
            balance_pol = self.w3.from_wei(balance_wei, 'ether')
            print(f"   Balance (POL): {balance_pol}")
            
            # Get POL price to calculate USD value
            pol_price = self.get_pol_price_usd()
            balance_usd = float(balance_pol) * pol_price
            print(f"   Balance (USD): ${balance_usd:.2f}")
            
            return {
                'balance_wei': balance_wei,
                'balance_pol': float(balance_pol),
                'balance_usd': balance_usd,
                'pol_price': pol_price
            }
            
        except Exception as e:
            print(f"❌ Balance check failed: {e}")
            return None
    
    def get_pol_price_usd(self):
        """Get current POL/MATIC price in USD"""
        try:
            # Try CoinGecko API
            url = "https://api.coingecko.com/api/v3/simple/price?ids=matic-network&vs_currencies=usd"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                price = data['matic-network']['usd']
                print(f"   POL price: ${price}")
                return price
        except Exception as e:
            print(f"   ⚠️ Price fetch failed: {e}")
        
        # Fallback price
        print("   ⚠️ Using fallback POL price: $0.80")
        return 0.80
    
    def detailed_network_info(self):
        """Get detailed network information"""
        if not self.w3:
            return
            
        try:
            print(f"\n🔍 DETAILED NETWORK INFO:")
            print(f"   Chain ID: {self.w3.eth.chain_id}")
            print(f"   Latest block: {self.w3.eth.block_number}")
            print(f"   Gas price: {self.w3.eth.gas_price} wei")
            print(f"   Node version: {self.w3.client_version}")
            
        except Exception as e:
            print(f"❌ Network info failed: {e}")

def main():
    # Your wallet address from the logs
    WALLET_ADDRESS = "0x49CF5f42914f8fa5746dAdC6fcb6E2E9EE1cd4E5"
    
    print("🚀 POLYGON BALANCE CHECKER")
    print("=" * 50)
    print(f"Wallet: {WALLET_ADDRESS}")
    print("=" * 50)
    
    checker = PolygonBalanceChecker()
    
    # Step 1: Connect to Polygon
    if not checker.connect_to_polygon():
        print("❌ Cannot connect to Polygon - exiting")
        return
    
    # Step 2: Get network info
    checker.detailed_network_info()
    
    # Step 3: Check balance
    balance_info = checker.check_balance(WALLET_ADDRESS)
    
    if balance_info:
        print(f"\n✅ BALANCE CHECK COMPLETE")
        print(f"   POL Balance: {balance_info['balance_pol']:.6f}")
        print(f"   USD Value: ${balance_info['balance_usd']:.2f}")
        
        if balance_info['balance_pol'] > 0:
            print(f"🎉 SUCCESS: Found {balance_info['balance_pol']:.6f} POL!")
        else:
            print(f"⚠️  ZERO BALANCE: No POL found in this wallet")
            print(f"   Make sure you sent POL to: {WALLET_ADDRESS}")
            print(f"   Check on PolygonScan: https://polygonscan.com/address/{WALLET_ADDRESS}")
    else:
        print("❌ Balance check failed")

if __name__ == "__main__":
    main()

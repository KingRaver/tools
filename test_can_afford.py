#!/usr/bin/env python3
"""
Test the specific can_afford_trade method that's failing
"""

import sys
import os
import asyncio
sys.path.append('src')

from multi_chain_manager import MultiChainManager

async def test_can_afford_trade():
    print("🔍 TESTING CAN_AFFORD_TRADE METHOD")
    print("=" * 50)
    
    WALLET_ADDRESS = "0x49CF5f42914f8fa5746dAdC6fcb6E2E9EE1cd4E5"
    
    manager = MultiChainManager()
    
    # Step 1: Connect and get balances (like the debug script did)
    await manager.connect_to_all_networks()
    await manager.get_all_balances(WALLET_ADDRESS)
    
    # Step 2: Check what's in the native_balances cache
    print("📊 Checking native_balances cache:")
    print(f"   Cache contents: {manager.native_balances}")
    
    if not manager.native_balances:
        print("❌ ISSUE FOUND: native_balances cache is empty!")
        print("💡 This is why can_afford_trade() always returns False")
        
        # Step 3: Update the cache manually
        print("\n🔧 Updating native_balances cache...")
        await manager.update_all_balances(WALLET_ADDRESS)
        print(f"   Updated cache: {manager.native_balances}")
    
    # Step 4: Test can_afford_trade with each network
    print(f"\n💰 Testing can_afford_trade for each network:")
    
    for network_name in ["polygon", "optimism", "base"]:
        if network_name in manager.active_connections:
            print(f"\n🔍 Testing {manager.networks[network_name].name}...")
            
            # Get a gas estimate first
            gas_estimate = await manager.get_gas_estimate(network_name, 10.0)
            
            if gas_estimate:
                print(f"   Gas estimate: ${gas_estimate.estimated_cost_usd:.3f}")
                print(f"   Native cost: {gas_estimate.estimated_cost_native:.6f} {manager.networks[network_name].native_token_symbol}")
                
                # Test can_afford_trade
                can_afford = manager.can_afford_trade(network_name, gas_estimate)
                print(f"   Can afford: {can_afford}")
                
                if not can_afford:
                    print(f"   ❌ This is why your bot says 'insufficient native tokens'")
                else:
                    print(f"   ✅ Should be able to trade on this network")
            else:
                print(f"   ❌ No gas estimate available")

if __name__ == "__main__":
    asyncio.run(test_can_afford_trade())

# =============================================================================
# 🚀 COMPLETE INTEGRATED CRYPTO TRADING BOT - WEB3 ENABLED
# =============================================================================

import os
import asyncio
import json
from web3 import Web3
from eth_account import Account
import time
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
import requests
from decimal import Decimal
import keyring
from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class SecureWalletManager:
    """Ultra-secure wallet management for crypto trading"""
    
    def __init__(self):
        self.account = None
        self.wallet_address = None
        self.w3 = None
        
    def setup_web3_connection(self):
        """Setup Web3 with multiple fallback RPCs"""
        # Free RPC endpoints - no API keys needed
        rpc_endpoints = [
            "https://cloudflare-eth.com",
            "https://ethereum.publicnode.com",
            "https://rpc.ankr.com/eth",
            "https://eth-mainnet.nodereal.io/v1/1659dfb40aa24bbb8153a677b98064d7",
            "https://ethereum.blockpi.network/v1/rpc/public"
        ]
        
        for rpc_url in rpc_endpoints:
            try:
                self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))
                if self.w3.is_connected():
                    print(f"✅ Connected to Ethereum via {rpc_url}")
                    return True
            except Exception as e:
                print(f"⚠️  Failed to connect to {rpc_url}: {e}")
                continue
        
        raise Exception("❌ Failed to connect to Ethereum network")
    
    def load_wallet_secure(self):
        """Load wallet using secure keyring (most secure option)"""
        try:
            # Try to load from system keyring first
            private_key = keyring.get_password("crypto_trading_bot", "wallet_private_key")
            
            if private_key:
                self.account = Account.from_key(private_key)
                self.wallet_address = self.account.address
                print(f"✅ Wallet loaded from keyring: {self.wallet_address}")
                return True
            else:
                print("⚠️  No wallet found in keyring")
                return False
                
        except Exception as e:
            print(f"❌ Failed to load wallet from keyring: {e}")
            return False
    
    def create_new_wallet(self):
        """Create new trading wallet and store securely"""
        try:
            # Generate new account
            self.account = Account.create()
            self.wallet_address = self.account.address
            
            print(f"\n🎯 NEW TRADING WALLET CREATED!")
            print(f"Address: {self.wallet_address}")
            print(f"Private Key: {self.account.key.hex()}")
            
            # Ask user if they want to save to keyring
            save_choice = input("\n💾 Save private key to secure keyring? (y/n): ").lower()
            
            if save_choice == 'y':
                keyring.set_password("crypto_trading_bot", "wallet_private_key", self.account.key.hex())
                print("✅ Private key saved to secure keyring")
            else:
                print("⚠️  Private key NOT saved - you'll need to enter it manually next time")
            
            print(f"\n💰 FUND THIS WALLET WITH $100 WORTH OF ETH TO START TRADING!")
            print(f"🔗 You can use any exchange to send ETH to: {self.wallet_address}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to create new wallet: {e}")
            return False
    
    def get_balance(self):
        """Get current wallet balance"""
        try:
            if not self.w3 or not self.wallet_address:
                return 0.0
                
            balance_wei = self.w3.eth.get_balance(self.wallet_address)
            balance_eth = self.w3.from_wei(balance_wei, 'ether')
            return float(balance_eth)
            
        except Exception as e:
            print(f"❌ Failed to get balance: {e}")
            return 0.0

class PredictionIntegratedTrader:
    """Main trading bot with prediction integration"""
    
    def __init__(self, initial_capital: float = 100.0):
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.positions = {}
        self.trade_history = []
        
        # Crypto-optimized risk settings
        self.DEFAULT_MIN_CONFIDENCE = 70  # Fallback value if needed
        self.MAX_POSITION_SIZE = 0.25   # Max 25% per trade
        
        # Dynamic stop losses by token
        self.STOP_LOSS_RANGES = {
            'BTC': {'min': 4, 'max': 8},      # Lower volatility needs tighter stops
            'ETH': {'min': 7, 'max': 12},     # Reduced by ~30%
            'SOL': {'min': 12, 'max': 20},    # High volatility still needs wider stops
            'XRP': {'min': 10, 'max': 16},
            'BNB': {'min': 7, 'max': 14},
            'AVAX': {'min': 10, 'max': 18},
            'DEFAULT': {'min': 10, 'max': 20}
        }
        
        # Dynamic take profits
        self.TAKE_PROFIT_RANGES = {
            'BTC': {'conservative': 15, 'aggressive': 30},   # BTC moves less, so adjust expectations
            'ETH': {'conservative': 20, 'aggressive': 45},
            'SOL': {'conservative': 30, 'aggressive': 65},   # Still can let SOL run more
            'XRP': {'conservative': 25, 'aggressive': 55},
            'BNB': {'conservative': 20, 'aggressive': 45},
            'AVAX': {'conservative': 25, 'aggressive': 55},
            'DEFAULT': {'conservative': 25, 'aggressive': 50}
        }
        
        # Trading limits
        self.MAX_DAILY_TRADES = 120
        self.DAILY_LOSS_LIMIT = 25.0    # $25 max daily loss
        
        # Performance tracking
        self.daily_pnl = 0.0
        self.total_trades_today = 0
        self.last_reset_date = datetime.now().date()
        self.emergency_stop = False
        
        # Initialize wallet and Web3
        self.wallet_manager = SecureWalletManager()
        self.setup_complete = False
        
    def setup_trading_system(self):
        """Complete setup of trading system"""
        print("🚀 SETTING UP INTEGRATED CRYPTO TRADING BOT...")
        print("=" * 60)
        
        try:
            # 1. Setup Web3 connection
            print("\n1. 🌐 Connecting to Ethereum network...")
            self.wallet_manager.setup_web3_connection()
            
            # 2. Load or create wallet
            print("\n2. 💳 Setting up trading wallet...")
            if not self.wallet_manager.load_wallet_secure():
                print("No existing wallet found. Creating new one...")
                if not self.wallet_manager.create_new_wallet():
                    raise Exception("Failed to create wallet")
            
            # 3. Check balance
            print("\n3. 💰 Checking wallet balance...")
            balance = self.wallet_manager.get_balance()
            print(f"Current balance: {balance:.4f} ETH")
            
            if balance < 0.01:  # Less than ~$25 worth
                print("⚠️  LOW BALANCE WARNING!")
                print(f"💡 Send ETH to {self.wallet_manager.wallet_address} to start trading")
            
            self.setup_complete = True
            print("\n✅ TRADING SYSTEM SETUP COMPLETE!")
            print("🎯 Ready for automated crypto trading!")
            
            return True
            
        except Exception as e:
            print(f"❌ Setup failed: {e}")
            return False
    
    def reset_daily_limits(self):
        """Reset daily trading limits"""
        current_date = datetime.now().date()
        if current_date > self.last_reset_date:
            self.daily_pnl = 0.0
            self.total_trades_today = 0
            self.last_reset_date = current_date
            print(f"🔄 Daily limits reset for {current_date}")

    def get_min_return_threshold(self, token, volatility_score, timeframe):
        """Dynamic minimum return threshold based on asset volatility"""
        base_threshold = {
            'BTC': 5.0,
            'ETH': 6.0,
            'SOL': 8.0,
            'XRP': 7.0,
            'BNB': 6.0,
            'AVAX': 7.0
        }.get(token, 8.0)
    
        # Adjust for timeframe
        if timeframe == '24h':
            base_threshold *= 1.5
        elif timeframe != '1h':
            base_threshold *= 2.0
    
        # Further adjust based on current volatility
        volatility_factor = max(0.8, min(1.2, volatility_score / 50))
        return base_threshold * volatility_factor  

    def get_min_confidence_threshold(self, token, volatility_score):
        """Dynamic confidence threshold based on asset and market conditions"""
        base_threshold = {
            'BTC': 75,  # Higher threshold for less volatile assets
            'ETH': 73,
            'SOL': 68,  # Lower for more volatile assets that move quickly
            'XRP': 70,
            'BNB': 73,
            'AVAX': 70
        }.get(token, 72)
    
        # Adjust for high volatility periods
        if volatility_score > 15:
            base_threshold += 5  # Require more confidence in highly volatile markets
    
        return base_threshold 

    def calculate_position_size(self, token, confidence, volatility_score, expected_return_pct):
        """Advanced position sizing with volatility adjustment"""
        # Base sizing by confidence
        confidence_factor = min(confidence / 100, 0.85)
    
        # Adjust for volatility - reduce size for extremely volatile assets
        volatility_factor = 1.0
        if volatility_score > 15:
            volatility_factor = 0.9
        if volatility_score > 20:
            volatility_factor = 0.8
    
        # Adjust for return expectation - bet more on higher expected returns
        return_factor = min(1.2, max(0.8, abs(expected_return_pct) / 10))
    
        # Calculate final position size
        base_size = self.current_capital * self.MAX_POSITION_SIZE
        position_size = base_size * confidence_factor * volatility_factor * return_factor
    
        # Ensure within limits
        max_per_trade = self.current_capital * 0.25  # Never use more than 25% of capital
        return min(position_size, max_per_trade)     
    
    def calculate_dynamic_stop_loss(self, token: str, volatility_score: float, confidence: float, 
                                  market_trend: str = 'unknown', trend_strength: float = 0) -> float:
        """Calculate dynamic stop loss based on volatility, confidence and market trend"""
        ranges = self.STOP_LOSS_RANGES.get(token, self.STOP_LOSS_RANGES['DEFAULT'])
        min_stop = ranges['min']
        max_stop = ranges['max']
    
        # Adjust based on volatility
        if volatility_score > 80:
            stop_loss_pct = max_stop
        elif volatility_score > 60:
            stop_loss_pct = min_stop + (max_stop - min_stop) * 0.7
        elif volatility_score > 40:
            stop_loss_pct = min_stop + (max_stop - min_stop) * 0.5
        else:
            stop_loss_pct = min_stop
    
        # Adjust based on confidence
        if confidence > 85:
            stop_loss_pct *= 0.85  # Tighter stops for high confidence
        elif confidence < 75:
            stop_loss_pct *= 1.15  # Wider stops for lower confidence
    
        # Adjust based on trend
        if market_trend != 'unknown' and trend_strength > 2:
            if market_trend == 'bullish':
                stop_loss_pct *= 0.9  # Tighter stops in uptrends
            else:  # bearish
                stop_loss_pct *= 1.1  # Wider stops in downtrends
    
        return stop_loss_pct
    
    def calculate_dynamic_take_profit(self, token: str, confidence: float, 
                                    expected_return: float, market_trend: str = 'unknown', 
                                    trend_strength: float = 0) -> float:
        """Calculate dynamic take profit with trend analysis"""
        ranges = self.TAKE_PROFIT_RANGES.get(token, self.TAKE_PROFIT_RANGES['DEFAULT'])
    
        if confidence > 85:
            base_tp = ranges['aggressive']
        elif confidence > 75:
            base_tp = (ranges['conservative'] + ranges['aggressive']) / 2
        else:
            base_tp = ranges['conservative']
    
        # Adjust based on expected return
        if abs(expected_return) > 20:
            base_tp *= 1.3
        elif abs(expected_return) > 10:
            base_tp *= 1.1
    
        # Adjust based on trend
        if market_trend != 'unknown' and trend_strength > 5:
            if market_trend == 'bullish' and expected_return > 0:
                # More aggressive take profits for longs in uptrends
                base_tp *= 1.1
            elif market_trend == 'bearish' and expected_return < 0:
                # More aggressive take profits for shorts in downtrends
                base_tp *= 1.1
    
        return base_tp
    
    def implement_trailing_stop_loss(self, position_id, current_price):
        """Implement trailing stop loss to protect profits"""
        if position_id not in self.positions:
            return
    
        position = self.positions[position_id]
    
        # For long positions
        if position['type'] == 'LONG':
            # Calculate profit percentage
            current_profit_pct = (current_price - position['entry_price']) / position['entry_price'] * 100
        
            # Set initial trailing stop if none exists
            if 'trailing_stop' not in position:
                # Only implement trailing stop when in profit
                if current_profit_pct > position['stop_loss_pct']:
                    position['trailing_stop'] = position['entry_price'] * (1 - position['stop_loss_pct'] / 100)
            else:
                # Calculate new potential stop
                new_stop = current_price * (1 - min(position['stop_loss_pct'], current_profit_pct * 0.5) / 100)
            
                # Only move stop up, never down
                if new_stop > position['trailing_stop']:
                    position['trailing_stop'] = new_stop
    
        # For short positions (similar logic, inverted)
        elif position['type'] == 'SHORT':
            # Calculate profit percentage (inverted for shorts)
            current_profit_pct = (position['entry_price'] - current_price) / position['entry_price'] * 100
        
            # Set initial trailing stop if none exists
            if 'trailing_stop' not in position:
                # Only implement trailing stop when in profit
                if current_profit_pct > position['stop_loss_pct']:
                    position['trailing_stop'] = position['entry_price'] * (1 + position['stop_loss_pct'] / 100)
            else:
                # Calculate new potential stop (inverted for shorts)
                new_stop = current_price * (1 + min(position['stop_loss_pct'], current_profit_pct * 0.5) / 100)
            
                # Only move stop down, never up (for shorts)
                if new_stop < position['trailing_stop']:
                    position['trailing_stop'] = new_stop

    def implement_partial_profit_taking(self, position_id: str, current_price: float) -> bool:
        """Take partial profits at predetermined levels"""
        if position_id not in self.positions:
            return False
    
        position = self.positions[position_id]
    
        # Skip if already took partial profits
        if position.get('partial_profit_taken', False):
            return False
    
        # For long positions
        if position['type'] == 'LONG':
            current_profit_pct = (current_price - position['entry_price']) / position['entry_price'] * 100
        
            # Take partial profits at 50% of target
            partial_target = position['take_profit_pct'] * 0.5
        
            if current_profit_pct >= partial_target:
                # Take profit on 40% of position
                partial_amount = position['amount'] * 0.4
                partial_pnl = partial_amount * (current_profit_pct / 100)
            
                # Update position
                position['amount'] -= partial_amount
                position['partial_profit_taken'] = True
                position['partial_profit_pct'] = current_profit_pct
                position['partial_profit_amount'] = partial_pnl
            
                # Update capital and daily P&L
                self.current_capital += partial_amount + partial_pnl
                self.daily_pnl += partial_pnl
            
                # Log the partial profit event
                partial_trade = {
                    'position_id': position_id,
                    'token': position['token'],
                    'type': 'PARTIAL_PROFIT',
                    'entry_time': position['entry_time'],
                    'exit_time': datetime.now(),
                    'entry_price': position['entry_price'],
                    'exit_price': current_price,
                    'amount': partial_amount,
                    'pnl': partial_pnl,
                    'return_pct': current_profit_pct,
                    'reason': 'PARTIAL_PROFIT',
                    'prediction_confidence': position.get('prediction_confidence', 0),
                    'remaining_amount': position['amount']
                }
                self.trade_history.append(partial_trade)
            
                print(f"💰 PARTIAL PROFIT: +${partial_pnl:.2f} ({current_profit_pct:.1f}%) | Remaining: ${position['amount']:.2f}")
                return True
    
        # For short positions
        elif position['type'] == 'SHORT':
            current_profit_pct = (position['entry_price'] - current_price) / position['entry_price'] * 100
        
            # Take partial profits at 50% of target
            partial_target = position['take_profit_pct'] * 0.5
        
            if current_profit_pct >= partial_target:
                # Take profit on 40% of position
                partial_amount = position['amount'] * 0.4
                partial_pnl = partial_amount * (current_profit_pct / 100)
            
                # Update position
                position['amount'] -= partial_amount
                position['partial_profit_taken'] = True
                position['partial_profit_pct'] = current_profit_pct
                position['partial_profit_amount'] = partial_pnl
            
                # Update capital and daily P&L
                self.current_capital += partial_amount + partial_pnl
                self.daily_pnl += partial_pnl
            
                # Log the partial profit event
                partial_trade = {
                    'position_id': position_id,
                    'token': position['token'],
                    'type': 'PARTIAL_PROFIT',
                    'entry_time': position['entry_time'],
                    'exit_time': datetime.now(),
                    'entry_price': position['entry_price'],
                    'exit_price': current_price,
                    'amount': partial_amount,
                    'pnl': partial_pnl,
                    'return_pct': current_profit_pct,
                    'reason': 'PARTIAL_PROFIT',
                    'prediction_confidence': position.get('prediction_confidence', 0),
                    'remaining_amount': position['amount']
                }
                self.trade_history.append(partial_trade)
            
                print(f"💰 PARTIAL PROFIT: +${partial_pnl:.2f} ({current_profit_pct:.1f}%) | Remaining: ${position['amount']:.2f}")
                return True
        
        return False                
    
    def safety_checks(self, trade_amount: float) -> bool:
        """Comprehensive safety checks"""
        self.reset_daily_limits()
        
        # Emergency stop check
        if self.emergency_stop:
            print("🚨 EMERGENCY STOP ACTIVE")
            return False
        
        # Daily loss limit
        if self.daily_pnl <= -self.DAILY_LOSS_LIMIT:
            print(f"🚨 Daily loss limit hit: ${self.daily_pnl:.2f}")
            self.emergency_stop = True
            return False
        
        # Daily trade limit
        if self.total_trades_today >= self.MAX_DAILY_TRADES:
            print(f"🚨 Daily trade limit hit: {self.total_trades_today}")
            return False
        
        # Capital check
        if trade_amount > self.current_capital:
            print(f"🚨 Insufficient capital: ${trade_amount:.2f} > ${self.current_capital:.2f}")
            return False
        
        # Gas price check
        try:
            if self.wallet_manager.w3:
                gas_price = self.wallet_manager.w3.eth.gas_price
                gas_price_gwei = self.wallet_manager.w3.from_wei(gas_price, 'gwei')
                if gas_price_gwei > 100:
                    print(f"🚨 Gas too high: {gas_price_gwei:.1f} gwei")
                    return False
        except:
            pass
        
        return True
    
    async def analyze_market_trend(self, token, market_data):
        """Analyze recent market trends to improve entry timing"""
        try:
            # Get historical price data
            token_map = {
                'BTC': 'bitcoin',
                'ETH': 'ethereum',
                'SOL': 'solana',
                'XRP': 'ripple',
                'BNB': 'binancecoin',
                'AVAX': 'avalanche-2'
            }
            coin_id = token_map.get(token)
        
            if not coin_id:
                return {'trend': 'unknown', 'strength': 0}
        
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
            params = {'vs_currency': 'usd', 'days': '1', 'interval': 'hourly'}
        
            response = requests.get(url, params=params, timeout=10)
            history = response.json()
        
            # Extract price data
            prices = [p[1] for p in history.get('prices', [])]
        
            if len(prices) < 6:
                return {'trend': 'unknown', 'strength': 0}
            
            # Calculate simple moving averages
            short_ma = sum(prices[-3:]) / 3
            long_ma = sum(prices[-6:]) / 6
        
            # Determine trend
            trend = 'bullish' if short_ma > long_ma else 'bearish'
            strength = abs(short_ma - long_ma) / long_ma * 100
        
            return {
                'trend': trend,
                'strength': strength,
                'current_price': prices[-1],
                'prices': prices
            }
        except Exception as e:
            print(f"❌ Trend analysis error: {e}")
            return {'trend': 'unknown', 'strength': 0}
        
    async def visualize_market_trend(self, token):
        """Visualize market trend for analysis and debugging"""
        try:
            # Get token data
            token_map = {
                'BTC': 'bitcoin',
                'ETH': 'ethereum',
                'SOL': 'solana',
                'XRP': 'ripple',
                'BNB': 'binancecoin',
                'AVAX': 'avalanche-2'
            }
            coin_id = token_map.get(token)
        
            if not coin_id:
                print(f"❌ Unknown token: {token}")
                return
        
            # Get historical data
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
            params = {'vs_currency': 'usd', 'days': '1', 'interval': 'hourly'}
        
            response = requests.get(url, params=params, timeout=10)
            history = response.json()
        
            # Extract prices and timestamps
            price_data = history.get('prices', [])
            timestamps = [datetime.fromtimestamp(p[0]/1000).strftime('%H:%M') for p in price_data]
            prices = [p[1] for p in price_data]
        
            if len(prices) < 6:
                print(f"❌ Not enough price data for {token}")
                return
            
            # Calculate moving averages
            short_ma = []
            long_ma = []
        
            for i in range(len(prices)):
                if i >= 2:  # Need at least 3 points for short MA
                    short_ma.append(sum(prices[max(0, i-2):i+1]) / min(3, i+1))
                else:
                    short_ma.append(None)
                
                if i >= 5:  # Need at least 6 points for long MA
                    long_ma.append(sum(prices[max(0, i-5):i+1]) / min(6, i+1))
                else:
                    long_ma.append(None)
        
            # Determine current trend
            current_trend = "BULLISH" if short_ma[-1] > long_ma[-1] else "BEARISH"
            trend_strength = abs(short_ma[-1] - long_ma[-1]) / long_ma[-1] * 100 if long_ma[-1] else 0
        
            # Print ASCII chart for terminal visualization
            print(f"\n📈 {token} TREND ANALYSIS:")
            print(f"   Current Trend: {current_trend} (Strength: {trend_strength:.2f}%)")
            print(f"   Current Price: ${prices[-1]:,.2f}")
            print(f"   Short MA (3h): ${short_ma[-1]:,.2f}")
            print(f"   Long MA (6h): ${long_ma[-1]:,.2f}")
        
            # Simple ASCII chart
            print("\n   Price Chart (Last 12 hours):")
            recent_prices = prices[-12:]
            max_price = max(recent_prices)
            min_price = min(recent_prices)
            range_price = max_price - min_price
        
            chart_height = 5
            for row in range(chart_height):
                chart_line = "   "
                price_level = max_price - (row * range_price / (chart_height - 1))
                if row == 0:
                    chart_line += f"${max_price:.2f} ┐"
                elif row == chart_height - 1:
                    chart_line += f"${min_price:.2f} └"
                else:
                    chart_line += f"${price_level:.2f} ┤"
                
                print(chart_line)
        
            time_axis = "     " + "".join([f"{t[-2:]} " for t in timestamps[-12:]])
            print(time_axis)
        
            return {
                'trend': current_trend.lower(),
                'strength': trend_strength,
                'current_price': prices[-1],
                'short_ma': short_ma[-1],
                'long_ma': long_ma[-1]
            }
        
        except Exception as e:
            print(f"❌ Trend visualization error: {e}")
            return None    
        
    def check_position_exits(self, position_id, current_price):
        """Check if position should be closed based on price and trailing stop"""
        if position_id not in self.positions:
            return False
        
        position = self.positions[position_id]
        should_close = False
        reason = ""
        pnl = 0
    
        # For long positions
        if position['type'] == 'LONG':
            # Calculate current profit
            profit_pct = (current_price - position['entry_price']) / position['entry_price'] * 100
            profit = position['amount'] * (profit_pct / 100)
        
            # Check take profit
            if profit_pct >= position['take_profit_pct']:
                should_close = True
                reason = "TAKE_PROFIT"
                pnl = profit
            # Check trailing stop if it exists
            elif 'trailing_stop' in position and current_price <= position['trailing_stop']:
                should_close = True
                reason = "TRAILING_STOP"
                pnl = profit
            # Check normal stop loss if no trailing stop or not yet activated
            elif current_price <= position['entry_price'] * (1 - position['stop_loss_pct'] / 100):
                should_close = True
                reason = "STOP_LOSS"
                pnl = -position['amount'] * (position['stop_loss_pct'] / 100)
    
        # For short positions
        elif position['type'] == 'SHORT':
            # Calculate current profit
            profit_pct = (position['entry_price'] - current_price) / position['entry_price'] * 100
            profit = position['amount'] * (profit_pct / 100)
        
            # Check take profit
            if profit_pct >= position['take_profit_pct']:
                should_close = True
                reason = "TAKE_PROFIT"
                pnl = profit
            # Check trailing stop if it exists
            elif 'trailing_stop' in position and current_price >= position['trailing_stop']:
                should_close = True
                reason = "TRAILING_STOP"
                pnl = profit
            # Check normal stop loss if no trailing stop or not yet activated
            elif current_price >= position['entry_price'] * (1 + position['stop_loss_pct'] / 100):
                should_close = True
                reason = "STOP_LOSS"
                pnl = -position['amount'] * (position['stop_loss_pct'] / 100)
    
        # Close position if needed
        if should_close:
            self.close_position(position_id, pnl, reason)
            return True
        
        return False    
    
    def execute_prediction_trade(self, prediction: Dict) -> bool:
        """Execute trade based on prediction with full automation"""
    
        if not self.setup_complete:
            print("❌ Trading system not setup. Run setup_trading_system() first.")
            return False
    
        # Check drawdown and potentially adjust risk parameters
        drawdown_status = self.manage_risk_based_on_drawdown()
    
        # Extract prediction data
        token = prediction.get('token', 'UNKNOWN')
        confidence = prediction.get('confidence', 0)
        target_price = prediction.get('target_price')
        current_price = prediction.get('current_price')
        volatility_score = prediction.get('volatility_score', 50)
        timeframe = prediction.get('timeframe', '1h')
    
        print(f"\n🎯 ANALYZING TRADE OPPORTUNITY: {token}")
        print(f"   📊 Confidence: {confidence:.1f}%")
        print(f"   💰 Current: ${current_price:.6f}")
        print(f"   🎪 Target: ${target_price:.6f}")
        print(f"   📈 Volatility: {volatility_score:.1f}")
    
        # Adjust confidence requirements based on drawdown status
        min_confidence = self.get_min_confidence_threshold(token, volatility_score)
    
        # Increase confidence requirements during drawdown periods
        if drawdown_status == "high_alert":
            min_confidence = min(95, min_confidence + 10)  # +10% during severe drawdown
            print(f"⚠️ DRAWDOWN ALERT: Confidence threshold increased to {min_confidence}%")
        elif drawdown_status == "medium_alert":
            min_confidence = min(95, min_confidence + 5)   # +5% during moderate drawdown
            print(f"⚠️ DRAWDOWN CAUTION: Confidence threshold increased to {min_confidence}%")
    
        # Confidence filter
        if confidence < min_confidence:
            print(f"🚫 REJECTED: Confidence {confidence:.1f}% < {min_confidence}%")
            return False
    
        # Calculate expected return
        if not target_price or not current_price:
            print(f"🚫 REJECTED: Missing price data")
            return False
    
        expected_return_pct = ((target_price - current_price) / current_price) * 100
    
        # Use dynamic minimum return threshold based on token and volatility
        min_return = self.get_min_return_threshold(token, volatility_score, timeframe)
    
        # Increase return requirements during drawdown periods
        if drawdown_status in ["high_alert", "medium_alert"]:
            min_return *= 1.25  # Require 25% higher returns during drawdown
            print(f"⚠️ DRAWDOWN ADJUSTMENT: Return threshold increased to {min_return:.1f}%")
    
        if abs(expected_return_pct) < min_return:
            print(f"🚫 REJECTED: Expected return {expected_return_pct:.1f}% < {min_return:.1f}%")
            return False
    
        # Determine trade direction and size
        trade_type = "LONG" if expected_return_pct > 0 else "SHORT"
    
        # Confidence-based position sizing
        confidence_multiplier = min(confidence / 100, 0.8)
        base_size = self.current_capital * self.MAX_POSITION_SIZE
        position_size = self.calculate_position_size(
            token, 
            confidence, 
            volatility_score, 
            expected_return_pct
        )
    
        # Reduce position size during high drawdown periods - already handled by manage_risk_based_on_drawdown
        # which adjusts self.MAX_POSITION_SIZE, but we can add an extra safety factor here
        if drawdown_status == "high_alert":
            position_size *= 0.8  # Additional 20% reduction during severe drawdown
            print(f"⚠️ DRAWDOWN SAFETY: Position size further reduced")
    
        # Safety checks
        if not self.safety_checks(position_size):
            return False
    
        # Calculate dynamic stops
        market_trend = prediction.get('market_trend', 'unknown')
        trend_strength = prediction.get('trend_strength', 0)
        stop_loss_pct = self.calculate_dynamic_stop_loss(token, volatility_score, confidence, 
                                                       market_trend, trend_strength)
        take_profit_pct = self.calculate_dynamic_take_profit(token, confidence, expected_return_pct, 
                                                          market_trend, trend_strength)
    
        # Tighten stops during drawdown periods
        if drawdown_status in ["high_alert", "medium_alert"]:
            stop_loss_pct *= 0.8  # 20% tighter stops during drawdown
            take_profit_pct *= 0.85  # 15% lower targets during drawdown
            print(f"⚠️ DRAWDOWN PROTECTION: Tighter stops applied")
    
        print(f"\n🚀 EXECUTING {trade_type}: ${position_size:.2f}")
        print(f"   📈 Expected: {expected_return_pct:.1f}%")
        print(f"   🛡️  Stop Loss: {stop_loss_pct:.1f}%")
        print(f"   💎 Take Profit: {take_profit_pct:.1f}%")
        print(f"   ⏰ Timeframe: {timeframe}")
        print(f"   💼 Size factors: confidence={confidence/100:.2f}, volatility={volatility_score/50:.2f}, return={abs(expected_return_pct)/10:.2f}")
    
        # Execute trade (simulate for now - replace with real DEX calls)
        success = self.simulate_trade_execution(
            token, trade_type, position_size, current_price, 
            stop_loss_pct, take_profit_pct, prediction
        )
    
        if success:
            self.total_trades_today += 1
            print(f"✅ TRADE #{self.total_trades_today} EXECUTED")
        
        return success
    
    def simulate_trade_execution(self, token: str, trade_type: str, amount: float, 
                                entry_price: float, stop_loss_pct: float, 
                                take_profit_pct: float, prediction: Dict) -> bool:
        """Simulate trade execution with realistic crypto outcomes"""
        
        # Calculate stops
        if trade_type == "LONG":
            stop_loss = entry_price * (1 - stop_loss_pct / 100)
            take_profit = entry_price * (1 + take_profit_pct / 100)
        else:
            stop_loss = entry_price * (1 + stop_loss_pct / 100)
            take_profit = entry_price * (1 - take_profit_pct / 100)
        
        # Store position
        position_id = f"{token}_{int(time.time())}"
        self.positions[position_id] = {
            'token': token,
            'type': trade_type,
            'amount': amount,
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'take_profit': take_profit,
            'stop_loss_pct': stop_loss_pct,
            'take_profit_pct': take_profit_pct,
            'entry_time': datetime.now(),
            'prediction': prediction,
            'status': 'OPEN'
        }
        
        # Reduce available capital
        self.current_capital -= amount
        
        # Simulate outcome based on volatility and confidence
        import random
        
        confidence = prediction.get('confidence', 70)
        volatility = prediction.get('volatility_score', 50)
        
        # Win probability based on confidence and stop loss width
        base_win_prob = min(0.7, confidence / 100)
        
        if stop_loss_pct <= 10:
            win_probability = base_win_prob * 0.7  # Tight stops hurt win rate
        elif stop_loss_pct <= 15:
            win_probability = base_win_prob * 0.85
        else:
            win_probability = base_win_prob * 1.0
        
        # Execute outcome immediately for demo
        if random.random() < win_probability:
            # Winning trade
            profit_pct = random.uniform(take_profit_pct * 0.4, take_profit_pct * 0.9)
            pnl = amount * (profit_pct / 100)
            self.close_position(position_id, pnl, "TAKE_PROFIT")
        else:
            # Losing trade
            loss_pct = random.uniform(stop_loss_pct * 0.3, stop_loss_pct * 0.95)
            pnl = -amount * (loss_pct / 100)
            self.close_position(position_id, pnl, "STOP_LOSS")
        
        return True
    
    def close_position(self, position_id: str, pnl: float, reason: str):
        """Close position and update metrics"""
        
        if position_id not in self.positions:
            return
        
        position = self.positions[position_id]
        
        # Update capital
        self.current_capital += position['amount'] + pnl
        self.daily_pnl += pnl
        
        # Record trade
        self.trade_history.append({
            'token': position['token'],
            'type': position['type'],
            'amount': position['amount'],
            'entry_price': position['entry_price'],
            'pnl': pnl,
            'return_pct': (pnl / position['amount']) * 100,
            'entry_time': position['entry_time'],
            'exit_time': datetime.now(),
            'reason': reason,
            'prediction_confidence': position['prediction'].get('confidence', 0)
        })
        
        # Remove position
        del self.positions[position_id]
        
        # Log result
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"${pnl:.2f}"
        print(f"💰 CLOSED: {pnl_str} ({reason}) | Capital: ${self.current_capital:.2f}")
        self.display_detailed_performance_report()
    
    def get_performance_stats(self) -> Dict:
        """Get comprehensive performance statistics"""
    
        if not self.trade_history:
            return {
                'total_return': 0.0,
                'total_return_pct': 0.0,
                'win_rate': 0.0,
                'total_trades': 0,
                'daily_pnl': self.daily_pnl,
                'current_capital': self.current_capital,
                'avg_confidence': 0.0,
                'current_drawdown_pct': 0.0,
                'max_drawdown_pct': 0.0
            }
    
        total_pnl = sum(trade['pnl'] for trade in self.trade_history)
        winning_trades = len([t for t in self.trade_history if t['pnl'] > 0])
        total_trades = len(self.trade_history)
        avg_confidence = sum(t['prediction_confidence'] for t in self.trade_history) / total_trades
    
        # Get drawdown metrics
        drawdown_data = self.calculate_drawdown()
    
        # Calculate advanced metrics if available
        advanced_metrics = {}
        if hasattr(self, 'calculate_advanced_performance_metrics'):
            try:
                advanced_metrics = self.calculate_advanced_performance_metrics()
            except Exception as e:
                print(f"Warning: Error calculating advanced metrics: {e}")
    
        return {
            'total_return': total_pnl,
            'total_return_pct': (total_pnl / self.initial_capital) * 100,
            'win_rate': (winning_trades / total_trades) * 100 if total_trades > 0 else 0,
            'total_trades': total_trades,
            'daily_pnl': self.daily_pnl,
            'current_capital': self.current_capital,
            'avg_confidence': avg_confidence,
            'winning_trades': winning_trades,
            'losing_trades': total_trades - winning_trades,
            'current_drawdown_pct': drawdown_data.get('current_drawdown_pct', 0.0),
            'max_drawdown_pct': drawdown_data.get('max_drawdown_pct', 0.0),
            'max_drawdown_end_time': drawdown_data.get('max_drawdown_end_time', None),
            # Include advanced metrics if available
            'profit_factor': advanced_metrics.get('profit_factor', 0.0),
            'risk_reward_ratio': advanced_metrics.get('risk_reward_ratio', 0.0),
            'expectancy': advanced_metrics.get('expectancy', 0.0),
            'consecutive_wins': advanced_metrics.get('consecutive_wins', 0),
            'consecutive_losses': advanced_metrics.get('consecutive_losses', 0)
        }
    
    def calculate_advanced_performance_metrics(self):
        """Calculate advanced trading performance metrics"""
        try:
            if not self.trade_history:
                return {
                    'profit_factor': 0,
                    'risk_reward_ratio': 0,
                    'expectancy': 0,
                    'consecutive_wins': 0,
                    'consecutive_losses': 0,
                    'max_consecutive_wins': 0,
                    'max_consecutive_losses': 0,
                    'gross_profit': 0,
                    'gross_loss': 0,
                    'avg_win': 0,
                    'avg_loss': 0,
                    'largest_win': 0,
                    'largest_loss': 0
               }
        
            # Separate winning and losing trades
            winning_trades = [t for t in self.trade_history if t['pnl'] > 0]
            losing_trades = [t for t in self.trade_history if t['pnl'] < 0]
        
            # Calculate basic profit/loss metrics
            gross_profit = sum(t['pnl'] for t in winning_trades)
            gross_loss = abs(sum(t['pnl'] for t in losing_trades))
        
            # Calculate profit factor (higher = better, >1.0 = profitable)
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf') if gross_profit > 0 else 0
        
            # Calculate average wins/losses
            avg_win = gross_profit / len(winning_trades) if winning_trades else 0
            avg_loss = gross_loss / len(losing_trades) if losing_trades else 0
        
            # Calculate risk-reward ratio
            risk_reward_ratio = avg_win / avg_loss if avg_loss > 0 else float('inf') if avg_win > 0 else 0
        
            # Calculate expectancy (expected value per trade)
            win_rate = len(winning_trades) / len(self.trade_history)
            expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
        
            # Find largest win/loss
            largest_win = max([t['pnl'] for t in winning_trades]) if winning_trades else 0
            largest_loss = abs(min([t['pnl'] for t in losing_trades])) if losing_trades else 0
        
            # Track consecutive wins/losses
            consecutive_wins = 0
            consecutive_losses = 0
            max_consecutive_wins = 0
            max_consecutive_losses = 0
            current_streak = 0
        
            for trade in self.trade_history:
                if trade['pnl'] > 0:  # Winning trade
                    if current_streak > 0:
                        current_streak += 1
                    else:
                        current_streak = 1
                    max_consecutive_wins = max(max_consecutive_wins, current_streak)
                else:  # Losing trade
                    if current_streak < 0:
                        current_streak -= 1
                    else:
                        current_streak = -1
                    max_consecutive_losses = max(max_consecutive_losses, abs(current_streak))
        
            # Current streaks
            consecutive_wins = max(0, current_streak)
            consecutive_losses = max(0, -current_streak)
        
            return {
                'profit_factor': round(profit_factor, 3),
                'risk_reward_ratio': round(risk_reward_ratio, 3),
                'expectancy': round(expectancy, 3),
                'consecutive_wins': consecutive_wins,
                'consecutive_losses': consecutive_losses,
                'max_consecutive_wins': max_consecutive_wins,
                'max_consecutive_losses': max_consecutive_losses,
                'gross_profit': round(gross_profit, 2),
                'gross_loss': round(gross_loss, 2),
                'avg_win': round(avg_win, 2),
                'avg_loss': round(avg_loss, 2),
                'largest_win': round(largest_win, 2),
                'largest_loss': round(largest_loss, 2)
            }
        
        except Exception as e:
            print(f"❌ Error calculating advanced metrics: {e}")
            return {
                'profit_factor': 0,
                'risk_reward_ratio': 0,
                'expectancy': 0,
                'consecutive_wins': 0,
                'consecutive_losses': 0,
                'max_consecutive_wins': 0,
                'max_consecutive_losses': 0,
                'gross_profit': 0,
                'gross_loss': 0,
                'avg_win': 0,
                'avg_loss': 0,
                'largest_win': 0,
                'largest_loss': 0
            }

    def get_comprehensive_performance_stats(self):
        """Get comprehensive performance statistics combining basic and advanced metrics"""
        try:
            # Get basic stats
            basic_stats = self.get_performance_stats()
        
            # Get advanced metrics
            advanced_metrics = self.calculate_advanced_performance_metrics()
        
            # Get drawdown metrics
            drawdown_metrics = self.calculate_drawdown()
        
            # Combine all metrics
            comprehensive_stats = {
                **basic_stats, 
                **advanced_metrics,
                'current_drawdown_pct': drawdown_metrics.get('current_drawdown_pct', 0),
                'max_drawdown_pct': drawdown_metrics.get('max_drawdown_pct', 0),
                'max_drawdown_end_time': drawdown_metrics.get('max_drawdown_end_time', None),
                'drawdown_history': drawdown_metrics.get('drawdown_history', [])
            }
        
            # Calculate return-to-drawdown ratio (a key risk-adjusted performance metric)
            if comprehensive_stats['max_drawdown_pct'] > 0:
                comprehensive_stats['return_to_drawdown'] = abs(comprehensive_stats['total_return_pct'] / 
                                                            comprehensive_stats['max_drawdown_pct'])
            else:
                comprehensive_stats['return_to_drawdown'] = float('inf') if comprehensive_stats['total_return_pct'] > 0 else 0
        
            return comprehensive_stats
    
        except Exception as e:
            print(f"❌ Error getting comprehensive performance stats: {e}")
            return self.get_performance_stats()  # Fallback to basic stats

    def display_detailed_performance_report(self):
        """Display detailed performance report after each trade"""
        try:
            stats = self.get_comprehensive_performance_stats()
        
            print(f"\n{'='*50}")
            print(f"📊 DETAILED PERFORMANCE REPORT - TRADE #{stats['total_trades']}")
            print(f"{'='*50}")
        
            # Capital Performance
            print(f"💰 CAPITAL:")
            print(f"   Current: ${stats['current_capital']:.2f}")
            print(f"   Total Return: ${stats['total_return']:+.2f} ({stats['total_return_pct']:+.1f}%)")
            print(f"   Daily P&L: ${stats['daily_pnl']:+.2f}")
        
            # Drawdown Section (NEW)
            print(f"\n📉 DRAWDOWN:")
            print(f"   Current: {stats['current_drawdown_pct']:.2f}%")
            print(f"   Maximum: {stats['max_drawdown_pct']:.2f}%")
            if 'return_to_drawdown' in stats:
                print(f"   Return/Drawdown: {stats['return_to_drawdown']:.2f}")
        
            # Trading Metrics
            print(f"\n📈 TRADING METRICS:")
            print(f"   Win Rate: {stats['win_rate']:.1f}% ({stats['winning_trades']}/{stats['total_trades']})")
            print(f"   Profit Factor: {stats['profit_factor']:.2f}")
            print(f"   Risk/Reward: {stats['risk_reward_ratio']:.2f}")
            print(f"   Expectancy: ${stats['expectancy']:+.2f}")
        
            # Win/Loss Analysis
            print(f"\n🏆 WIN/LOSS:")
            print(f"   Avg Win: ${stats['avg_win']:.2f} | Avg Loss: ${stats['avg_loss']:.2f}")
            print(f"   Largest Win: ${stats['largest_win']:.2f} | Largest Loss: ${stats['largest_loss']:.2f}")
            print(f"   Gross Profit: ${stats['gross_profit']:.2f} | Gross Loss: ${stats['gross_loss']:.2f}")
        
            # Streaks
            print(f"\n🔥 STREAKS:")
            print(f"   Current: {stats['consecutive_wins']} wins, {stats['consecutive_losses']} losses")
            print(f"   Max: {stats['max_consecutive_wins']} wins, {stats['max_consecutive_losses']} losses")
        
            # Enhanced Performance Assessment (including drawdown)
            print(f"\n🎯 ASSESSMENT:")
        
            # Using both profit factor and drawdown for assessment
            if stats['profit_factor'] > 1.5 and stats.get('return_to_drawdown', 0) > 2.5:
                print(f"   ✅ EXCELLENT - Strong profitable system with good risk control")
            elif stats['profit_factor'] > 1.2 and stats.get('return_to_drawdown', 0) > 1.5:
                print(f"   ✅ VERY GOOD - Profitable with reasonable drawdowns")
            elif stats['profit_factor'] > 1.0 and stats['expectancy'] > 0:
                print(f"   ⚠️  GOOD - Profitable, but drawdown control needs improvement")
            elif stats['total_return_pct'] > 0:
                print(f"   ⚠️  MARGINAL - Small profit, needs optimization")
            else:
                print(f"   ❌ POOR - Losing system, major adjustments needed")
        
            print(f"{'='*50}\n")
        
        except Exception as e:
            print(f"❌ Error displaying detailed performance report: {e}")

    def calculate_drawdown(self):
        """Calculate current and maximum drawdown"""
        if not self.trade_history:
            return {'current_drawdown_pct': 0, 'max_drawdown_pct': 0}
    
        # Create equity curve with timestamps
        equity = [(self.initial_capital, self.trade_history[0]['time'] if self.trade_history else datetime.now())]
        for trade in self.trade_history:
            equity.append((equity[-1][0] + trade['pnl'], trade.get('exit_time', trade.get('time', None))))
    
        # Calculate drawdowns
        peak = equity[0][0]
        drawdowns = []
        current_drawdown = 0
        max_drawdown = 0
        max_drawdown_start_time = None
        max_drawdown_end_time = None
    
        for value, timestamp in equity:
            if value > peak:
                peak = value
        
            drawdown = (peak - value) / peak * 100 if peak > 0 else 0
            drawdowns.append((drawdown, timestamp))
        
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                max_drawdown_end_time = timestamp
        
            current_drawdown = drawdown
    
        return {
            'current_drawdown_pct': current_drawdown,
            'max_drawdown_pct': max_drawdown,
            'max_drawdown_end_time': max_drawdown_end_time,
            'drawdown_history': drawdowns
        }     

    def manage_risk_based_on_drawdown(self):
        """Adjust trading parameters based on current drawdown"""
        # Get current drawdown data
        drawdown_data = self.calculate_drawdown()
        current_dd = drawdown_data['current_drawdown_pct']
    
        # Define drawdown thresholds and corresponding actions
        if current_dd > 20:
            # Severe drawdown - reduce position sizes significantly
            # Save original position size before reducing
            if not hasattr(self, '_original_max_position_size'):
                self._original_max_position_size = self.MAX_POSITION_SIZE
        
            # Apply the reduction
            self.MAX_POSITION_SIZE *= 0.5
            self._risk_adjusted = True
            print(f"⚠️ HIGH DRAWDOWN ALERT: {current_dd:.2f}% - Reducing position size by 50%")
            return "high_alert"
    
        elif current_dd > 15:
            # Significant drawdown - reduce position sizes moderately
            # Save original position size before reducing
            if not hasattr(self, '_original_max_position_size'):
                self._original_max_position_size = self.MAX_POSITION_SIZE
            
            # Apply the reduction
            self.MAX_POSITION_SIZE *= 0.75
            self._risk_adjusted = True
            print(f"⚠️ DRAWDOWN ALERT: {current_dd:.2f}% - Reducing position size by 25%")
            return "medium_alert"
    
        elif current_dd > 10:
            # Moderate drawdown - increase confidence thresholds
            if hasattr(self, 'MIN_CONFIDENCE'):
                # If the class uses a single confidence threshold
                # Save original confidence threshold before increasing
                if not hasattr(self, '_original_min_confidence'):
                    self._original_min_confidence = self.MIN_CONFIDENCE
                
                # Apply the increase (cap at 95)
                self.MIN_CONFIDENCE = min(95, self.MIN_CONFIDENCE + 5)
                self._risk_adjusted = True
                print(f"⚠️ DRAWDOWN CAUTION: {current_dd:.2f}% - Increased MIN_CONFIDENCE to {self.MIN_CONFIDENCE}")
            else:
                # Generic message if no confidence attribute exists
                print(f"⚠️ DRAWDOWN CAUTION: {current_dd:.2f}% - Consider increasing trade quality requirements")
            return "caution"
    
        # If drawdown is low enough, gradually restore normal trading parameters
        elif current_dd < 5 and hasattr(self, '_risk_adjusted'):
            # Reset position size to original value if we saved it
            if hasattr(self, '_original_max_position_size'):
                self.MAX_POSITION_SIZE = self._original_max_position_size
                # Remove the backup after restoring
                delattr(self, '_original_max_position_size')
            
            # Reset confidence threshold to original value if we saved it
            if hasattr(self, '_original_min_confidence'):
                self.MIN_CONFIDENCE = self._original_min_confidence
                # Remove the backup after restoring
                delattr(self, '_original_min_confidence')
            
            print(f"✅ DRAWDOWN RECOVERED: {current_dd:.2f}% - Restoring normal trading parameters")
            delattr(self, '_risk_adjusted')
            return "normal"
    
        # If none of the above conditions are met, return normal status
        return "normal"   
    
    def save_drawdown_history(self):
        """Save drawdown history to file"""
        drawdown_data = self.calculate_drawdown()
    
        # Prepare data for saving
        save_data = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'current_drawdown': drawdown_data['current_drawdown_pct'],
            'max_drawdown': drawdown_data['max_drawdown_pct'],
            'equity_curve': [self.initial_capital] + [self.initial_capital + sum(t['pnl'] for t in self.trade_history[:i+1]) 
                             for i in range(len(self.trade_history))]
        }
    
        # Save to file or database
        try:
            with open('drawdown_history.json', 'a') as f:
                f.write(json.dumps(save_data) + '\n')
        except Exception as e:
            print(f"Error saving drawdown history: {e}")

# =============================================================================
# 🔄 INTEGRATION WITH YOUR PREDICTION ENGINE
# =============================================================================

class PredictionTradeIntegration:
    """Integration layer between prediction engine and trading bot"""
    
    def __init__(self, trading_bot: PredictionIntegratedTrader):
        self.trading_bot = trading_bot
        self.last_trade_time = {}
        self.prediction_cache = {}
        
    async def get_market_data(self) -> Dict:
        """Get real-time market data from CoinGecko"""
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            params = {
                'ids': 'bitcoin,ethereum,solana,ripple,binancecoin,avalanche-2',
                'vs_currencies': 'usd',
                'include_24hr_change': 'true',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true'
            }
            
            response = requests.get(url, params=params, timeout=10)
            return response.json()
            
        except Exception as e:
            print(f"❌ Market data error: {e}")
            return {}
    
    async def generate_enhanced_prediction(self, token: str, market_data: Dict) -> Optional[Dict]:
        """Generate prediction with enhanced trend analysis integration"""
    
        try:
            token_map = {
                'BTC': 'bitcoin',
                'ETH': 'ethereum',
                'SOL': 'solana',
                'XRP': 'ripple',
                'BNB': 'binancecoin',
                'AVAX': 'avalanche-2'
            }
        
            coin_id = token_map.get(token)
            if not coin_id or coin_id not in market_data:
                return None
        
            data = market_data[coin_id]
            current_price = data['usd']
            change_24h = data.get('usd_24h_change', 0)
            volume_24h = data.get('usd_24h_vol', 0)
            market_cap = data.get('usd_market_cap', 0)
        
            # Calculate volatility score based on 24h change
            volatility_score = min(100, abs(change_24h) * 5)  # Scale to 0-100
        
            # Your prediction engine integration goes here
            # For demo, generating realistic predictions
            import random
        
            # Adjust confidence based on market conditions
            base_confidence = random.uniform(65, 95)
        
            # Reduce confidence for high volatility
            if volatility_score > 60:
                base_confidence *= 0.9
            elif volatility_score > 80:
                base_confidence *= 0.8
        
            # Generate price prediction
            price_change_pct = random.uniform(-15, 18)
            target_price = current_price * (1 + price_change_pct / 100)
        
            # Add trend analysis to improve predictions
            trend_data = await self.trading_bot.analyze_market_trend(token, market_data)
        
            # Adjust prediction based on trend
            if trend_data['trend'] == 'bullish' and trend_data['strength'] > 2:
                # In strong uptrend, slightly boost bullish predictions
                if price_change_pct > 0:
                    price_change_pct *= 1.1
                    base_confidence *= 1.05
                else:
                    # Be more cautious with bearish predictions in uptrend
                    price_change_pct *= 0.9
                    base_confidence *= 0.95
            elif trend_data['trend'] == 'bearish' and trend_data['strength'] > 2:
                # In strong downtrend, slightly boost bearish predictions
                if price_change_pct < 0:
                    price_change_pct *= 1.1
                    base_confidence *= 1.05
                else:
                    # Be more cautious with bullish predictions in downtrend
                    price_change_pct *= 0.9
                    base_confidence *= 0.95
        
            # Update target price and confidence based on trend analysis
            target_price = current_price * (1 + price_change_pct / 100)
        
            # Create the prediction with trend-enhanced values
            prediction = {
                'token': token,
                'current_price': current_price,
                'target_price': target_price,
                'confidence': min(99, base_confidence),  # Cap at 99%
                'volatility_score': volatility_score,
                'expected_return_pct': price_change_pct,
                'timeframe': '1h',
                'volume_24h': volume_24h,
                'market_cap': market_cap,
                'change_24h': change_24h,
                'market_trend': trend_data['trend'],
                'trend_strength': trend_data['strength'],
                'timestamp': datetime.now().isoformat()
            }
        
            # Print prediction analysis
            print(f"\n🎯 ANALYZING TRADE OPPORTUNITY: {token}")
            print(f"   📊 Confidence: {prediction['confidence']:.1f}%")
            print(f"   💰 Current: ${prediction['current_price']:.6f}")
            print(f"   🎪 Target: ${prediction['target_price']:.6f}")
            print(f"   📈 Volatility: {prediction['volatility_score']:.1f}")
        
            return prediction
        
        except Exception as e:
            print(f"❌ Prediction error for {token}: {e}")
            return None
    
    async def continuous_trading_loop(self):
        """Main automated trading loop"""

        print("🚀 STARTING CONTINUOUS AUTO TRADING...")
        print("💡 Bot will trade automatically based on dynamic confidence thresholds")
        print("🛑 Daily limits: $25 loss | 120 trades max")
        print("⏰ Checking market every 2 minutes...")

        while True:
            try:
                # Check if trading system is ready
                if not self.trading_bot.setup_complete:
                    print("⚠️  Trading system not setup. Waiting...")
                    await asyncio.sleep(30)
                    continue
        
                # Get market data
                market_data = await self.get_market_data()
                if not market_data:
                    await asyncio.sleep(60)
                    continue
        
                # Check status of open positions
                for position_id, position in list(self.trading_bot.positions.items()):
                    try:
                        # Get current price from market data
                        token = position['token']
                        token_map = {'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana', 
                                    'XRP': 'ripple', 'BNB': 'binancecoin', 'AVAX': 'avalanche-2'}
                        coin_id = token_map.get(token)
                
                        if coin_id and coin_id in market_data:
                            current_price = market_data[coin_id]['usd']
                    
                            # NEW: Check for partial profit taking first
                            self.trading_bot.implement_partial_profit_taking(position_id, current_price)
                        
                            # Update trailing stop
                            self.trading_bot.implement_trailing_stop_loss(position_id, current_price)
                    
                            # Check if position should be closed
                            self.trading_bot.check_position_exits(position_id, current_price)
                    
                            # Log position status
                            self.log_position_status(position_id, current_price)
                    except Exception as e:
                        print(f"❌ Error monitoring position {position_id}: {e}")
        
                # Check each token for trading opportunities
                tokens = ['BTC', 'ETH', 'SOL', 'XRP', 'BNB', 'AVAX']
        
                for token in tokens:
                    # Rate limiting - don't trade same token too frequently
                    last_trade = self.last_trade_time.get(token, 0)
                    if time.time() - last_trade < 600:  # 10 min cooldown
                        continue
            
                    # Generate prediction
                    prediction = await self.generate_enhanced_prediction(token, market_data)
            
                    if prediction:
                        token = prediction.get('token', 'UNKNOWN')
                        volatility_score = prediction.get('volatility_score', 50)
                        min_confidence = self.trading_bot.get_min_confidence_threshold(token, volatility_score)

                        if prediction['confidence'] >= min_confidence:
                            # Attempt trade
                            trade_executed = self.trading_bot.execute_prediction_trade(prediction)

                            if trade_executed:
                                self.last_trade_time[token] = time.time()
        
                # Show performance every loop
                stats = self.trading_bot.get_performance_stats()
        
                print(f"\n📊 PERFORMANCE UPDATE:")
                print(f"   💰 Total P&L: ${stats['total_return']:+.2f} ({stats['total_return_pct']:+.1f}%)")
                print(f"   🎯 Win Rate: {stats['win_rate']:.1f}% ({stats['winning_trades']}/{stats['total_trades']})")
                print(f"   📈 Capital: ${stats['current_capital']:.2f}")
                print(f"   📅 Daily P&L: ${stats['daily_pnl']:+.2f}")
                print(f"   🔄 Trades Today: {self.trading_bot.total_trades_today}")
        
                # Wait before next cycle
                await asyncio.sleep(120)  # Check every 2 minutes
        
            except KeyboardInterrupt:
                print("\n🛑 Trading stopped by user")
                break
            except Exception as e:
                print(f"❌ Trading loop error: {e}")
                await asyncio.sleep(60)

    def log_position_status(self, position_id, current_price):
        """Log current position status with profit information"""
        position = self.trading_bot.positions.get(position_id)
        if not position:
            return
        
        # Calculate current profit/loss
        if position['type'] == 'LONG':
            profit_pct = (current_price - position['entry_price']) / position['entry_price'] * 100
        else:  # SHORT
            profit_pct = (position['entry_price'] - current_price) / position['entry_price'] * 100
        
        profit = position['amount'] * (profit_pct / 100)
    
        # Only log if significant change
        if abs(profit_pct) > 5 or abs(profit) > 1.0:
            trailing_stop_info = ""
            if 'trailing_stop' in position:
                if position['type'] == 'LONG':
                    distance_pct = (current_price - position['trailing_stop']) / current_price * 100
                else:
                    distance_pct = (position['trailing_stop'] - current_price) / current_price * 100
                trailing_stop_info = f" | 🛡️ Trailing Stop: ${position['trailing_stop']:.2f} ({distance_pct:.1f}% away)"
            
            print(f"📊 POSITION UPDATE [{position_id}]: {profit_pct:+.1f}% (${profit:+.2f}){trailing_stop_info}")            

# =============================================================================
# 🚀 MAIN EXECUTION FUNCTIONS
# =============================================================================

def setup_and_start_trading():
    """Complete setup and start trading"""
    
    print("🔥 INTEGRATED CRYPTO TRADING BOT")
    print("=" * 50)
    print("💡 This bot combines advanced predictions with automated Web3 trading")
    print("🎯 Target: Turn $100 into generational wealth through smart automation")
    
    # Initialize trading bot
    bot = PredictionIntegratedTrader(initial_capital=100.0)
    
    # Setup trading system
    if not bot.setup_trading_system():
        print("❌ Setup failed. Please check your environment and try again.")
        return
    
    # Create integration
    integration = PredictionTradeIntegration(bot)
    
    # Start trading
    print("\n🚀 Starting automated trading...")
    print("Press Ctrl+C to stop trading at any time")
    
    try:
        asyncio.run(integration.continuous_trading_loop())
    except KeyboardInterrupt:
        print("\n✅ Trading stopped gracefully")
        
        # Final stats
        stats = bot.get_performance_stats()
        print(f"\n📊 FINAL PERFORMANCE:")
        print(f"   💰 Total Return: ${stats['total_return']:+.2f} ({stats['total_return_pct']:+.1f}%)")
        print(f"   🎯 Win Rate: {stats['win_rate']:.1f}%")
        print(f"   📈 Final Capital: ${stats['current_capital']:.2f}")
        print(f"   🔄 Total Trades: {stats['total_trades']}")

def test_prediction_generation():
    """Test the prediction generation system"""
    
    print("🧪 TESTING PREDICTION GENERATION...")
    
    # Create test bot
    bot = PredictionIntegratedTrader(100.0)
    integration = PredictionTradeIntegration(bot)
    
    async def test():
        market_data = await integration.get_market_data()
        
        print(f"📊 Market Data Retrieved: {len(market_data)} tokens")
        
        for token in ['BTC', 'ETH', 'SOL']:
            prediction = await integration.generate_enhanced_prediction(token, market_data)
            
            if prediction:
                print(f"\n🎯 {token} Prediction:")
                print(f"   Price: ${prediction['current_price']:,.2f}")
                print(f"   Target: ${prediction['target_price']:,.2f}")
                print(f"   Confidence: {prediction['confidence']:.1f}%")
                print(f"   Expected Return: {prediction['expected_return_pct']:+.1f}%")
                print(f"   Volatility Score: {prediction['volatility_score']:.1f}")
    
    asyncio.run(test())

if __name__ == "__main__":
    # Run the complete trading system
    setup_and_start_trading()

#!/usr/bin/env python3
"""
Signal Checker for Options Trading
Analyzes streaming data with calculated indicators to determine entry/exit signals
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple
import json
import os


class SignalChecker:
    """
    Signal checker for options trading using technical indicators
    Tracks trades per symbol and provides entry/exit signals
    """
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        
        # Trade tracking per symbol
        self.trades: Dict[str, List[Dict]] = {}  # symbol -> list of completed trades
        self.active_trades: Dict[str, Dict[str, Dict]] = {}  # symbol -> contract_type -> active trade info
        
        # Trade configuration
        self.trailing_stop_pct = 0.9  # 10% trailing stop
        self.stop_loss_pct = 0.95     # 5% stop loss
        
        # Trading restrictions configuration
        self.trading_restrictions = {
            'target_dte': 1,  # Only trade options with exactly this DTE (x-1 if tracking x and x-1)
            'max_trades_per_symbol': {'C': 1, 'P': 1},  # Max 1 call + 1 put per base symbol
            'require_closest_strike': True,  # Only trade the strike closest to underlying price
        }
        
        # Dynamic ITM strike tracking per base symbol and contract type
        self.current_itm_strikes = {}  # base_symbol -> {'C': strike_below_price, 'P': strike_above_price}
        
        # Signal configuration
        self.signal_config = {
            'buy': {
                'trend_conditions_required': 2,      # Need at least 2 trend conditions AND
                'momentum_conditions_required': 2    # Need at least 2 momentum conditions (AND logic)
            },
            'sell': {
                'trend_conditions_required': 1,      # Never sell on technical signals (max possible: 2)
                'momentum_conditions_required': 2    # Never sell on technical signals (max possible: 2)
            }
        }

        # Ensure trades directory exists
        os.makedirs('data/trades', exist_ok=True)

    
    def log_trade_open(self, trade_info: Dict):
        """
        Log trade opening to data/trades/open.csv
        
        Args:
            trade_info: Trade information dictionary
        """
        if self.debug:
            print(f"📝 log_trade_open called with: {trade_info}")
        try:
            csv_file = 'data/trades/open.csv'   
            
            # Extract indicator values from entry data
            entry_indicators = trade_info.get('entry_indicators', {})
            
            # Prepare trade data for CSV in exact column order (no full_symbol, clean format)
            trade_data = [
                trade_info.get('entry_timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                trade_info.get('symbol', ''),
                trade_info.get('contract_type', ''),
                trade_info.get('strike_price', 0),
                trade_info.get('entry_price', 0),
                entry_indicators.get('entry_ema', 0),
                entry_indicators.get('entry_ema_fast', 0),
                entry_indicators.get('entry_ema_slow', 0),
                entry_indicators.get('entry_vwma', 0),
                entry_indicators.get('entry_macd_line', 0),
                entry_indicators.get('entry_macd_signal', 0),
                entry_indicators.get('entry_stoch_rsi_k', 0),
                entry_indicators.get('entry_stoch_rsi_d', 0),
                entry_indicators.get('entry_roc', 0),
                entry_indicators.get('entry_roc_fast', 0),
                entry_indicators.get('entry_roc_slow', 0),
                entry_indicators.get('entry_roc_of_roc', 0)
            ]
            
            # Convert to DataFrame with explicit column order
            df = pd.DataFrame([trade_data], columns=columns)
            
            # Write to CSV (append if exists, create with headers if not)
            if os.path.exists(csv_file):
                df.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                df.to_csv(csv_file, mode='w', header=True, index=False)
                

        except Exception as e:
            pass
    
    def log_trade_close(self, completed_trade: Dict):
        """
        Log trade closing to data/trades/close.csv
        
        Args:
            completed_trade: Completed trade information dictionary
        """
        if self.debug:
            print(f"📝 log_trade_close called with: {completed_trade}")
        try:
            # Validate required fields
            if not completed_trade or 'contract_type' not in completed_trade:
                print(f"❌ Invalid completed_trade: missing contract_type field")
                return
                
            csv_file = 'data/trades/close.csv'
            
            # Calculate trade duration in minutes
            try:
                entry_time = datetime.strptime(completed_trade['entry_timestamp'], "%Y-%m-%d %H:%M:%S")
                exit_time = datetime.strptime(completed_trade['exit_timestamp'], "%Y-%m-%d %H:%M:%S")
                duration_minutes = (exit_time - entry_time).total_seconds() / 60
            except:
                duration_minutes = 0

            # Extract indicator values from entry and exit data
            entry_indicators = completed_trade.get('entry_indicators', {})
            exit_indicators = completed_trade.get('exit_indicators', {})
            
            # Prepare trade data for CSV in exact column order (no full_symbol, clean format)
            trade_data = [
                completed_trade.get('symbol', ''),
                completed_trade.get('contract_type', ''),
                completed_trade.get('strike_price', 0),
                completed_trade.get('entry_price', 0),
                completed_trade.get('exit_price', 0),
                completed_trade.get('profit', 0),
                completed_trade.get('profit_pct', 0),
                duration_minutes,
                completed_trade.get('exit_reason', ''),
                completed_trade.get('max_unrealized_profit', 0),
                completed_trade.get('max_drawdown', 0),
                completed_trade.get('entry_timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                completed_trade.get('exit_timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                entry_indicators.get('entry_ema_fast', 0),
                entry_indicators.get('entry_ema_slow', 0),
                entry_indicators.get('entry_vwma', 0),
                entry_indicators.get('entry_macd_line', 0),
                entry_indicators.get('entry_macd_signal', 0),
                entry_indicators.get('entry_stoch_rsi_k', 0),
                entry_indicators.get('entry_stoch_rsi_d', 0),
                entry_indicators.get('entry_roc', 0),
                entry_indicators.get('entry_roc_fast', 0),
                entry_indicators.get('entry_roc_slow', 0),
                entry_indicators.get('entry_roc_of_roc', 0),
                exit_indicators.get('exit_ema_fast', 0),
                exit_indicators.get('exit_ema_slow', 0),
                exit_indicators.get('exit_vwma', 0),
                exit_indicators.get('exit_macd_line', 0),
                exit_indicators.get('exit_macd_signal', 0),
                exit_indicators.get('exit_stoch_rsi_k', 0),
                exit_indicators.get('exit_stoch_rsi_d', 0),
                exit_indicators.get('exit_roc', 0),
                exit_indicators.get('exit_roc_fast', 0),
                exit_indicators.get('exit_roc_slow', 0),
                exit_indicators.get('exit_roc_of_roc', 0)
            ]
            
            # Convert to DataFrame with explicit column order
            df = pd.DataFrame([trade_data], columns=columns)
            
            # Write to CSV (append if exists, create with headers if not)
            if os.path.exists(csv_file):
                df.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                df.to_csv(csv_file, mode='w', header=True, index=False)
                

        except Exception as e:
            pass
    
    def remove_from_open_trades_csv(self, symbol: str, contract_type: str, strike_price: float):
        """
        Remove a closed trade from the open trades CSV file using symbol+contract_type+strike_price
        
        Args:
            symbol: Option symbol to remove from open trades
        """
        try:
            csv_file = 'data/trades/open.csv'
            
            # Check if file exists
            if not os.path.exists(csv_file):
                return
        
            # Read existing open trades with error handling for malformed CSV
            try:
                df = pd.read_csv(csv_file)
            except pd.errors.ParserError:
                print(f"❌ Error reading open.csv: {e}")
                return
            
            
            # Use symbol + contract_type + strike_price for exact matching
            # Convert strike_price to float for comparison to handle potential data type mismatches
            mask = (df['symbol'] == symbol) & \
                   (df['contract_type'] == contract_type) & \
                   (df['strike_price'].astype(float) == float(strike_price))
            
            if self.debug:
                print(f"🔍 Looking for trade to remove: {symbol} {contract_type} {strike_price}")
                print(f"📊 Found {mask.sum()} matching trades in open.csv")
                print(f"📊 Total trades in open.csv: {len(df)}")
            
            df = df[~mask]
            
            
            # Write back to CSV
            if len(df) > 0:
                df.to_csv(csv_file, index=False)
                if self.debug:
                    print(f"📝 Updated open.csv: {len(df)} trades remaining")
            else:
                # If no trades left, create empty file with headers
                df.to_csv(csv_file, index=False)
                if self.debug:
                    print(f"📝 Cleared open.csv: no trades remaining")        
                
        except Exception as e:
            if self.debug:
                print(f"❌ Error removing from open trades CSV: {e}")
            pass
    
    def get_open_trades_from_csv(self) -> pd.DataFrame:
        """
        Get current open trades from CSV file
        
        Returns:
            DataFrame with open trades, empty DataFrame if file doesn't exist
        """
        try:
            csv_file = 'data/trades/open.csv'
            if os.path.exists(csv_file):
                return pd.read_csv(csv_file)
            else:
                return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()
    
    def check_signal_combos(self, row: pd.Series, signal_type: str) -> Tuple[bool, Dict]:
        """
        Check if signal conditions are met for entry/exit
        
        Args:
            row: Data row with calculated indicators
            signal_type: 'buy' or 'sell'
            
        Returns:
            Tuple of (signal_triggered)
        """
        print(f"🔍 check_signal_combos called with signal_type: {signal_type}")
        print(f"🔍 DEBUG: Available indicators in row: {[k for k in row.keys() if k not in ['symbol', 'contract_type', 'mark_price', 'strike_price', 'timestamp']]}")
        
        def safe_float(value):
            """Safely convert value to float, return None if conversion fails"""
            if pd.isna(value) or value == "" or value is None:
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                print(f"⚠️ Warning: Could not convert '{value}' to float")
                return None
        
        trend_conditions_met = 0
        momentum_conditions_met = 0
        
        if signal_type == 'buy':
            if 'roc' in row:    
                roc_val = safe_float(row['roc'])
                if roc_val is not None and roc_val > 0:
                    momentum_conditions_met += 1

            if 'stoch_rsi_k' in row and 'stoch_rsi_d' in row:
                stoch_k = safe_float(row['stoch_rsi_k'])
                stoch_d = safe_float(row['stoch_rsi_d'])
                if stoch_k is not None and stoch_d is not None and stoch_k > stoch_d and stoch_k <= 40:
                    momentum_conditions_met += 1

            if 'macd_line' in row and 'macd_signal' in row:
                macd_line = safe_float(row['macd_line'])
                macd_signal = safe_float(row['macd_signal'])
                if macd_line is not None and macd_signal is not None and macd_line > macd_signal:
                    trend_conditions_met += 1
                
            if 'ema' in row and 'vwma' in row:
                ema = safe_float(row['ema'])
                vwma = safe_float(row['vwma'])
                if ema is not None and vwma is not None and ema > vwma:
                    trend_conditions_met += 1
                
            if 'ema_fast' in row and 'ema_slow' in row:
                ema_fast = safe_float(row['ema_fast'])
                ema_slow = safe_float(row['ema_slow'])
                if ema_fast is not None and ema_slow is not None and ema_fast > ema_slow:
                    trend_conditions_met += 1

            if 'roc_fast' in row and 'roc_slow' in row:
                roc_fast = safe_float(row['roc_fast'])
                roc_slow = safe_float(row['roc_slow'])
                if roc_fast is not None and roc_slow is not None:
                    if roc_fast > 0 and roc_slow > 0:
                        momentum_conditions_met += 1
                    elif roc_fast < 0 and roc_slow < 0 and roc_fast > roc_slow:
                        momentum_conditions_met += 1

            config = self.signal_config['buy']
        elif signal_type == 'sell':
            if 'ema' in row and 'vwma' in row:
                ema = safe_float(row['ema'])
                vwma = safe_float(row['vwma'])
                if ema is not None and vwma is not None and ema < vwma:
                    trend_conditions_met += 1

            # SELL signals use opposite conditions of BUY signals
            if 'roc' in row:
                roc_val = safe_float(row['roc'])
                if roc_val is not None and roc_val < 0:
                    momentum_conditions_met += 1
                
            if 'ema_fast' in row and 'ema_slow' in row:
                ema_fast = safe_float(row['ema_fast'])
                ema_slow = safe_float(row['ema_slow'])
                if ema_fast is not None and ema_slow is not None and ema_fast < ema_slow:
                    trend_conditions_met += 1

            if 'roc_fast' in row and 'roc_slow' in row:
                roc_fast = safe_float(row['roc_fast'])
                roc_slow = safe_float(row['roc_slow'])
                if roc_fast is not None and roc_slow is not None:
                    if roc_fast < 0 and roc_slow < 0:
                        momentum_conditions_met += 1
                    elif roc_fast > 0 and roc_slow > 0 and roc_fast < roc_slow:
                        momentum_conditions_met += 1
            
            if 'stoch_rsi_k' in row and 'stoch_rsi_d' in row:
                stoch_k = safe_float(row['stoch_rsi_k'])
                stoch_d = safe_float(row['stoch_rsi_d'])
                if stoch_k is not None and stoch_d is not None and (stoch_k < stoch_d or stoch_k >= 60):
                    momentum_conditions_met += 1

            if 'macd_line' in row and 'macd_signal' in row:
                macd_line = safe_float(row['macd_line'])
                macd_signal = safe_float(row['macd_signal'])
                if macd_line is not None and macd_signal is not None and macd_line < macd_signal:
                    trend_conditions_met += 1
            
            config = self.signal_config['sell']

        signal_triggered = (trend_conditions_met >= config['trend_conditions_required'] and 
                           momentum_conditions_met >= config['momentum_conditions_required'])
        
        print(f"🔍 DEBUG: Signal check result - trend_conditions_met: {trend_conditions_met}, momentum_conditions_met: {momentum_conditions_met}, signal_triggered: {signal_triggered}")
        
        return signal_triggered, {
            'trend_conditions_met': trend_conditions_met,
            'momentum_conditions_met': momentum_conditions_met,
            'trend_conditions_required': config['trend_conditions_required'],
            'momentum_conditions_required': config['momentum_conditions_required']
        }

    def should_enter_trade(self, symbol: str, row: pd.Series) -> Tuple[bool, Dict]:
        """
        Determine if we should enter a trade for this symbol
        
        Args:
            symbol: Option symbol
            row: Current data row with indicators
            
        Returns:
            Tuple of (should_enter)
        """
        print(f"🔍 DEBUG: should_enter_trade called for {symbol}")
        
        # Validate required fields
        if 'contract_type' not in row or 'mark_price' not in row:
            print(f"❌ Invalid row data for {symbol}: missing required fields in should_enter_trade")
            return False, {'reason': 'invalid_data'}
            
        # Don't enter if trade already open
        if symbol in self.active_trades and row['contract_type'] in self.active_trades[symbol]:
            print(f"🔍 DEBUG: Trade already open for {symbol} {row['contract_type']}")
            return False, {'reason': 'trade_already_open'}
        
        print(f"🔍 DEBUG: No active trade, checking buy signal for {symbol}")
        
        # Check for buy signal
        signal_triggered, signal_details = self.check_signal_combos(row, 'buy')
        
        if signal_triggered:
            return True, {
                'reason': 'buy_signal',
                'trend_conditions_met': signal_details.get('trend_conditions_met', 0),
                'momentum_conditions_met': signal_details.get('momentum_conditions_met', 0)
            }
        else:
            return False, signal_details
    
    def should_exit_trade(self, symbol: str, row: pd.Series) -> Tuple[bool, Dict]:
        """
        Determine if we should exit an active trade for this symbol
        
        Args:
            symbol: Option symbol
            row: Current data row with indicators
            
        Returns:
            Tuple of (should_exit, exit_details)
        """
        # Validate required fields
        if 'contract_type' not in row or 'mark_price' not in row:
            print(f"❌ Invalid row data for {symbol}: missing required fields in should_exit_trade")
            return False, {'reason': 'invalid_data'}
            
        # No active trade to exit
        if symbol not in self.active_trades or row['contract_type'] not in self.active_trades[symbol]:
            return False, {'reason': 'no_active_trade'}
        
        active_trade = self.active_trades[symbol][row['contract_type']]
        current_price = row['mark_price']
        entry_price = active_trade['entry_price']
        max_price_seen = max(active_trade['max_price_seen'], current_price)
        
        exit_details = {
            'current_price': row['mark_price'],
            'entry_price': entry_price,
            'max_price_seen': max_price_seen,
            'unrealized_pnl': current_price - entry_price,
            'max_unrealized_pnl': max_price_seen - entry_price
        }
        

        # Check stop loss
        if row['mark_price'] <= entry_price * self.stop_loss_pct:
            exit_details.update({
                'exit_reason': 'stop_loss',
                'stop_loss_level': entry_price * self.stop_loss_pct
            })

            return True, exit_details
        
        # Check trailing stop
        if row['mark_price'] <= max_price_seen * self.trailing_stop_pct:
            exit_details.update({
                'exit_reason': 'trailing_stop',
                'trailing_stop_level': max_price_seen * self.trailing_stop_pct
            })

            return True, exit_details
        
        # Check sell signal
        signal_triggered, exit_details = self.check_signal_combos(row, 'sell')
        if signal_triggered:
            exit_details.update({
                'exit_reason': 'sell_signal',
            })

            return True, exit_details
        
        return False, exit_details
    
    def should_exit_trade_stops_only(self, symbol: str, row: pd.Series) -> Tuple[bool, Dict]:
        """
        Check only stop loss and trailing stop conditions (not sell signals).
        Used for mark_price change detection to catch risk management exits faster.
        
        Args:
            symbol: Option symbol
            row: Current data row with mark_price
            
        Returns:
            Tuple of (should_exit, exit_details)
        """
        # Validate required fields
        if 'contract_type' not in row or 'mark_price' not in row:
            print(f"❌ Invalid row data for {symbol}: missing required fields in should_exit_trade_stops_only")
            return False, {'reason': 'invalid_data'}
            
        # No active trade to exit
        if symbol not in self.active_trades or row['contract_type'] not in self.active_trades[symbol]:
            return False, {'reason': 'no_active_trade'}
        
        active_trade = self.active_trades[symbol][row['contract_type']]
        current_price = row['mark_price']
        entry_price = active_trade['entry_price']
        max_price_seen = max(active_trade['max_price_seen'], current_price)
        
        exit_details = {
            'current_price': current_price,
            'entry_price': entry_price,
            'max_price_seen': max_price_seen,
            'unrealized_pnl': current_price - entry_price,
            'max_unrealized_pnl': max_price_seen - entry_price,
            'price_source': 'mark_price'
        }
        
        # Check stop loss (using mark_price)
        if current_price <= entry_price * self.stop_loss_pct:
            exit_details.update({
                'exit_reason': 'stop_loss',
                'stop_loss_level': entry_price * self.stop_loss_pct
            })



            return True, exit_details
        
        # Check trailing stop (using mark_price)
        if current_price <= max_price_seen * self.trailing_stop_pct:
            exit_details.update({
                'exit_reason': 'trailing_stop',
                'trailing_stop_level': max_price_seen * self.trailing_stop_pct
            })

            return True, exit_details
        
        return False, exit_details
    
    def enter_trade(self, symbol: str, row: pd.Series) -> Dict:
        """
        Enter a new trade for the symbol
        
        Args:
            symbol: Option symbol
            row: Current data row
            
        Returns:
            Trade entry details
        """
        # Validate required fields
        if 'contract_type' not in row or 'mark_price' not in row:
            print(f"❌ Invalid row data for {symbol}: missing required fields")
            return {}
            
        entry_price = row['mark_price']
        
        # Capture entry indicator values dynamically - only include indicators that exist
        entry_indicators = {}
        base_fields = ['symbol', 'contract_type', 'mark_price', 'strike_price', 'timestamp', 'tick_open', 'tick_high', 'tick_low', 'tick_close', 'tick_volume', 'total_volume', 'open_interest', 'bid_price', 'ask_price', 'last_price', 'high_price', 'low_price', 'close_price', 'volatility', 'money_intrinsic_value', 'expiration_year', 'multiplier', 'digits', 'open_price', 'bid_size', 'ask_size', 'last_size', 'net_change', 'underlying', 'expiration_month', 'deliverables', 'time_value', 'expiration_day', 'days_to_expiration', 'delta', 'gamma', 'theta', 'vega', 'rho', 'security_status', 'theoretical_option_value', 'underlying_price', 'uv_expiration_type', 'quote_time', 'trade_time', 'exchange', 'exchange_name', 'last_trading_day', 'settlement_type', 'net_percent_change', 'mark_price_net_change', 'mark_price_percent_change', 'implied_yield', 'is_penny_pilot', 'option_root', 'week_52_high', 'week_52_low', 'indicative_ask_price', 'indicative_bid_price', 'indicative_quote_time', 'exercise_type']
        
        # Find all indicator fields (anything not in base_fields)
        for key, value in row.items():
            if key not in base_fields and value != "" and value != 0 and not pd.isna(value):
                entry_indicators[f'entry_{key}'] = value
        
        if self.debug:
            print(f"🔍 DEBUG: Captured entry indicators: {list(entry_indicators.keys())}")
        
        trade_info = {
            'symbol': symbol,
            'contract_type': row['contract_type'],
            'strike_price': row['strike_price'],
            'entry_price': entry_price,
            'entry_timestamp': row.get('timestamp', ''),
            'max_price_seen': entry_price,
            'min_price_seen': entry_price,
            'entry_indicators': entry_indicators
        }
        
        # Initialize active_trades structure for this symbol if needed
        if symbol not in self.active_trades:
            self.active_trades[symbol] = {}
        
        self.active_trades[symbol][row['contract_type']] = trade_info
        
        # Log trade opening to CSV
        if self.debug:
            print(f"📝 Logging trade open: {trade_info}")
        self.log_trade_open(trade_info)
        

        
        return trade_info
    
    def exit_trade(self, symbol: str, row: pd.Series, exit_details: Dict) -> Dict:
        """
        Exit an active trade for the symbol
        
        Args:
            symbol: Option symbol
            row: Current data row
            exit_details: Details about the exit reason
            
        Returns:
            Completed trade details
        """
        if self.debug:
            print(f"🔍 exit_trade called with symbol={symbol}, row keys={list(row.keys()) if hasattr(row, 'keys') else 'not a dict'}, exit_details={exit_details}")
        
        # Validate required fields
        if 'contract_type' not in row or 'mark_price' not in row:
            print(f"❌ Invalid row data for {symbol}: missing required fields")
            if self.debug:
                print(f"❌ Row data: {row}")
            return {}
            
        if symbol not in self.active_trades or row['contract_type'] not in self.active_trades[symbol]:
            if self.debug:
                print(f"❌ No active trade found for {symbol} {row.get('contract_type', 'NO_CONTRACT_TYPE')}")
                print(f"❌ Active trades: {self.active_trades}")
            return {}
        
        active_trade = self.active_trades[symbol][row['contract_type']] 
        exit_price = row['mark_price']
        # Calculate trade metrics
        profit = exit_price - active_trade['entry_price']
        profit_pct = (profit / active_trade['entry_price']) * 100
        max_unrealized_profit = active_trade['max_price_seen'] - active_trade['entry_price']
        max_drawdown = active_trade['min_price_seen'] - active_trade['entry_price']
        
        # Capture exit indicator values dynamically - only include indicators that exist
        exit_indicators = {}
        base_fields = ['symbol', 'contract_type', 'mark_price', 'strike_price', 'timestamp', 'tick_open', 'tick_high', 'tick_low', 'tick_close', 'tick_volume', 'total_volume', 'open_interest', 'bid_price', 'ask_price', 'last_price', 'high_price', 'low_price', 'close_price', 'volatility', 'money_intrinsic_value', 'expiration_year', 'multiplier', 'digits', 'open_price', 'bid_size', 'ask_size', 'last_size', 'net_change', 'underlying', 'expiration_month', 'deliverables', 'time_value', 'expiration_day', 'days_to_expiration', 'delta', 'gamma', 'theta', 'vega', 'rho', 'security_status', 'theoretical_option_value', 'underlying_price', 'uv_expiration_type', 'quote_time', 'trade_time', 'exchange', 'exchange_name', 'last_trading_day', 'settlement_type', 'net_percent_change', 'mark_price_net_change', 'mark_price_percent_change', 'implied_yield', 'is_penny_pilot', 'option_root', 'week_52_high', 'week_52_low', 'indicative_ask_price', 'indicative_bid_price', 'indicative_quote_time', 'exercise_type']
        
        # Find all indicator fields (anything not in base_fields)
        for key, value in row.items():
            if key not in base_fields and value != "" and value != 0 and not pd.isna(value):
                exit_indicators[f'exit_{key}'] = value
        
        if self.debug:
            print(f"🔍 DEBUG: Captured exit indicators: {list(exit_indicators.keys())}")
        
        completed_trade = {
            'symbol': symbol,
            'contract_type': row['contract_type'],
            'strike_price': row['strike_price'],
            'entry_price': active_trade['entry_price'],
            'exit_price': exit_price,
            'entry_timestamp': active_trade['entry_timestamp'],
            'exit_timestamp': row.get('timestamp', ''),
            'profit': profit,
            'profit_pct': profit_pct,
            'max_unrealized_profit': max_unrealized_profit,
            'max_drawdown': max_drawdown,
            'entry_indicators': active_trade['entry_indicators'],
            'exit_indicators': exit_indicators,
            'exit_reason': exit_details.get('exit_reason', 'unknown'),
            'exit_details': exit_details
        }
        
        # Store completed trade
        if symbol not in self.trades:
            self.trades[symbol] = []
        self.trades[symbol].append(completed_trade)
        
        # Log trade closing to CSV
        if self.debug:
            print(f"📝 Logging trade close: {completed_trade}")
        self.log_trade_close(completed_trade)
        
        # Remove from open trades CSV
        if self.debug:
            print(f"🗑️ Removing trade from open.csv: {symbol} {completed_trade['contract_type']} {completed_trade['strike_price']}")
            print(f"🗑️ Completed trade: {completed_trade}")
        self.remove_from_open_trades_csv(symbol, completed_trade['contract_type'], completed_trade['strike_price'])
        
        # Remove from active trades
        del self.active_trades[symbol][row['contract_type']]
        

        
        return completed_trade
    
    def update_trade_tracking(self, symbol: str, current_price: float, contract_type: str):
        """
        Update max/min price tracking for active trade
        
        Args:
            symbol: Option symbol
            current_price: Current mark_price
            contract_type: Contract type (C or P)
        """
        # Validate inputs
        if not contract_type or not symbol:
            print(f"❌ Invalid inputs for update_trade_tracking: symbol={symbol}, contract_type={contract_type}")
            return
            
        if symbol in self.active_trades and contract_type in self.active_trades[symbol]:
            trade = self.active_trades[symbol][contract_type]
            trade['max_price_seen'] = max(trade['max_price_seen'], current_price)
            trade['min_price_seen'] = min(trade['min_price_seen'], current_price)
    
    def process_streaming_row(self, symbol: str, row: pd.Series) -> Dict:
        """
        Process a single streaming row with calculated indicators
        
        Args:
            symbol: Option symbol
            row: Data row with calculated indicators
            
        Returns:
            Dictionary with processing results
        """
        print(f"🔍 DEBUG: process_streaming_row called for {symbol}")
        print(f"🔍 DEBUG: Available fields in row: {list(row.keys()) if hasattr(row, 'keys') else 'not a dict'}")
        
        # Validate required fields
        if 'contract_type' not in row or 'mark_price' not in row:
            print(f"❌ Invalid row data for {symbol}: missing required fields")
            return {'action': 'error', 'error': 'missing_required_fields'}
            
        current_price = row['mark_price']
        if current_price == 0 or current_price is None:
            print(f"🔍 DEBUG: No price data for {symbol}")
            return {'action': 'no_price_data'}
        
        print(f"🔍 DEBUG: Processing {symbol} with price {current_price}")
        
        # Update active trade tracking
        self.update_trade_tracking(symbol, current_price, row['contract_type'])
        
        result = {
            'symbol': symbol,
            'timestamp': row.get('timestamp', ''),
            'current_price': current_price,
            'action': 'none'
        }
        
        # Check if we should exit an active trade (all exit conditions)
        should_exit, exit_details = self.should_exit_trade(symbol, row)
        if should_exit:
            completed_trade = self.exit_trade(symbol, row, exit_details)
            result.update({
                'action': 'exit_trade',
                'trade_details': completed_trade
            })
            return result
        
        # Check if we should enter a new trade
        should_enter, entry_details = self.should_enter_trade(symbol, row)
        if should_enter:
            print(f"🔍 DEBUG: Entering trade for {symbol}")
            trade_info = self.enter_trade(symbol, row)
            result.update({
                'action': 'enter_trade',
                'trade_details': trade_info
            })
            return result
        
        # No action taken
        if symbol in self.active_trades and row['contract_type'] in self.active_trades[symbol]:
            result.update({
                'action': 'holding',
                'unrealized_pnl': current_price - self.active_trades[symbol][row['contract_type']]['entry_price']
            })
        else:
            result.update({
                'action': 'no_action',
                'reason': entry_details.get('reason', 'no_signal')
            })
        
        print(f"🔍 DEBUG: No action taken for {symbol}, result: {result['action']}")
        return result
    
    def get_trade_summary(self) -> Dict:
        """Get summary of all trades"""
        all_trades = []
        for symbol_trades in self.trades.values():
            all_trades.extend(symbol_trades)
        
        # Get open trades from CSV for comparison
        open_trades_csv = self.get_open_trades_from_csv()
        
        if not all_trades:
            return {
                'total_trades': 0,  # total trades
                'active_trades': len(self.active_trades),  # active trades
                'open_trades_csv': len(open_trades_csv),  # open trades in csv
                'csv_sync_status': 'synced' if len(self.active_trades) == len(open_trades_csv) else 'out_of_sync',  # sync status
                'total_profit': 0,  # total profit
                'win_rate': 0,  # win rate
                'avg_profit': 0  # average profit
            }
        
        total_profit = sum(trade['profit'] for trade in all_trades)
        winning_trades = len([trade for trade in all_trades if trade['profit'] > 0])
        
        return {
            'total_trades': len(all_trades),
            'active_trades': len(self.active_trades),
            'open_trades_csv': len(open_trades_csv),
            'csv_sync_status': 'synced' if len(self.active_trades) == len(open_trades_csv) else 'out_of_sync',
            'total_profit': total_profit,
            'win_rate': (winning_trades / len(all_trades)) * 100,
            'avg_profit': total_profit / len(all_trades),
            'winning_trades': winning_trades,
            'losing_trades': len(all_trades) - winning_trades
        }
    
    def save_trades_to_file(self, filename: str = 'trades_history.json'):
        """Save all completed trades to a JSON file"""
        try:
            with open(filename, 'w') as f:
                json.dump({
                    'trades': self.trades,
                    'active_trades': self.active_trades,  # active trades
                    'summary': self.get_trade_summary()
                }, f, indent=2, default=str)

        except Exception as e:
            print(f"❌ Error saving trades: {e}")


# Example usage and testing
if __name__ == "__main__":
    # Test the signal checker with sample data
    import numpy as np
    
    signal_checker = SignalChecker(debug=True)
    
    # Create sample data with indicators
    sample_data = pd.Series({
        'timestamp': '2025-01-29 10:30:00',
        'mark_price': 2.50,
        'ema': 2.48,
        'vwma': 2.46,
        'roc': 1.5,
        'roc_of_roc': 0.8,
        'macd_line': 0.05,
        'macd_signal': 0.03,
        'stoch_rsi_k': 65,
        'stoch_rsi_d': 60
    })
    
    symbol = "QQQ250731C00567000"
    
    print("\n🧪 Testing SignalChecker...")
    result = signal_checker.process_streaming_row(symbol, sample_data)
    print(f"Result: {result}")
    
    print(f"\n📊 Trade Summary: {signal_checker.get_trade_summary()}")


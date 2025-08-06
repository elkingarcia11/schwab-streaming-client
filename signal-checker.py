#!/usr/bin/env python3
"""
Signal Checker for Options Trading
Analyzes streaming data with calculated indicators to determine entry/exit signals
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Optional, List, Tuple
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
            'max_strike_distance': 0.05  # Maximum allowed distance from underlying (5% safety net)
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
                'trend_conditions_required': 10,      # Never sell on technical signals (max possible: 2)
                'momentum_conditions_required': 10    # Never sell on technical signals (max possible: 2)
            }
        }
        
        if self.debug:
            print("🎯 SignalChecker initialized")
            print(f"   Trailing stop: {(1-self.trailing_stop_pct)*100:.1f}%")
            print(f"   Stop loss: {(1-self.stop_loss_pct)*100:.1f}%")
            print(f"   Trading restrictions:")
            print(f"     - Target DTE: {self.trading_restrictions['target_dte']}")
            print(f"     - Max trades per symbol: {self.trading_restrictions['max_trades_per_symbol']}")
            print(f"     - Require closest strike: {self.trading_restrictions['require_closest_strike']}")
            print(f"     - Max strike distance: {self.trading_restrictions['max_strike_distance']:.1%}")
        
        # Ensure trades directory exists
        os.makedirs('data/trades', exist_ok=True)

    
    def log_trade_open(self, trade_info: Dict):
        """
        Log trade opening to data/trades/open.csv
        
        Args:
            trade_info: Trade information dictionary
        """
        try:
            csv_file = 'data/trades/open.csv'   
            # Define explicit column order for consistency (using symbol+contract_type+strike_price format)
            columns = [
                'timestamp', 'symbol', 'contract_type', 'strike_price', 'entry_price',
                'signal_type', 'trend_conditions', 'momentum_conditions',
                'trend_conditions_met', 'momentum_conditions_met'
            ]
            
            # Prepare trade data for CSV in exact column order (no full_symbol, clean format)
            trade_data = [
                trade_info.get('entry_timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                trade_info['symbol'],
                trade_info['contract_type'],
                trade_info['strike_price'],
                trade_info['entry_price'],
                trade_info.get('signal_type', 'buy'),
                trade_info.get('trend_conditions', []),
                trade_info.get('momentum_conditions', []),
                trade_info.get('trend_conditions_met', 0),
                trade_info.get('momentum_conditions_met', 0)
            ]
            
            # Convert to DataFrame with explicit column order
            df = pd.DataFrame([trade_data], columns=columns)
            
            # Write to CSV (append if exists, create with headers if not)
            if os.path.exists(csv_file):
                df.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                df.to_csv(csv_file, mode='w', header=True, index=False)
                
            if self.debug:
                print(f"📝 Logged trade open to {csv_file}")
                
        except Exception as e:
            if self.debug:
                print(f"❌ Error logging trade open: {e}")
    
    def log_trade_close(self, completed_trade: Dict):
        """
        Log trade closing to data/trades/close.csv
        
        Args:
            completed_trade: Completed trade information dictionary
        """
        try:
            csv_file = 'data/trades/close.csv'
            
            # Calculate trade duration in minutes
            try:
                entry_time = datetime.strptime(completed_trade['entry_timestamp'], "%Y-%m-%d %H:%M:%S")
                exit_time = datetime.strptime(completed_trade['exit_timestamp'], "%Y-%m-%d %H:%M:%S")
                duration_minutes = (exit_time - entry_time).total_seconds() / 60
            except:
                duration_minutes = 0
            
            # Define explicit column order for consistency (using symbol+contract_type+strike_price format)
            columns = [
                'symbol', 'contract_type', 'strike_price', 'entry_price', 'exit_price',
                'profit', 'profit_pct', 'duration_minutes', 'exit_reason', 'max_unrealized_profit',
                'max_drawdown', 'entry_timestamp', 'exit_timestamp',
                'entry_ema', 'entry_vwma', 'entry_macd_line', 'entry_macd_signal', 'entry_stoch_k', 'entry_stoch_d','entry_roc','entry_roc_of_roc',
                'exit_ema', 'exit_vwma', 'exit_macd_line', 'exit_macd_signal', 'exit_stoch_k', 'exit_stoch_d','exit_roc','exit_roc_of_roc'
            ]
            
            # Extract indicator values from entry and exit data
            entry_indicators = completed_trade.get('entry_indicators', {})
            exit_indicators = completed_trade.get('exit_indicators', {})
            
            # Prepare trade data for CSV in exact column order (no full_symbol, clean format)
            trade_data = [
                completed_trade['symbol'],
                completed_trade['contract_type'],
                completed_trade['strike_price'],
                completed_trade['entry_price'],
                completed_trade['exit_price'],
                completed_trade['profit'],
                completed_trade['profit_pct'],
                duration_minutes,
                completed_trade['exit_reason'],
                completed_trade['max_unrealized_profit'],
                completed_trade['max_drawdown'],
                completed_trade.get('entry_timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                completed_trade.get('exit_timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                entry_indicators.get('entry_ema', 0),
                entry_indicators.get('entry_vwma', 0),
                entry_indicators.get('entry_macd_line', 0),
                entry_indicators.get('entry_macd_signal', 0),
                entry_indicators.get('entry_stoch_k', 0),
                entry_indicators.get('entry_stoch_d', 0),
                entry_indicators.get('entry_roc', 0),
                entry_indicators.get('entry_roc_of_roc', 0),
                exit_indicators.get('exit_ema', 0),
                exit_indicators.get('exit_vwma', 0),
                exit_indicators.get('exit_macd_line', 0),
                exit_indicators.get('exit_macd_signal', 0),
                exit_indicators.get('exit_stoch_k', 0),
                exit_indicators.get('exit_stoch_d', 0),
                exit_indicators.get('exit_roc', 0),
                exit_indicators.get('exit_roc_of_roc', 0)
            ]
            
            # Convert to DataFrame with explicit column order
            df = pd.DataFrame([trade_data], columns=columns)
            
            # Write to CSV (append if exists, create with headers if not)
            if os.path.exists(csv_file):
                df.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                df.to_csv(csv_file, mode='w', header=True, index=False)
                
            if self.debug:
                print(f"📝 Logged trade close to {csv_file}")
                
        except Exception as e:
            if self.debug:
                print(f"❌ Error logging trade close: {e}")
    
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
                if self.debug:
                    print(f"📝 Open trades CSV not found: {csv_file}")
                return
        
            # Read existing open trades with error handling for malformed CSV
            try:
                df = pd.read_csv(csv_file)
            except pd.errors.ParserError:
                if self.debug:
                    print(f"⚠️ Malformed CSV detected, attempting manual fix...")
                self._fix_malformed_open_csv(csv_file)
                df = pd.read_csv(csv_file)
            
            
            # Use symbol + contract_type + strike_price for exact matching
            mask = (df['symbol'] == symbol) & \
                   (df['contract_type'] == contract_type) & \
                   (df['strike_price'] == strike_price)
            df = df[~mask]
            
            
            # Write back to CSV
            if len(df) > 0:
                df.to_csv(csv_file, index=False)        
            if self.debug:
                print(f"🗑️ Removed {symbol} {contract_type} {strike_price} from open trades CSV")
                
        except Exception as e:
            if self.debug:
                print(f"❌ Error removing from open trades CSV: {e}")
    
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
                # Return empty DataFrame with correct columns
                columns = [
                    'timestamp', 'symbol', 'contract_type', 'full_symbol', 'entry_price',
                    'signal_type', 'trend_conditions', 'momentum_conditions',
                    'trend_conditions_met', 'momentum_conditions_met'
                ]
                return pd.DataFrame(columns=columns)
        except Exception as e:
            if self.debug:
                print(f"❌ Error reading open trades CSV: {e}")
            return pd.DataFrame()
    
    def check_indicators_available(self, row: pd.Series, required_indicators: List[str]) -> bool:
        """Check if all required indicators are available and not empty"""
        for indicator in required_indicators:
            if indicator not in row or row[indicator] == "" or pd.isna(row[indicator]):
                return False
        return True
    
    def check_signal_combos(self, row: pd.Series, signal_type: str) -> Tuple[bool, Dict]:
        """
        Check if signal conditions are met for entry/exit
        
        Args:
            row: Data row with calculated indicators
            signal_type: 'buy' or 'sell'
            
        Returns:
            Tuple of (signal_triggered)
        """
        
        # Required indicators for signal analysis
        required_indicators = ['ema', 'vwma', 'macd_line', 'macd_signal', 
                             'stoch_rsi_k', 'stoch_rsi_d', 'roc', 'roc_of_roc']
        
        # Check if we have sufficient indicators
        if not self.check_indicators_available(row, required_indicators):
            return False, {'reason': 'insufficient_indicators', 'missing': [
                ind for ind in required_indicators 
                if ind not in row or row[ind] == "" or pd.isna(row[ind])
            ]}
        
        trend_conditions_met = 0
        momentum_conditions_met = 0
        
        if signal_type == 'buy':
            if row['roc'] > 0 and row['roc_of_roc'] > 0:
                momentum_conditions_met += 1

            if row['stoch_rsi_k'] > row['stoch_rsi_d']:
                momentum_conditions_met += 1

            if row['macd_line'] > row['macd_signal']:
                trend_conditions_met += 1
                
            if row['ema'] > row['vwma']:
                trend_conditions_met += 1
                
            
            config = self.signal_config['buy']
        elif signal_type == 'sell':
            # SELL signals use opposite conditions of BUY signals
            if row['roc'] < 0 and row['roc_of_roc'] < 0:
                momentum_conditions_met += 1

            if row['stoch_rsi_k'] < row['stoch_rsi_d']:
                momentum_conditions_met += 1

            if row['macd_line'] < row['macd_signal']:
                trend_conditions_met += 1
                
            if row['ema'] < row['vwma']:
                trend_conditions_met += 1
            
            config = self.signal_config['sell']
        
        return (trend_conditions_met >= config['trend_conditions_required'] and 
                    momentum_conditions_met >= config['momentum_conditions_required'])

    def should_enter_trade(self, symbol: str, row: pd.Series) -> Tuple[bool, Dict]:
        """
        Determine if we should enter a trade for this symbol
        
        Args:
            symbol: Option symbol
            row: Current data row with indicators
            
        Returns:
            Tuple of (should_enter)
        """
        # Don't enter if trade already open
        if symbol in self.active_trades and row['contract_type'] in self.active_trades[symbol]:
            return False, {'reason': 'trade_already_open'}
        
        # Check for buy signal
        signal_triggered = self.check_signal_combos(row, 'buy')
        
        if signal_triggered and self.debug:
            print(f"🟢 BUY signal for {symbol} {row['contract_type']} {row['strike_price']}")
        
        return signal_triggered
    
    def should_exit_trade(self, symbol: str, row: pd.Series) -> Tuple[bool, Dict]:
        """
        Determine if we should exit an active trade for this symbol
        
        Args:
            symbol: Option symbol
            row: Current data row with indicators
            
        Returns:
            Tuple of (should_exit, exit_details)
        """
        # No active trade to exit
        if symbol not in self.active_trades or row['contract_type'] not in self.active_trades[symbol]:
            return False, {'reason': 'no_active_trade'}
        
        active_trade = self.active_trades[symbol][row['contract_type']]
        current_price = row['mark_price']
        entry_price = active_trade['entry_price']
        max_price_seen = active_trade['max_price_seen']
        
        exit_details = {
            'current_price': current_price,
            'entry_price': entry_price,
            'max_price_seen': max_price_seen,
            'unrealized_pnl': current_price - entry_price,
            'max_unrealized_pnl': max_price_seen - entry_price
        }
        
        # Check sell signal
        signal_triggered = self.check_signal_combos(row, 'sell')
        if signal_triggered:
            exit_details.update({
                'exit_reason': 'sell_signal',
            })
            if self.debug:
                print(f"🔴 SELL signal for {symbol} {row['contract_type']} {row['strike_price']}")
            return True, exit_details
        
        # Check stop loss
        if current_price <= entry_price * self.stop_loss_pct:
            exit_details.update({
                'exit_reason': 'stop_loss',
                'stop_loss_level': entry_price * self.stop_loss_pct
            })
            if self.debug:
                print(f"🛑 STOP LOSS triggered for {symbol}: {current_price:.4f} <= {entry_price * self.stop_loss_pct:.4f}")
            return True, exit_details
        
        # Check trailing stop
        if current_price <= max_price_seen * self.trailing_stop_pct:
            exit_details.update({
                'exit_reason': 'trailing_stop',
                'trailing_stop_level': max_price_seen * self.trailing_stop_pct
            })
            if self.debug:
                print(f"📉 TRAILING STOP triggered for {symbol}: {current_price:.4f} <= {max_price_seen * self.trailing_stop_pct:.4f}")
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
        # No active trade to exit
        if symbol not in self.active_trades or row['contract_type'] not in self.active_trades[symbol]:
            return False, {'reason': 'no_active_trade'}
        
        active_trade = self.active_trades[symbol][row['contract_type']]
        current_price = row['mark_price']
        entry_price = active_trade['entry_price']
        max_price_seen = active_trade['max_price_seen']
        
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
            if self.debug:
                print(f"🛑 STOP LOSS triggered (mark_price) for {symbol}: {current_price:.4f} <= {entry_price * self.stop_loss_pct:.4f}")
            return True, exit_details
        
        # Check trailing stop (using mark_price)
        if current_price <= max_price_seen * self.trailing_stop_pct:
            exit_details.update({
                'exit_reason': 'trailing_stop',
                'trailing_stop_level': max_price_seen * self.trailing_stop_pct
            })
            if self.debug:
                print(f"📉 TRAILING STOP triggered (mark_price) for {symbol}: {current_price:.4f} <= {max_price_seen * self.trailing_stop_pct:.4f}")
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
        entry_price = row['mark_price']
        timestamp = row.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Capture entry indicator values
        entry_indicators = {
            'entry_ema': row.get('ema', 0),
            'entry_vwma': row.get('vwma', 0),
            'entry_macd_line': row.get('macd_line', 0),
            'entry_macd_signal': row.get('macd_signal', 0),
            'entry_stoch_k': row.get('stoch_rsi_k', 0),
            'entry_stoch_d': row.get('stoch_rsi_d', 0),
            'entry_roc': row.get('roc', 0),
            'entry_roc_of_roc': row.get('roc_of_roc', 0)
        }
        
        trade_info = {
            'symbol': symbol,
            'entry_price': entry_price,
            'entry_timestamp': timestamp,
            'max_price_seen': entry_price,
            'min_price_seen': entry_price,
            'entry_indicators': entry_indicators
        }
        
        # Initialize active_trades structure for this symbol if needed
        if symbol not in self.active_trades:
            self.active_trades[symbol] = {}
        
        self.active_trades[symbol][row['contract_type']] = trade_info
        
        # Log trade opening to CSV
        self.log_trade_open(trade_info)
        
        if self.debug:
            print(f"💰 ENTERED trade: {symbol} at ${entry_price:.4f}")
        
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
        if symbol not in self.active_trades or row['contract_type'] not in self.active_trades[symbol]:
            return {}
        
        active_trade = self.active_trades[symbol][row['contract_type']] 
        exit_price = row['mark_price']
        exit_timestamp = row.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        # Calculate trade metrics
        profit = exit_price - active_trade['entry_price']
        profit_pct = (profit / active_trade['entry_price']) * 100
        max_unrealized_profit = active_trade['max_price_seen'] - active_trade['entry_price']
        max_drawdown = active_trade['min_price_seen'] - active_trade['entry_price']
        
        # Capture exit indicator values
        exit_indicators = {
            'exit_ema': row.get('ema', 0),
            'exit_vwma': row.get('vwma', 0),
            'exit_macd_line': row.get('macd_line', 0),
            'exit_macd_signal': row.get('macd_signal', 0),
            'exit_stoch_k': row.get('stoch_rsi_k', 0),
            'exit_stoch_d': row.get('stoch_rsi_d', 0),
            'exit_roc': row.get('roc', 0),
            'exit_roc_of_roc': row.get('roc_of_roc', 0)
        }
        
        completed_trade = {
            'symbol': symbol,
            'entry_price': active_trade['entry_price'],
            'exit_price': exit_price,
            'entry_timestamp': active_trade['entry_timestamp'],
            'exit_timestamp': exit_timestamp,
            'profit': profit,
            'profit_pct': profit_pct,
            'max_unrealized_profit': max_unrealized_profit,
            'max_drawdown': max_drawdown,
            'entry_indicators': active_trade.get('entry_indicators', {}),
            'exit_indicators': exit_indicators,
            'exit_reason': exit_details.get('exit_reason', 'unknown'),
            'exit_details': exit_details
        }
        
        # Store completed trade
        if symbol not in self.trades:
            self.trades[symbol] = []
        self.trades[symbol].append(completed_trade)
        
        # Log trade closing to CSV
        self.log_trade_close(completed_trade)
        
        # Remove from open trades CSV
        self.remove_from_open_trades_csv(symbol, completed_trade['contract_type'], completed_trade['strike_price'])
        
        # Remove from active trades
        del self.active_trades[symbol][row['contract_type']]
        
        if self.debug:
            print(f"📤 EXITED trade: {symbol} strike {completed_trade['strike_price']} contract type {completed_trade['contract_type']} at ${exit_price:.4f}")
            print(f"   Profit: ${profit:.4f} ({profit_pct:.2f}%)")
            print(f"   Reason: {exit_details.get('exit_reason', 'unknown')}")
        
        return completed_trade
    
    def update_trade_tracking(self, symbol: str, current_price: float, contract_type: str):
        """
        Update max/min price tracking for active trade
        
        Args:
            symbol: Option symbol
            current_price: Current mark_price
        """
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
        current_price = row.get('mark_price', 0)
        if current_price == 0:
            return {'action': 'no_price_data'}
        
        # Update active trade tracking
        self.update_trade_tracking(symbol, current_price, row['contract_type'])
        
        result = {
            'symbol': symbol,
            'timestamp': row.get('timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
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
        should_enter = self.should_enter_trade(symbol, row)
        if should_enter:
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
            if self.debug:
                print(f"💾 Saved trades to {filename}")
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


#!/usr/bin/env python3
"""
Email Manager for Trading Notifications
Integrates with the email client to send trade notifications and daily summaries
"""

import os
import sys
import zipfile
from datetime import datetime, time
from typing import Dict, List, Optional
import pandas as pd
from pathlib import Path

# Add email-client to path
sys.path.append('email-client')
from email_client import EmailClient


class TradingEmailManager:
    """
    Email manager for sending trading notifications and daily summaries
    """
    
    def __init__(self, debug: bool = False):
        """
        Initialize the email manager
        
        Args:
            debug: Enable debug logging
        """
        self.debug = debug
        self.email_client = None
        self.recipient_emails = []
        self.daily_summary_sent = False
        self.market_close_time = time(16, 0)  # 4:00 PM ET
        
        # Try to initialize email client
        try:
            self.email_client = EmailClient()
            if self.debug:
                print("📧 Email manager initialized successfully")
        except Exception as e:
            if self.debug:
                print(f"⚠️ Email manager initialization failed: {e}")
            self.email_client = None
    
    def set_recipients(self, emails: List[str]):
        """
        Set the list of recipient email addresses
        
        Args:
            emails: List of email addresses to send notifications to
        """
        self.recipient_emails = emails
        if self.debug:
            print(f"📧 Set recipients: {emails}")
    
    def extract_strike_and_expiry(self, full_symbol: str) -> tuple[str, str]:
        """
        Extract strike price and expiry from full option symbol
        
        Args:
            full_symbol: Full option symbol like "QQQ250731C00567000"
            
        Returns:
            Tuple of (strike_price, expiry_date)
        """
        try:
            if not full_symbol or len(full_symbol) <= 10:
                return "N/A", "N/A"
                
            # Extract expiry (YYMMDD format)
            expiry_part = full_symbol[3:9]  # e.g., "250731"
            year = "20" + expiry_part[:2]
            month = expiry_part[2:4]
            day = expiry_part[4:6]
            expiry = f"{year}-{month}-{day}"
            
            # Extract strike price (after the C/P)
            for i, char in enumerate(full_symbol[9:], 9):
                if char in ['C', 'P']:
                    strike_part = full_symbol[i+1:]
                    # Convert to decimal (strike is in cents)
                    strike_price = float(strike_part) / 1000
                    return f"${strike_price:.2f}", expiry
            
            return "N/A", expiry
        except Exception as e:
            print(f"❌ Error extracting strike and expiry from '{full_symbol}': {e}")
            return "N/A", "N/A"
    
    def send_trade_notification(self, action: str, symbol: str, contract_type: str, 
                              price: float, additional_info: Optional[Dict] = None) -> bool:
        """
        Send trade notification email
        
        Args:
            action: 'BUY' or 'SELL'
            symbol: Base symbol (e.g., 'QQQ')
            contract_type: 'C' for Call, 'P' for Put
            price: Trade price
            additional_info: Additional trade information
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        if not self.email_client or not self.recipient_emails:
            return False
        
        try:
            # Format contract type for display (do this first to avoid reference error)
            contract_display = "Call" if contract_type == "C" else "Put" if contract_type == "P" else contract_type
            
            # Extract strike and expiry from full symbol
            full_symbol = additional_info.get('full_symbol', '') if additional_info else ''
            strike_price, expiry = self.extract_strike_and_expiry(full_symbol)
            
            # Create timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Create subject line
            subject = f"{action} {symbol} {contract_display} {strike_price} - {timestamp}"
            
            if action == "BUY":
                body = f"""
🚀 TRADE NOTIFICATION - BUY

📊 Trade Details:
- Action: {action}
- Symbol: {symbol}
- Contract Type: {contract_display} ({contract_type})
- Strike Price: {strike_price}
- Expiry: {expiry}
- Entry Price: ${price:.4f}
- Time: {timestamp}
"""
            else:  # SELL
                body = f"""
🔴 TRADE NOTIFICATION - SELL

📊 Trade Details:
- Action: {action}
- Symbol: {symbol}
- Contract Type: {contract_display} ({contract_type})
- Strike Price: {strike_price}
- Expiry: {expiry}
- Exit Price: ${price:.4f}
- Time: {timestamp}
"""
            
            # Add exit details for SELL actions
            if action == "SELL" and additional_info:
                body += "\n💰 Exit Details:\n"
                exit_reason = additional_info.get('exit_reason', 'N/A')
                # Format exit reason with emoji
                exit_emoji = {
                    'stop_loss': '🛑',
                    'trailing_stop': '📉',
                    'sell_signal': '🔴',
                    'Stop Loss or Trailing Stop': '🛑'
                }.get(exit_reason.lower(), '📊')
                body += f"- Exit Reason: {exit_emoji} {exit_reason}\n"
                
                # Add profit information with formatting
                profit = additional_info.get('profit', 'N/A')
                profit_pct = additional_info.get('profit_pct', 'N/A')
                
                # Format profit with emoji based on positive/negative
                if profit != 'N/A' and profit != 0:
                    try:
                        profit_val = float(profit.replace('$', '').replace(',', ''))
                        profit_emoji = "✅" if profit_val > 0 else "❌"
                        body += f"- Profit: {profit_emoji} {profit}\n"
                        body += f"- Profit %: {profit_emoji} {profit_pct}\n"
                    except:
                        body += f"- Profit: {profit}\n"
                        body += f"- Profit %: {profit_pct}\n"
                else:
                    body += f"- Profit: {profit}\n"
                    body += f"- Profit %: {profit_pct}\n"
                
                # Add entry price for reference
                entry_price = additional_info.get('entry_price', 'N/A')
                if entry_price != 'N/A':
                    body += f"- Entry Price: ${entry_price:.4f}\n"
                
                # Add duration if available
                duration = additional_info.get('duration', 'N/A')
                if duration != 'N/A':
                    body += f"- Duration: {duration} minutes\n"
                
                # Add exit summary
                body += f"""
🎯 Exit Summary:
- Exit triggered by: {exit_reason}
- Risk management: Stop loss and trailing stop active
"""
                
                # Add technical indicators at exit
                # Format indicator values
                ema = additional_info.get('ema', 'N/A')
                vwma = additional_info.get('vwma', 'N/A')
                roc = additional_info.get('roc', 'N/A')
                roc_of_roc = additional_info.get('roc_of_roc', 'N/A')
                macd_line = additional_info.get('macd_line', 'N/A')
                macd_signal = additional_info.get('macd_signal', 'N/A')
                stoch_k = additional_info.get('stoch_rsi_k', 'N/A')
                stoch_d = additional_info.get('stoch_rsi_d', 'N/A')
                
                body += f"""
📈 Technical Indicators at Exit:
- EMA: {ema}
- VWMA: {vwma}
- ROC: {roc}
- ROC of ROC: {roc_of_roc}
- MACD Line: {macd_line}
- MACD Signal: {macd_signal}
- Stochastic RSI K: {stoch_k}
- Stochastic RSI D: {stoch_d}

📋 Full Symbol: {full_symbol}
"""
            
            # Add signal conditions and indicators for BUY actions
            if action == "BUY" and additional_info:
                # Add signal summary
                body += f"""
🎯 Signal Summary:
- Entry triggered by technical indicators
- All required trend and momentum conditions met
"""
                # Format indicator values
                ema = additional_info.get('ema', 'N/A')
                vwma = additional_info.get('vwma', 'N/A')
                roc = additional_info.get('roc', 'N/A')
                roc_of_roc = additional_info.get('roc_of_roc', 'N/A')
                macd_line = additional_info.get('macd_line', 'N/A')
                macd_signal = additional_info.get('macd_signal', 'N/A')
                stoch_k = additional_info.get('stoch_rsi_k', 'N/A')
                stoch_d = additional_info.get('stoch_rsi_d', 'N/A')
                
                body += f"""
📈 Technical Indicators at Entry:
- Entry Time: {additional_info.get('entry_time', 'N/A')}
- EMA: {ema}
- VWMA: {vwma}
- ROC: {roc}
- ROC of ROC: {roc_of_roc}
- MACD Line: {macd_line}
- MACD Signal: {macd_signal}
- Stochastic RSI K: {stoch_k}
- Stochastic RSI D: {stoch_d}

📋 Full Symbol: {full_symbol}
"""
            

            
            # Add footer
            body += f"""

---
🤖 Automated Trading System
📧 Generated at {timestamp}
"""
            
            # Send email
            success = self.email_client.send_email(
                to_emails=self.recipient_emails,
                subject=subject,
                body=body.strip()
            )
            
            if success and self.debug:
                print(f"📧 Sent {action} notification for {symbol}{contract_type}")
            elif not success and self.debug:
                print(f"❌ Failed to send {action} notification for {symbol}{contract_type}")
            
            return success
                
        except Exception as e:
            if self.debug:
                print(f"❌ Error sending trade notification: {e}")
            return False
    
    def send_daily_summary(self, trades_file: str = "data/trades/close.csv"):
        """
        Send daily trading summary email with attached trades CSV
        
        Args:
            trades_file: Path to the trades CSV file
        """
        if not self.email_client or not self.recipient_emails:
            return
        
        # Check if we already sent today's summary
        today = datetime.now().date()
        if hasattr(self, '_last_summary_date') and self._last_summary_date == today:
            return
        
        try:
            # Check if trades file exists
            if not os.path.exists(trades_file):
                if self.debug:
                    print(f"📧 No trades file found: {trades_file}")
                return
            
            # Read trades data
            df = pd.read_csv(trades_file)
            
            # Check if DataFrame is empty or missing required columns
            if df.empty:
                if self.debug:
                    print(f"📧 Trades file is empty: {trades_file}")
                return
            
            required_columns = ['exit_timestamp', 'profit', 'profit_pct', 'symbol']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                if self.debug:
                    print(f"📧 Trades file missing required columns: {missing_columns}")
                return
            
            # Filter for today's trades using exit_timestamp
            today_str = today.strftime("%Y-%m-%d")
            today_trades = df[df['exit_timestamp'].str.startswith(today_str)]
            
            if len(today_trades) == 0:
                if self.debug:
                    print("📧 No trades found for today")
                return
            
            # Calculate summary statistics
            total_trades = len(today_trades)
            winning_trades = len(today_trades[today_trades['profit'] > 0])
            losing_trades = len(today_trades[today_trades['profit'] < 0])
            win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
            
            total_profit = today_trades['profit'].sum()
            total_profit_pct = today_trades['profit_pct'].sum()
            
            avg_profit = today_trades['profit'].mean()
            avg_profit_pct = today_trades['profit_pct'].mean()
            
            # Create email content
            subject = f"Daily Trading Summary - {today_str}"
            
            body = f"""
Daily Trading Summary - {today_str}

Trading Statistics:
- Total Trades: {total_trades}
- Winning Trades: {winning_trades}
- Losing Trades: {losing_trades}
- Win Rate: {win_rate:.1f}%

Profit/Loss:
- Total P&L: ${total_profit:.2f} ({total_profit_pct:.2f}%)
- Average P&L per Trade: ${avg_profit:.2f} ({avg_profit_pct:.2f}%)

Trades by Symbol:
"""
            
            # Add breakdown by symbol
            symbol_summary = today_trades.groupby('symbol').agg({
                'profit': ['count', 'sum', 'mean'],
                'profit_pct': 'mean'
            }).round(2)
            
            for symbol in symbol_summary.index:
                count = symbol_summary.loc[symbol, ('profit', 'count')]
                total = symbol_summary.loc[symbol, ('profit', 'sum')]
                avg = symbol_summary.loc[symbol, ('profit', 'mean')]
                avg_pct = symbol_summary.loc[symbol, ('profit_pct', 'mean')]
                
                body += f"- {symbol}: {count} trades, ${total:.2f} total, ${avg:.2f} avg ({avg_pct:.2f}%)\n"
            
            body += f"\nComplete trading data (trades, options data, etc.) is attached as a zip file.\n"
            
            # Create zip file of entire data folder
            data_zip_file = self._create_data_folder_zip(today_str)
            
            # Send email with data folder zip attachment
            attachments = [data_zip_file] if data_zip_file else [trades_file]  # Fallback to trades file if zip fails
            success = self.email_client.send_email(
                to_emails=self.recipient_emails,
                subject=subject,
                body=body.strip(),
                attachments=attachments
            )
            
            # Clean up the temporary zip file
            if data_zip_file and os.path.exists(data_zip_file):
                try:
                    os.remove(data_zip_file)
                    if self.debug:
                        print(f"🗑️ Cleaned up temporary zip file: {data_zip_file}")
                except Exception as e:
                    if self.debug:
                        print(f"⚠️ Could not clean up zip file {data_zip_file}: {e}")
            
            if success:
                self._last_summary_date = today
                if self.debug:
                    print(f"📧 Sent daily summary for {today_str} with {total_trades} trades")
            elif self.debug:
                print(f"❌ Failed to send daily summary for {today_str}")
                
        except Exception as e:
            if self.debug:
                print(f"❌ Error sending daily summary: {e}")
    
    def _create_data_folder_zip(self, date_str: str) -> str:
        """
        Create a zip file of the entire data folder for email attachment
        
        Args:
            date_str: Date string for filename (e.g., "2025-08-01")
            
        Returns:
            str: Path to created zip file, or None if failed
        """
        try:
            data_folder = "data"
            
            # Check if data folder exists
            if not os.path.exists(data_folder):
                if self.debug:
                    print(f"⚠️ Data folder '{data_folder}' not found")
                return None
            
            # Create zip filename with date
            zip_filename = f"trading_data_{date_str}.zip"
            
            if self.debug:
                print(f"📦 Creating data folder zip: {zip_filename}")
            
            # Create zip file
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Walk through data directory and add all files
                for root, dirs, files in os.walk(data_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        # Add file to zip with relative path
                        arcname = os.path.relpath(file_path, os.path.dirname(data_folder))
                        zipf.write(file_path, arcname)
                        
                        if self.debug:
                            file_size = os.path.getsize(file_path)
                            print(f"   📄 Added: {arcname} ({file_size:,} bytes)")
            
            # Check if zip was created successfully
            if os.path.exists(zip_filename):
                zip_size = os.path.getsize(zip_filename)
                if self.debug:
                    print(f"✅ Created zip file: {zip_filename} ({zip_size:,} bytes)")
                return zip_filename
            else:
                if self.debug:
                    print(f"❌ Failed to create zip file: {zip_filename}")
                return None
                
        except Exception as e:
            if self.debug:
                print(f"❌ Error creating data folder zip: {e}")
            return None
    
    def check_and_send_daily_summary(self):
        """
        Check if market is closed and send daily summary if needed
        """
        now = datetime.now()
        current_time = now.time()
        
        # Check if it's after market close and we haven't sent today's summary
        if (current_time >= self.market_close_time and 
            not self.daily_summary_sent and 
            hasattr(self, '_last_summary_date') and 
            self._last_summary_date != now.date()):
            
            self.send_daily_summary()
            self.daily_summary_sent = True
    
    def reset_daily_summary_flag(self):
        """
        Reset the daily summary flag (call this at market open)
        """
        self.daily_summary_sent = False
        if self.debug:
            print("📧 Reset daily summary flag")
    
    def test_email_connection(self) -> bool:
        """
        Test email connection
        
        Returns:
            True if connection successful, False otherwise
        """
        if not self.email_client:
            return False
        
        try:
            return self.email_client.test_connection()
        except Exception as e:
            if self.debug:
                print(f"❌ Email connection test failed: {e}")
            return False


# Example usage
if __name__ == "__main__":
    # Test the email manager
    email_manager = TradingEmailManager(debug=True)
    
    # Set recipients (replace with actual email addresses)
    email_manager.set_recipients(['your-email@example.com'])
    
    # Test connection
    if email_manager.test_email_connection():
        print("✅ Email connection successful")
        
        # Test trade notification
        email_manager.send_trade_notification(
            action="BUY",
            symbol="QQQ",
            contract_type="C",
            price=1.25,
            additional_info={
                'strike': 567,
                'expiry': '2025-07-31',
                'signal_strength': 'Strong'
            }
        )
        
        # Test daily summary
        email_manager.send_daily_summary()
    else:
        print("❌ Email connection failed") 
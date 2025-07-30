#!/usr/bin/env python3
"""
Test script for email recipient functionality
"""

import os
from dotenv import load_dotenv
from email_manager import TradingEmailManager

def test_recipient_functionality():
    """Test the email recipient functionality"""
    
    # Load environment variables
    load_dotenv('email.env.example')  # Load from example file for testing
    
    print("📧 Email Recipient Test")
    print("=" * 50)
    
    # Show current configuration
    print("Current Configuration:")
    print(f"  SMTP Email: {os.getenv('EMAIL_ADDRESS', 'Not set')}")
    print(f"  Sender Email: {os.getenv('SENDER_EMAIL', 'Not set (will use SMTP email)')}")
    print(f"  Sender Name: {os.getenv('SENDER_NAME', 'Not set (will use sender email)')}")
    print(f"  Recipients: {os.getenv('EMAIL_RECIPIENTS', 'Not set')}")
    print()
    
    # Initialize email manager
    email_manager = TradingEmailManager(debug=True)
    
    if not email_manager.email_client:
        print("❌ Email client not initialized")
        return False
    
    # Test connection
    print("1. Testing Email Connection...")
    if not email_manager.test_email_connection():
        print("❌ Email connection failed")
        return False
    print("✅ Email connection successful")
    
    # Get recipients from environment
    recipients_str = os.getenv('EMAIL_RECIPIENTS', '')
    if not recipients_str:
        print("❌ No EMAIL_RECIPIENTS configured")
        print("   Please set EMAIL_RECIPIENTS in your .env file")
        return False
    
    recipients = [email.strip() for email in recipients_str.split(',')]
    print(f"📧 Recipients: {recipients}")
    
    # Set recipients
    email_manager.set_recipients(recipients)
    
    # Test trade notification
    print("\n2. Testing Trade Notification...")
    success = email_manager.send_trade_notification(
        action="BUY",
        symbol="QQQ",
        contract_type="C",
        price=1.25,
        additional_info={
            'full_symbol': 'QQQ250731C00567000',
            'trend_conditions_met': 3,
            'momentum_conditions_met': 4
        }
    )
    
    if success:
        print("✅ Trade notification sent successfully")
    else:
        print("❌ Failed to send trade notification")
    
    # Test trade exit notification
    print("\n3. Testing Trade Exit Notification...")
    success = email_manager.send_trade_notification(
        action="SELL",
        symbol="QQQ",
        contract_type="C",
        price=1.45,
        additional_info={
            'full_symbol': 'QQQ250731C00567000',
            'entry_price': 1.25,
            'profit': '$0.20',
            'profit_pct': '16.00%',
            'exit_reason': 'sell_signal',
            'duration': 45,
            'exit_trend_conditions_met': 2,
            'exit_momentum_conditions_met': 3,
            'exit_trend_conditions': ['ema_cross', 'macd_signal'],
            'exit_momentum_conditions': ['rsi_overbought', 'stoch_overbought', 'roc_decline']
        }
    )
    
    if success:
        print("✅ Trade exit notification sent successfully")
    else:
        print("❌ Failed to send trade exit notification")
    
    print("\n🎉 Email recipient test completed!")
    print("📧 Check your email inbox for the test messages")
    return True

if __name__ == "__main__":
    test_recipient_functionality() 
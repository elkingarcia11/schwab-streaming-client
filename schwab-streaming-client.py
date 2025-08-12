"""
Schwab Streaming Client - Real-time Data Collection
    - Connects to WebSocket API
    - Subscribes to option symbol data
    - Subscribes to chart option data for equity symbols
    - Parses and saves data to CSV in real-time
    - Calculates technical indicators on streaming data
    - No batch saving needed - all data saved as it arrives
"""


import json
import time
import threading
import httpx
import websocket
import os
import traceback
import pandas as pd
from datetime import datetime
import pytz
from typing import List, Optional, Dict
import sys
import importlib.util
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'options-symbol-finder', 'charles-schwab-authentication-module'))
from schwab_auth import SchwabAuth
sys.path.append(os.path.join(os.path.dirname(__file__), 'options-symbol-finder'))

# Import OptionsSymbolFinder from the hyphenated filename
spec = importlib.util.spec_from_file_location(
    "options_symbol_finder", 
    os.path.join(os.path.dirname(__file__), 'options-symbol-finder', 'options-symbol-finder.py')
)
options_symbol_finder_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(options_symbol_finder_module)
OptionsSymbolFinder = options_symbol_finder_module.OptionsSymbolFinder

# Import market-data-aggregator functions
sys.path.append(os.path.join(os.path.dirname(__file__), 'market-data-aggregator'))
from main import process_streaming_tick

# Import indicator-calculator module
sys.path.append(os.path.join(os.path.dirname(__file__), 'indicator-calculator'))
from indicator_calculator import IndicatorCalculator

# Import signal-checker module
spec = importlib.util.spec_from_file_location(
    "signal_checker", 
    os.path.join(os.path.dirname(__file__), 'signal-checker.py')
)
signal_checker_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(signal_checker_module)
SignalChecker = signal_checker_module.SignalChecker

# Import email manager
from email_manager import TradingEmailManager

class SchwabStreamingClient:
    """Streaming client for Schwab API - handles both option data and chart data"""
    
    def __init__(self, debug: bool = False, equity_symbols: List[str] = None, option_symbols: List[str] = None, 
                 gcs_bucket: str = None, option_symbols_file: str = 'option_symbols.txt', 
                 equity_symbols_file: str = 'equity_symbols.txt'):
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        self.debug = debug
        self.auth = SchwabAuth()
        self.gcs_bucket = gcs_bucket
        self.option_symbols_file = option_symbols_file
        self.equity_symbols_file = equity_symbols_file
        
        # Load symbols from GCS if bucket is provided
        if gcs_bucket:
            # Load option symbols and DTE mapping
            self.equity_symbols_for_options, self.dte_mapping = self.load_option_symbols_from_gcs(gcs_bucket, option_symbols_file)
            
            # Load equity symbols for chart data
            self.equity_symbols = self.load_equity_symbols_from_gcs(gcs_bucket, equity_symbols_file)
            

        else:
            # Default symbols if none provided
            if equity_symbols is None:
                equity_symbols = ['SPY']  # Common equity symbols
            self.equity_symbols = equity_symbols
            self.equity_symbols_for_options = equity_symbols  # Use same symbols for options
            self.dte_mapping = {symbol: 1 for symbol in equity_symbols}  # Default 1 DTE
        
        if option_symbols is None:
            option_symbols = []  # Will be populated based on equity symbols
        
        self.option_symbols = option_symbols
        
        # Initialize options symbol finder
        self.symbol_finder = OptionsSymbolFinder(self.auth)
        
        # Data storage
        self.option_data = {}  # Store option symbol/contract_type data for streaming (real-time updates)
        self.chart_option_data = {}  # Store chart option data for equity symbols
        
        # Recording DataFrames - only updated when volume changes
        self.option_recording_data = {}  # Store option/contract_type data for CSV recording
        self.chart_recording_data = {}  # Store chart data for CSV recording
        
        # 5-minute aggregated data structures
        self.chart_1m_data = {}  # Store 1-minute chart data for aggregation
        self.chart_5m_data = {}  # Store completed 5-minute aggregated bars
        
        # Track previous values for each option symbol to handle partial updates
        self.previous_option_values = {}
        
        # Track previous values for recording condition checks
        self.mark_price_changed = {}  # symbol -> contract_type -> bool
        self.last_price_changed = {}  # symbol -> contract_type -> bool
        self.total_volume_changed = {}  # symbol -> contract_type -> bool
        
        # Tick tracking for OHLCV data
        self.tick_tracking = {}  # symbol -> contract_type -> tick_data
        self.previous_total_volume = {}  # symbol -> contract_type -> previous_total_volume
        self.last_recorded_volume = {}  # symbol -> contract_type -> last_recorded_volume
        
        # Track symbol order for CHART_EQUITY data correlation
        self.chart_equity_symbol_order = []
        
        # Initialize indicator calculator
        self.indicator_calculator = IndicatorCalculator()
        self.indicator_periods = self.load_indicator_periods()
        
        # Initialize signal checker for trade signals
        self.signal_checker = SignalChecker(debug=debug)
        
        # Base strikes information from options symbol finder
        self.base_strikes = {}
        self.tradeable_strikes = {}
        
        # Initialize email manager for trade notifications
        self.email_manager = TradingEmailManager(debug=debug)
        # Set default recipients (can be overridden later)
        self.email_manager.set_recipients(['your-email@example.com'])  # Replace with actual email
        
        # WebSocket connection
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self.connected = False
        self.request_id = 1
        self.subscriptions = {}
        
        # Get user preferences and store SchwabClientCustomerId
        self.user_preferences = self.get_user_preferences()
        if not self.user_preferences or 'streamerInfo' not in self.user_preferences:
            raise Exception("Could not get user preferences")
        
        # Store streamer info
        self.streamer_info = self.user_preferences['streamerInfo'][0]
        self.schwab_client_customer_id = self.streamer_info.get('schwabClientCustomerId')
        if not self.schwab_client_customer_id:
            raise Exception("Could not get SchwabClientCustomerId from user preferences")
        
        # Market hours (Eastern Time)
        self.et_tz = pytz.timezone('US/Eastern')
        self.market_open = datetime.strptime('09:30', '%H:%M').time()
        self.market_close = datetime.strptime('16:00', '%H:%M').time()
    
    def set_email_recipients(self, emails: List[str]):
        """
        Set email recipients for trade notifications and daily summaries
        
        Args:
            emails: List of email addresses to send notifications to
        """
        self.email_manager.set_recipients(emails)
    
    def test_email_connection(self) -> bool:
        """
        Test email connection
        
        Returns:
            True if connection successful, False otherwise
        """
        return self.email_manager.test_email_connection()
    
    def load_option_symbols_from_gcs(self, bucket_name: str, file_name: str) -> tuple[List[str], Dict[str, int]]:
        """
        Load option symbols and DTE from a file in Google Cloud Storage.
        
        Args:
            bucket_name (str): GCS bucket name
            file_name (str): Name of the file in the bucket
            
        Returns:
            tuple[List[str], Dict[str, int]]: List of symbols and mapping of symbol to DTE
        """
        try:
            # Import GCS client from the authentication module in options-symbol-finder
            sys.path.append(os.path.join(os.path.dirname(__file__), 'options-symbol-finder', 'charles-schwab-authentication-module', 'gcs-python-module'))
            from gcs_client import GCSClient
            
            gcs_client = GCSClient()
            
            # Download the file from GCS
            print(f"📥 Downloading {file_name} from GCS bucket: {bucket_name}")
            success = gcs_client.download_file(bucket_name, file_name, file_name)
            
            if not success:
                print(f"❌ Failed to download {file_name} from GCS bucket: {bucket_name}")
                return [], {}
            
            # Read the file
            symbols = []
            dte_mapping = {}
            
            with open(file_name, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):  # Skip empty lines and comments
                        parts = line.split(',')
                        if len(parts) >= 2:
                            symbol = parts[0].strip()
                            dte = int(parts[1].strip())
                            symbols.append(symbol)
                            dte_mapping[symbol] = dte
            
            print(f"✅ Loaded {len(symbols)} option symbols from GCS")
            return symbols, dte_mapping
            
        except Exception as e:
            print(f"❌ Error loading option symbols from GCS: {e}")
            return [], {}

    def load_equity_symbols_from_gcs(self, bucket_name: str, file_name: str) -> List[str]:
        """
        Load equity symbols for chart data from a file in Google Cloud Storage.
        
        Args:
            bucket_name (str): GCS bucket name
            file_name (str): Name of the file in the bucket
            
        Returns:
            List[str]: List of equity symbols for chart data
        """
        try:
            # Import GCS client from the authentication module in options-symbol-finder
            sys.path.append(os.path.join(os.path.dirname(__file__), 'options-symbol-finder', 'charles-schwab-authentication-module', 'gcs-python-module'))
            from gcs_client import GCSClient
            
            gcs_client = GCSClient()
            
            # Download the file from GCS
            print(f"📥 Downloading {file_name} from GCS bucket: {bucket_name}")
            success = gcs_client.download_file(bucket_name, file_name, file_name)
            
            if not success:
                print(f"❌ Failed to download {file_name} from GCS bucket: {bucket_name}")
                return []
            
            # Read the file
            symbols = []
            
            with open(file_name, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):  # Skip empty lines and comments
                        symbol = line.strip()
                        symbols.append(symbol)
            
            print(f"✅ Loaded {len(symbols)} equity symbols from GCS")
            return symbols
            
        except Exception as e:
            print(f"❌ Error loading equity symbols from GCS: {e}")
            return []
    
    def load_indicator_periods_from_gcs(self, bucket_name: str, file_name: str) -> Dict:
        """
        Load indicator periods configuration from a JSON file in Google Cloud Storage.
        
        Args:
            bucket_name (str): GCS bucket name
            file_name (str): Name of the JSON file in the bucket
            
        Returns:
            Dict: Indicator periods configuration
        """
        try:
            # Import GCS client from the authentication module in options-symbol-finder
            sys.path.append(os.path.join(os.path.dirname(__file__), 'options-symbol-finder', 'charles-schwab-authentication-module', 'gcs-python-module'))
            from gcs_client import GCSClient
            
            gcs_client = GCSClient()
            
            # Download the JSON file from GCS
            print(f"📦 Downloading indicator periods from GCS bucket: {bucket_name}")
            downloaded_file = gcs_client.download_file(bucket_name, file_name, file_name)
            
            if downloaded_file and os.path.exists(file_name):
                # Load and parse the JSON file
                with open(file_name, 'r') as f:
                    indicator_periods = json.load(f)
                
                print(f"📊 Successfully loaded indicator periods from GCS")
                return indicator_periods
            else:
                print(f"❌ Failed to download or find indicator periods file from GCS")
                return {}
                
        except Exception as e:
            print(f"❌ Error loading indicator periods from GCS: {e}")
            return {}
    
    def load_indicator_periods(self) -> Dict:
        """Load indicator periods configuration from GCS bucket or local indicator_periods.json file"""
        try:
            # First try to load from GCS bucket if available
            if self.gcs_bucket:
                try:
                    indicator_periods = self.load_indicator_periods_from_gcs(self.gcs_bucket, 'indicator_periods.json')
                    if indicator_periods:
                        return indicator_periods
                except Exception as e:
                    pass
                    
            # Fallback to local file
            config_file = 'indicator_periods.json'
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    indicator_periods = json.load(f)
                return indicator_periods
            else:
                return {}
        except Exception as e:
            print(f"❌ Error loading indicator periods: {e}")
            return {}
    
    def calculate_1m_indicators(self, symbol: str, chart_data: dict) -> dict:
        """Calculate technical indicators for 1-minute streaming data"""
        try:
            # Check if we have indicator periods for this symbol and timeframe
            if symbol not in self.indicator_periods or '1m' not in self.indicator_periods[symbol]:
                return chart_data  # Return unchanged if no configuration
            
            indicator_config = self.indicator_periods[symbol]['1m']
            
            # Get existing 1-minute data for this symbol
            if symbol not in self.chart_1m_data:
                self.chart_1m_data[symbol] = pd.DataFrame()
            
            existing_data = self.chart_1m_data[symbol]

            # Convert chart_data to pandas Series format expected by indicator calculator
            new_row = pd.Series({
                'timestamp': chart_data['timestamp (ms)'],
                'open': chart_data['open'],
                'high': chart_data['high'],
                'low': chart_data['low'],
                'close': chart_data['close'],
                'volume': chart_data['volume']
            })
                
            # Calculate indicators if we have sufficient data
            if len(existing_data) > 0:
                enhanced_row = self.indicator_calculator.calculate_latest_tick_indicators(
                    existing_data=existing_data,
                    new_row=new_row,
                    indicator_periods=indicator_config,
                )
                
                # Add indicator values to chart_data
                for col in enhanced_row.index:
                    if col not in ['timestamp', 'open', 'high', 'low', 'close', 'volume']:
                        chart_data[col] = enhanced_row[col]
                
            return chart_data
            
        except Exception as e:
            return chart_data
    
    def calculate_option_indicators(self, symbol: str, option_data: dict) -> dict:
        """
        Calculate technical indicators for option data using RECORDED history.
        Each period in indicator calculations represents a recorded row (meaningful changes only).
        Only called when last_price has changed to optimize performance.
        """
        try:
            # Extract underlying symbol from option symbol: "SPY   250807P00633000" -> "SPY"
            underlying_symbol = symbol[:3].strip()  # First 3 characters
            
            # Check if we have indicator periods for this underlying symbol and timeframe
            if underlying_symbol not in self.indicator_periods or '1m' not in self.indicator_periods[underlying_symbol]:
                return option_data  # Return unchanged if no configuration
            
            indicator_config = self.indicator_periods[underlying_symbol]['1m']
            
            # Get existing RECORDED data for this symbol/contract_type to build history (not streaming data)
            contract_type = option_data.get('contract_type', '')
            if (symbol not in self.option_recording_data or 
                contract_type not in self.option_recording_data[symbol] or 
                len(self.option_recording_data[symbol][contract_type]) == 0):
                return option_data  # Not enough recorded data for indicators
            
            # Convert RECORDED option data history to DataFrame format expected by indicator calculator
            # Include ALL recorded data (including current row) for indicator calculation
            option_history = []
            for opt_record in self.option_recording_data[symbol][contract_type]:  # Include all rows
                option_history.append({
                    'timestamp': opt_record.get('timestamp (ms)', pd.Timestamp.now().value // 10**6),  # Current timestamp in ms
                    'open': opt_record.get('tick_open', opt_record.get('mark_price', 0)),
                    'high': opt_record.get('tick_high', opt_record.get('mark_price', 0)),
                    'low': opt_record.get('tick_low', opt_record.get('mark_price', 0)),
                    'close': opt_record.get('tick_close', opt_record.get('mark_price', 0)),
                    'volume': opt_record.get('tick_volume', opt_record.get('total_volume', 0))
                })
            
            existing_data = pd.DataFrame(option_history)  # All recorded data including current row
            
            # For single-tick calculation, we pass the last row as new_row
            # This allows indicators to start from Row 1
            if len(existing_data) > 0:
                new_row = existing_data.iloc[-1]  # Use the last row as new_row
                # Only remove the last row if we have more than 1 row (not the first message)
                if len(existing_data) > 1:
                    existing_data = existing_data.iloc[:-1]  # Remove last row from existing_data
                # For first message (snapshot), keep the data as is so indicators can calculate
            else:
                new_row = pd.Series({
                    'timestamp': pd.Timestamp.now().value // 10**6,
                    'open': option_data.get('tick_open', option_data.get('mark_price', 0)),
                    'high': option_data.get('tick_high', option_data.get('mark_price', 0)),
                    'low': option_data.get('tick_low', option_data.get('mark_price', 0)),
                    'close': option_data.get('tick_close', option_data.get('mark_price', 0)),
                    'volume': option_data.get('tick_volume', option_data.get('total_volume', 0))
                })
            
            # Calculate indicators - each indicator will return empty if insufficient data
            enhanced_row = self.indicator_calculator.calculate_latest_tick_indicators(
                existing_data=existing_data,
                new_row=new_row,
                indicator_periods=indicator_config,
            )
            
            # Add ALL indicator values to option_data (including empty ones for CSV consistency)
            for col in enhanced_row.index:
                if col not in ['timestamp', 'open', 'high', 'low', 'close', 'volume']:
                    value = enhanced_row[col]
                    # Always add the indicator field to option_data, even if empty
                    option_data[col] = value if value != "" and value != 0 and not pd.isna(value) else ""
            
            # Add timestamp to option_data for CSV and signal processing
            if 'timestamp' not in option_data or not option_data['timestamp']:
                # Convert current timestamp to readable format
                current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                option_data['timestamp'] = current_timestamp
            
            return option_data
            
        except Exception as e:
            return option_data
    
    def _has_sufficient_data_for_signals(self, symbol: str, contract_type: str) -> bool:
        """
        Check if we have enough recorded data for reliable trading signals.
        Requires at least macd_slow * 3 + 1 entries to ensure indicators are stable.
        
        Args:
            symbol: Option symbol to check
            
        Returns:
            bool: True if we have sufficient data for signal checking
        """
        try:
            # Extract underlying symbol from option symbol: "SPY   250807P00634000" -> "SPY"
            underlying_symbol = symbol[:3].strip()  # First 3 characters
            
            # Get macd_slow period from indicator configuration
            macd_slow = 26  # Default value
            if underlying_symbol in self.indicator_periods and '1m' in self.indicator_periods[underlying_symbol]:
                config = self.indicator_periods[underlying_symbol]['1m']
                macd_slow = config.get('macd_slow', 26)
            
            # Calculate minimum entries needed: macd_slow * 3 +1 for current row
            min_entries_required = macd_slow * 3 + 1
            
            # Get the current recorded data count for this symbol
            recorded_count = 0
            if symbol in self.option_recording_data and contract_type in self.option_recording_data[symbol]:
                recorded_count = len(self.option_recording_data[symbol][contract_type])
            
            sufficient = recorded_count >= min_entries_required
            return sufficient
            
        except Exception as e:
            # Default to not allow signals if we can't determine
            return False
    
    def _is_trading_eligible_strike(self, option_symbol: str, contract_type: str, strike_price: float) -> bool:
        """
        Check if this option is eligible for trading.
        
        Args:
            option_symbol: Full option symbol (e.g., "SPY   250807P00634000")
            contract_type: 'C' or 'P'
            strike_price: Strike price as float
            
        Returns:
            bool: True if eligible for trading (now allows ALL strikes)
        """
        try:
            # Extract underlying symbol from option symbol (first 3 characters for most symbols)
            underlying_symbol = option_symbol[:3].strip()
            
            # Debug: Print the check details
            print(f"🔍 DEBUG: Checking trading eligibility for {option_symbol}")
            print(f"  Underlying symbol: {underlying_symbol}")
            print(f"  Contract type: {contract_type}")
            print(f"  Strike price: {strike_price}")
            
            # ALL STRIKES ARE NOW TRADEABLE
            is_eligible = True
            
            print(f"  Decision: {is_eligible} (ALL STRIKES ENABLED - contract_type: {contract_type}, strike: {strike_price})")
            return is_eligible
            
        except Exception as e:
            # Default to allowing trading if we can't determine eligibility
            print(f"  Decision: True (exception occurred, defaulting to allow trading)")
            return True
    
    def process_trading_signals(self, symbol: str, option_data: dict) -> dict:
        """
        Process trading signals for options with calculated indicators
        
        Args:
            symbol: Option symbol (e.g., "QQQ250731C00567000")
            option_data: Option data with calculated indicators
            
        Returns:
            Signal processing result
        """
        try:
            # Validate that option_data has required fields
            if not option_data or 'contract_type' not in option_data:
                print(f"❌ Invalid option_data for {symbol}: missing contract_type field")
                return {'action': 'error', 'error': 'missing_contract_type'}
            
            # Convert option_data dict to pandas Series for signal checker
            row_data = pd.Series(option_data) 
            
            # Debug: Print the row data being checked for signals
            if self.debug:
                print(f"\n🔍 DEBUG: Processing signals for {symbol}")
                print(f"  Contract Type: {option_data.get('contract_type', 'N/A')}")
                print(f"  Mark Price: {option_data.get('mark_price', 'N/A')}")
                print(f"  Tick Close: {option_data.get('tick_close', 'N/A')}")
                print(f"  Tick Volume: {option_data.get('tick_volume', 'N/A')}")
                
                # Print all indicator values dynamically
                base_fields = ['timestamp', 'symbol', 'description', 'bid_price', 'ask_price', 'last_price', 'high_price', 'low_price', 'close_price', 'total_volume', 'open_interest', 'volatility', 'money_intrinsic_value', 'expiration_year', 'multiplier', 'digits', 'open_price', 'bid_size', 'ask_size', 'last_size', 'net_change', 'strike_price', 'contract_type', 'underlying', 'expiration_month', 'deliverables', 'time_value', 'expiration_day', 'days_to_expiration', 'delta', 'gamma', 'theta', 'vega', 'rho', 'security_status', 'theoretical_option_value', 'underlying_price', 'uv_expiration_type', 'mark_price', 'quote_time', 'trade_time', 'exchange', 'exchange_name', 'last_trading_day', 'settlement_type', 'net_percent_change', 'mark_price_net_change', 'mark_price_percent_change', 'implied_yield', 'is_penny_pilot', 'option_root', 'week_52_high', 'week_52_low', 'indicative_ask_price', 'indicative_bid_price', 'indicative_quote_time', 'exercise_type', 'tick_open', 'tick_high', 'tick_low', 'tick_close', 'tick_volume']
                
                # Find all indicator fields (anything not in base_fields)
                indicator_fields = [key for key in option_data.keys() if key not in base_fields]
                
                if indicator_fields:
                    print(f"  Indicators ({len(indicator_fields)}):")
                    for field in sorted(indicator_fields):
                        value = option_data.get(field, 'N/A')
                        print(f"    {field}: {value}")
                else:
                    print(f"  Indicators: None found")
                
                print(f"  Row data keys: {list(option_data.keys())}")
            
            # Process the row through signal checker
            signal_result = self.signal_checker.process_streaming_row(symbol, row_data)
            
            # Debug: Print signal result
            if self.debug:
                print(f"  Signal Result: {signal_result}")
                if signal_result.get('action') != 'no_action':
                    print(f"  ⚠️ Signal detected: {signal_result.get('action')}")
            
            # Log trade actions and send email notifications
            if signal_result.get('action') == 'enter_trade':
                trade_details = signal_result.get('trade_details', {})
                
                # Get configured indicators for this symbol
                underlying_symbol = symbol[:3].strip()
                indicator_config = {}
                if underlying_symbol in self.indicator_periods and '1m' in self.indicator_periods[underlying_symbol]:
                    indicator_config = self.indicator_periods[underlying_symbol]['1m']
                
                # Build additional_info with dynamic indicator mapping
                additional_info = {
                    'full_symbol': symbol,
                    'entry_time': trade_details.get('entry_timestamp', ''),
                }
                
                # Add configured indicators from entry_indicators
                entry_indicators = trade_details.get('entry_indicators', {})
                
                if self.debug:
                    print(f"🔍 DEBUG: Entry indicators available: {list(entry_indicators.keys())}")
                    print(f"🔍 DEBUG: Configured indicators: {list(indicator_config.keys())}")
                
                for indicator_name in indicator_config.keys():
                    # Use exact column names from indicator_periods.json
                    if indicator_name == 'macd_fast' or indicator_name == 'macd_slow':
                        # Both macd_fast and macd_slow create the same macd_line column
                        additional_info['macd_line'] = entry_indicators.get('entry_macd_line', 0)
                    elif indicator_name == 'stoch_rsi_period':
                        # Skip period config, we want the actual K/D values
                        continue
                    else:
                        # Use the exact indicator name from indicator_periods.json
                        additional_info[indicator_name] = entry_indicators.get(f'entry_{indicator_name}', 0)
                
                # Send email notification for trade entry
                self.email_manager.send_trade_notification(
                    action="BUY",
                    symbol=symbol,
                    contract_type=option_data['contract_type'],
                    price=trade_details.get('entry_price', 0),
                    additional_info=additional_info
                )
                    
            elif signal_result.get('action') == 'exit_trade':
                trade_details = signal_result.get('trade_details', {})
                
                # Get configured indicators for this symbol
                underlying_symbol = symbol[:3].strip()
                indicator_config = {}
                if underlying_symbol in self.indicator_periods and '1m' in self.indicator_periods[underlying_symbol]:
                    indicator_config = self.indicator_periods[underlying_symbol]['1m']
                
                # Build additional_info with dynamic indicator mapping
                additional_info = {
                    'full_symbol': symbol,
                    'entry_price': trade_details.get('entry_price', 0),
                    'profit': f"${trade_details.get('profit', 0):.4f}",
                    'profit_pct': f"{trade_details.get('profit_pct', 0):.2f}%",
                    'exit_reason': trade_details.get('exit_reason', 'unknown'),
                    'duration': trade_details.get('duration_minutes', 0),
                }
                
                # Add configured indicators from entry_indicators and exit_indicators
                entry_indicators = trade_details.get('entry_indicators', {})
                exit_indicators = trade_details.get('exit_indicators', {})
                
                if self.debug:
                    print(f"🔍 DEBUG: Entry indicators available: {list(entry_indicators.keys())}")
                    print(f"🔍 DEBUG: Exit indicators available: {list(exit_indicators.keys())}")
                    print(f"🔍 DEBUG: Configured indicators: {list(indicator_config.keys())}")
                
                for indicator_name in indicator_config.keys():
                    # Use exact column names from indicator_periods.json
                    if indicator_name == 'macd_fast' or indicator_name == 'macd_slow':
                        # Both macd_fast and macd_slow create the same macd_line column
                        additional_info['entry_macd_line'] = entry_indicators.get('entry_macd_line', 0)
                        additional_info['exit_macd_line'] = exit_indicators.get('exit_macd_line', 0)
                    elif indicator_name == 'stoch_rsi_period':
                        # Skip period config, we want the actual K/D values
                        continue
                    else:
                        # Use the exact indicator name from indicator_periods.json with entry/exit prefix
                        additional_info[f'entry_{indicator_name}'] = entry_indicators.get(f'entry_{indicator_name}', 0)
                        additional_info[f'exit_{indicator_name}'] = exit_indicators.get(f'exit_{indicator_name}', 0)
                
                # Send email notification for trade exit               
                self.email_manager.send_trade_notification(
                    action="SELL",
                    symbol=symbol,
                    contract_type=option_data['contract_type'],
                    price=trade_details.get('exit_price', 0),
                    additional_info=additional_info
                )
                        

            
            # Save trades periodically - DISABLED: Using close.csv for email instead
            # summary = self.signal_checker.get_trade_summary()
            # if summary['total_trades'] > 0 and summary['total_trades'] % 10 == 0:
            #     self.signal_checker.save_trades_to_file(f'data/trades_history_{datetime.now().strftime("%Y%m%d")}.json')
            
            return signal_result
            
        except Exception as e:
            print(f"❌ Error processing trading signals for {symbol}: {e}")
            return {'action': 'error', 'error': str(e)}
    
    def get_trading_summary(self) -> dict:
        """
        Get comprehensive trading summary including active and completed trades
        
        Returns:
            Dictionary with trading performance metrics
        """
        try:
            summary = self.signal_checker.get_trade_summary()
            
            # Add active trades details
            active_trades_details = []
            for symbol, contract_type in self.signal_checker.active_trades.items():        
                active_trades_details.append({
                    'symbol': f"{symbol}",
                    'full_symbol': symbol,
                    'entry_price': self.signal_checker.active_trades[symbol][contract_type].get('entry_price', 0),
                    'entry_timestamp': self.signal_checker.active_trades[symbol][contract_type].get('entry_timestamp', ''),
                    'unrealized_pnl': self.signal_checker.active_trades[symbol][contract_type].get('max_price_seen', 0) - self.signal_checker.active_trades[symbol][contract_type].get('entry_price', 0)
                })
            
            summary['active_trades_details'] = active_trades_details
            summary['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return summary
            
        except Exception as e:
            return {'error': f"Failed to get trading summary: {e}"}

    def wait_for_market_open(self):
        """
        Check if it's before 9:30 AM ET and wait until market opens if needed.
        Returns True if market is open or will be open today, False if it's weekend.
        """
        now_et = datetime.now(self.et_tz)
        current_time = now_et.time()
        current_date = now_et.date()
        
        # Check if it's weekend
        if current_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            print(f"📅 Weekend detected ({current_date.strftime('%A')}), market is closed")
            return False
        
        # Check if it's before market hours
        if current_time < self.market_open:
            # Calculate time until market open
            market_open_dt = datetime.combine(current_date, self.market_open)
            market_open_dt = self.et_tz.localize(market_open_dt)
            
            wait_seconds = (market_open_dt - now_et).total_seconds()
            
            if wait_seconds > 0:
                wait_hours = int(wait_seconds // 3600)
                wait_minutes = int((wait_seconds % 3600) // 60)
                wait_secs = int(wait_seconds % 60)
                
                print(f"⏰ Before market hours. Current time: {now_et.strftime('%H:%M:%S %Z')}")
                print(f"📈 Market opens at: {market_open_dt.strftime('%H:%M:%S %Z')}")
                print(f"⏳ Waiting {wait_hours:02d}:{wait_minutes:02d}:{wait_secs:02d} until market open...")
                
                # Wait with progress indicator
                start_time = time.time()
                while time.time() - start_time < wait_seconds:
                    remaining = wait_seconds - (time.time() - start_time)
                    remaining_hours = int(remaining // 3600)
                    remaining_minutes = int((remaining % 3600) // 60)
                    remaining_secs = int(remaining % 60)
                    
                    print(f"\r⏳ Waiting: {remaining_hours:02d}:{remaining_minutes:02d}:{remaining_secs:02d} remaining...", end='', flush=True)
                    time.sleep(1)
                
                print(f"\n🎉 Market is now open! Starting streaming...")
            else:
                print(f"✅ Market is already open (current time: {now_et.strftime('%H:%M:%S %Z')})")
        else:
            print(f"✅ Market is already open (current time: {now_et.strftime('%H:%M:%S %Z')})")
        
        return True
    
    def subscribe_option_data(self, option_symbols: List[str]):
        """Subscribe to option symbol data"""
        if not self.connected:
            print("❌ Not connected to WebSocket")
            return

        try:
            # Prepare the subscription request for option data
            request = {
                "service": "LEVELONE_OPTIONS",  # Correct service name for options
                "command": "SUBS",
                "requestid": self.request_id,
                "SchwabClientCustomerId": self.schwab_client_customer_id,
                "SchwabClientCorrelId": f"option_{int(time.time() * 1000)}",
                "parameters": {
                    "keys": ",".join(option_symbols),
                    "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55"  # All available fields
                }
            }



            # Send the subscription request
            if self.ws:
                self.ws.send(json.dumps(request))
            self.request_id += 1

            # Store the subscription
            self.subscriptions["LEVELONE_OPTIONS"] = option_symbols

            print(f"✅ Subscribed to LEVELONE_OPTIONS data for: {', '.join(option_symbols)}")

        except Exception as e:
            print(f"❌ Error subscribing to LEVELONE_OPTIONS data: {e}")
            raise

    def subscribe_chart_equity_data(self, equity_symbols: List[str]):
        """Subscribe to chart equity data for equity symbols"""
        if not self.connected:
            print("❌ Not connected to WebSocket")
            return

        try:
            # Prepare the subscription request for chart equity data
            request = {
                "service": "CHART_EQUITY",  # Chart equity data service
                "command": "SUBS",
                "requestid": self.request_id,
                "SchwabClientCustomerId": self.schwab_client_customer_id,
                "SchwabClientCorrelId": f"chart_equity_{int(time.time() * 1000)}",
                "parameters": {
                    "keys": ",".join(equity_symbols),
                    "fields": "0,1,2,3,4,5,6,7,8"  # OHLCV and time fields
                }
            }



            # Send the subscription request
            if self.ws:
                self.ws.send(json.dumps(request))
            self.request_id += 1

            # Store the subscription with order mapping for data correlation
            self.subscriptions["CHART_EQUITY"] = equity_symbols
            self.chart_equity_symbol_order = equity_symbols  # Track symbol order for data correlation

            print(f"✅ Subscribed to CHART_EQUITY data for: {', '.join(equity_symbols)}")

        except Exception as e:
            print(f"❌ Error subscribing to CHART_EQUITY data: {e}")
            raise

    def connect(self):
        """Connect to WebSocket and start streaming"""
        try:
            # Get streamer info from user preferences
            if not self.streamer_info:
                raise Exception("No streamer info available")

            # Get WebSocket URL
            ws_url = self.streamer_info.get('streamerSocketUrl')
            if not ws_url:
                raise Exception("No WebSocket URL in streamer info")

            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )

            # Start WebSocket connection in a separate thread
            def run_ws():
                if self.ws:
                    self.ws.run_forever()

            # Start the WebSocket thread
            ws_thread = threading.Thread(target=run_ws, daemon=True)
            ws_thread.start()

            # Wait for connection
            timeout = 30
            start_time = time.time()
            while not self.connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)

            if not self.connected:
                raise Exception("Failed to connect to WebSocket within timeout")

            print("✅ Connected to Schwab Streaming API")

        except Exception as e:
            print(f"❌ Error connecting to WebSocket: {e}")
            raise

    def disconnect(self):
        """Disconnect from WebSocket"""
        if self.ws:
            self.ws.close()
        self.connected = False
        self.running = False
        print("🔌 Disconnected from Schwab Streaming API")
    
    def get_user_preferences(self):
        """Get user preferences using the SchwabAuth token"""
        try:
            # Get fresh token
            access_token = self.auth.get_valid_access_token(use_gcs_refresh_token=True)
            if not access_token:
                raise Exception("Failed to get valid access token")
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }
            
            url = "https://api.schwabapi.com/trader/v1/userPreference"
            
            with httpx.Client() as client:
                response = client.get(url, headers=headers)
                
            if response.status_code != 200:
                raise Exception(f"User preferences request failed: {response.status_code} - {response.text}")
            
            return response.json()
            
        except Exception as e:
            raise

    def parse_option_data(self, content_item: dict, contract_type: str) -> dict:
        """Parse option data from WebSocket message, merging with previous values"""
        try:
            symbol = content_item.get('key', '')
            if not symbol:
                print(f"⚠️ Warning: Empty symbol in option data: {content_item}")
                return {}
            
            # Update only the fields that are present in the new message
            field_mapping = {
                '0': 'symbol',
                '1': 'description',
                '2': 'bid_price',
                '3': 'ask_price',
                '4': 'last_price',
                '5': 'high_price',
                '6': 'low_price',
                '7': 'close_price',
                '8': 'total_volume',
                '9': 'open_interest',
                '10': 'volatility',
                '11': 'money_intrinsic_value',
                '12': 'expiration_year',
                '13': 'multiplier',
                '14': 'digits',
                '15': 'open_price',
                '16': 'bid_size',
                '17': 'ask_size',
                '18': 'last_size',
                '19': 'net_change',
                '20': 'strike_price',
                '21': 'contract_type',
                '22': 'underlying',
                '23': 'expiration_month',
                '24': 'deliverables',
                '25': 'time_value',
                '26': 'expiration_day',
                '27': 'days_to_expiration',
                '28': 'delta',
                '29': 'gamma',
                '30': 'theta',
                '31': 'vega',
                '32': 'rho',
                '33': 'security_status',
                '34': 'theoretical_option_value',
                '35': 'underlying_price',
                '36': 'uv_expiration_type',
                '37': 'mark_price',
                '38': 'quote_time',
                '39': 'trade_time',
                '40': 'exchange',
                '41': 'exchange_name',
                '42': 'last_trading_day',
                '43': 'settlement_type',
                '44': 'net_percent_change',
                '45': 'mark_price_net_change',
                '46': 'mark_price_percent_change',
                '47': 'implied_yield',
                '48': 'is_penny_pilot',
                '49': 'option_root',
                '50': 'week_52_high',
                '51': 'week_52_low',
                '52': 'indicative_ask_price',
                '53': 'indicative_bid_price',
                '54': 'indicative_quote_time',
                '55': 'exercise_type'
            }
            
            # Initialize nested dictionaries for this symbol if needed
            if symbol not in self.mark_price_changed:
                self.mark_price_changed[symbol] = {}
            if contract_type not in self.mark_price_changed[symbol]:
                self.mark_price_changed[symbol][contract_type] = True
                
            if symbol not in self.last_price_changed:
                self.last_price_changed[symbol] = {}
            if contract_type not in self.last_price_changed[symbol]:
                self.last_price_changed[symbol][contract_type] = True
                
            if symbol not in self.total_volume_changed:
                self.total_volume_changed[symbol] = {}
            if contract_type not in self.total_volume_changed[symbol]:
                self.total_volume_changed[symbol][contract_type] = 0

            if symbol not in self.previous_option_values:
                self.previous_option_values[symbol] = {}
            if contract_type not in self.previous_option_values[symbol]:
                # Parse symbol to extract static information: "SPY   250807C00631000"
                # Extract underlying (first 3 chars), date, call/put, strike
                symbol_clean = symbol.strip()
                underlying = symbol_clean[:3]
                
                # Parse the option symbol format: SPY   250807C00631000
                # Format: UNDERLYING + YYMMDD + C/P + STRIKE(5 digits + 3 decimals)
                if len(symbol_clean) >= 18:
                    date_part = symbol_clean[6:12]  # 250807
                    contract_char = symbol_clean[12]  # C or P
                    strike_part = symbol_clean[13:]   # 00631000
                    
                    # Convert strike: 00631000 -> 631.000
                    try:
                        strike_price = float(strike_part) / 1000
                    except:
                        strike_price = 0
                        
                    # Convert date: 250807 -> year=2025, month=8, day=7
                    try:
                        year = 2000 + int(date_part[:2])  # 25 -> 2025
                        month = int(date_part[2:4])       # 08 -> 8
                        day = int(date_part[4:6])         # 07 -> 7
                    except:
                        year, month, day = 0, 0, 0
                        
                    # Create description: "SPY 08/07/2025 631.00 C"
                    description = f"{underlying} {month:02d}/{day:02d}/{year} {strike_price:.2f} {contract_char}"
                else:
                    strike_price = 0
                    year, month, day = 0, 0, 0
                    description = ""
                
                # Initialize with parsed static data and defaults for streaming fields
                self.previous_option_values[symbol][contract_type] = {
                'symbol': underlying,       # 0: Symbol (Ticker symbol in upper case)
                'description': description, # 1: Description (Company, index or fund name)
                'bid_price': 0,         # 2: Bid Price (Current Bid Price)
                'ask_price': 0,         # 3: Ask Price (Current Ask Price)
                'last_price': 0,        # 4: Last Price (Price at which the last trade was matched)
                'high_price': 0,        # 5: High Price (Day's high trade price)
                'low_price': 0,         # 6: Low Price (Day's low trade price)
                'close_price': 0,       # 7: Close Price (Previous day's closing price)
                'total_volume': 0,      # 8: Total Volume (Aggregated contracts traded)
                'open_interest': 0,     # 9: Open Interest
                'volatility': 0,        # 10: Volatility (Option Risk/Volatility Measurement/Implied)
                'money_intrinsic_value': 0,  # 11: Money Intrinsic Value
                'expiration_year': year,     # 12: Expiration Year
                'multiplier': 100,      # 13: Multiplier (standard for equity options)
                'digits': 2,            # 14: Digits (Number of decimal places)
                'open_price': 0,        # 15: Open Price (Day's Open Price)
                'bid_size': 0,          # 16: Bid Size (Number of contracts for bid)
                'ask_size': 0,          # 17: Ask Size (Number of contracts for ask)
                'last_size': 0,         # 18: Last Size (Number of contracts traded with last trade)
                'net_change': 0,        # 19: Net Change (Current Last-Prev Close)
                'strike_price': strike_price,    # 20: Strike Price (Contract strike price)
                'contract_type': contract_char,  # 21: Contract Type
                'underlying': underlying,        # 22: Underlying
                'expiration_month': month,       # 23: Expiration Month
                'deliverables': f"100 {underlying}",  # 24: Deliverables
                'time_value': 0,        # 25: Time Value
                'expiration_day': day,  # 26: Expiration Day
                'days_to_expiration': 0, # 27: Days to Expiration
                'delta': 0,             # 28: Delta
                'gamma': 0,             # 29: Gamma
                'theta': 0,             # 30: Theta
                'vega': 0,              # 31: Vega
                'rho': 0,               # 32: Rho
                'security_status': 'Normal',  # 33: Security Status
                'theoretical_option_value': 0, # 34: Theoretical Option Value
                'underlying_price': 0,  # 35: Underlying Price
                'uv_expiration_type': 'W',  # 36: UV Expiration Type (W for weekly)
                'mark_price': 0,        # 37: Mark Price
                'quote_time': 0,        # 38: Quote Time in Long (milliseconds since Epoch)
                'trade_time': 0,        # 39: Trade Time in Long (milliseconds since Epoch)
                'exchange': 'O',        # 40: Exchange (Exchange character)
                'exchange_name': 'OPR', # 41: Exchange Name (Display name of exchange)
                'last_trading_day': 0,  # 42: Last Trading Day
                'settlement_type': 'P', # 43: Settlement Type (Settlement type character)
                'net_percent_change': 0, # 44: Net Percent Change
                'mark_price_net_change': 0, # 45: Mark Price Net Change
                'mark_price_percent_change': 0, # 46: Mark Price Percent Change
                'implied_yield': 0,     # 47: Implied Yield
                'is_penny_pilot': True, # 48: isPennyPilot
                'option_root': underlying, # 49: Option Root
                'week_52_high': 0,      # 50: 52 Week High
                'week_52_low': 0,       # 51: 52 Week Low
                'indicative_ask_price': 0, # 52: Indicative Ask Price
                'indicative_bid_price': 0, # 53: Indicative Bid Price
                'indicative_quote_time': 0, # 54: Indicative Quote Time
                'exercise_type': 'A'    # 55: Exercise Type (A for American)
            }

            # Get previous values for this symbol/contract_type (starts with defaults on first call)
            previous_option_values = self.previous_option_values[symbol][contract_type]
            

            
            # Create new option data by merging previous values with new updates
            option_data = previous_option_values.copy()
            
            # Only update fields that are present in the streaming message (changed fields only)
            for field_key, field_name in field_mapping.items():
                if field_key in content_item:
                    if field_name == 'mark_price':
                        self.mark_price_changed[symbol][contract_type] = True
                    elif field_name == 'total_volume':
                        self.total_volume_changed[symbol][contract_type] = True

                    option_data[field_name] = content_item[field_key]
            
            # Add symbol to the data for internal use (will be removed before CSV save)
            option_data['_symbol'] = symbol
            
            # Ensure contract_type is always present
            if 'contract_type' not in option_data:
                option_data['contract_type'] = contract_type
            
            # Store the updated values for next time - only update changed fields
            for field_key, field_name in field_mapping.items():
                if field_key in content_item:
                    self.previous_option_values[symbol][contract_type][field_name] = option_data[field_name]

            
            # Don't store volume delta in previous_option_values - it should be calculated fresh each time
            # The volume field is only for CSV output, not for persistence
            
            # Initialize tick tracking for this symbol/contract_type if not exists
            if symbol not in self.tick_tracking:
                self.tick_tracking[symbol] = {}
            if contract_type not in self.tick_tracking[symbol]:
                self.tick_tracking[symbol][contract_type] = {
                    'open': option_data.get('mark_price', 0),
                    'high': option_data.get('mark_price', 0),
                    'low': option_data.get('mark_price', 0),
                    'close': option_data.get('mark_price', 0),
                    'volume': option_data.get('total_volume', 0)
                }
            
            # Update tick tracking OHLCV data continuously
            current_mark_price = option_data.get('mark_price', 0)
            current_total_volume = option_data.get('total_volume', 0)
            
            if current_mark_price > 0:
                # Update OHLC for current tick
                tick_data = self.tick_tracking[symbol][contract_type]
                tick_data['high'] = max(tick_data['high'], current_mark_price)
                tick_data['low'] = min(tick_data['low'], current_mark_price)
                tick_data['close'] = current_mark_price
                
                # Add current tick OHLCV data to option_data
                option_data['tick_open'] = tick_data['open']
                option_data['tick_high'] = tick_data['high']
                option_data['tick_low'] = tick_data['low']
                option_data['tick_close'] = tick_data['close']
                
                # Calculate delta volume for this streaming message
                if symbol not in self.previous_total_volume:
                    self.previous_total_volume[symbol] = {}
                if contract_type not in self.previous_total_volume[symbol]:
                    self.previous_total_volume[symbol][contract_type] = current_total_volume
                
                previous_total_volume = self.previous_total_volume[symbol][contract_type]
                delta_volume = current_total_volume - previous_total_volume
                
                # Accumulate volume for current tick (don't overwrite)
                tick_data['volume'] += delta_volume
                option_data['tick_volume'] = tick_data['volume']
                
                # Update previous total volume for next calculation
                self.previous_total_volume[symbol][contract_type] = current_total_volume
            
            return option_data
        except Exception as e:
            print(f"❌ Error parsing option data: {e}")
            # Return a minimal valid option_data structure with contract_type
            return {
                '_symbol': symbol,
                'contract_type': contract_type,
                'mark_price': 0,
                'strike_price': 0
            }

    def parse_chart_equity_data(self, content_item: dict) -> dict:
        """Parse chart equity data from WebSocket message with corrected field mapping"""
        try:
            # Corrected CHART_EQUITY field definitions:
            # 0=key (ticker symbol), 1=sequence, 2=open, 3=high, 4=low, 5=close, 6=volume, 7=chart_time, 8=chart_day
            
            # Get symbol from field 0 (key) or fallback to 'key' field
            symbol = content_item.get('0', '')
            if not symbol:
                symbol = content_item.get('key', '')
            
            chart_data = {
                'timestamp (ms)': content_item.get('7', 0),     # Field 7: chart time (milliseconds since epoch)
                'symbol': symbol,                              # Field 0: ticker symbol
                'sequence': content_item.get('1', 0),          # Field 1: sequence
                'open': content_item.get('2', 0),              # Field 2: open price
                'high': content_item.get('3', 0),              # Field 3: high price
                'low': content_item.get('4', 0),               # Field 4: low price
                'close': content_item.get('5', 0),             # Field 5: close price
                'volume': content_item.get('6', 0),            # Field 6: volume
                'chart_day': content_item.get('8', 0)          # Field 8: chart day
            }
            
            return chart_data
        except Exception as e:
            print(f"❌ Error parsing chart equity data: {e}")
            return {}

    def on_message(self, _, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            


            # Handle response messages (subscription confirmations, errors, login responses)
            if 'response' in data:
                for response_item in data['response']:
                    service = response_item.get('service', '')
                    command = response_item.get('command', '')
                    content = response_item.get('content', {})
                    
                    # Handle login response
                    if service == 'ADMIN' and command == 'LOGIN':
                        self.handle_login_response(content)
                    # Handle subscription responses
                    if content.get('code') == 21:  # Bad command formatting
                        print(f"⚠️ Bad command formatting for {service} service. Attempting to fix...")
                        # Try to resubscribe with corrected format
                        if service == 'LEVELONE_OPTIONS':
                            self.resubscribe_option_data()
                        elif service == 'CHART_EQUITY':
                            # Equity streaming disabled for now
                            # self.resubscribe_chart_equity_data()
                            pass
                    elif content.get('code') == 0:  # Success
                        print(f"✅ {service} subscription successful")
                    elif content.get('code') == 3:  # Login denied
                        print(f"❌ {service} login denied: {content.get('msg', '')}")
                        # Try to reconnect and login again
                        time.sleep(5)
                        self.connect()
                    else:
                        print(f"⚠️ {service} response: {content}")
                
                return
            
            # Handle data messages
            if 'data' in data:
                for data_item in data['data']:
                    service = data_item.get('service', '')
                    # For option data
                    if service == 'LEVELONE_OPTIONS':
                        # Handle option data - the actual option data is in the 'content' array
                        content_array = data_item.get('content', [])
                        for content_item in content_array:
                            
                            # Extract contract type from symbol: "SPY   250807C00631000" -> "C"
                            symbol_key = content_item.get('key', '')
                            contract_type = ''
                            if len(symbol_key) >= 13:
                                contract_type = symbol_key[12]  # 13th character is C or P
                            
                            # Note: parse_option_data already merges partial updates with previous_option_values
                            # This gives us a complete record with all 56 fields for this update
                            option_data = self.parse_option_data(content_item, contract_type)
                            if option_data and '_symbol' in option_data and 'contract_type' in option_data:
                                symbol = option_data['_symbol'] # Get symbol from parsed data
                                
                                # Initialize option_data structure for this symbol if needed
                                if symbol not in self.option_data:
                                    self.option_data[symbol] = {}
                                if option_data['contract_type'] not in self.option_data[symbol]:
                                    self.option_data[symbol][option_data['contract_type']] = []
                                
                                # Initialize recording DataFrame structure for this symbol if needed
                                if symbol not in self.option_recording_data:
                                    self.option_recording_data[symbol] = {}
                                if option_data['contract_type'] not in self.option_recording_data[symbol]:
                                    self.option_recording_data[symbol][option_data['contract_type']] = []
                                    
                                # Always update streaming DataFrame (real-time updates with complete data)
                                self.option_data[symbol][option_data['contract_type']].append(option_data)                             
                                

                                # Check if this is the first message for this symbol (snapshot) or if volume threshold is met
                                is_first_message = len(self.option_recording_data[symbol][option_data['contract_type']]) == 0
                                
                                # Check if volume has increased by 50+ contracts since last recorded tick
                                if symbol not in self.last_recorded_volume:
                                    self.last_recorded_volume[symbol] = {}
                                if option_data['contract_type'] not in self.last_recorded_volume[symbol]:
                                    self.last_recorded_volume[symbol][option_data['contract_type']] = option_data.get('total_volume', 0)
                                
                                last_recorded_volume = self.last_recorded_volume[symbol][option_data['contract_type']]
                                volume_increase = option_data.get('total_volume', 0) - last_recorded_volume
                                volume_threshold_met = volume_increase >= 10
                                
                                if is_first_message or volume_threshold_met:
                                    # Volume threshold met → Record and save to CSV
                                    
                                    # Add to recording DataFrame FIRST (so calculate_option_indicators can access historical data)
                                    self.option_recording_data[symbol][option_data['contract_type']].append(option_data)
                                    
                                    # TRADEABLE STRIKE: Calculate indicators and process all signals
                                    option_data = self.calculate_option_indicators(symbol, option_data)
                                    # Check if this is a tradeable strike using row data
                                    is_tradeable = self._is_trading_eligible_strike(symbol, option_data['contract_type'], option_data['strike_price'])
                                    
                                    if is_tradeable and self._has_sufficient_data_for_signals(symbol, option_data['contract_type']):
                                        
                                        # Process all trading signals (entries, signal exits, AND stops)
                                        self.process_trading_signals(symbol, option_data)
                                        
                                        # Update the recorded data with calculated indicators
                                        self.option_recording_data[symbol][option_data['contract_type']][-1] = option_data
                                    
                                    # Save to CSV
                                    self.save_option_data_to_csv(symbol, option_data)
                                    
                                    # Update last recorded volume (this becomes the new baseline for next tick)
                                    self.last_recorded_volume[symbol][option_data['contract_type']] = option_data.get('total_volume', 0)
                                    
                                    # Start new tick with current values
                                    current_mark_price = option_data.get('mark_price', 0)
                                    current_total_volume = option_data.get('total_volume', 0)
                                    self.tick_tracking[symbol][option_data['contract_type']] = {
                                        'open': current_mark_price,      # New tick starts with current price
                                        'high': current_mark_price,      # Reset high to current price
                                        'low': current_mark_price,       # Reset low to current price
                                        'close': current_mark_price,     # Reset close to current price
                                        'volume': 0                      # Reset volume to 0 for new tick
                                    }
                                    
                                    if self.debug and volume_threshold_met:
                                        print(f"📊 Tick recorded for {symbol} {option_data['contract_type']}: Volume increase = {volume_increase}")

                                elif self.mark_price_changed[symbol][option_data['contract_type']]:
                                    # Check if active trade for SYMBOL CONTRACT TYPE exists
                                    if symbol in self.signal_checker.active_trades and option_data['contract_type'] in self.signal_checker.active_trades[symbol]:
                                        self.signal_checker.update_trade_tracking(symbol, option_data['mark_price'], option_data['contract_type'])
                                        
                                        # Check only stop loss and trailing stop
                                        should_exit, exit_details = self.signal_checker.should_exit_trade_stops_only(symbol, option_data)
                                        if self.debug:
                                            print(f"🔍 Stop check for {symbol}: should_exit={should_exit}, exit_details={exit_details}")
                                        if should_exit:
                                            # Process stop exit
                                            if self.debug:
                                                print(f"🛑 Stop loss triggered for {symbol}, calling exit_trade")
                                            completed_trade = self.signal_checker.exit_trade(symbol, option_data, exit_details)
                                            
                                            # Send email notification for stop exit
                                            if self.email_manager:
                                                # Get configured indicators for this symbol
                                                underlying_symbol = symbol[:3].strip()
                                                indicator_config = {}
                                                if underlying_symbol in self.indicator_periods and '1m' in self.indicator_periods[underlying_symbol]:
                                                    indicator_config = self.indicator_periods[underlying_symbol]['1m']
                                                
                                                # Build additional_info with dynamic indicator mapping
                                                additional_info = {
                                                    'full_symbol': symbol,
                                                    'entry_price': completed_trade.get('entry_price', 0),
                                                    'profit': f"${completed_trade.get('profit', 0):.4f}",
                                                    'profit_pct': f"{completed_trade.get('profit_pct', 0):.2f}%",
                                                    'exit_reason': exit_details.get('exit_reason', 'Stop Loss or Trailing Stop'),
                                                    'duration': completed_trade.get('duration_minutes', 0),
                                                }
                                                
                                                # Add configured indicators from entry_indicators and exit_indicators
                                                entry_indicators = completed_trade.get('entry_indicators', {})
                                                exit_indicators = completed_trade.get('exit_indicators', {})
                                                
                                                if self.debug:
                                                    print(f"🔍 DEBUG: Stop loss - Entry indicators available: {list(entry_indicators.keys())}")
                                                    print(f"🔍 DEBUG: Stop loss - Exit indicators available: {list(exit_indicators.keys())}")
                                                    print(f"🔍 DEBUG: Stop loss - Configured indicators: {list(indicator_config.keys())}")
                                                
                                                for indicator_name in indicator_config.keys():
                                                    # Use exact column names from indicator_periods.json
                                                    if indicator_name == 'macd_fast' or indicator_name == 'macd_slow':
                                                        # Both macd_fast and macd_slow create the same macd_line column
                                                        additional_info['entry_macd_line'] = entry_indicators.get('entry_macd_line', 0)
                                                        additional_info['exit_macd_line'] = exit_indicators.get('exit_macd_line', 0)
                                                    elif indicator_name == 'stoch_rsi_period':
                                                        # Skip period config, we want the actual K/D values
                                                        continue
                                                    else:
                                                        # Use the exact indicator name from indicator_periods.json with entry/exit prefix
                                                        additional_info[f'entry_{indicator_name}'] = entry_indicators.get(f'entry_{indicator_name}', 0)
                                                        additional_info[f'exit_{indicator_name}'] = exit_indicators.get(f'exit_{indicator_name}', 0)
                                                
                                                self.email_manager.send_trade_notification(
                                                    action="SELL",
                                                    symbol=symbol,
                                                    contract_type=option_data['contract_type'],
                                                    price=completed_trade.get('exit_price', 0),
                                                    additional_info=additional_info
                                                )

                                    # Reset the mark_price_changed flag after processing
                                    self.mark_price_changed[symbol][option_data['contract_type']] = False
                                

                    
                    elif service == 'CHART_EQUITY':
                        # Handle chart equity data - the actual chart data is in the 'content' array
                        content_array = data_item.get('content', [])
                        
                        for content_item in content_array:
                            
                            chart_data = self.parse_chart_equity_data(content_item)
                            if chart_data:
                                symbol = chart_data['symbol']
                                
                                if not symbol:
                                    continue  # Skip this data if symbol is empty
                                
                                # Calculate 1-minute indicators for streaming data
                                chart_data_with_indicators = self.calculate_1m_indicators(symbol, chart_data)
                                
                                # Always update streaming DataFrame (real-time updates)
                                if symbol not in self.chart_option_data:
                                    self.chart_option_data[symbol] = []
                                self.chart_option_data[symbol].append(chart_data_with_indicators)
                                
                                # Always record chart data (chart data typically represents discrete events)
                                if symbol not in self.chart_recording_data:
                                    self.chart_recording_data[symbol] = []
                                self.chart_recording_data[symbol].append(chart_data_with_indicators)
                                
                                # Save to CSV file (1-minute data with indicators)
                                self.save_chart_data_to_csv(symbol, chart_data_with_indicators)
                                
                                # 5-minute aggregation processing (use original chart_data without indicators)
                                self.process_5minute_aggregation(symbol, chart_data)
                                



        except Exception as e:
            print(f"❌ Error processing message: {e}")

    def on_error(self, _, error):
        """Handle WebSocket errors"""
        print(f"❌ WebSocket error: {error}")

    def on_close(self, _, close_status_code, close_msg):
        """Handle WebSocket close"""
        self.connected = False
        print(f"🔌 WebSocket closed: {close_status_code} - {close_msg}")
        
        # Try to reconnect if it's during market hours
        if self.is_market_open():
            print("🔄 Attempting to reconnect...")
            time.sleep(5)  # Wait 5 seconds before reconnecting
            self.connect()

    def resubscribe_option_data(self):
        """Resubscribe to option data with corrected format"""
        try:
            # Try with all 56 fields for complete data
            request = {
            "service": "LEVELONE_OPTIONS",
            "command": "SUBS",
            "requestid": self.request_id,
                "SchwabClientCustomerId": self.schwab_client_customer_id,
            "SchwabClientCorrelId": f"option_retry_{int(time.time() * 1000)}",
                "parameters": {
                "keys": ",".join(self.option_symbols[:10]),  # Limit to first 10 symbols
                "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55"  # All 56 fields
                }
            }

            if self.ws:
                self.ws.send(json.dumps(request))
            self.request_id += 1
            print(f"🔄 Resubscribed to LEVELONE_OPTIONS data with all 56 fields")

        except Exception as e:
            print(f"❌ Error resubscribing to LEVELONE_OPTIONS data: {e}")

    def resubscribe_chart_equity_data(self):
        """Resubscribe to chart equity data"""
        try:
            request = {
                "service": "CHART_EQUITY",
                "command": "SUBS",
                "requestid": self.request_id,
                "SchwabClientCustomerId": self.schwab_client_customer_id,
                "SchwabClientCorrelId": f"chart_equity_retry_{int(time.time() * 1000)}",
                "parameters": {
                    "keys": ",".join(self.equity_symbols),
                    "fields": "0,1,2,3,4,5,6,7,8"
                }
            }

            if self.ws:
                self.ws.send(json.dumps(request))
            self.request_id += 1
            print(f"🔄 Resubscribed to CHART_EQUITY data")

        except Exception as e:
            print(f"❌ Error resubscribing to CHART_EQUITY data: {e}")

    def is_market_open(self):
        """Check if market is currently open"""
        now_et = datetime.now(self.et_tz)
        current_time = now_et.time()
        current_date = now_et.date()
        
        # Check if it's weekend
        if current_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Check if it's during market hours
        return self.market_open <= current_time <= self.market_close

    def on_open(self, _):
        """Handle WebSocket open"""
        self.connected = True
        print("🔌 WebSocket connection opened")
        
        # First, send LOGIN command to authenticate
        self.login_to_streamer()

    def login_to_streamer(self):
        """Send LOGIN command to authenticate with the streamer"""
        try:
            # Get fresh access token
            access_token = self.auth.get_valid_access_token(use_gcs_refresh_token=True)
            if not access_token:
                raise Exception("Failed to get valid access token")

            # Prepare login request
            login_request = {
                "requests": [
                    {
                        "requestid": self.request_id,
                        "service": "ADMIN",
                        "command": "LOGIN",
                        "SchwabClientCustomerId": self.schwab_client_customer_id,
                        "SchwabClientCorrelId": f"login_{int(time.time() * 1000)}",
                        "parameters": {
                            "Authorization": access_token,
                            "SchwabClientChannel": "N9",  # Default channel
                            "SchwabClientFunctionId": "APIAPP"  # Default function ID
                        }
                    }
                ]
            }



            # Send login request
            if self.ws:
                self.ws.send(json.dumps(login_request))
            self.request_id += 1

            print("🔐 Login request sent, waiting for response...")

        except Exception as e:
            print(f"❌ Error sending login request: {e}")

    def handle_login_response(self, response_content):
        """Handle login response from streamer"""
        try:
            code = response_content.get('code', -1)
            msg = response_content.get('msg', '')
            
            if code == 0:  # Success
                print(f"✅ Login successful: {msg}")
                # Now subscribe to data streams
                if self.option_symbols:
                    self.subscribe_option_data(self.option_symbols)
                
                # Equity streaming disabled for now
                # if self.equity_symbols:
                #     self.subscribe_chart_equity_data(self.equity_symbols)
            else:
                print(f"❌ Login failed (code {code}): {msg}")
                # Close connection on login failure
                self.disconnect()

        except Exception as e:
            print(f"❌ Error handling login response: {e}")

    def save_option_data_to_csv(self, symbol: str, data: dict):
        """
        Save a new row of option data to CSV file.
        Called every time last_price changes.
        
        Args:
            symbol (str): Option symbol name (e.g., "SPY   250721C00630000")
            data (dict): Option data row to save (includes calculated indicators, some may be empty)
        """
        try:
            # Create options directory if it doesn't exist
            os.makedirs('data/options', exist_ok=True)
            
            # Clean the symbol for filename (remove spaces and special characters)
            # This matches the format from option-symbol-finder: "SPY250721C00630000"
            clean_symbol = symbol.replace(' ', '').replace('/', '_').replace('\\', '_')
            
            # Define CSV file path
            csv_file = f'data/options/{clean_symbol}.csv'
            

            
            # Remove internal fields that shouldn't be in CSV
            csv_data = data.copy()
            csv_data.pop('_symbol', None)  # Remove symbol since filename contains it
            
            # Use timestamp from data if available, otherwise use current time
            if 'timestamp' in csv_data and csv_data['timestamp']:
                # If timestamp is already in the data, use it
                timestamp = csv_data['timestamp']
            else:
                # Generate timestamp from current time
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Note: volume delta is already calculated in parse_option_data()
            
            # Create ordered dictionary with timestamp first
            ordered_data = {'timestamp': timestamp}
            ordered_data.update(csv_data)
            
            # Ensure all potential indicator columns are present (even if empty) for consistent headers
            # Group related indicators together in desired column order
            # Extract underlying symbol from option symbol: "SPY   250807P00634000" -> "SPY"
            underlying_symbol = symbol[:3].strip()  # First 3 characters
            if underlying_symbol in self.indicator_periods and '1m' in self.indicator_periods[underlying_symbol]:
                indicator_config = self.indicator_periods[underlying_symbol]['1m']
                
                # Dynamically add all configured indicators to ordered_data
                # This makes the system flexible to any indicator in the configuration
                for indicator_name in indicator_config.keys():
                    # Map configuration keys to actual indicator column names
                    indicator_mapping = {
                        'ema': 'ema',
                        'ema_fast': 'ema_fast', 
                        'ema_slow': 'ema_slow',
                        'vwma': 'vwma',
                        'roc': 'roc',
                        'roc_fast': 'roc_fast',
                        'roc_slow': 'roc_slow', 
                        'roc_of_roc': 'roc_of_roc',
                        'macd_fast': 'macd_line',  # macd_fast config -> macd_line column
                        'macd_slow': 'macd_line',  # macd_slow config -> macd_line column  
                        'macd_signal': 'macd_signal',
                        'rsi': 'rsi',
                        'sma': 'sma',
                        'atr': 'atr',
                        'stoch_rsi_period': 'stoch_rsi_period',  # Add the period configuration
                        'stoch_rsi_k': 'stoch_rsi_k',
                        'stoch_rsi_d': 'stoch_rsi_d',
                        'bollinger_bands': 'bollinger_upper'  # Will add all 3 BB columns
                    }
                    
                    # Get the actual column name for this indicator
                    column_name = indicator_mapping.get(indicator_name, indicator_name)
                    
                    # Add the indicator column if not already present
                    if column_name not in ordered_data:
                        ordered_data[column_name] = ""
                    
                    # Special handling for MACD (both fast/slow configs create the same line column)
                    if indicator_name in ['macd_fast', 'macd_slow'] and 'macd_line' not in ordered_data:
                        ordered_data['macd_line'] = ""
                    
                    # Special handling for Bollinger Bands (one config creates 3 columns)
                    if indicator_name == 'bollinger_bands':
                        for bb_column in ['bollinger_upper', 'bollinger_lower', 'bollinger_bands_width']:
                            if bb_column not in ordered_data:
                                ordered_data[bb_column] = ""
            
          
            
            # Define explicit column order for consistent CSV structure
            # Start with core streaming data columns
            base_columns = [
                'timestamp', 'symbol', 'description', 'bid_price', 'ask_price', 'last_price',
                'high_price', 'low_price', 'close_price', 'total_volume', 'open_interest',
                'volatility', 'money_intrinsic_value', 'expiration_year', 'multiplier', 'digits',
                'open_price', 'bid_size', 'ask_size', 'last_size', 'net_change', 'strike_price',
                'contract_type', 'underlying', 'expiration_month', 'deliverables', 'time_value',
                'expiration_day', 'days_to_expiration', 'delta', 'gamma', 'theta', 'vega', 'rho',
                'security_status', 'theoretical_option_value', 'underlying_price', 'uv_expiration_type',
                'mark_price', 'quote_time', 'trade_time', 'exchange', 'exchange_name',
                'last_trading_day', 'settlement_type', 'net_percent_change', 'mark_price_net_change',
                'mark_price_percent_change', 'implied_yield', 'is_penny_pilot', 'option_root',
                'week_52_high', 'week_52_low', 'indicative_ask_price', 'indicative_bid_price',
                'indicative_quote_time', 'exercise_type', 'tick_open', 'tick_high', 'tick_low', 'tick_close', 'tick_volume'
            ]
            
            # Dynamically create column order: base columns first, then any indicator columns
            # This makes the system flexible to any indicators that exist in ordered_data
            base_columns_set = set(base_columns)
            indicator_columns = [col for col in ordered_data.keys() if col not in base_columns_set]
            
            # Create final column order: base columns + indicator columns
            final_columns = base_columns + indicator_columns
            
            # Create ordered data list matching the column order
            ordered_values = [ordered_data.get(col, "") for col in final_columns]
            
            # Convert to DataFrame with explicit column order
            df_new = pd.DataFrame([ordered_values], columns=final_columns)
            
            # Check if file exists
            if os.path.exists(csv_file):
                # Append to existing file
                df_new.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                # Create new file with headers
                df_new.to_csv(csv_file, index=False)
            

            
        except Exception as e:
            print(f"❌ Error saving option data for {symbol}: {e}")

    def save_chart_data_to_csv(self, symbol: str, data: dict):
        """
        Save a new row of chart data to CSV file.
        
        Args:
            symbol (str): Equity symbol name
            data (dict): Chart data row to save (already contains timestamp from chart_time)
        """
        try:
            # Create equity directory if it doesn't exist
            os.makedirs('data/equity', exist_ok=True)
            
            # Clean the symbol for filename (remove spaces and special characters)
            clean_symbol = symbol.replace(' ', '').replace('/', '_').replace('\\', '_')
            
            # Define CSV file path
            csv_file = f'data/equity/{clean_symbol}.csv'
            
            # Data already contains timestamp converted from chart_time, use as-is
            # Convert data to DataFrame
            df_new = pd.DataFrame([data])
            
            # Check if file exists
            if os.path.exists(csv_file):
                # Append to existing file
                df_new.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                # Create new file with headers
                df_new.to_csv(csv_file, index=False)
            

                
        except Exception as e:
            print(f"❌ Error saving chart data for {symbol}: {e}")

    def save_5minute_chart_data_to_csv(self, symbol: str, data: dict):
        """
        Save a new row of 5-minute aggregated chart data to CSV file.
        
        Args:
            symbol (str): Equity symbol name
            data (dict): 5-minute aggregated data row to save
        """
        try:
            # Create 5m directory structure
            os.makedirs('data/equity/5m', exist_ok=True)
            
            # Clean the symbol for filename (remove spaces and special characters)
            clean_symbol = symbol.replace(' ', '').replace('/', '_').replace('\\', '_')
            
            # Define CSV file path for 5-minute data
            csv_file = f'data/equity/5m/{clean_symbol}.csv'
            
            # Convert timestamp to readable datetime
            timestamp_ms = data['timestamp']
            current_time = datetime.fromtimestamp(timestamp_ms / 1000, self.et_tz)
            datetime_str = current_time.strftime('%Y-%m-%d %H:%M:%S EDT')
            
            # Prepare data for CSV with timestamp as first column
            csv_data = {
                'timestamp (ms)': timestamp_ms,
                'datetime': datetime_str,
                'symbol': symbol,
                'open': data['open'],
                'high': data['high'],
                'low': data['low'],
                'close': data['close'],
                'volume': data['volume']
            }
            
            # Convert data to DataFrame
            df_new = pd.DataFrame([csv_data])
            
            # Check if file exists
            if os.path.exists(csv_file):
                # Append to existing file
                df_new.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                # Create new file with headers
                df_new.to_csv(csv_file, index=False)
            

                
        except Exception as e:
            print(f"❌ Error saving 5-minute chart data for {symbol}: {e}")

    def save_data_to_csv(self):
        """
        Legacy method - no longer needed since data is saved in real-time.
        Kept for backward compatibility and manual cleanup if needed.
        """
        print("ℹ️  Data is already saved in real-time - no batch saving needed")
                        
        # Clear memory to prevent buildup since data is saved in real-time
        if self.option_recording_data:
            print(f"🧹 Clearing {len(self.option_recording_data)} option recording entries from memory")
            self.option_recording_data.clear()
            
        if self.chart_recording_data:
            print(f"🧹 Clearing {len(self.chart_recording_data)} chart recording entries from memory")
            self.chart_recording_data.clear()

    def generate_option_symbols(self, days_to_expiration: int = None) -> List[str]:
        """
        Automatically generate option symbols for the equity symbols.
        
        Args:
            days_to_expiration (int): Minimum days to expiration for options (uses DTE mapping if None)
            
        Returns:
            List[str]: List of option symbols to stream
        """
        try:
            print(f"🔍 Generating option symbols for {len(self.equity_symbols_for_options)} equity symbols...")
            
            # Use DTE mapping if no specific DTE provided
            if days_to_expiration is None:
                # Use the DTE mapping loaded from GCS or default
                all_option_symbols = {}
                for symbol in self.equity_symbols_for_options:
                    dte = self.dte_mapping.get(symbol, 1)  # Default to 1 DTE
                    print(f"📅 Generating options for {symbol} with {dte} DTE")
                    
                    # Get option symbols for this specific symbol and DTE
                    symbol_option_symbols = self.symbol_finder.get_option_symbols_for_multiple_symbols(
                        [symbol], dte
                    )
                    
                    if symbol in symbol_option_symbols:
                        all_option_symbols[symbol] = symbol_option_symbols[symbol]
            else:
                # Use the specified DTE for all symbols
                all_option_symbols = self.symbol_finder.get_option_symbols_for_multiple_symbols(
                    self.equity_symbols_for_options, days_to_expiration
                )
            
            # Store the base strikes information and determine tradeable strikes
            self.base_strikes = {}
            self.tradeable_strikes = {}
            
            for symbol, option_data in all_option_symbols.items():
                if 'strikes' in option_data:
                    self.base_strikes[symbol] = option_data['strikes']
                    
                    # Determine tradeable strikes: LOWER call, HIGHER put (for reference only - ALL strikes are now tradeable)
                    call_strikes = option_data['strikes']['calls']
                    put_strikes = option_data['strikes']['puts']
                    
                    tradeable_call = min(call_strikes) if call_strikes else None  # LOWER call strike
                    tradeable_put = max(put_strikes) if put_strikes else None    # HIGHER put strike
                    
                    self.tradeable_strikes[symbol] = {
                        'call': tradeable_call,
                        'put': tradeable_put
                    }
                    
                    # Debug: Print tradeable strikes for this symbol
                    print(f"🎯 DEBUG: Tradeable strikes for {symbol}:")
                    print(f"  Call strike: {tradeable_call}")
                    print(f"  Put strike: {tradeable_put}")
                    print(f"  Available call strikes: {call_strikes}")
                    print(f"  Available put strikes: {put_strikes}")
                    

            
            # Flatten the option symbols into a single list and remove duplicates
            option_symbols = []
            for symbol, option_data in all_option_symbols.items():
                option_symbols.extend(option_data['calls'])
                option_symbols.extend(option_data['puts'])
            
            # Remove duplicates while preserving order
            option_symbols = list(dict.fromkeys(option_symbols))
            
            print(f"✅ Generated {len(option_symbols)} option symbols")
            
            return option_symbols
            
        except Exception as e:
            print(f"❌ Error generating option symbols: {e}")
            return []

    def auto_setup_option_streaming(self, days_to_expiration: int = None):
        """
        Automatically set up option streaming by generating option symbols.
        
        Args:
            days_to_expiration (int): Minimum days to expiration for options (uses DTE mapping if None)
        """
        try:
            print("🚀 Setting up automatic option streaming...")
            
            # Generate option symbols using DTE mapping if available
            generated_symbols = self.generate_option_symbols(days_to_expiration)
            
            if generated_symbols:
                # Update the option symbols
                self.option_symbols = generated_symbols
                print(f"✅ Auto-setup complete: {len(self.option_symbols)} option symbols ready for streaming")
            else:
                print("⚠️ No option symbols generated, using existing symbols")
                
        except Exception as e:
            print(f"❌ Error in auto-setup: {e}")

    def get_data_summary(self) -> Dict:
        """Get summary of collected data (both streaming and recording)"""
        summary = {
            'streaming_data': {
                'option_symbols': {},
                'chart_equity_symbols': {}
            },
            'recording_data': {
                'option_symbols': {},
                'chart_equity_symbols': {}
            },
            'aggregated_data': {
                'chart_1m_symbols': {},
                'chart_5m_symbols': {}
            }
        }
        
        # Streaming data counts (all updates)
        for symbol, data_list in self.option_data.items():
            summary['streaming_data']['option_symbols'][symbol] = len(data_list)
        
        for symbol, data_list in self.chart_option_data.items():
            summary['streaming_data']['chart_equity_symbols'][symbol] = len(data_list)
        
        # Recording data counts (only volume changes for options)
        for symbol, data_list in self.option_recording_data.items():
            summary['recording_data']['option_symbols'][symbol] = len(data_list)
        
        for symbol, data_list in self.chart_recording_data.items():
            summary['recording_data']['chart_equity_symbols'][symbol] = len(data_list)
        
        # Aggregated data counts
        for symbol, data_df in self.chart_1m_data.items():
            summary['aggregated_data']['chart_1m_symbols'][symbol] = len(data_df)
            
        for symbol, data_list in self.chart_5m_data.items():
            summary['aggregated_data']['chart_5m_symbols'][symbol] = len(data_list)
        
        return summary

    def process_5minute_aggregation(self, symbol: str, data: dict):
        """
        Process 1-minute chart data to create 5-minute aggregated bars using market-data-aggregator.
        
        Args:
            symbol (str): The equity symbol
            data (dict): The 1-minute chart data
        """
        try:
            # Convert chart data to the format expected by process_streaming_tick
            # Add datetime field for compatibility
            timestamp_ms = data['timestamp (ms)']
            current_time = datetime.fromtimestamp(timestamp_ms / 1000, self.et_tz)
            datetime_str = current_time.strftime('%Y-%m-%d %H:%M:%S EDT')
            
            new_row = {
                'timestamp': timestamp_ms,
                'datetime': datetime_str,
                'open': data['open'],
                'high': data['high'], 
                'low': data['low'],
                'close': data['close'],
                'volume': data['volume']
            }
            
            # Initialize symbol data if not exists
            if symbol not in self.chart_1m_data:
                self.chart_1m_data[symbol] = pd.DataFrame()
            
            # Process streaming tick for 5-minute aggregation (1m to 5m)
            result = process_streaming_tick(
                existing_data=self.chart_1m_data[symbol],
                new_row=new_row,
                original_timeframe=1,  # 1-minute source data
                new_timeframe=5        # 5-minute target
            )
            
            # Update the 1-minute data storage
            self.chart_1m_data[symbol] = result['window_data']
            
            # If 5-minute bar is ready, save it
            if result['ready']:
                aggregated_bar = result['aggregated_bar']
                
                # Calculate 5-minute indicators for aggregated data
                aggregated_bar_with_indicators = self.calculate_5m_indicators(symbol, aggregated_bar)
                
                # Initialize 5-minute data storage for symbol
                if symbol not in self.chart_5m_data:
                    self.chart_5m_data[symbol] = []
                
                # Add the completed 5-minute bar (with indicators)
                self.chart_5m_data[symbol].append(aggregated_bar_with_indicators)
                
                # Save 5-minute bar to CSV (with indicators)
                self.save_5minute_chart_data_to_csv(symbol, aggregated_bar_with_indicators)
                

                
        except Exception as e:
            print(f"❌ Error in 5-minute aggregation for {symbol}: {e}")


if __name__ == "__main__":
    # Load environment variables from .env file
    from dotenv import load_dotenv
    load_dotenv()
    
    # Example usage with GCS bucket from .env file
    gcs_bucket = os.getenv('GCS_BUCKET_NAME')  # Get from .env file
    
    if gcs_bucket:
        # Create client with GCS bucket (will load symbols from GCS)
        print(f"🔍 Using GCS bucket from .env: {gcs_bucket}")
        client = SchwabStreamingClient(
            debug=True,
            gcs_bucket=gcs_bucket,
            option_symbols_file='option_symbols.txt',
            equity_symbols_file='equity_symbols.txt'
        )
    else:
        # Fallback to local symbols
        print("📋 Using local symbols (no GCS_BUCKET_NAME in .env file)")
        equity_symbols = ['SPY']  # Equity symbols for chart option data
        
        client = SchwabStreamingClient(
            debug=True,
            equity_symbols=equity_symbols,
            option_symbols=[]  # Will be populated automatically
        )
    
    # Configure email notifications
    email_recipients = os.getenv('EMAIL_RECIPIENTS', 'your-email@example.com').split(',')
    client.set_email_recipients(email_recipients)
    
    # Test email connection
    print("📧 Testing email connection...")
    if client.test_email_connection():
        print("✅ Email connection successful")
    else:
        print("⚠️ Email connection failed - notifications will be disabled")
    
    try:
        # Check if it's before market hours and wait until 9:30 AM if needed
        if not client.wait_for_market_open():
            print("❌ Market is closed (weekend). Exiting...")
            exit(0)
        
        # Auto-setup option streaming AFTER market opens (uses settled prices from DTE mapping)
        print("🎯 Setting up option streaming with market open prices...")
        client.auto_setup_option_streaming()
        
        # Connect to streaming
        print("🔌 Connecting to Schwab Streaming API...")
        client.connect()
        
        # Keep the script running during market hours
        print("🔄 Starting market hours monitoring loop...")
        last_save_time = time.time()
        
        # Reset daily summary flag at market open
        client.email_manager.reset_daily_summary_flag()
        
        while client.is_market_open():
            time.sleep(1)
            
            # Check connection status
            if not client.connected:
                print("⚠️ Connection lost. Attempting to reconnect...")
                client.connect()
                continue
            
            # Check for daily summary (after market close)
            client.email_manager.check_and_send_daily_summary()
            
            # Print data summary every 5 minutes (data is saved in real-time)
            if time.time() - last_save_time >= 300:  # 5 minutes
                print("📊 Periodic status check...")
                last_save_time = time.time()
                
                # Print data summary
                summary = client.get_data_summary()
                print(f"📊 Data Summary: {summary}")
        
        print("🏁 Market hours ended. Shutting down...")
        
        # Send final daily summary if not already sent
        client.email_manager.send_daily_summary()
            
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        # Print final summary (data already saved in real-time)
        summary = client.get_data_summary()
        print(f"📊 Final Data Summary: {summary}")
        client.disconnect()
    except Exception as e:
        print(f"❌ Error: {e}")
        client.disconnect()
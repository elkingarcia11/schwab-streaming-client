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
            
            if self.debug:
                print(f"📋 Loaded {len(self.equity_symbols_for_options)} symbols for options from GCS: {self.equity_symbols_for_options}")
                print(f"📈 Loaded {len(self.equity_symbols)} equity symbols for chart data from GCS: {self.equity_symbols}")
                print(f"📅 DTE mapping: {self.dte_mapping}")
        else:
            # Default symbols if none provided
            if equity_symbols is None:
                equity_symbols = ['SPY', 'QQQ']  # Common equity symbols
            self.equity_symbols = equity_symbols
            self.equity_symbols_for_options = equity_symbols  # Use same symbols for options
            self.dte_mapping = {symbol: 0 for symbol in equity_symbols}  # Default 2 DTE
        
        if option_symbols is None:
            option_symbols = []  # Will be populated based on equity symbols
        
        self.option_symbols = option_symbols
        
        # Initialize options symbol finder
        self.symbol_finder = OptionsSymbolFinder(self.auth)
        
        # Data storage
        self.option_data = {}  # Store option symbol data for streaming (real-time updates)
        self.chart_option_data = {}  # Store chart option data for equity symbols
        
        # Recording DataFrames - only updated when volume changes
        self.option_recording_data = {}  # Store option data for CSV recording
        self.chart_recording_data = {}  # Store chart data for CSV recording
        
        # 5-minute aggregated data structures
        self.chart_1m_data = {}  # Store 1-minute chart data for aggregation
        self.chart_5m_data = {}  # Store completed 5-minute aggregated bars
        
        # Track previous values for each option symbol to handle partial updates
        self.previous_option_values = {}
        
        # Track previous values for recording condition checks
        self.previous_last_prices = {}
        
        # Track symbol order for CHART_EQUITY data correlation
        self.chart_equity_symbol_order = []
        
        # Initialize indicator calculator
        self.indicator_calculator = IndicatorCalculator()
        self.indicator_periods = self.load_indicator_periods()
        
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
    
    def load_indicator_periods(self) -> Dict:
        """Load indicator periods configuration from indicator_periods.json file"""
        try:
            config_file = 'indicator_periods.json'
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    indicator_periods = json.load(f)
                if self.debug:
                    print(f"📊 Loaded indicator periods configuration for {len(indicator_periods)} symbols")
                    for symbol, timeframes in indicator_periods.items():
                        for timeframe, indicators in timeframes.items():
                            print(f"   {symbol} {timeframe}: {indicators}")
                return indicator_periods
            else:
                if self.debug:
                    print(f"⚠️ Indicator periods file {config_file} not found, skipping indicator calculations")
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
                    indicator_periods=indicator_config
                )
                
                # Add indicator values to chart_data
                for col in enhanced_row.index:
                    if col not in ['timestamp', 'open', 'high', 'low', 'close', 'volume']:
                        chart_data[col] = enhanced_row[col]
                
                if self.debug:
                    indicator_values = {k: v for k, v in chart_data.items() if k not in ['timestamp (ms)', 'symbol', 'sequence', 'chart_day']}
                    print(f"📊 1m indicators for {symbol}: {indicator_values}")
            
            return chart_data
            
        except Exception as e:
            if self.debug:
                print(f"❌ Error calculating 1m indicators for {symbol}: {e}")
            return chart_data
    
    def calculate_5m_indicators(self, symbol: str, aggregated_bar: dict) -> dict:
        """Calculate technical indicators for 5-minute aggregated data"""
        try:
            # Check if we have indicator periods for this symbol and timeframe
            if symbol not in self.indicator_periods or '5m' not in self.indicator_periods[symbol]:
                return aggregated_bar  # Return unchanged if no configuration
            
            indicator_config = self.indicator_periods[symbol]['5m']
            
            # Get existing 5-minute data for this symbol
            if symbol not in self.chart_5m_data:
                self.chart_5m_data[symbol] = []
            
            # Convert existing 5m data to DataFrame
            if len(self.chart_5m_data[symbol]) > 0:
                existing_data = pd.DataFrame(self.chart_5m_data[symbol])
            else:
                existing_data = pd.DataFrame()
            
            # Convert aggregated_bar to pandas Series format
            new_row = pd.Series({
                'timestamp': aggregated_bar['timestamp'],
                'open': aggregated_bar['open'],
                'high': aggregated_bar['high'],
                'low': aggregated_bar['low'],
                'close': aggregated_bar['close'],
                'volume': aggregated_bar['volume']
            })
            
            # Calculate indicators if we have sufficient data
            if len(existing_data) > 0:
                enhanced_row = self.indicator_calculator.calculate_latest_tick_indicators(
                    existing_data=existing_data,
                    new_row=new_row,
                    indicator_periods=indicator_config
                )
                
                # Add indicator values to aggregated_bar
                for col in enhanced_row.index:
                    if col not in ['timestamp', 'open', 'high', 'low', 'close', 'volume']:
                        aggregated_bar[col] = enhanced_row[col]
                
                if self.debug:
                    indicator_values = {k: v for k, v in aggregated_bar.items() if k not in ['timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume']}
                    print(f"📊 5m indicators for {symbol}: {indicator_values}")
            
            return aggregated_bar
            
        except Exception as e:
            if self.debug:
                print(f"❌ Error calculating 5m indicators for {symbol}: {e}")
            return aggregated_bar
    
    def calculate_option_indicators(self, symbol: str, option_data: dict) -> dict:
        """
        Calculate technical indicators for option data using RECORDED history.
        Each period in indicator calculations represents a recorded row (meaningful changes only).
        Only called when last_price has changed to optimize performance.
        """
        try:
            # Extract base symbol for option contracts (e.g., "QQQ" from "QQQ250725P00564000")
            base_symbol = symbol[:3] if len(symbol) > 5 else symbol  # Get first 3 chars for options
            
            # Check if we have indicator periods for this base symbol and timeframe
            if base_symbol not in self.indicator_periods or '1m' not in self.indicator_periods[base_symbol]:
                if self.debug:
                    print(f"⚠️  No indicator config found for {base_symbol}")
                return option_data  # Return unchanged if no configuration
            
            indicator_config = self.indicator_periods[base_symbol]['1m']
            
            # Get existing RECORDED data for this symbol to build history (not streaming data)
            if symbol not in self.option_recording_data or len(self.option_recording_data[symbol]) == 0:
                if self.debug:
                    print(f"⚠️  No recorded data for {symbol} - skipping indicators")
                return option_data  # Not enough recorded data for indicators
            
            total_recorded_count = len(self.option_recording_data[symbol])
            historical_count = total_recorded_count - 1  # Exclude current row from history
            if self.debug:
                print(f"📊 Calculating indicators for {symbol}: {historical_count} historical + 1 current = {total_recorded_count} total")
                print(f"📋 Indicator config: {indicator_config}")
            
            # Convert RECORDED option data history to DataFrame format expected by indicator calculator
            # Use all recorded data EXCEPT the current row (which we'll pass as new_row)
            option_history = []
            for opt_record in self.option_recording_data[symbol][:-1]:  # Exclude the last (current) row
                # Convert option fields to OHLCV-like format for indicator calculation
                price = opt_record.get('last_price', 0)
                option_history.append({
                    'timestamp': pd.Timestamp.now().value // 10**6,  # Current timestamp in ms
                    'open': price,
                    'high': price,  # Options don't have OHLC, use price for all
                    'low': price,
                    'close': price,
                    'last_price': opt_record.get('last_price', 0),  # Key field for options
                    'volume': opt_record.get('total_volume', 0)
                })
            
            existing_data = pd.DataFrame(option_history)  # Historical data without current row
            
            # Convert current option data to pandas Series format
            price = option_data.get('last_price', 0)
            new_row = pd.Series({
                'timestamp': pd.Timestamp.now().value // 10**6,
                'open': price,
                'high': price,
                'low': price,
                'close': price, 
                'last_price': option_data.get('last_price', 0),
                'volume': option_data.get('total_volume', 0)
            })
            
            # Always attempt to calculate indicators - let each indicator decide if it has enough data
            historical_rows = len(existing_data)
            total_available = historical_rows + 1  # existing + new row
            
            if self.debug:
                print(f"🔍 Available data: {historical_rows} historical + 1 current = {total_available} rows for indicator calculation")
            
            # Calculate indicators - each indicator will return empty if insufficient data
            enhanced_row = self.indicator_calculator.calculate_latest_tick_indicators(
                existing_data=existing_data,
                new_row=new_row,
                indicator_periods=indicator_config,
                is_option=True  # Use last_price for calculations
            )
            
            # Add indicator values to option_data (only non-empty values)
            indicators_added = []
            indicators_empty = []
            for col in enhanced_row.index:
                if col not in ['timestamp', 'open', 'high', 'low', 'close', 'last_price', 'volume']:
                    value = enhanced_row[col]
                    if value != "" and value != 0 and not pd.isna(value):  # Only add meaningful values
                        option_data[col] = value
                        indicators_added.append(f"{col}={value}")
                    else:
                        indicators_empty.append(col)
            
            if self.debug:
                if indicators_added:
                    print(f"📊 Added indicators for {symbol}: {', '.join(indicators_added)}")
                if indicators_empty:
                    print(f"⏭️  Empty indicators for {symbol}: {', '.join(indicators_empty)} (insufficient data)")
            
            return option_data
            
        except Exception as e:
            if self.debug:
                print(f"❌ Error calculating option indicators for {symbol}: {e}")
            return option_data
    
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

            if self.debug:
                print(f"📤 Sending LEVELONE_OPTIONS subscription request: {json.dumps(request, indent=2)}")

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

            if self.debug:
                print(f"📤 Sending CHART_EQUITY subscription request: {json.dumps(request, indent=2)}")

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
            if self.debug:
                print(f"❌ Error getting user preferences: {e}")
            raise

    def parse_option_data(self, content_item: dict) -> dict:
        """Parse option data from WebSocket message, merging with previous values"""
        try:
            symbol = content_item.get('key', '')
            if not symbol:
                print(f"⚠️ Warning: Empty symbol in option data: {content_item}")
                return {}
            
            # Get previous values for this symbol, or create default structure
            previous_values = self.previous_option_values.get(symbol, {
                'symbol': content_item.get('0', ''),           # 0: Symbol (Ticker symbol in upper case)
                'description': content_item.get('1', ''),      # 1: Description (Company, index or fund name)
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
                'expiration_year': 0,   # 12: Expiration Year
                'multiplier': 0,        # 13: Multiplier
                'digits': 0,            # 14: Digits (Number of decimal places)
                'open_price': 0,        # 15: Open Price (Day's Open Price)
                'bid_size': 0,          # 16: Bid Size (Number of contracts for bid)
                'ask_size': 0,          # 17: Ask Size (Number of contracts for ask)
                'last_size': 0,         # 18: Last Size (Number of contracts traded with last trade)
                'net_change': 0,        # 19: Net Change (Current Last-Prev Close)
                'strike_price': 0,      # 20: Strike Price (Contract strike price)
                'contract_type': '',    # 21: Contract Type
                'underlying': '',       # 22: Underlying
                'expiration_month': 0,  # 23: Expiration Month
                'deliverables': '',     # 24: Deliverables
                'time_value': 0,        # 25: Time Value
                'expiration_day': 0,    # 26: Expiration Day
                'days_to_expiration': 0, # 27: Days to Expiration
                'delta': 0,             # 28: Delta
                'gamma': 0,             # 29: Gamma
                'theta': 0,             # 30: Theta
                'vega': 0,              # 31: Vega
                'rho': 0,               # 32: Rho
                'security_status': '',  # 33: Security Status
                'theoretical_option_value': 0, # 34: Theoretical Option Value
                'underlying_price': 0,  # 35: Underlying Price
                'uv_expiration_type': '', # 36: UV Expiration Type
                'mark_price': 0,        # 37: Mark Price
                'quote_time': 0,        # 38: Quote Time in Long (milliseconds since Epoch)
                'trade_time': 0,        # 39: Trade Time in Long (milliseconds since Epoch)
                'exchange': '',         # 40: Exchange (Exchange character)
                'exchange_name': '',    # 41: Exchange Name (Display name of exchange)
                'last_trading_day': 0,  # 42: Last Trading Day
                'settlement_type': '',  # 43: Settlement Type (Settlement type character)
                'net_percent_change': 0, # 44: Net Percent Change
                'mark_price_net_change': 0, # 45: Mark Price Net Change
                'mark_price_percent_change': 0, # 46: Mark Price Percent Change
                'implied_yield': 0,     # 47: Implied Yield
                'is_penny_pilot': False, # 48: isPennyPilot
                'option_root': '',      # 49: Option Root
                'week_52_high': 0,      # 50: 52 Week High
                'week_52_low': 0,       # 51: 52 Week Low
                'indicative_ask_price': 0, # 52: Indicative Ask Price
                'indicative_bid_price': 0, # 53: Indicative Bid Price
                'indicative_quote_time': 0, # 54: Indicative Quote Time
                'exercise_type': ''     # 55: Exercise Type
            })
            
            # Create new option data by merging previous values with new updates
            option_data = previous_values.copy()
            
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
            
            # Update fields that are present in the new message
            for field_key, field_name in field_mapping.items():
                if field_key in content_item:
                    option_data[field_name] = content_item[field_key]
            
            # Add symbol to the data for internal use (will be removed before CSV save)
            option_data['_symbol'] = symbol
            
            # Store the updated values for next time
            self.previous_option_values[symbol] = option_data.copy()
            
            return option_data
        except Exception as e:
            print(f"❌ Error parsing option data: {e}")
            return {}

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
            
            if self.debug:
                print(f"📥 Received message: {json.dumps(data, indent=2)}")

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
            
                    if service == 'LEVELONE_OPTIONS':
                        # Handle option data - the actual option data is in the 'content' array
                        content_array = data_item.get('content', [])
                        for content_item in content_array:
                            if self.debug:
                                print(f"🔍 Processing LEVELONE_OPTIONS data: {content_item.get('key', 'NO_KEY')}")
                            
                            # Note: parse_option_data already merges partial updates with previous_option_values
                            # This gives us a complete record with all 56 fields for this update
                            option_data = self.parse_option_data(content_item)
                            if option_data:
                                symbol = option_data['_symbol'] # Get symbol from parsed data
                                
                                # Always update streaming DataFrame (real-time updates with complete data)
                                if symbol not in self.option_data:
                                    self.option_data[symbol] = []
                                
                                # Second level: Fill any remaining empty values with previous streaming data
                                # This ensures streaming DataFrame maintains the most complete state possible
                                if self.option_data[symbol]:  # If we have previous streaming data
                                    previous_streaming_data = self.option_data[symbol][-1]  # Last complete streaming record
                                    complete_option_data = previous_streaming_data.copy()  # Start with previous complete data
                                    
                                    # Update only the fields that have new values (non-zero, non-empty)
                                    for key, value in option_data.items():
                                        if value != 0 and value != '' and value is not None:
                                            complete_option_data[key] = value
                                    
                                    # Ensure we maintain the symbol reference
                                    complete_option_data['_symbol'] = symbol
                                    self.option_data[symbol].append(complete_option_data)
                                else:
                                    # First record - use as is (already complete from parse_option_data)
                                    self.option_data[symbol].append(option_data)
                                
                                # Get the complete streaming data (latest record with all fields filled)
                                complete_streaming_data = self.option_data[symbol][-1]
                                
                                # Get current last_price value (simple field check)
                                current_last_price = complete_streaming_data.get('last_price', 0)  # Key 4: Last Price
                                previous_last_price = self.previous_last_prices.get(symbol, 0)
                                
                                # Check if last_price has changed - this is our recording condition
                                last_price_changed = current_last_price != previous_last_price
                                
                                # Record every time last_price changes
                                if last_price_changed:
                                    # Initialize recording DataFrame if needed
                                    if symbol not in self.option_recording_data:
                                        self.option_recording_data[symbol] = []
                                    
                                    # Add to recording DataFrame first (without indicators)
                                    self.option_recording_data[symbol].append(complete_streaming_data)
                                    
                                    # Now calculate indicators using the recorded history + this new row
                                    complete_streaming_data = self.calculate_option_indicators(symbol, complete_streaming_data)
                                    
                                    # Update the streaming data with indicators
                                    self.option_data[symbol][-1] = complete_streaming_data
                                    
                                    # Update the recorded data with calculated indicators
                                    self.option_recording_data[symbol][-1] = complete_streaming_data
                                    
                                    # Save to CSV with whatever indicators are available (some may be empty)
                                    self.save_option_data_to_csv(symbol, complete_streaming_data)
                                    
                                    if self.debug:
                                        print(f"💾 Last Price changed for {symbol}: {previous_last_price} -> {current_last_price}")
                                        # Show which indicators were calculated
                                        indicators_calculated = [k for k in complete_streaming_data.keys() 
                                                               if k not in ['timestamp', 'symbol', 'last_price', 'mark_price', 'total_volume', '_symbol']
                                                               and complete_streaming_data[k] != "" and complete_streaming_data[k] != 0]
                                        if indicators_calculated:
                                            print(f"📊 Calculated indicators: {indicators_calculated}")
                                
                                # Always update previous last_price for next comparison
                                self.previous_last_prices[symbol] = current_last_price
                                
                                # Debug output when last_price didn't change
                                if self.debug and not last_price_changed:
                                    print(f"⏭️  {symbol}: Last price unchanged ({current_last_price}) - no recording")
                            else:
                                if self.debug:
                                    print(f"⚠️ Failed to parse option data for: {content_item.get('key', 'NO_KEY')}")
                    
                    elif service == 'CHART_EQUITY':
                        # Handle chart equity data - the actual chart data is in the 'content' array
                        content_array = data_item.get('content', [])
                        
                        for content_item in content_array:
                            if self.debug:
                                print(f"🔍 Processing CHART_EQUITY raw data: {content_item}")
                            
                            chart_data = self.parse_chart_equity_data(content_item)
                            if chart_data:
                                symbol = chart_data['symbol']
                                
                                if not symbol:
                                    if self.debug:
                                        print(f"⚠️ Warning: Empty symbol in chart data: {content_item}")
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
                                
                                if self.debug:
                                    print(f"📈 Chart equity data for {symbol}: {chart_data}")
                    else:
                                if self.debug:
                                    print(f"⚠️ Failed to parse chart equity data: {content_item}")

        except Exception as e:
            print(f"❌ Error processing message: {e}")
            if self.debug:
                traceback.print_exc()

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

            if self.debug:
                print(f"🔐 Sending LOGIN request: {json.dumps(login_request, indent=2)}")

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
            
            # Add timestamp as first column
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Create ordered dictionary with timestamp first
            ordered_data = {'timestamp': timestamp}
            ordered_data.update(csv_data)
            
            # Ensure all potential indicator columns are present (even if empty) for consistent headers
            # Group related indicators together in desired column order
            base_symbol = symbol[:3] if len(symbol) > 5 else symbol
            if base_symbol in self.indicator_periods and '1m' in self.indicator_periods[base_symbol]:
                indicator_config = self.indicator_periods[base_symbol]['1m']
                
                # Define ordered indicator groups
                indicator_groups = [
                    # Group 1: Moving Averages (EMA and VWMA together)
                    ['ema', 'vwma'],
                    # Group 2: Rate of Change (ROC and ROC of ROC together)  
                    ['roc', 'roc_of_roc'],
                    # Group 3: MACD (line, signal, histogram together)
                    ['macd_line', 'macd_signal', 'macd_histogram'],
                    # Group 4: Other single indicators
                    ['rsi', 'sma', 'volatility', 'atr'],
                    # Group 5: Stochastic RSI
                    ['stoch_rsi_k', 'stoch_rsi_d'],
                    # Group 6: Bollinger Bands
                    ['bollinger_upper', 'bollinger_lower', 'bollinger_bands_width']
                ]
                
                # Add indicators in the specified order, only if they're configured
                for group in indicator_groups:
                    for indicator in group:
                        # Check if this indicator should exist based on config
                        should_add = False
                        if indicator == 'ema' and 'ema' in indicator_config:
                            should_add = True
                        elif indicator == 'vwma' and 'vwma' in indicator_config:
                            should_add = True
                        elif indicator == 'roc' and 'roc' in indicator_config:
                            should_add = True
                        elif indicator == 'roc_of_roc' and 'roc_of_roc' in indicator_config:
                            should_add = True
                        elif indicator in ['macd_line', 'macd_signal', 'macd_histogram'] and any(k in indicator_config for k in ['macd_fast', 'macd_slow', 'macd_signal']):
                            should_add = True
                        elif indicator == 'rsi' and 'rsi' in indicator_config:
                            should_add = True
                        elif indicator == 'sma' and 'sma' in indicator_config:
                            should_add = True
                        elif indicator == 'volatility' and 'volatility' in indicator_config:
                            should_add = True
                        elif indicator == 'atr' and 'atr' in indicator_config:
                            should_add = True
                        elif indicator in ['stoch_rsi_k', 'stoch_rsi_d'] and any(k in indicator_config for k in ['stoch_rsi_k', 'stoch_rsi_d']):
                            should_add = True
                        elif indicator in ['bollinger_upper', 'bollinger_lower', 'bollinger_bands_width'] and 'bollinger_bands' in indicator_config:
                            should_add = True
                        
                        # Add indicator column if it should exist and isn't already present
                        if should_add and indicator not in ordered_data:
                            ordered_data[indicator] = ""
            
            # Calculate volume delta (current total_volume - previous row total_volume)
            current_total_volume = ordered_data.get('total_volume', 0)
            volume_delta = 0
            
            if os.path.exists(csv_file):
                # Read the last row to get previous total_volume
                try:
                    existing_df = pd.read_csv(csv_file)
                    if not existing_df.empty and 'total_volume' in existing_df.columns:
                        previous_total_volume = existing_df.iloc[-1]['total_volume']
                        volume_delta = current_total_volume - previous_total_volume
                    else:
                        volume_delta = current_total_volume  # First row case
                except Exception as e:
                    volume_delta = current_total_volume  # Fallback to total volume
            else:
                volume_delta = current_total_volume  # Initial row case
            
            ordered_data['volume'] = volume_delta
            
            # Convert data to DataFrame
            df_new = pd.DataFrame([ordered_data])
            
            # Check if file exists
            if os.path.exists(csv_file):
                # Append to existing file
                df_new.to_csv(csv_file, mode='a', header=False, index=False)
            else:
                # Create new file with headers
                df_new.to_csv(csv_file, index=False)
            
            if self.debug:
                print(f"💾 Saved option data for {symbol} to {csv_file} (volume delta: {volume_delta})")
            
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
            
            if self.debug:
                print(f"💾 Saved chart data for {symbol} to {csv_file}")
                
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
            
            if self.debug:
                print(f"💾 Saved 5-minute aggregated data for {symbol} to {csv_file}")
                
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
                    dte = self.dte_mapping.get(symbol, 2)  # Default to 2 DTE
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
            
            # Flatten the option symbols into a single list
            option_symbols = []
            for symbol, option_data in all_option_symbols.items():
                option_symbols.extend(option_data['calls'])
                option_symbols.extend(option_data['puts'])
            
            print(f"✅ Generated {len(option_symbols)} option symbols")
            if self.debug:
                print(f"📋 Option symbols: {option_symbols}")
            
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
                
                if self.debug:
                    print(f"📊 5-minute bar completed for {symbol}: OHLCV({aggregated_bar_with_indicators['open']:.2f}, {aggregated_bar_with_indicators['high']:.2f}, {aggregated_bar_with_indicators['low']:.2f}, {aggregated_bar_with_indicators['close']:.2f}, {aggregated_bar_with_indicators['volume']})")
            
            if self.debug:
                print(f"🔄 5m aggregation for {symbol}: {result['bars_available']}/{result['bars_needed']} bars (ready: {result['ready']})")
                
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
        equity_symbols = ['SPY', 'QQQ']  # Equity symbols for chart option data
        
        client = SchwabStreamingClient(
            debug=True,
            equity_symbols=equity_symbols,
            option_symbols=[]  # Will be populated automatically
        )
    
    # Auto-setup option streaming (uses DTE mapping from GCS if available)
    client.auto_setup_option_streaming()
    
    try:
        # Check if it's before market hours and wait until 9:30 AM if needed
        if not client.wait_for_market_open():
            print("❌ Market is closed (weekend). Exiting...")
            exit(0)
        
        # Connect to streaming
        print("🔌 Connecting to Schwab Streaming API...")
        client.connect()
        
        # Keep the script running during market hours
        print("🔄 Starting market hours monitoring loop...")
        last_save_time = time.time()
        
        while client.is_market_open():
            time.sleep(1)
            
            # Check connection status
            if not client.connected:
                print("⚠️ Connection lost. Attempting to reconnect...")
                client.connect()
                continue
            
            # Print data summary every 5 minutes (data is saved in real-time)
            if time.time() - last_save_time >= 300:  # 5 minutes
                print("📊 Periodic status check...")
                last_save_time = time.time()
                
                # Print data summary
                summary = client.get_data_summary()
                print(f"📊 Data Summary: {summary}")
        
        print("🏁 Market hours ended. Shutting down...")
            
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        # Print final summary (data already saved in real-time)
        summary = client.get_data_summary()
        print(f"📊 Final Data Summary: {summary}")
        client.disconnect()
    except Exception as e:
        print(f"❌ Error: {e}")
        client.disconnect()
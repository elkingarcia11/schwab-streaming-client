# Schwab Streaming Client

A Python client for streaming data from Charles Schwab's WebSocket API. This client is designed to collect option symbol data and chart option data for regular equity symbols.

## 🎯 **Use Cases**

- **Option Symbol Data Streaming**: Real-time data for specific option contracts
- **Chart Option Data**: OHLCV data for options related to equity symbols
- **Data Collection**: Simple, focused data collection without complex processing

## ✨ **Features**

- 🔐 **OAuth 2.0 Authentication**: Uses the Charles Schwab authentication module
- 📊 **Option Data Streaming**: Real-time option contract data (bid, ask, volume, Greeks, etc.)
- 📈 **Chart Option Data**: OHLCV data for options related to equity symbols
- 🔍 **Automatic Option Symbol Generation**: Automatically finds and generates option symbols for streaming
- ☁️ **GCS Symbol Management**: Load symbols and DTE from Google Cloud Storage
- 💾 **Data Persistence**: Save collected data to CSV files
- ⏰ **Market Hours**: Respects market hours (9:30 AM - 4:00 PM ET)
- 🐛 **Debug Mode**: Optional detailed logging for development
- 📋 **Data Summary**: Get counts and summaries of collected data

## 🚀 **Quick Start**

### Prerequisites

- Python 3.6+
- Charles Schwab Developer Account
- Schwab API credentials
- Required Python packages (see requirements.txt)

### Installation

1. **Clone the repository with submodules:**

   ```bash
   git clone --recursive <your-repo-url>
   cd schwab-streaming-client
   ```

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables:**
   Create a `.env` file with your Schwab API credentials and GCS bucket:

   ```env
   SCHWAB_APP_KEY=your_app_key_here
   SCHWAB_APP_SECRET=your_app_secret_here
   GCS_BUCKET_NAME=your_gcs_bucket_name (optional)
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json (optional)
   ```

4. **Run the client:**
   ```bash
   python schwab-streaming-client.py
   ```

## 📊 **Data Types**

### Option Symbol Data

Streams real-time data for specific option contracts:

```python
option_data = {
    'symbol': 'SPY240315C00500000',
    'timestamp': 1703123456789,
    'bid': 2.50,
    'ask': 2.55,
    'last': 2.52,
    'volume': 1500,
    'open_interest': 50000,
    'implied_volatility': 0.25,
    'delta': 0.65,
    'gamma': 0.02,
    'theta': -0.15,
    'vega': 0.08
}
```

### Chart Option Data

OHLCV data for options related to equity symbols:

```python
chart_option_data = {
    'symbol': 'SPY',
    'timestamp': 1703123456789,
    'open': 450.25,
    'high': 451.50,
    'low': 449.75,
    'close': 450.80,
    'volume': 2500,
    'sequence': 12345
}
```

## 🔧 **Configuration**

### Client Initialization

```python
from schwab-streaming-client import SchwabStreamingClient

# Create client with GCS bucket (loads symbols from GCS)
client = SchwabStreamingClient(
    debug=True,  # Enable debug logging
    gcs_bucket='your-gcs-bucket-name',
    symbols_file='option_symbols.txt'  # File in GCS bucket
)

# Create client with custom symbols
client = SchwabStreamingClient(
    debug=True,  # Enable debug logging
    equity_symbols=['SPY', 'QQQ', 'IWM'],  # For chart option data
    option_symbols=['SPY240315C00500000', 'SPY240315P00500000']  # Specific options
)
```

### GCS Symbol Management

The client can load symbols and DTE (days to expiration) from a file in Google Cloud Storage:

**File Format** (`option_symbols.txt`):

```
SPY,2
QQQ,3
AAPL,5
TSLA,3
```

**Environment Variable** (in `.env` file):

```env
GCS_BUCKET_NAME=your-gcs-bucket-name
```

### Default Symbols

If no GCS bucket is provided, the client uses these defaults:

- **Equity Symbols**: `['SPY', 'QQQ', 'IWM']`
- **Option Symbols**: `[]` (empty list)
- **Default DTE**: 2 days for all symbols

## 📁 **File Structure**

```
schwab-streaming-client/
├── schwab-streaming-client.py          # Main streaming client
├── requirements.txt                    # Python dependencies
├── README.md                          # This file
├── .env                               # Environment variables (create this)
├── data/                              # Data storage directory (auto-created)
│   ├── option_data_SPY240315C00500000_20240101_143022.csv
│   └── chart_option_data_SPY_20240101_143022.csv
└── options-symbol-finder/             # Options symbol finder submodule
    ├── options-symbol-finder.py       # Options symbol finder class
    └── charles-schwab-authentication-module/  # Authentication submodule
        ├── schwab_auth.py             # Authentication class
        └── gcs-python-module/         # Google Cloud Storage module
```

## 🎮 **Usage Examples**

### Basic Usage

```python
# Simple usage with default symbols
client = SchwabStreamingClient(debug=True)

# Wait for market open and connect
if client.wait_for_market_open():
    client.connect()

    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.save_data_to_csv()
        summary = client.get_data_summary()
        print(f"Data Summary: {summary}")
        client.disconnect()
```

### Automatic Option Symbol Generation

```python
# Let the client automatically generate option symbols
equity_symbols = ['AAPL', 'TSLA', 'NVDA']

client = SchwabStreamingClient(
    debug=True,
    equity_symbols=equity_symbols,
    option_symbols=[]  # Will be auto-generated
)

# Auto-setup generates option symbols for 2 days to expiration
client.auto_setup_option_streaming(days_to_expiration=2)
```

### Manual Option Symbols

```python
# Custom equity and option symbols
equity_symbols = ['AAPL', 'TSLA', 'NVDA']
option_symbols = [
    'AAPL240315C00180000',  # AAPL March 15, 2024 $180 Call
    'TSLA240315P00200000',  # TSLA March 15, 2024 $200 Put
    'NVDA240315C00500000'   # NVDA March 15, 2024 $500 Call
]

client = SchwabStreamingClient(
    debug=True,
    equity_symbols=equity_symbols,
    option_symbols=option_symbols
)
```

### Data Management

```python
# Save data to CSV files
client.save_data_to_csv()

# Get data summary
summary = client.get_data_summary()
print(f"Option data points: {summary['option_symbols']}")
print(f"Chart option data points: {summary['chart_option_symbols']}")

# Access raw data
option_data = client.option_data
chart_option_data = client.chart_option_data

# Generate option symbols manually
option_symbols = client.generate_option_symbols(days_to_expiration=5)
print(f"Generated {len(option_symbols)} option symbols")
```

## 🔌 **WebSocket Services**

The client subscribes to two Schwab WebSocket services:

### 1. OPTION Service

- **Purpose**: Real-time option contract data
- **Data**: Bid, ask, last, volume, open interest, Greeks, implied volatility
- **Symbols**: Specific option contract symbols (e.g., 'SPY240315C00500000')

### 2. CHART_OPTION Service

- **Purpose**: OHLCV data for options related to equity symbols
- **Data**: Open, high, low, close, volume, timestamp
- **Symbols**: Equity symbols (e.g., 'SPY', 'QQQ')

## 🔍 **Options Symbol Finder**

The client integrates with the Options Symbol Finder module to automatically generate option symbols:

### Features

- **Expiration Chain Discovery**: Finds available expiration dates for any symbol
- **Strike Price Selection**: Automatically selects nearest strikes plus 3 above and 3 below
- **Multiple Symbol Support**: Processes multiple equity symbols simultaneously
- **Days to Expiration Filtering**: Filters options by minimum days to expiration

### Usage

```python
# Auto-generate option symbols for 2 days to expiration
client.auto_setup_option_streaming(days_to_expiration=2)

# Manually generate option symbols
option_symbols = client.generate_option_symbols(days_to_expiration=5)
```

## ⏰ **Market Hours**

The client respects market hours:

- **Market Open**: 9:30 AM ET
- **Market Close**: 4:00 PM ET
- **Weekends**: Automatically detects and waits for next trading day

## 🐛 **Debug Mode**

Enable debug mode for detailed logging:

```python
client = SchwabStreamingClient(debug=True)
```

Debug output includes:

- WebSocket connection status
- Subscription requests
- Incoming data messages
- Data parsing details
- Error messages with stack traces

## 📊 **Data Storage**

### In-Memory Storage

Data is stored in memory during streaming:

- `client.option_data`: Dictionary of option symbol data
- `client.chart_option_data`: Dictionary of chart option data

### CSV Export

Data can be saved to timestamped CSV files:

```python
client.save_data_to_csv()
# Creates files like:
# - data/option_data_SPY240315C00500000_20240101_143022.csv
# - data/chart_option_data_SPY_20240101_143022.csv
```

## 🔐 **Authentication**

The client uses the Charles Schwab authentication module (included as a submodule) for OAuth 2.0 authentication. The authentication module:

- Handles token refresh automatically
- Supports Google Cloud Storage for token backup
- Provides fresh tokens on each run
- Manages all authentication complexity

## 🛠 **Dependencies**

### Core Dependencies

- `websocket-client`: WebSocket connections
- `httpx`: HTTP requests for API calls
- `pandas`: Data manipulation and CSV export
- `pytz`: Timezone handling

### Authentication Dependencies

- `requests`: HTTP requests for authentication
- `python-dotenv`: Environment variable management
- `google-cloud-storage`: Cloud storage integration (optional)

## 🚨 **Error Handling**

The client includes comprehensive error handling for:

- WebSocket connection failures
- Authentication errors
- Data parsing errors
- Market hours validation
- File I/O errors

## 🔄 **Reconnection**

The client automatically handles:

- Connection timeouts
- WebSocket disconnections
- Authentication token refresh
- Market hours validation

## 📝 **Logging**

The client provides detailed logging with emojis for easy identification:

- 🔌 Connection events
- 📤 Subscription requests
- 📥 Data reception
- 📊 Data processing
- ❌ Error messages
- ✅ Success confirmations

## 🤝 **Contributing**

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 **License**

This project is licensed under the MIT License.

## ⚠️ **Disclaimer**

This tool is for educational and development purposes. Always follow Charles Schwab's API terms of service and rate limiting guidelines. The data collected should be used responsibly and in compliance with applicable regulations.

## 🆘 **Support**

For issues related to:

- **Schwab API**: Contact Charles Schwab Developer Support
- **Authentication**: Check the authentication module documentation
- **This Tool**: Open an issue in this repository

## 📈 **Future Enhancements**

Potential future features:

- Real-time data visualization
- Option chain analysis
- Greeks calculations
- Risk metrics
- Historical data integration
- Multiple exchange support

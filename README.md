# Schwab Streaming Client with Technical Indicators

A comprehensive Python client for streaming market data from Charles Schwab's WebSocket API with integrated technical analysis capabilities. This client streams option and equity data while calculating real-time technical indicators.

## 🎯 **Use Cases**

- **Option Symbol Data Streaming**: Real-time data for specific option contracts with volume-based recording
- **Equity Chart Data Streaming**: 1-minute OHLCV data for equity symbols
- **Technical Analysis**: Real-time calculation of indicators (RSI, MACD, EMA, Bollinger Bands, etc.)
- **Multi-Timeframe Analysis**: 1-minute streaming data aggregated to 5-minute bars with indicators
- **Data Collection & Analytics**: Advanced data storage with calculated technical indicators

## ✨ **Features**

### 🔐 **Authentication & Connectivity**

- OAuth 2.0 Authentication via Charles Schwab authentication module
- WebSocket connection management with automatic reconnection
- Market hours awareness (9:30 AM - 4:00 PM ET)

### 📊 **Data Streaming**

- **Option Data**: Real-time streaming of 56+ option fields (bid, ask, volume, Greeks, etc.)
- **Equity Chart Data**: 1-minute OHLCV streaming for equity symbols
- **Dual DataFrame System**: Separate streaming (all updates) and recording (filtered) data
- **Volume-Based Recording**: Options data only recorded when total volume changes

### 📈 **Technical Analysis Integration**

- **Real-time Indicators**: Calculate technical indicators on streaming 1-minute data
- **5-Minute Aggregation**: Automatic conversion from 1m to 5m bars with indicators
- **Symbol-Specific Configuration**: Customizable indicator periods via `indicator_periods.json`
- **Comprehensive Indicator Suite**:
  - **Momentum**: RSI, Stochastic RSI, ROC, ROC of ROC
  - **Trend**: MACD (line, signal, histogram), EMA, SMA, VWMA
  - **Volatility**: Bollinger Bands, ATR, Volatility, Price Change

### 🔍 **Symbol Management**

- **Automatic Option Symbol Generation**: Dynamic option symbol creation with DTE configuration
- **GCS Integration**: Load symbols and DTE mappings from Google Cloud Storage
- **Configurable Expiration**: Customizable Days To Expiration (DTE) per symbol

### 💾 **Enhanced Data Storage**

- **CSV Output with Indicators**: All data includes calculated technical indicators
- **Structured Directories**:
  - `data/options/{symbol}.csv` - Option data with timestamps and volume deltas
  - `data/equity/{symbol}.csv` - 1-minute equity data with indicators
  - `data/equity/5m/{symbol}.csv` - 5-minute aggregated bars with indicators
- **Timestamps**: All records include precise timestamp information

### 🎛️ **Configuration & Control**

- **Indicator Periods Configuration**: `indicator_periods.json` for symbol/timeframe-specific settings
- **Debug Mode**: Comprehensive logging for development and troubleshooting
- **Data Summaries**: Real-time statistics on streaming, recording, and aggregated data

## 🏗️ **Architecture**

### **Module Integration**

```
schwab-streaming-client/
├── indicator-calculator/           # Technical indicators module
│   ├── indicator_calculator.py     # RSI, MACD, Bollinger Bands, etc.
│   └── requirements.txt
├── market-data-aggregator/         # Timeframe conversion
│   ├── main.py                     # 1m → 5m aggregation
│   └── requirements.txt
├── options-symbol-finder/          # Option symbol generation
│   ├── options-symbol-finder.py    # Symbol finding logic
│   └── charles-schwab-authentication-module/
├── schwab-streaming-client.py      # Main streaming application
├── indicator_periods.json          # Indicator configuration
└── data/                          # Output directory structure
```

### **Data Flow**

1. **WebSocket Connection**: Authenticate and connect to Schwab API
2. **Symbol Generation**: Create option symbols based on equity symbols + DTE
3. **Streaming**: Receive real-time option and equity data
4. **Indicator Calculation**: Calculate technical indicators per configuration
5. **Data Storage**: Save enhanced data with indicators to CSV files
6. **Aggregation**: Convert 1m equity data to 5m bars with indicators

## 🚀 **Quick Start**

### Prerequisites

- Python 3.8+
- Charles Schwab Developer Account
- Schwab API credentials
- Git (for submodule management)

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
   GCS_BUCKET_NAME=your_gcs_bucket_name
   GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
   ```

4. **Configure indicators:**
   Create or modify `indicator_periods.json`:

   ```json
   {
     "SPY": {
       "1m": {
         "ema": 12,
         "macd_fast": 12,
         "macd_slow": 26,
         "macd_signal": 9,
         "rsi": 14
       },
       "5m": {
         "ema": 20,
         "macd_fast": 12,
         "macd_slow": 26,
         "macd_signal": 9
       }
     }
   }
   ```

5. **Run the client:**
   ```bash
   python schwab-streaming-client.py
   ```

## ⚙️ **Configuration**

### **Indicator Periods Configuration**

The `indicator_periods.json` file defines which indicators to calculate for each symbol and timeframe:

```json
{
  "QQQ": {
    "1m": {
      "ema": 12,
      "vwma": 11,
      "roc": 13,
      "roc_of_roc": 11,
      "macd_fast": 22,
      "macd_slow": 35,
      "macd_signal": 28
    },
    "5m": {
      "ema": 6,
      "vwma": 11,
      "roc": 19,
      "roc_of_roc": 16,
      "macd_fast": 22,
      "macd_slow": 36,
      "macd_signal": 30
    }
  }
}
```

### **Available Indicators**

| Indicator       | Key                                     | Description                               |
| --------------- | --------------------------------------- | ----------------------------------------- |
| RSI             | `rsi`                                   | Relative Strength Index                   |
| EMA             | `ema`                                   | Exponential Moving Average                |
| SMA             | `sma`                                   | Simple Moving Average                     |
| VWMA            | `vwma`                                  | Volume Weighted Moving Average            |
| MACD            | `macd_fast`, `macd_slow`, `macd_signal` | Moving Average Convergence Divergence     |
| Bollinger Bands | `bollinger_bands`                       | Upper/lower bands with standard deviation |
| ROC             | `roc`                                   | Rate of Change                            |
| ROC of ROC      | `roc_of_roc`                            | Second derivative of price change         |
| Stochastic RSI  | `stoch_rsi_k`, `stoch_rsi_d`            | Stochastic oscillator applied to RSI      |
| ATR             | `atr`                                   | Average True Range                        |
| Volatility      | `volatility`                            | Rolling standard deviation                |
| Price Change    | `price_change`                          | Percentage change                         |

### **Client Initialization**

```python
from schwab_streaming_client import SchwabStreamingClient

# With GCS bucket (recommended)
client = SchwabStreamingClient(
    debug=True,
    gcs_bucket='your-gcs-bucket-name',
    option_symbols_file='option_symbols.txt',
    equity_symbols_file='equity_symbols.txt'
)

# With local symbols
client = SchwabStreamingClient(
    debug=True,
    equity_symbols=['SPY', 'QQQ'],
    option_symbols=[]  # Auto-generated
)
```

## 📊 **Data Output**

### **1-Minute Equity Data with Indicators**

`data/equity/SPY.csv`:

```csv
timestamp,symbol,sequence,open,high,low,close,volume,chart_day,ema,macd_line,macd_signal,roc
1703123456789,SPY,12345,450.25,451.50,449.75,450.80,2500,20240101,450.45,0.15,0.12,0.03,0.25
```

### **5-Minute Aggregated Data with Indicators**

`data/equity/5m/SPY.csv`:

```csv
timestamp,datetime,open,high,low,close,volume,ema,macd_line,macd_signal,roc
1703120400000,2024-01-01 10:00:00 EDT,450.00,451.75,449.50,451.25,12500,450.85,0.35,0.28,0.07,0.45
```

### **Option Data with Volume Deltas**

`data/options/SPY240315C00450000.csv`:

```csv
timestamp,symbol,bid,ask,last,volume,delta,gamma,theta,vega,implied_volatility
1703123456789,SPY240315C00450000,2.50,2.55,2.52,150,0.65,0.02,-0.15,0.08,0.25
```

## 📈 **Usage Examples**

### **Real-time Streaming with Indicators**

```python
# Configure indicators and start streaming
client = SchwabStreamingClient(debug=True, gcs_bucket='your-bucket')

# Auto-setup with option symbol generation
client.auto_setup_option_streaming()

# Stream data with real-time indicator calculations
if client.wait_for_market_open():
    client.connect()

    try:
        while True:
            time.sleep(60)  # Save data every minute
            client.save_data_to_csv()

            # Get data summary with indicators
            summary = client.get_data_summary()
            print(f"📊 Data Summary: {summary}")

    except KeyboardInterrupt:
        client.disconnect()
```

### **Custom Indicator Configuration**

```python
# Create custom indicator periods
custom_periods = {
    "AAPL": {
        "1m": {
            "rsi": 21,
            "ema": 9,
            "macd_fast": 12,
            "macd_slow": 26,
            "macd_signal": 9
        },
        "5m": {
            "bollinger_bands": 20,
            "atr": 14,
            "volatility": 20
        }
    }
}

# Save to indicator_periods.json
import json
with open('indicator_periods.json', 'w') as f:
    json.dump(custom_periods, f, indent=2)
```

## 🔧 **Advanced Features**

### **Market Data Aggregation**

- Automatic 1-minute to 5-minute bar conversion
- Configurable timeframe aggregation via `market-data-aggregator` module
- Preservation of OHLCV integrity during aggregation

### **Volume-Based Recording**

- Option data only recorded when `total_volume` changes
- Reduces noise and focuses on actual trading activity
- Separate streaming vs. recording DataFrames

### **Symbol Management**

- GCS integration for dynamic symbol loading
- Automatic option symbol generation with DTE filtering
- Configurable expiration dates per equity symbol

### **Error Handling & Resilience**

- Automatic WebSocket reconnection
- Graceful handling of missing indicator data
- Market hours validation and waiting

## 🛠️ **Dependencies**

### **Core Modules**

- `websocket-client`: WebSocket connectivity
- `httpx`: HTTP API requests
- `pandas`: Data manipulation and analysis
- `pytz`: Timezone handling

### **Integrated Submodules**

- `indicator-calculator`: Technical analysis calculations
- `market-data-aggregator`: Timeframe conversion
- `options-symbol-finder`: Option symbol generation
- `charles-schwab-authentication-module`: OAuth 2.0 authentication

## 📝 **Logging & Debugging**

Enable debug mode for comprehensive logging:

```python
client = SchwabStreamingClient(debug=True)
```

Debug output includes:

- 🔌 WebSocket connection events
- 📊 Indicator calculations
- 📈 Data aggregation status
- 📤 Subscription confirmations
- ❌ Error details with stack traces

## ⚠️ **Important Notes**

### **Market Hours**

- Automatic market hours detection (9:30 AM - 4:00 PM ET)
- Weekend detection and waiting for next trading day
- Graceful handling of market closures

### **Rate Limiting**

- Respects Schwab API rate limits
- Automatic connection management
- Single connection per user enforcement

### **Data Integrity**

- All timestamps in milliseconds since epoch
- Volume deltas calculated for option data
- OHLCV data validated during aggregation

## 🤝 **Contributing**

1. Fork the repository
2. Create a feature branch
3. Update submodules if needed: `git submodule update --recursive`
4. Make your changes with comprehensive testing
5. Submit a pull request

## 📄 **License**

This project is licensed under the MIT License.

## ⚠️ **Disclaimer**

This tool is for educational and development purposes. Always follow Charles Schwab's API terms of service and rate limiting guidelines. The calculated indicators are for informational purposes only and should not be considered financial advice.

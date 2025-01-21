# Cryptocurrency Trading Project

This project focuses on building a data-driven framework for cryptocurrency trading. The primary components of the project include data fetching, storage, signal computation, and analysis. Below is an overview of the key elements and their functions:

## Key Components

### 1. **Data Fetching**
- Utilizes the `ccxt` library to fetch OHLCV (Open, High, Low, Close, Volume) data from the Binance exchange.
- Data is fetched for the `BTC/USDT` trading pair with a 1-day timeframe and a limit of 200 candlesticks.
- The data is converted into a Pandas DataFrame and sorted by timestamp.

### 2. **Data Storage**
- The fetched data is stored in an SQLite database (`crypto_btc.db`) under the table `btc_prices`.
- A mechanism is implemented to avoid storing duplicate records by checking the latest timestamp in the database.
- The database structure includes columns for `timestamp`, `open`, `high`, `low`, `close`, and `volume`.

### 3. **Signal Computation**
- Reads the stored data and computes technical indicators using the `pandas_ta` library:
  - **EMA (Exponential Moving Average)**
  - **RSI (Relative Strength Index)**
  - **VWAP (Volume-Weighted Average Price)**
  - **Bollinger Bands**
- A confluence approach is used to generate trading signals:
  - **Buy Signal (1):** Generated when bullish conditions are met (e.g., price above EMA and VWAP, RSI between 50 and 70).
  - **Sell Signal (-1):** Generated when bearish conditions are met (e.g., price below EMA and VWAP, RSI below 50).
  - **Hold Signal (0):** Default when no conditions are met.

### 4. **Analysis with CryptoAnalyzer**
- A custom class `CryptoAnalyzer` is provided for further analysis of trading signals.
- The class counts and categorizes the number of buy, sell, and hold signals.
- Provides a quick summary of market trends based on computed signals.

## Main Workflow

1. Fetch data from Binance using the `fetch_data` function.
2. Store the fetched data into an SQLite database using the `store_data_in_sql` function.
3. Compute trading signals using the `compute_signals` function.
4. Analyze the computed signals with the `CryptoAnalyzer` class.

## Technologies Used

- **Python**: Core programming language.
- **ccxt**: Library for accessing cryptocurrency exchange APIs.
- **pandas**: Data manipulation and analysis.
- **pandas_ta**: Technical analysis indicators.
- **SQLite**: Lightweight database for data storage.

## Future Enhancements

- Support for multiple trading pairs and timeframes.
- Integration with a live trading system for automated execution of signals.
- Advanced signal computation using machine learning models.
- Enhanced reporting and visualization of trading signals and performance.

---

This project is a foundational step toward creating a systematic and automated trading system for cryptocurrency markets.

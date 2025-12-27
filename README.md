# Volume Spike Ranking System

A system that detects volume spike stocks in Shanghai A-shares, STAR Market (科创板), and New Third Board (新三板, including Beijing Stock Exchange) and generates rankings.

## Module Structure

This system consists of the following 6 modules:

### 1. `config.py` - Configuration and Constants
- **Role**: Defines configuration values and constants used throughout the system
- **Main Contents**:
  - Directory settings (`DATA_DIR`, `OUTPUT_DIR`)
  - Analysis parameters (`MA_WINDOW`, `VOL_MULTIPLE`, `MIN_PCT_CHG`, `MIN_AMOUNT`, `Z_THRESHOLD`, `STD_FLOOR`)
  - Date settings (`TODAY`, `DEFAULT_START`)
  - File paths (`STOCK_LIST_PATH`)

### 2. `stock_list.py` - Stock List Acquisition
- **Role**: Stock list acquisition and management
- **Main Functions**:
  - `get_stock_list()`: Stock list acquisition and management (uses saved list if available, otherwise fetches and saves)
  - `get_stocks_from_api()`: Fetches stock list from API for Shanghai A-shares, STAR Market, and New Third Board (including Beijing Stock Exchange)
  - `get_xsb_stocks_only()`: Fetches only New Third Board (including Beijing Stock Exchange) stock list

### 3. `data_loader.py` - Data Acquisition and Download
- **Role**: Individual stock data acquisition and download
- **Main Functions**:
  - `load_or_download(code, latest_trading_date=None)`: Main data acquisition function
    - Downloads and appends missing data if existing CSV is available
    - Returns existing data on network errors
    - Attempts to fetch a wider date range if existing data is old (2+ days)
  - `get_latest_trading_date(stocks)`: Checks the latest trading date using a sample stock
- **Internal Helper Functions**:
  - `_load_existing_data()`: Loads existing data and normalizes dates
  - `_check_if_update_needed()`: Checks if update is needed
  - `_determine_start_date()`: Determines start date
  - `_fetch_stock_data()`: Fetches data from akshare
  - `_merge_and_save_data()`: Merges and saves data
  - `_handle_date_type_error()`: Handles date type errors

### 4. `volume_analyzer.py` - Volume Spike Detection
- **Role**: Volume spike detection and analysis
- **Main Functions**:
  - `detect_volume_spike(df, target_date_str=None)`: Detects volume spikes
    - Supports both ratio method (≥`VOL_MULTIPLE` times) and Z-score method (≥`Z_THRESHOLD`)
    - Calculates ranking using data from specified date (if `target_date_str` is provided)
    - Common filters: price change ≥3%, <9.5%, trading amount ≥100 million
  - `calc_forward_return(df, days)`: Calculates forward return from latest date to `days` days later

### 5. `output.py` - Output and Display
- **Role**: Result display and CSV saving
- **Main Functions**:
  - `print_data_acquisition_summary()`: Displays data acquisition summary
  - `save_failed_stocks()`: Displays and saves failed stock information to CSV
  - `save_old_data_stocks()`: Displays and saves old data (2+ days old) stock information to CSV
  - `retry_failed_stocks()`: Retries failed stocks (with user input)
  - `save_ranking_results()`: Saves and displays ranking results

### 6. `main.py` - Main Processing
- **Role**: Integrates all modules and executes main processing
- **Main Processing**:
  1. Stock list acquisition
  2. Latest trading date confirmation
  3. Data acquisition and volume spike detection for each stock
  4. Statistics collection (including processing statistics by stock type)
  5. Result display and saving

## How to Run

### Basic Execution

```bash
python main.py
```

### Execution Flow

1. **Stock List Acquisition**
   - Loads saved stock list if available
   - Otherwise fetches from API and saves
   - Updates with latest list in background

2. **Latest Trading Date Confirmation**
   - Checks latest trading date using a sample stock
   - Uses latest trading date if today is not a trading day

3. **Data Acquisition and Analysis**
   - For each stock:
     - Downloads missing data if existing data is available
     - Detects volume spikes using data from latest trading date
     - Adds stocks meeting conditions to ranking

4. **Result Display and Saving**
   - Displays data acquisition summary
   - Displays and saves list of failed stocks
   - Displays and saves list of stocks with old data
   - Displays and saves ranking results to CSV

### Output Files

- `data/daily/stock_list_sh_xsb.csv`: Stock list
- `data/daily/{code}.csv`: Daily data for each stock
- `output/volume_spike_ratio_rank.csv`: Ranking by ratio method
- `output/volume_spike_z_rank.csv`: Ranking by Z-score method
- `output/failed_stocks.csv`: List of stocks that failed data acquisition
- `output/old_data_stocks.csv`: List of stocks with old data (2+ days old)
- `output/retry_failed_stocks.csv`: List of stocks that failed after retry (if retry was executed)

## Configuration Parameters

The following parameters can be adjusted in `config.py`:

- `MA_WINDOW = 20`: Moving average period (days)
- `VOL_MULTIPLE = 2.0`: Volume ratio threshold (2x average or more)
- `MIN_PCT_CHG = 3.0`: Minimum price change percentage (%)
- `MIN_AMOUNT = 1e8`: Minimum trading amount (100 million)
- `Z_THRESHOLD = 2.5`: Z-score threshold
- `STD_FLOOR = 1e-6`: Standard deviation floor value (to prevent zero variance)

## Supported Stocks

- **Shanghai A-shares**: Codes starting with 60 (e.g., 600000, 600001)
- **STAR Market (科创板)**: Codes starting with 68 (e.g., 688001, 688002)
- **New Third Board (新三板, including Beijing Stock Exchange)**:
  - Codes starting with 43 (Basic Layer)
  - Codes starting with 83 (Innovation Layer)
  - Codes starting with 87 (Select Layer)
  - Codes starting with 8 (Beijing Stock Exchange)
  - Codes starting with 9 (Beijing Stock Exchange)

## Notes

- Initial execution takes time as it downloads data for all stocks
- Uses existing data if network errors occur
- Stocks without data for the latest trading date are not included in rankings
- Failed stocks can be retried using the retry option

## Dependencies

- `akshare`: Chinese stock data acquisition
- `pandas`: Data processing
- `tqdm`: Progress bar display

## License

This project is intended for personal use.

import struct
import os
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import pytz
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()

# Define the bar formats (matching the output of parse_book_tops.py and parse_book_fills.py)
BAR_FORMAT_TOPS = "<Qdddd"
BAR_SIZE_TOPS = struct.calcsize(BAR_FORMAT_TOPS)

BAR_FORMAT_FILLS = "<Qddddi"
BAR_SIZE_FILLS = struct.calcsize(BAR_FORMAT_FILLS)

# Define approximate seconds per trading period
SECONDS_PER_TRADING_DAY = 6.5 * 60 * 60 # Approx 23400
SECONDS_PER_TRADING_WEEK = 5 * SECONDS_PER_TRADING_DAY # Approx 117000

ET = pytz.timezone("US/Eastern")

def read_bar_data(file_path, bar_format, bar_size, columns):
    """Reads binary bar data from a file into a pandas DataFrame."""
    timestamps = []
    data_rows = []

    if not os.path.exists(file_path):
        print(f"Warning: File not found {file_path}")
        return pd.DataFrame(columns=['timestamp'] + columns).set_index('timestamp')

    try:
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(bar_size)
                if not chunk:
                    break
                if len(chunk) < bar_size:
                    print(f"Warning: Incomplete bar data found in {file_path}")
                    break

                unpacked_data = struct.unpack(bar_format, chunk)
                # Convert Unix timestamp (seconds) to datetime localized to ET
                ts = datetime.fromtimestamp(unpacked_data[0], ET)
                timestamps.append(ts)
                data_rows.append(unpacked_data[1:]) # Exclude timestamp

        df = pd.DataFrame(data_rows, columns=columns)
        df['timestamp'] = timestamps
        df = df.set_index('timestamp')
        # Ensure data types are appropriate (floats for prices, int for volume)
        for col in df.columns:
            if col != 'volume': # Assuming volume is the only integer column
                 df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                 df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64') # Use nullable Int
        return df

    except Exception as e:
        print(f"Error reading bar file {file_path}: {e}")
        return pd.DataFrame(columns=['timestamp'] + columns).set_index('timestamp')

def load_and_merge_data(date, feed, symbol):
    """Loads fills and L1, L2, L3 tops bar data and merges them for a specific date."""
    base_path = f"/home/vir/{date}/{feed.lower()}/bars/{feed.upper()}." # Adjust if your path differs
    print(f"--- Loading data for {date} ---")

    # Define file paths for fills and all levels of tops
    fills_file = f"{base_path}fills_bars.{symbol.upper()}.bin"
    bid_l1_file = f"{base_path}bid_bars_L1.{symbol.upper()}.bin"
    ask_l1_file = f"{base_path}ask_bars_L1.{symbol.upper()}.bin"
    bid_l2_file = f"{base_path}bid_bars_L2.{symbol.upper()}.bin"
    ask_l2_file = f"{base_path}ask_bars_L2.{symbol.upper()}.bin"
    bid_l3_file = f"{base_path}bid_bars_L3.{symbol.upper()}.bin"
    ask_l3_file = f"{base_path}ask_bars_L3.{symbol.upper()}.bin"

    # Define columns based on BAR_FORMATs (excluding timestamp)
    # Fills: high, low, open, close, volume
    cols_fills = ['fills_high', 'fills_low', 'fills_open', 'fills_close', 'volume']
    # Tops: open, high, low, close
    cols_tops = ['tops_open', 'tops_high', 'tops_low', 'tops_close']

    # Read data
    print(f"Reading fills data from: {fills_file}")
    df_fills = read_bar_data(fills_file, BAR_FORMAT_FILLS, BAR_SIZE_FILLS, cols_fills)

    print(f"Reading L1 bid data from: {bid_l1_file}")
    df_bid_l1 = read_bar_data(bid_l1_file, BAR_FORMAT_TOPS, BAR_SIZE_TOPS, cols_tops).add_prefix('bid_L1_')
    print(f"Reading L1 ask data from: {ask_l1_file}")
    df_ask_l1 = read_bar_data(ask_l1_file, BAR_FORMAT_TOPS, BAR_SIZE_TOPS, cols_tops).add_prefix('ask_L1_')

    print(f"Reading L2 bid data from: {bid_l2_file}")
    df_bid_l2 = read_bar_data(bid_l2_file, BAR_FORMAT_TOPS, BAR_SIZE_TOPS, cols_tops).add_prefix('bid_L2_')
    print(f"Reading L2 ask data from: {ask_l2_file}")
    df_ask_l2 = read_bar_data(ask_l2_file, BAR_FORMAT_TOPS, BAR_SIZE_TOPS, cols_tops).add_prefix('ask_L2_')

    print(f"Reading L3 bid data from: {bid_l3_file}")
    df_bid_l3 = read_bar_data(bid_l3_file, BAR_FORMAT_TOPS, BAR_SIZE_TOPS, cols_tops).add_prefix('bid_L3_')
    print(f"Reading L3 ask data from: {ask_l3_file}")
    df_ask_l3 = read_bar_data(ask_l3_file, BAR_FORMAT_TOPS, BAR_SIZE_TOPS, cols_tops).add_prefix('ask_L3_')


    # Merge dataframes based on timestamp index
    # Use outer join to keep all timestamps, then decide how to handle NaNs
    print("Merging dataframes...")
    all_dfs = [df_fills, df_bid_l1, df_ask_l1, df_bid_l2, df_ask_l2, df_bid_l3, df_ask_l3]
    # Filter out empty dataframes before merging
    all_dfs = [df for df in all_dfs if not df.empty]

    if not all_dfs:
        print("Warning: No data found in any file.")
        return pd.DataFrame()

    df_merged = pd.concat(all_dfs, axis=1, join='outer')

    # Handle missing values - forward fill is a common strategy for time series
    df_merged = df_merged.ffill()

    # Drop rows that still have NaNs after ffill (usually at the beginning)
    df_merged = df_merged.dropna()

    print(f"Merged dataframe shape for {date}: {df_merged.shape}")
    return df_merged

def prepare_features_and_target(df, shift_periods, horizon_label, data_label=""):
    """Prepares features and target variable for linear regression for a specific horizon."""
    print(f"--- Preparing features and target for {horizon_label} ({shift_periods}s shift) using {data_label} data ---")
    # Features: Use current prices, volume, and spreads (same as before)

    # Calculate spreads for each level if data exists
    if 'ask_L1_tops_close' in df.columns and 'bid_L1_tops_close' in df.columns:
        df['spread_L1'] = df['ask_L1_tops_close'] - df['bid_L1_tops_close']
    if 'ask_L2_tops_close' in df.columns and 'bid_L2_tops_close' in df.columns:
        df['spread_L2'] = df['ask_L2_tops_close'] - df['bid_L2_tops_close']
    if 'ask_L3_tops_close' in df.columns and 'bid_L3_tops_close' in df.columns:
        df['spread_L3'] = df['ask_L3_tops_close'] - df['bid_L3_tops_close']

    # Define potential feature columns
    potential_features = [
        'fills_close', 'volume',
        'bid_L1_tops_close', 'ask_L1_tops_close', 'spread_L1',
        'bid_L2_tops_close', 'ask_L2_tops_close', 'spread_L2',
        'bid_L3_tops_close', 'ask_L3_tops_close', 'spread_L3'
    ]

    # Select only the features that actually exist in the merged dataframe
    feature_cols = [col for col in potential_features if col in df.columns]

    # Create a copy to avoid SettingWithCopyWarning when adding target
    df_copy = df.copy()
    features = df_copy[feature_cols]

    # Target: Predict the 'fills_close' price 'shift_periods' seconds ahead
    df_copy['target'] = df_copy['fills_close'].shift(-shift_periods)

    # Combine features and target to handle NaNs created by shifting
    combined = pd.concat([features, df_copy['target']], axis=1)

    # Drop rows where target is NaN (at the end due to shift)
    # Also drop rows where features might be NaN (at the beginning from ffill)
    combined = combined.dropna()

    if combined.empty:
        print(f"Warning: No valid data remains after shifting by {shift_periods} periods for {horizon_label} ({data_label}).")
        return pd.DataFrame(columns=feature_cols), pd.Series(name='target')


    features_final = combined[feature_cols]
    target_final = combined['target']

    if features_final.empty:
         print(f"Warning: Feature set is empty for {horizon_label} ({data_label}). Check data length and shift period.")

    return features_final, target_final

def train_model(features, target, horizon_label, eval_date_str):
    """Trains a linear regression model."""
    print(f"--- Training Model for {horizon_label} on {eval_date_str} Data ---") # Updated print

    if features.empty or target.empty:
        print(f"Skipping training for {horizon_label} due to empty features or target.")
        return None

    print("Training Linear Regression model...")
    model = LinearRegression()
    try:
        model.fit(features, target)
        print("Model training complete.")
        return model
    except Exception as e:
        print(f"Error during model training for {horizon_label}: {e}")
        return None

def predict_and_evaluate(model, features_test, target_actual, horizon_label, eval_date_str):
    """Uses a trained model to predict on test data and evaluates the results."""
    print(f"--- Predicting and Evaluating for {horizon_label} on {eval_date_str} Test Data ---") # Updated print

    if model is None:
        print(f"Skipping prediction for {horizon_label}: No model was trained.")
        return

    if features_test.empty or target_actual.empty:
        print(f"Skipping prediction for {horizon_label}: Test features or actual targets are empty.")
        return

    print(f"Test set size: {features_test.shape[0]}")

    print("Making predictions on test set...") # Updated print
    try:
        predictions = model.predict(features_test)
    except Exception as e:
        print(f"Error during prediction for {horizon_label}: {e}")
        return

    # Ensure indices align for evaluation
    if not target_actual.index.equals(features_test.index):
         print(f"Warning: Index mismatch between test features and actual targets for {horizon_label}. Evaluation might be incorrect.")
         target_actual = target_actual.reindex(features_test.index).dropna()
         if target_actual.empty:
             print("Error: Could not align targets with features after reindexing.")
             return

    # Evaluate
    try:
        mse = mean_squared_error(target_actual, predictions)
        rmse = np.sqrt(mse)
        print(f"\nModel Evaluation ({horizon_label} on {eval_date_str} Test Set):") # Updated print
        print(f"  Mean Squared Error (MSE): {mse}")
        print(f"  Root Mean Squared Error (RMSE): {rmse}")
    except Exception as e:
        print(f"Error during evaluation for {horizon_label}: {e}")

def main():
    """Main function to train and evaluate on a single day's data."""
    eval_date_str = input("Enter evaluation date (YYYYMMDD): ") # Changed prompt
    feed = input("Enter file feed: ")
    symbol = input("Enter symbol: ")

    # Validate date format
    try:
        eval_dt = datetime.strptime(eval_date_str, "%Y%m%d")
    except ValueError:
        print("Invalid date format. Please use YYYYMMDD.")
        return

    print(f"\nAttempting to train and evaluate on data from: {eval_date_str}") # Updated print
    print(f"Feed: {feed.upper()}, Symbol: {symbol.upper()}")

    # 1. Load Data for the Evaluation Day
    df_data = load_and_merge_data(eval_date_str, feed, symbol)
    if df_data.empty:
        print(f"No data loaded for evaluation date {eval_date_str}. Exiting.")
        return

    # Define prediction horizons
    horizons = {
        "1 Second Ahead": 1,
        "1 Minute Ahead": 60,
        "10 Minutes Ahead": 600,
    }

    # Define train/test split ratio (e.g., 80% train, 20% test)
    train_split_ratio = 0.8

    for label, shift in horizons.items():
        print(f"\n===== Processing Horizon: {label} (Shift: {shift}s) =====")

        # 2. Prepare Features and Target for the Entire Day
        features_all, target_all = prepare_features_and_target(
            df_data.copy(), shift, label, data_label=f"Eval ({eval_date_str})"
        )

        if features_all.empty or target_all.empty:
            print(f"Skipping horizon {label}: Not enough data after processing.")
            continue

        # 3. Split Data into Training and Testing Sets (Chronological Split)
        split_index = int(len(features_all) * train_split_ratio)
        if split_index == 0 or split_index == len(features_all):
            print(f"Skipping horizon {label}: Not enough data to create both train and test sets after split.")
            continue

        features_train = features_all.iloc[:split_index]
        target_train = target_all.iloc[:split_index]
        features_test = features_all.iloc[split_index:]
        target_test = target_all.iloc[split_index:]

        print(f"Train set size: {features_train.shape[0]}, Test set size: {features_test.shape[0]}")

        # 4. Train the Model on the Training Set
        model = train_model(features_train, target_train, label, eval_date_str)

        if model is None:
            print(f"Skipping evaluation for {label}: Model training failed or was skipped.")
            continue

        # 5. Predict and Evaluate on the Testing Set
        predict_and_evaluate(model, features_test, target_test, label, eval_date_str)

if __name__ == "__main__":
    main()

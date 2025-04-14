import struct
import numpy as np

# Define the binary format for fills bars
FILLS_BAR_FORMAT = "<Qddddi"
FILLS_BAR_SIZE = struct.calcsize(FILLS_BAR_FORMAT)

# Define the binary format for tops bars
TOPS_BAR_FORMAT = "<Qdddd"
TOPS_BAR_SIZE = struct.calcsize(TOPS_BAR_FORMAT)

def read_fills_bar_file(file_path):
    """Reads a binary fills bar file and extracts closing prices."""
    closing_prices = []
    try:
        with open(file_path, "rb") as file:
            while True:
                data = file.read(FILLS_BAR_SIZE)
                if len(data) < FILLS_BAR_SIZE:
                    break
                _, _, _, _, close, _ = struct.unpack(FILLS_BAR_FORMAT, data)
                closing_prices.append(close)
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    return closing_prices

def read_tops_bar_file(file_path):
    """Reads a binary tops bar file and extracts closing prices."""
    closing_prices = []
    try:
        with open(file_path, "rb") as file:
            while True:
                data = file.read(TOPS_BAR_SIZE)
                if len(data) < TOPS_BAR_SIZE:
                    break
                _, _, _, _, close = struct.unpack(TOPS_BAR_FORMAT, data)
                closing_prices.append(close)
    except FileNotFoundError:
        print(f"Error: File not found - {file_path}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    return closing_prices

def trim_to_same_length(list1, list2):
    """Trims two lists to the same length by evenly removing entries from the longer list."""
    len1, len2 = len(list1), len(list2)

    # If either list is empty, return empty lists to signal bad data
    if len1 == 0 or len2 == 0:
        return [], []

    if len1 > len2:
        step = max(1, len1 // len2)
        list1 = [list1[i] for i in range(0, len1, step)][:len2]
    elif len2 > len1:
        step = max(1, len2 // len1)
        list2 = [list2[i] for i in range(0, len2, step)][:len1]
    return list1, list2

def calculate_correlation(file1, file2, fills_flag, min_length=10):
    """Calculates the correlation between closing prices of two files."""
    
    # Checks if files are fills or tops and reads accordingly
    if fills_flag:
        prices1 = read_fills_bar_file(file1)
        prices2 = read_fills_bar_file(file2)
    else:
        prices1 = read_tops_bar_file(file1)
        prices2 = read_tops_bar_file(file2)

    # Skip if either list is empty
    if not prices1 or not prices2:
        print(f"Skipping: Empty data in files:\n  {file1}\n  {file2}")
        return None

    # Trim lists to the same length
    prices1, prices2 = trim_to_same_length(prices1, prices2)

    if len(prices1) < min_length or len(prices2) < min_length:
        print(f"Skipping after trimming (too little data):\n  {file1} ({len(prices1)} entries)\n  {file2} ({len(prices2)} entries)")
        return None
    
    # Calculate correlation
    correlation = np.corrcoef(prices1, prices2)[0, 1]
    return correlation

def calculate_weighted_correlation(correlations, weights):
    """Calculates a weighted average of the given correlations."""
    valid_correlations = [(corr, weight) for corr, weight in zip(correlations, weights) if corr is not None]
    
    if not valid_correlations:
        print("Error: No valid correlations to calculate the overall correlation.")
        return None

    # Separate valid correlations and their weights
    valid_corr_values, valid_weights = zip(*valid_correlations)

    # Calculate weighted average
    weighted_sum = sum(c * w for c, w in zip(valid_corr_values, valid_weights))
    total_weight = sum(valid_weights)
    return weighted_sum / total_weight

if __name__ == "__main__":
    date = input("Enter file date (yearMonthDay): ")
    feed = input("Enter file feed: ")
    symbol1 = input("Enter first symbol: ")
    symbol2 = input("Enter second symbol: ")

    base_path = f"/home/vir/{date}/{feed.lower()}/bars/{feed.upper()}"
    # Input paths for the files
    fills_file_stock1 = f"{base_path}.fills_bars.{symbol1.upper()}.bin"
    L1_bid_file_stock1 = f"{base_path}.bid_bars_L1.{symbol1.upper()}.bin"
    L1_ask_file_stock1 = f"{base_path}.ask_bars_L1.{symbol1.upper()}.bin"
    L2_bid_file_stock1 = f"{base_path}.bid_bars_L2.{symbol1.upper()}.bin"
    L2_ask_file_stock1 = f"{base_path}.ask_bars_L2.{symbol1.upper()}.bin"
    L3_bid_file_stock1 = f"{base_path}.bid_bars_L3.{symbol1.upper()}.bin"
    L3_ask_file_stock1 = f"{base_path}.ask_bars_L3.{symbol1.upper()}.bin"

    fills_file_stock2 = f"{base_path}.fills_bars.{symbol2.upper()}.bin"
    L1_bid_file_stock2 = f"{base_path}.bid_bars_L1.{symbol2.upper()}.bin"
    L1_ask_file_stock2 = f"{base_path}.ask_bars_L1.{symbol2.upper()}.bin"
    L2_bid_file_stock2 = f"{base_path}.bid_bars_L2.{symbol2.upper()}.bin"
    L2_ask_file_stock2 = f"{base_path}.ask_bars_L2.{symbol2.upper()}.bin"
    L3_bid_file_stock2 = f"{base_path}.bid_bars_L3.{symbol2.upper()}.bin"
    L3_ask_file_stock2 = f"{base_path}.ask_bars_L3.{symbol2.upper()}.bin"

    # Calculate correlations
    correlation_fills = calculate_correlation(fills_file_stock1, fills_file_stock2, True)
    correlation_L1_bid = calculate_correlation(L1_bid_file_stock1, L1_bid_file_stock2, False)
    correlation_L1_ask = calculate_correlation(L1_ask_file_stock1, L1_ask_file_stock2, False)
    correlation_L2_bid = calculate_correlation(L2_bid_file_stock1, L2_bid_file_stock2, False)
    correlation_L2_ask = calculate_correlation(L2_ask_file_stock1, L2_ask_file_stock2, False)
    correlation_L3_bid = calculate_correlation(L3_bid_file_stock1, L3_bid_file_stock2, False)
    correlation_L3_ask = calculate_correlation(L3_ask_file_stock1, L3_ask_file_stock2, False)

    # Calculate weighted correlation
    correlations = [
        correlation_fills,
        correlation_L1_bid, correlation_L1_ask,
        correlation_L2_bid, correlation_L2_ask,
        correlation_L3_bid, correlation_L3_ask
    ]
    weights = [0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125] # Equal weights for each correlation
    overall_correlation = calculate_weighted_correlation(correlations, weights)

    # Display results
    if correlation_fills is not None:
        print(f"Correlation between fills closing prices of {symbol1.upper()} and {symbol2.upper()}: {correlation_fills:.4f}")
    if correlation_L1_bid is not None:
        print(f"Correlation between L1 bid closing prices of {symbol1.upper()} and {symbol2.upper()}: {correlation_L1_bid:.4f}")
    if correlation_L1_ask is not None:
        print(f"Correlation between L1 ask closing prices of {symbol1.upper()} and {symbol2.upper()}: {correlation_L1_ask:.4f}")
    if correlation_L2_bid is not None:
        print(f"Correlation between L2 bid closing prices of {symbol1.upper()} and {symbol2.upper()}: {correlation_L2_bid:.4f}")
    if correlation_L2_ask is not None:
        print(f"Correlation between L2 ask closing prices of {symbol1.upper()} and {symbol2.upper()}: {correlation_L2_ask:.4f}")
    if correlation_L3_bid is not None:
        print(f"Correlation between L3 bid closing prices of {symbol1.upper()} and {symbol2.upper()}: {correlation_L3_bid:.4f}")
    if correlation_L3_ask is not None:
        print(f"Correlation between L3 ask closing prices of {symbol1.upper()} and {symbol2.upper()}: {correlation_L3_ask:.4f}")
    if overall_correlation is not None:
        print(f"Overall correlation between {symbol1.upper()} and {symbol2.upper()}: {overall_correlation:.4f}")
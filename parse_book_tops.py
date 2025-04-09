import struct
import os
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pytz

# Define the header format (little-endian) matching book_tops_file_hdr_t
HEADER_FORMAT = "<QIIQ"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Define the book_top_t format (little-endian)
BOOK_TOP_FORMAT = "<QQqqIIqqIIqqII" # ts (uint64), seqno (uint64), 3 levels of book_top_level_t
BOOK_TOP_SIZE = struct.calcsize(BOOK_TOP_FORMAT)

BAR_FORMAT = "<Qdddd"
BAR_SIZE = struct.calcsize(BAR_FORMAT)

UTC = pytz.utc
ET = pytz.timezone("US/Eastern")

def read_header(file):
    """Reads and prints the file header."""
    header_data = file.read(HEADER_SIZE)
    if len(header_data) < HEADER_SIZE:
        print("Error: File is too small to contain a valid header.")
        return None

    feed_id, dateint, number_of_tops, symbol_idx = struct.unpack(HEADER_FORMAT, header_data)

    print("Header Information:")
    print(f"  Feed ID: {feed_id}")
    print(f"  Date (int): {dateint}")
    print(f"  Number of Tops: {number_of_tops}")
    print(f"  Symbol Index: {symbol_idx}")

    return number_of_tops  

def read_data(input_file, number_of_tops):
    """Reads book top data, extracting timestamps, bid/ask prices for all three levels."""
    print("\nProcessing Book Fill Snapshots...")

    timestamps = []
    bid_prices, ask_prices = [[], [], []], [[], [], []]  # Three levels

    for _ in range(number_of_tops):
        data = input_file.read(BOOK_TOP_SIZE)
        if len(data) < BOOK_TOP_SIZE:
            print("Warning: Reached end of file earlier than expected.")
            break
        
        unpacked_data = struct.unpack(BOOK_TOP_FORMAT, data)

        # Extract timestamp and convert to ET
        ts_nanos = unpacked_data[0]  # timestamp (uint64_t)
        timestamp = datetime.fromtimestamp(ts_nanos / 1e9, ET)
        timestamps.append(timestamp)

        # Extract bid/ask prices for all three levels
        for level in range(3):
            bid_price_nanos = unpacked_data[2 + level * 4]  # int64_t
            ask_price_nanos = unpacked_data[3 + level * 4]  # int64_t
            bid_qty = unpacked_data[4 + level * 4]  # uint32_t
            ask_qty = unpacked_data[5 + level * 4]  # uint32_t

            if bid_price_nanos != 0 and bid_qty != 0:  # Ensure valid bid data exists
                bid_price = bid_price_nanos / 1e9
                bid_prices[level].append(bid_price)
            else:
                bid_prices[level].append(None)

            if ask_price_nanos != 0 and ask_qty != 0:  # Ensure valid ask data exists
                ask_price = ask_price_nanos / 1e9
                ask_prices[level].append(ask_price)
            else:
                ask_prices[level].append(None)

    return timestamps, bid_prices, ask_prices

def create_and_store_bars(timestamps, prices, output_file, last_timestamp):
    """Creates OHLC bars from price data and writes them to a binary file if the timestamp is at least 1 second later."""
    if os.path.exists(output_file):
        os.remove(output_file)
    
    ensure_file_exists(output_file)  # Ensure file exists before writing

    bars = {}
    
    for t, price in zip(timestamps, prices):
        if price is None:  
            continue  # Skip missing price data

        bar_time = t.replace(microsecond=0)  # Round to the nearest second

        # Ensure at least 1 second has passed since the last recorded timestamp
        if last_timestamp is not None and (bar_time.timestamp() <= last_timestamp + 1):
            continue  

        if bar_time not in bars:
            bars[bar_time] = {"open": price, "high": price, "low": price, "close": price}
        else:
            bars[bar_time]["high"] = max(bars[bar_time]["high"], price)
            bars[bar_time]["low"] = min(bars[bar_time]["low"], price)
            bars[bar_time]["close"] = price

    # Write bars to binary file
    with open(output_file, "wb") as f:  # Overwrite file
        for bar_time in sorted(bars.keys()):
            bar = bars[bar_time]
            try:
                packed_data = struct.pack(
                    BAR_FORMAT,
                    int(bar_time.timestamp()),
                    float(bar["open"]),
                    float(bar["high"]),
                    float(bar["low"]),
                    float(bar["close"])
                )
                f.write(packed_data)
                last_timestamp = bar_time.timestamp()  # Update last timestamp
            except (TypeError, ValueError) as e:
                print(f"Error packing data for bar_time {bar_time}: {e}")
                continue

    return last_timestamp  # Return updated last timestamp

def ensure_file_exists(file_path):
    """Ensures the binary file exists before writing to it."""
    if not os.path.exists(file_path):
        with open(file_path, "wb") as f:
            pass  # Create an empty file
        print(f"Created file: {file_path}")

def read_last_timestamp(file_path):
    """Reads the last timestamp from an existing binary file."""
    if not os.path.exists(file_path) or os.path.getsize(file_path) < BAR_SIZE:
        return None  # File does not exist or is empty

    with open(file_path, "rb") as f:
        f.seek(-BAR_SIZE, os.SEEK_END)  # Move to the last bar's position
        data = f.read(BAR_SIZE)
        if len(data) == BAR_SIZE:
            unpacked_data = struct.unpack(BAR_FORMAT, data)
            return unpacked_data[0]  # Return the last stored timestamp
    return None

def process_and_store_bars(timestamps, bid_prices, ask_prices, output_file_path_base, symbol):
    """Processes bid/ask prices into OHLC bars and stores them."""
    last_timestamps = {"bids": [None, None, None], "asks": [None, None, None]}
    
    for level in range(3):
        bid_bar_file = f"{output_file_path_base}bid_bars_L{level+1}.{symbol.upper()}.bin"
        ask_bar_file = f"{output_file_path_base}ask_bars_L{level+1}.{symbol.upper()}.bin"
        
        # Read last stored timestamps
        last_timestamps["bids"][level] = read_last_timestamp(bid_bar_file)
        last_timestamps["asks"][level] = read_last_timestamp(ask_bar_file)

        last_timestamps["bids"][level] = create_and_store_bars(timestamps, bid_prices[level], bid_bar_file, last_timestamps["bids"][level])
        last_timestamps["asks"][level] = create_and_store_bars(timestamps, ask_prices[level], ask_bar_file, last_timestamps["asks"][level])

def aggregate_prices(timestamps, bid_prices, ask_prices):
    """Aggregates bid and ask prices for all three levels into 1-second, 1-minute, and 1-hour bins."""
    def aggregate(interval, level):
        bins = {}
        for t, bid, ask in zip(timestamps, bid_prices[level], ask_prices[level]):
            key = t.replace(second=0, microsecond=0) if interval >= 60 else t.replace(microsecond=0)
            key = key.replace(minute=0) if interval >= 3600 else key  # Round to hour if needed
            
            if key not in bins:
                bins[key] = {"bids": [], "asks": []}
            if bid is not None:
                bins[key]["bids"].append(bid)
            if ask is not None:
                bins[key]["asks"].append(ask)
        
        aggregated_timestamps = sorted(bins.keys())
        aggregated_bids = [np.mean(bins[t]["bids"]) if bins[t]["bids"] else None for t in aggregated_timestamps]
        aggregated_asks = [np.mean(bins[t]["asks"]) if bins[t]["asks"] else None for t in aggregated_timestamps]

        return aggregated_timestamps, aggregated_bids, aggregated_asks

    aggregated_results = {}
    for level in range(3):
        aggregated_results[f"{level+1}-1-second"] = aggregate(1, level)
        aggregated_results[f"{level+1}-1-minute"] = aggregate(60, level)
        aggregated_results[f"{level+1}-1-hour"] = aggregate(3600, level)

    return aggregated_results

def plot_aggregated_bid_ask_prices(aggregated_data):
    """Plots the bid and ask prices for all levels over time."""
    levels = [1, 2, 3]
    intervals = ["1-second", "1-minute", "1-hour"]

    for level in levels:
        for interval in intervals:
            key = f"{level}-{interval}"
            timestamps, bid_prices, ask_prices = aggregated_data[key]

            plt.figure(figsize=(10, 5))

            # Filter out None values
            bid_times = [t for t, p in zip(timestamps, bid_prices) if p is not None]
            bid_values = [p for p in bid_prices if p is not None]
            
            ask_times = [t for t, p in zip(timestamps, ask_prices) if p is not None]
            ask_values = [p for p in ask_prices if p is not None]

            # Plot bids and asks
            plt.plot(bid_times, bid_values, label=f"Bid Price L{level}", color='blue', linestyle='-')
            plt.plot(ask_times, ask_values, label=f"Ask Price L{level}", color='red', linestyle='-')

            # Format x-axis labels
            plt.xticks(rotation=45, ha="right", fontsize=8)
            plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%H:%M:%S"))

            # Labels and title
            plt.xlabel("Time (ET)")
            plt.ylabel("Price (USD)")
            plt.title(f"Bid/Ask Prices Over Time (L{level} - {interval})")
            plt.legend()
            plt.show()

def process_file():
    """Handles file input and calls functions to read the header and data."""
    date = input("Enter file date (yearMonthDay): ")
    feed = input("Enter file feed: ")
    symbol = input("Enter symbol: ")

    input_file_path = f"/home/vir/{date}/{feed.lower()}/books/{feed.upper()}.book_tops.{symbol.upper()}.bin"
    output_file_path_base = f"/home/vir/{date}/{feed.lower()}/bars/{feed.upper()}."
    
    try:
        with open(input_file_path, "rb") as input_file:
            number_of_tops = read_header(input_file)
            if number_of_tops:
                timestamps, bid_prices, ask_prices = read_data(input_file, number_of_tops)
                
                # Aggregate and plot the data
                aggregated_data = aggregate_prices(timestamps, bid_prices, ask_prices)
                plot_aggregated_bid_ask_prices(aggregated_data)
                process_and_store_bars(timestamps, bid_prices, ask_prices, output_file_path_base, symbol)

    except FileNotFoundError:
        print("Error: File not found.")
    except Exception as e:
        print("Error reading file: {}".format(e))

if __name__ == "__main__":
    process_file()
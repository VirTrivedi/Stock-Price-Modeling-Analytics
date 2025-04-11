import struct
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import pytz
import os

# Define the header format (little-endian)
HEADER_FORMAT = "<QIIQ"  
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Define the data format (little-endian)
DATA_FORMAT = "<QQQ?qIQIIQ?qIqiI"  
DATA_SIZE = struct.calcsize(DATA_FORMAT)

# Define the binary format for storing bars
BAR_FORMAT = "<Qddddi"
BAR_SIZE = struct.calcsize(BAR_FORMAT)

UTC = pytz.utc
ET = pytz.timezone("US/Eastern")

def read_header(file):
    """Reads and prints the file header."""
    header_data = file.read(HEADER_SIZE)
    if len(header_data) < HEADER_SIZE:
        print("Error: File is too small to contain a valid header.")
        return None

    feed_id, dateint, number_of_fills, symbol_idx = struct.unpack(HEADER_FORMAT, header_data)

    print("Header Information:")
    print(f"  Feed ID: {feed_id}")
    print(f"  Date (int): {dateint}")
    print(f"  Number of Fills: {number_of_fills}")
    print(f"  Symbol Index: {symbol_idx}")

    return number_of_fills  

def read_data(input_file, number_of_fills, output_file):
    """Reads data and stores prices for plotting."""
    print("\nProcessing Book Fill Snapshots...")
    
    timestamps = []
    prices = []

    # Bar tracking variables for bids
    current_bar_time = None
    high = float('-inf')
    low = float('inf')
    open = close = None
    total_volume = 0

    for i in range(number_of_fills):
        data = input_file.read(DATA_SIZE)
        if len(data) < DATA_SIZE:
            print("Warning: Reached end of file earlier than expected.")
            break
        
        unpacked_data = struct.unpack(DATA_FORMAT, data)

        # Extract relevant fields
        raw_timestamp = unpacked_data[0]  # ts (uint64_t)
        trade_price = unpacked_data[4]  # trade_price (int64_t)
        trade_qty = unpacked_data[5]  # trade_qty (uint32_t)

        # Convert Timestamp from Nanoseconds to Seconds
        timestamp = raw_timestamp / 1e9
        timestamp_et = datetime.fromtimestamp(timestamp)

        timestamps.append(timestamp_et)
        
        bar_time = timestamp_et.replace(microsecond=0)

        trade_price = trade_price / 1e9

        if current_bar_time and bar_time != current_bar_time:
            write_bar(output_file, current_bar_time, high, low, open, close, total_volume)

        current_bar_time = bar_time
        
        if open is None:
            open = trade_price

        close = trade_price

        high = max(high, trade_price)
        low = min(low, trade_price)

        total_volume += trade_qty

        prices.append(trade_price)

    # Write the last bid and ask bars after loop ends
    if current_bar_time:
        write_bar(output_file, current_bar_time, high, low, open, close, total_volume)

    # Aggregate data and plot graphs
    aggregated_data = aggregate_prices(timestamps, prices)
    plot_aggregated_prices(aggregated_data)

def write_bar(output_file, bar_time, high, low, open, close, volume):
    """Writes a bar to the binary file."""
    timestamp = int(bar_time.timestamp())
    bar_data = struct.pack(BAR_FORMAT, timestamp, high, low, open, close, volume)
    output_file.write(bar_data)

def aggregate_prices(timestamps, prices):
    """Aggregates bid and ask prices into 1-second, 1-minute, and 1-hour bins."""
    def aggregate(interval):
        bins = {}
        for t, price in zip(timestamps, prices):
            key = t.replace(second=0, microsecond=0) if interval >= 60 else t.replace(microsecond=0)
            key = key.replace(minute=0) if interval >= 3600 else key
            
            if key not in bins:
                bins[key] = []
            if price is not None:
                bins[key].append(price)
        
        aggregated_timestamps = sorted(bins.keys())
        aggregated_prices = [np.mean(bins[t]) if bins[t] else None for t in aggregated_timestamps]

        return aggregated_timestamps, aggregated_prices

    return {
        "1-second": aggregate(1),
        "1-minute": aggregate(60),
        "1-hour": aggregate(3600)
    }

def plot_aggregated_prices(aggregated_data):
    """Plots the bid and ask prices at different time intervals with ET timestamps."""
    intervals = ["1-second", "1-minute", "1-hour"]
    
    for interval in intervals:
        timestamps, prices = aggregated_data[interval]

        plt.figure(figsize=(10, 5))
        
        # Filter out None values
        valid_times = [t for t, p in zip(timestamps, prices) if p is not None]
        valid_prices = [p for p in prices if p is not None]

        # Plot bids and asks as lines
        plt.plot(valid_times, valid_prices, label="Price", color='blue', linestyle='-')

        # Format x-axis labels
        plt.xticks(rotation=45, ha="right", fontsize=8)
        plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%H:%M:%S"))

        # Graph labels
        plt.xlabel("Time (ET)")
        plt.ylabel("Price (USD)")
        plt.title(f"Bid and Ask Prices Over Time ({interval} intervals)")
        plt.legend()
        
        plt.show()

def process_file():
    """Handles file input and calls functions to read the header and data."""
    date = input("Enter file date (yearMonthDay): ")
    feed = input("Enter file feed: ")
    symbol = input("Enter symbol: ")

    input_file_path = f"/home/vir/{date}/{feed.lower()}/books/{feed.upper()}.book_fills.{symbol.upper()}.bin"
    output_file_path = f"/home/vir/{date}/{feed.lower()}/bars/{feed.upper()}.fills_bars.{symbol.upper()}.bin"

    try:
        with open(input_file_path, "rb") as input_file, open(output_file_path, "wb") as output_file:
            print(f"\nSaving bars to {output_file_path} (Overwriting if exists)...")
            
            number_of_fills = read_header(input_file)
            if number_of_fills:
                read_data(input_file, number_of_fills, output_file)
                print(f"Bars saved to {output_file_path}")

    except FileNotFoundError:
        print("Error: File not found.")
    except Exception as e:
        print("Error reading file: {}".format(e))

if __name__ == "__main__":
    process_file()
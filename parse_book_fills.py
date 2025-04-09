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

def read_data(input_file, number_of_fills, bid_output_file, ask_output_file):
    """Reads data, prints top bids/asks, and stores prices for plotting."""
    print("\nProcessing Book Fill Snapshots...")
    
    timestamps = []
    bid_prices = []
    ask_prices = []

    # Bar tracking variables for bids
    bid_current_bar_time = None
    bid_high = float('-inf')
    bid_low = float('inf')
    bid_open = bid_close = None
    bid_total_volume = 0

    # Bar tracking variables for asks
    ask_current_bar_time = None
    ask_high = float('-inf')
    ask_low = float('inf')
    ask_open = ask_close = None
    ask_total_volume = 0

    for i in range(number_of_fills):
        data = input_file.read(DATA_SIZE)
        if len(data) < DATA_SIZE:
            print("Warning: Reached end of file earlier than expected.")
            break
        
        unpacked_data = struct.unpack(DATA_FORMAT, data)

        # Extract relevant fields
        raw_timestamp = unpacked_data[0]  # ts (uint64_t)
        resting_order_id = unpacked_data[2]  # resting_order_id (uint64_t)
        trade_price = unpacked_data[4]  # trade_price (int64_t)
        trade_qty = unpacked_data[5]  # trade_qty (uint32_t)
        resting_side_is_bid = unpacked_data[9]  # resting_side_is_bid (bool)

        # Convert Timestamp from Nanoseconds to Seconds
        timestamp = raw_timestamp / 1e9
        timestamp_et = datetime.fromtimestamp(timestamp)

        timestamps.append(timestamp_et)
        
        bar_time = timestamp_et.replace(microsecond=0)

        trade_price = trade_price / 1e9

        if resting_side_is_bid:
            if bid_current_bar_time and bar_time != bid_current_bar_time:
                write_bar(bid_output_file, bid_current_bar_time, bid_high, bid_low, bid_open, bid_close, bid_total_volume)

            bid_current_bar_time = bar_time
            
            if bid_open is None:
                bid_open = trade_price

            bid_close = trade_price

            bid_high = max(bid_high, trade_price)
            bid_low = min(bid_low, trade_price)

            bid_total_volume += trade_qty

            bid_prices.append(trade_price)
            ask_prices.append(None)
        else:
            if ask_current_bar_time and bar_time != ask_current_bar_time:
                write_bar(ask_output_file, ask_current_bar_time, ask_high, ask_low, ask_open, ask_close, ask_total_volume)

            ask_current_bar_time = bar_time

            if ask_open is None:
                ask_open = trade_price

            ask_close = trade_price

            ask_high = max(ask_high, trade_price)
            ask_low = min(ask_low, trade_price)

            ask_total_volume += trade_qty

            ask_prices.append(trade_price)
            bid_prices.append(None)

    # Write the last bid and ask bars after loop ends
    if bid_current_bar_time:
        write_bar(bid_output_file, bid_current_bar_time, bid_high, bid_low, bid_open, bid_close, bid_total_volume)
    if ask_current_bar_time:
        write_bar(ask_output_file, ask_current_bar_time, ask_high, ask_low, ask_open, ask_close, ask_total_volume)

    # Aggregate data and plot graphs
    aggregated_data = aggregate_prices(timestamps, bid_prices, ask_prices)
    plot_aggregated_bid_ask_prices(aggregated_data)

def write_bar(output_file, bar_time, high, low, open, close, volume):
    """Writes a bar to the binary file."""
    timestamp = int(bar_time.timestamp())
    bar_data = struct.pack(BAR_FORMAT, timestamp, high, low, open, close, volume)
    output_file.write(bar_data)

def aggregate_prices(timestamps, bid_prices, ask_prices):
    """Aggregates bid and ask prices into 1-second, 1-minute, and 1-hour bins."""
    def aggregate(interval):
        bins = {}
        for t, bid, ask in zip(timestamps, bid_prices, ask_prices):
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

    return {
        "1-second": aggregate(1),
        "1-minute": aggregate(60),
        "1-hour": aggregate(3600)
    }

def plot_aggregated_bid_ask_prices(aggregated_data):
    """Plots the bid and ask prices at different time intervals with ET timestamps."""
    intervals = ["1-second", "1-minute", "1-hour"]
    
    for interval in intervals:
        timestamps, bid_prices, ask_prices = aggregated_data[interval]

        plt.figure(figsize=(10, 5))
        
        # Filter out None values
        bid_times = [t for t, p in zip(timestamps, bid_prices) if p is not None]
        bid_values = [p for p in bid_prices if p is not None]
        
        ask_times = [t for t, p in zip(timestamps, ask_prices) if p is not None]
        ask_values = [p for p in ask_prices if p is not None]

        # Plot bids and asks as lines
        plt.plot(bid_times, bid_values, label="Bid Price", color='blue', linestyle='-')
        plt.plot(ask_times, ask_values, label="Ask Price", color='red', linestyle='-')

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
    output_dir = f"/home/vir/{date}/{feed.lower()}/bars/"
    bid_output_file_path = f"{output_dir}{feed.upper()}.trade_bid_bars.{symbol.upper()}.bin"
    ask_output_file_path = f"{output_dir}{feed.upper()}.trade_ask_bars.{symbol.upper()}.bin"

    os.makedirs(output_dir, exist_ok=True)

    try:
        with open(input_file_path, "rb") as input_file, open(bid_output_file_path, "wb") as bid_output_file, open(ask_output_file_path, "wb") as ask_output_file:
            print(f"\nSaving bid bars to {bid_output_file_path} (Overwriting if exists)...")
            print(f"Saving ask bars to {ask_output_file_path} (Overwriting if exists)...")
            
            number_of_fills = read_header(input_file)
            if number_of_fills:
                read_data(input_file, number_of_fills, bid_output_file, ask_output_file)
                print(f"Bid bars saved to {bid_output_file_path}")
                print(f"Ask bars saved to {ask_output_file_path}")

    except FileNotFoundError:
        print("Error: File not found.")
    except Exception as e:
        print("Error reading file: {}".format(e))

if __name__ == "__main__":
    process_file()
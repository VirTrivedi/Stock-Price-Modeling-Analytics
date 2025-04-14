import os
import re
import itertools
import csv
from price_correlation import calculate_correlation, calculate_weighted_correlation, read_fills_bar_file, read_tops_bar_file

def extract_symbols_from_folder(folder_path):
    """Extracts unique stock symbols from .bin filenames."""
    pattern = re.compile(r'\.(?:fills|bid_bars_L\d|ask_bars_L\d)\.([A-Z]+)\.bin$')
    symbols = set()
    for filename in os.listdir(folder_path):
        match = pattern.search(filename)
        if match:
            symbols.add(match.group(1))
    return sorted(symbols)

def generate_file_paths(base_path, symbol):
    """Creates a dictionary of all required file paths for a symbol."""
    return {
        'fills': f"{base_path}.fills_bars.{symbol}.bin",
        'L1_bid': f"{base_path}.bid_bars_L1.{symbol}.bin",
        'L1_ask': f"{base_path}.ask_bars_L1.{symbol}.bin",
        'L2_bid': f"{base_path}.bid_bars_L2.{symbol}.bin",
        'L2_ask': f"{base_path}.ask_bars_L2.{symbol}.bin",
        'L3_bid': f"{base_path}.bid_bars_L3.{symbol}.bin",
        'L3_ask': f"{base_path}.ask_bars_L3.{symbol}.bin"
    }

def is_symbol_valid(base_path, symbol, min_length=10):
    """Check if all required files for a symbol contain data."""
    paths = generate_file_paths(base_path, symbol)

    checks = [
        read_fills_bar_file(paths['fills']),
        read_tops_bar_file(paths['L1_bid']),
        read_tops_bar_file(paths['L1_ask']),
        read_tops_bar_file(paths['L2_bid']),
        read_tops_bar_file(paths['L2_ask']),
        read_tops_bar_file(paths['L3_bid']),
        read_tops_bar_file(paths['L3_ask']),
    ]

    return all(len(data) >= min_length for data in checks)

def compute_overall_correlations(symbols, base_path):
    """Computes overall correlation for all symbol pairs."""
    results = []

    total = len(symbols) * (len(symbols) - 1) // 2
    for i, (sym1, sym2) in enumerate(itertools.combinations(symbols, 2), 1):
        print(f"[{i}/{total}] Processing: {sym1} vs {sym2}")
        
        paths1 = generate_file_paths(base_path, sym1)
        paths2 = generate_file_paths(base_path, sym2)

        correlations = [
            calculate_correlation(paths1['fills'], paths2['fills'], True),
            calculate_correlation(paths1['L1_bid'], paths2['L1_bid'], False),
            calculate_correlation(paths1['L1_ask'], paths2['L1_ask'], False),
            calculate_correlation(paths1['L2_bid'], paths2['L2_bid'], False),
            calculate_correlation(paths1['L2_ask'], paths2['L2_ask'], False),
            calculate_correlation(paths1['L3_bid'], paths2['L3_bid'], False),
            calculate_correlation(paths1['L3_ask'], paths2['L3_ask'], False)
        ]
        weights = [0.125] * len(correlations)
        overall = calculate_weighted_correlation(correlations, weights)

        if overall is not None:
            results.append({
                "symbol1": sym1,
                "symbol2": sym2,
                "overall_correlation": round(overall, 4)
            })

    return results

def save_correlations_to_csv(results, output_file):
    """Saves only overall correlation values to a CSV."""
    with open(output_file, mode="w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["symbol1", "symbol2", "overall_correlation"])
        writer.writeheader()
        writer.writerows(results)

def main():
    date = input("Enter file date (YYYYMMDD): ")
    feed = input("Enter file feed: ")
    base_folder = f"/home/vir/{date}/{feed.lower()}/bars"
    base_path = f"{base_folder}/{feed.upper()}"

    print("Finding symbols...")
    symbols = extract_symbols_from_folder(base_folder)

    print(f"Found {len(symbols)} symbols. Validating data files...")
    valid_symbols = []
    invalid_symbols = []

    for symbol in symbols:
        if is_symbol_valid(base_path, symbol):
            valid_symbols.append(symbol)
        else:
            invalid_symbols.append(symbol)

    print(f"{len(valid_symbols)} symbols have valid data.")
    print(f"{len(invalid_symbols)} symbols were skipped due to missing or empty files.")

    print("Computing overall correlations...")
    results = compute_overall_correlations(valid_symbols, base_path)

    print("Saving results...")
    save_correlations_to_csv(results, output_file=f"{base_folder}/overall_correlations.csv")

    print("Done. Output saved to 'overall_correlations.csv'.")

if __name__ == "__main__":
    main()
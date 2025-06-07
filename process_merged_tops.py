import argparse
import struct
import os
from collections import defaultdict

# --- Constants ---
INPUT_FILE_HEADER_SIZE = 24
INPUT_FILE_HEADER_FORMAT = '<QIIQ'

MERGED_ENTRY_PREFIX_FEED_ID_FORMAT = '<Q'
MERGED_ENTRY_PREFIX_FEED_ID_SIZE = struct.calcsize(MERGED_ENTRY_PREFIX_FEED_ID_FORMAT)

TOPS_RECORD_FLAT_PY_FORMAT = '<QQ q q I I q q I I q q I I'
TOPS_RECORD_SIZE = struct.calcsize(TOPS_RECORD_FLAT_PY_FORMAT)

MERGED_TOPS_FULL_ENTRY_SIZE = MERGED_ENTRY_PREFIX_FEED_ID_SIZE + TOPS_RECORD_SIZE

OUTPUT_FILE_HEADER_SIZE = 24
OUTPUT_FILE_HEADER_FORMAT = '<QIIQ'

SNAPSHOT_HEADER_FORMAT = '<QBB'
SNAPSHOT_HEADER_SIZE = struct.calcsize(SNAPSHOT_HEADER_FORMAT)

LEVEL_HEADER_FORMAT = '<qB'
LEVEL_HEADER_SIZE = struct.calcsize(LEVEL_HEADER_FORMAT)

VENUE_AT_LEVEL_FORMAT = '<IQ'
VENUE_AT_LEVEL_SIZE = struct.calcsize(VENUE_AT_LEVEL_FORMAT)

NUM_LEVELS_TO_SNAPSHOT = 3
PROCESSED_SNAPSHOT_FILE_FEED_ID = 0

# Helper function to create a snapshot from the latest quotes of a minute
def create_snapshot_from_latest_quotes(latest_quotes_map, num_levels_to_snapshot):
    """
    Consolidates latest quotes from multiple venues into a single top-N snapshot.
    """
    bids_accumulator = defaultdict(list)
    asks_accumulator = defaultdict(list)

    for original_feed_id, rec_data in latest_quotes_map.items():
        # Level 1
        if rec_data['l1bp'] != 0 and rec_data['l1bq'] > 0:
            bids_accumulator[rec_data['l1bp']].append((rec_data['l1bq'], original_feed_id))
        if rec_data['l1ap'] != 0 and rec_data['l1aq'] > 0:
            asks_accumulator[rec_data['l1ap']].append((rec_data['l1aq'], original_feed_id))
        # Level 2
        if rec_data['l2bp'] != 0 and rec_data['l2bq'] > 0:
            bids_accumulator[rec_data['l2bp']].append((rec_data['l2bq'], original_feed_id))
        if rec_data['l2ap'] != 0 and rec_data['l2aq'] > 0:
            asks_accumulator[rec_data['l2ap']].append((rec_data['l2aq'], original_feed_id))
        # Level 3
        if rec_data['l3bp'] != 0 and rec_data['l3bq'] > 0:
            bids_accumulator[rec_data['l3bp']].append((rec_data['l3bq'], original_feed_id))
        if rec_data['l3ap'] != 0 and rec_data['l3aq'] > 0:
            asks_accumulator[rec_data['l3ap']].append((rec_data['l3aq'], original_feed_id))

    final_bid_levels = []
    # Sort bid prices in descending order
    for price in sorted(bids_accumulator.keys(), reverse=True)[:num_levels_to_snapshot]:
        venues_at_price = bids_accumulator[price]
        if venues_at_price: 
            final_bid_levels.append({'price': price, 'venues': venues_at_price})
            
    final_ask_levels = []
    # Sort ask prices in ascending order
    for price in sorted(asks_accumulator.keys())[:num_levels_to_snapshot]:
        venues_at_price = asks_accumulator[price]
        if venues_at_price:
            final_ask_levels.append({'price': price, 'venues': venues_at_price})
            
    return final_bid_levels, final_ask_levels

def write_snapshot_data(f_out, snapshot_ts, bid_levels, ask_levels):
    """Writes a single snapshot's data to the file."""
    f_out.write(struct.pack(SNAPSHOT_HEADER_FORMAT, 
                            snapshot_ts, 
                            len(bid_levels), 
                            len(ask_levels)))
    # Write all bid levels for this snapshot
    for level_data in bid_levels:
        price = level_data['price']
        venues = level_data['venues']
        f_out.write(struct.pack(LEVEL_HEADER_FORMAT, price, len(venues)))
        for qty, venue_feed_id in venues:
            f_out.write(struct.pack(VENUE_AT_LEVEL_FORMAT, qty, venue_feed_id))
    
    # Write all ask levels for this snapshot
    for level_data in ask_levels:
        price = level_data['price']
        venues = level_data['venues']
        f_out.write(struct.pack(LEVEL_HEADER_FORMAT, price, len(venues)))
        for qty, venue_feed_id in venues:
            f_out.write(struct.pack(VENUE_AT_LEVEL_FORMAT, qty, venue_feed_id))

def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Process a merged_tops.bin file to generate a 3-level order book snapshot feed. "
                    "The script expects the input merged_tops file to have each record prefixed by an 8-byte feed_id "
                    "followed by an 88-byte TopsRecord."
    )
    parser.add_argument("--input-file", required=True, help="Path to the input merged_tops.bin file.")
    parser.add_argument("--output-file", required=True, help="Path for the output snapshot .bin file.")
    return parser.parse_args()

def main():
    args = parse_args()

    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}")
        return
    
    if TOPS_RECORD_SIZE != 88:
        print(f"Critical Error: TOPS_RECORD_SIZE is {TOPS_RECORD_SIZE}, expected 88. Check struct format.")
        return
    if MERGED_TOPS_FULL_ENTRY_SIZE != 96:
        print(f"Critical Error: MERGED_TOPS_FULL_ENTRY_SIZE is {MERGED_TOPS_FULL_ENTRY_SIZE}, expected 96.")
        return

    input_file_dateint = 0
    input_file_symbol_idx = 0
    total_input_records_read = 0
    num_snapshots_written = 0

    latest_venue_quotes = {} 
    last_written_bids = [] 
    last_written_asks = []

    try:
        with open(args.input_file, 'rb') as f_in, open(args.output_file, 'wb') as f_out:
            # Write placeholder for the main output file header
            f_out.write(b'\0' * OUTPUT_FILE_HEADER_SIZE)

            # Read the main header of the input merged tops file
            header_bytes = f_in.read(INPUT_FILE_HEADER_SIZE)
            if len(header_bytes) < INPUT_FILE_HEADER_SIZE:
                print(f"Error: Input file '{args.input_file}' is too small to contain a valid header.")
                return
            
            _input_main_feed_id, input_file_dateint, input_total_records_in_file, input_file_symbol_idx = \
                struct.unpack(INPUT_FILE_HEADER_FORMAT, header_bytes)
            
            print(f"Input file ('{args.input_file}') header: DateInt={input_file_dateint}, SymbolIdx={input_file_symbol_idx}, TotalRecords={input_total_records_in_file}")

            # Process each merged entry from the input file
            while True:
                entry_bytes = f_in.read(MERGED_TOPS_FULL_ENTRY_SIZE)
                if not entry_bytes:
                    break
                if len(entry_bytes) < MERGED_TOPS_FULL_ENTRY_SIZE:
                    print(f"Warning: Encountered an incomplete final entry in '{args.input_file}'. Skipping.")
                    break
                
                total_input_records_read += 1
                if total_input_records_read > 0 and total_input_records_read % 10000 == 0:
                    print(f"  Processed {total_input_records_read} input records...")

                original_source_feed_id = struct.unpack_from(MERGED_ENTRY_PREFIX_FEED_ID_FORMAT, entry_bytes, 0)[0]
                
                tops_tuple = struct.unpack_from(TOPS_RECORD_FLAT_PY_FORMAT, entry_bytes, MERGED_ENTRY_PREFIX_FEED_ID_SIZE)
                
                current_record_ts = tops_tuple[0]
                
                _seqno, l1bp, l1ap, l1bq, l1aq, \
                l2bp, l2ap, l2bq, l2aq, \
                l3bp, l3ap, l3bq, l3aq = tops_tuple[1:]

                # Update the latest quote from this venue
                latest_venue_quotes[original_source_feed_id] = {
                    'l1bp': l1bp, 'l1ap': l1ap, 'l1bq': l1bq, 'l1aq': l1aq,
                    'l2bp': l2bp, 'l2ap': l2ap, 'l2bq': l2bq, 'l2aq': l2aq,
                    'l3bp': l3bp, 'l3ap': l3ap, 'l3bq': l3bq, 'l3aq': l3aq
                }

                current_bids, current_asks = create_snapshot_from_latest_quotes(
                    latest_venue_quotes, NUM_LEVELS_TO_SNAPSHOT
                )

                # Write snapshot if the top 3 levels have changed
                if current_bids or current_asks:
                    if current_bids != last_written_bids or current_asks != last_written_asks:
                        write_snapshot_data(f_out, current_record_ts, current_bids, current_asks)
                        num_snapshots_written += 1
                        last_written_bids = current_bids
                        last_written_asks = current_asks
            
            f_out.seek(0)
            f_out.write(struct.pack(OUTPUT_FILE_HEADER_FORMAT, 
                                    PROCESSED_SNAPSHOT_FILE_FEED_ID, 
                                    input_file_dateint, 
                                    num_snapshots_written, 
                                    input_file_symbol_idx))
            
        print(f"Successfully generated snapshot file: '{args.output_file}' with {num_snapshots_written} snapshots.")

    except IOError as e:
        print(f"IOError occurred: {e}")
        return
    except struct.error as e:
        print(f"Struct packing/unpacking error: {e}")
        return

if __name__ == "__main__":
    main()
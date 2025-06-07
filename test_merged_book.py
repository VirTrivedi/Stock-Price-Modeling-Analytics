import os
import struct
import sys
import argparse

# --- Constants ---
HEADER_SIZE = 24
HEADER_FULL_STRUCT_FORMAT = '<QIIQ'

#feed_id prefix
FEED_ID_SIZE = 8
FEED_ID_STRUCT_FORMAT = '<Q'

# Fills Record
FILLS_RECORD_STRUCT_FORMAT = '<QQQ?qIQIIQ?qIqII'
FILLS_RECORD_SIZE = struct.calcsize(FILLS_RECORD_STRUCT_FORMAT)

# Tops Record
TOPS_RECORD_STRUCT_FORMAT = '<QQqqq qqqIII III'
TOPS_RECORD_SIZE = struct.calcsize(TOPS_RECORD_STRUCT_FORMAT)

TIMESTAMP_OFFSET = 0
TIMESTAMP_STRUCT_FORMAT = '<Q'


def read_header_from_file(filepath):
    """
    Reads the header from a given binary file.
    Returns a tuple (header_values, num_recs_from_header) or (None, 0) on error.
    header_values is (feed_id, dateint, num_recs, symbol_idx)
    """
    try:
        with open(filepath, 'rb') as f:
            header_bytes = f.read(HEADER_SIZE)
            if len(header_bytes) < HEADER_SIZE:
                print(f"Error: Could not read full header from {filepath}. File too small.")
                return None, 0
            
            header_values = struct.unpack(HEADER_FULL_STRUCT_FORMAT, header_bytes)
            num_recs_from_header = header_values[2] 
            return header_values, num_recs_from_header
    except IOError as e:
        print(f"Error reading header from {filepath}: {e}")
        return None, 0
    except struct.error as e:
        print(f"Error unpacking header from {filepath}: {e}")
        return None, 0


def read_all_records_and_check_timestamps(filepath, data_record_size):
    """
    Reads all data records from the file, skipping the header.
    Returns a list of timestamps and the total count of records read.
    Also checks if timestamps are sorted.
    Returns (list_of_timestamps, actual_record_count, is_sorted_correctly)
    """
    timestamps = []
    actual_record_count = 0
    is_sorted_correctly = True
    previous_timestamp = 0

    try:
        with open(filepath, 'rb') as f:
            f.seek(HEADER_SIZE)
            while True:
                feed_id_bytes = f.read(FEED_ID_SIZE)
                if not feed_id_bytes:
                    break
                if len(feed_id_bytes) < FEED_ID_SIZE:
                    print(f"Warning: Incomplete feed_id found at end of {filepath}. Expected {FEED_ID_SIZE}, got {len(feed_id_bytes)}. Ignored.")
                    break
                
                payload_bytes = f.read(data_record_size)
                if not payload_bytes:
                    print(f"Warning: Missing payload after feed_id at end of {filepath}. Ignored.")
                    break
                if len(payload_bytes) < data_record_size:
                    print(f"Warning: Incomplete data record found at end of {filepath}. Expected {data_record_size}, got {len(payload_bytes)}. Ignored.")
                    break
                
                current_timestamp = struct.unpack_from(TIMESTAMP_STRUCT_FORMAT, payload_bytes, TIMESTAMP_OFFSET)[0]
                timestamps.append(current_timestamp)
                actual_record_count += 1

                if actual_record_count > 1 and current_timestamp < previous_timestamp:
                    is_sorted_correctly = False
                previous_timestamp = current_timestamp
        return timestamps, actual_record_count, is_sorted_correctly
    except IOError as e:
        print(f"Error reading records from {filepath}: {e}")
        return [], 0, False
    except struct.error as e:
        print(f"Error unpacking record data from {filepath}: {e}")
        return [], actual_record_count, False

def test_merged_file(merged_filepath, expected_data_record_size):
    """
    Tests a single merged file for header consistency and timestamp order.
    Returns True if all tests pass, False otherwise.
    """
    print(f"\n--- Testing merged file: {merged_filepath} ---")
    
    if not os.path.exists(merged_filepath):
        print(f"FAIL: Merged file does not exist: {merged_filepath}")
        return False
    
    file_size = os.path.getsize(merged_filepath)
    if file_size < HEADER_SIZE:
        print(f"FAIL: Merged file is too small to contain a header (size: {file_size} bytes): {merged_filepath}")
        return False

    header_values, num_recs_from_header = read_header_from_file(merged_filepath)
    if header_values is None:
        print(f"FAIL: Could not read or parse header from {merged_filepath}.")
        return False
    
    feed_id, dateint_from_header, _, symbol_idx = header_values
    print(f"  Header Info: Feed ID={feed_id}, DateInt={dateint_from_header}, NumRecsInHeader={num_recs_from_header}, SymbolIdx={symbol_idx}")

    expected_total_data_payload_size = num_recs_from_header * (FEED_ID_SIZE + expected_data_record_size)
    expected_total_file_size = HEADER_SIZE + expected_total_data_payload_size

    if file_size != expected_total_file_size:
        print(f"  Warning: File size ({file_size}) does not match expected size based on header ({expected_total_file_size}). "
              f"Header count: {num_recs_from_header}, Record entry size: {FEED_ID_SIZE + expected_data_record_size}.")

    timestamps_list, actual_num_records_in_file, sorted_correctly = read_all_records_and_check_timestamps(merged_filepath, expected_data_record_size)
    
    print(f"  Actual records found in data portion: {actual_num_records_in_file}")
    
    pass_overall = True

    if num_recs_from_header != actual_num_records_in_file:
        print(f"FAIL: Header record count ({num_recs_from_header}) does not match actual records in file ({actual_num_records_in_file}).")
        pass_overall = False
    else:
        print("  PASS: Header record count matches actual records in file.")

    if actual_num_records_in_file == 0:
        if num_recs_from_header == 0:
             print("  INFO: Merged file contains no data records (header indicates 0 records). This is valid.")
    elif not sorted_correctly:
        print(f"FAIL: Records are NOT sorted correctly by timestamp in {merged_filepath}.")
        pass_overall = False
    else:
        print("  PASS: Records are sorted correctly by timestamp.")
    
    if pass_overall:
        print(f"--- Test PASSED for {merged_filepath} ---")
        return True
    else:
        print(f"--- Test FAILED for {merged_filepath} ---")
        return False

def main():
    """
    Main function to test a single merged file based on command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Test a single merged book .bin file.")
    parser.add_argument("--filepath", required=True, help="Full path to the merged .bin file to test.")
    parser.add_argument("--type", required=True, choices=["fills", "tops"], help="Type of the merged file (fills or tops).")
    
    args = parser.parse_args()

    expected_data_record_size = 0
    if args.type == "fills":
        expected_data_record_size = FILLS_RECORD_SIZE
    elif args.type == "tops":
        expected_data_record_size = TOPS_RECORD_SIZE
    else:
        print(f"Error: Unknown file type '{args.type}'. Cannot determine record size.")
        sys.exit(1)

    if test_merged_file(args.filepath, expected_data_record_size):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
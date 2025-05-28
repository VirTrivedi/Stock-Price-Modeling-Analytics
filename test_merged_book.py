import os
import struct
import sys

# --- Constants ---
HEADER_SIZE = 24
HEADER_FULL_STRUCT_FORMAT = '<QIIQ'

# Fills Record
FILLS_RECORD_SIZE = struct.calcsize('<QQQ?qIQIIQ?qIqII')
# Tops Record
TOPS_RECORD_SIZE = struct.calcsize('<QQqqq qqqIII III')

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


def read_all_records_and_check_timestamps(filepath, record_size):
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
                record_data_bytes = f.read(record_size)
                if not record_data_bytes:
                    break
                if len(record_data_bytes) < record_size:
                    print(f"Warning: Incomplete record found at end of {filepath} (size {len(record_data_bytes)}/{record_size}). Ignored.")
                    break
                
                current_timestamp = struct.unpack_from(TIMESTAMP_STRUCT_FORMAT, record_data_bytes, TIMESTAMP_OFFSET)[0]
                timestamps.append(current_timestamp)
                actual_record_count += 1

                if current_timestamp < previous_timestamp:
                    is_sorted_correctly = False
                previous_timestamp = current_timestamp
        return timestamps, actual_record_count, is_sorted_correctly
    except IOError as e:
        print(f"Error reading records from {filepath}: {e}")
        return [], 0, False
    except struct.error as e:
        print(f"Error unpacking record timestamp from {filepath}: {e}")
        return [], actual_record_count, False


def test_merged_file(merged_filepath, expected_record_size):
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

    timestamps_list, actual_num_records_in_file, sorted_correctly = read_all_records_and_check_timestamps(merged_filepath, expected_record_size)
    
    print(f"  Actual records found in data portion: {actual_num_records_in_file}")
    if num_recs_from_header != actual_num_records_in_file:
        print(f"FAIL: Header record count ({num_recs_from_header}) does not match actual records in file ({actual_num_records_in_file}).")
    else:
        print("  PASS: Header record count matches actual records in file.")

    if actual_num_records_in_file == 0:
        if num_recs_from_header == 0:
             print("  INFO: Merged file contains no data records (header indicates 0 records). This is valid.")
             print(f"--- Test PASSED (for empty file) for {merged_filepath} ---")
             return True

    if not sorted_correctly:
        print(f"FAIL: Records are NOT sorted correctly by timestamp in {merged_filepath}.")
        return False
    
    print("  PASS: Records are sorted correctly by timestamp.")
    
    if num_recs_from_header != actual_num_records_in_file:
        return False

    print(f"--- Test PASSED for {merged_filepath} ---")
    return True


def main_test_suite():
    """
    Main function to run the test suite.
    """
    date = input("Enter the date of the merged files (e.g., YYYYMMDD): ").strip()
    symbol = input("Enter the stock symbol of the merged files (e.g., AAPL): ").strip().upper()

    base_date_path = f"/home/vir/{date}" 
    merged_books_folder = os.path.join(base_date_path, "mergedbooks")

    if not os.path.isdir(merged_books_folder):
        print(f"FAIL: Merged books folder not found: {merged_books_folder}")
        print("Please ensure merged_book.py has been run successfully for the given date and symbol.")
        sys.exit(1)

    all_tests_passed_flag = True

    # Test merged fills file
    merged_fills_filename = f"merged_fills.{symbol}.bin"
    merged_fills_filepath = os.path.join(merged_books_folder, merged_fills_filename)
    if not test_merged_file(merged_fills_filepath, FILLS_RECORD_SIZE):
        all_tests_passed_flag = False

    # Test merged tops file
    merged_tops_filename = f"merged_tops.{symbol}.bin"
    merged_tops_filepath = os.path.join(merged_books_folder, merged_tops_filename)
    if not test_merged_file(merged_tops_filepath, TOPS_RECORD_SIZE):
        all_tests_passed_flag = False

    if all_tests_passed_flag:
        print("\n==========================")
        print("All tests passed successfully!")
        print("==========================")
    else:
        print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("Some tests FAILED.")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!")
        sys.exit(1)


if __name__ == "__main__":
    main_test_suite()
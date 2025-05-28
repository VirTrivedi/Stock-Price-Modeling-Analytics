import os
import struct
import heapq

# --- Constants for Header ---
HEADER_SIZE = 24
RECORD_COUNT_OFFSET = 12
RECORD_COUNT_STRUCT_FORMAT = '<I'
HEADER_FULL_STRUCT_FORMAT = '<QIIQ'

# --- Constants for Record Structures ---
# Fills Record
FILLS_RECORD_STRUCT_FORMAT = '<QQQ?qIQIIQ?qIqII'
FILLS_RECORD_SIZE = struct.calcsize(FILLS_RECORD_STRUCT_FORMAT)

# Tops Record
TOPS_RECORD_STRUCT_FORMAT = '<QQqqq qqqIII III'
TOPS_RECORD_SIZE = struct.calcsize(TOPS_RECORD_STRUCT_FORMAT)

def get_user_input():
    """Prompts the user for date and stock symbol."""
    date = input("Enter the date (e.g., YYYYMMDD): ").strip()
    symbol = input("Enter the stock symbol (e.g., AAPL): ").strip().upper()
    return date, symbol

def find_venue_folders(base_date_path):
    """Finds all venue subdirectories in the base date path."""
    venue_folders = []
    if not os.path.isdir(base_date_path):
        print(f"Error: Base date directory not found: {base_date_path}")
        return venue_folders
    
    for item in os.listdir(base_date_path):
        item_path = os.path.join(base_date_path, item)
        if os.path.isdir(item_path) and item.lower() != "mergedbooks":
            venue_folders.append(item)
    return venue_folders

def read_next_record_with_timestamp(file_handle, record_size):
    """
    Reads one record from the file, extracts its timestamp (assumed to be the first uint64_t).
    Returns (timestamp, record_data_bytes) or None if EOF or incomplete record.
    """
    record_data_bytes = file_handle.read(record_size)
    if len(record_data_bytes) < record_size:
        return None 
    
    timestamp = struct.unpack_from('<Q', record_data_bytes, 0)[0]
    return timestamp, record_data_bytes


def merge_files_for_symbol_by_timestamp(base_date_path, venue_folders, symbol, file_type_suffix, merged_output_folder):
    """
    Merges specific types of book files (fills or tops) for a given symbol across venues,
    ordering records by timestamp.
    """
    merged_filename_key = file_type_suffix.split('_')[1]
    merged_filename = f"merged_{merged_filename_key}.{symbol}.bin"
    merged_filepath = os.path.join(merged_output_folder, merged_filename)

    record_size = 0
    if file_type_suffix == "book_fills":
        record_size = FILLS_RECORD_SIZE
    elif file_type_suffix == "book_tops":
        record_size = TOPS_RECORD_SIZE
    else:
        print(f"Error: Unknown file_type_suffix for merging: {file_type_suffix}")
        return

    source_files_to_process = []
    for venue in venue_folders:
        venue_upper = venue.upper()
        books_folder_path = os.path.join(base_date_path, venue, "books")
        source_filename = f"{venue_upper}.{file_type_suffix}.{symbol}.bin"
        source_filepath = os.path.join(books_folder_path, source_filename)

        if os.path.exists(source_filepath) and os.path.getsize(source_filepath) >= HEADER_SIZE:
            source_files_to_process.append(source_filepath)

    if not source_files_to_process:
        print(f"No '{file_type_suffix}' files found or valid for symbol {symbol} in any venue.")
        return

    file_handles = []
    first_valid_header_data = None
    min_heap = []

    try:
        for i, source_filepath in enumerate(source_files_to_process):
            try:
                fh = open(source_filepath, 'rb')
                header_bytes = fh.read(HEADER_SIZE)
                if len(header_bytes) < HEADER_SIZE:
                    print(f"Warning: Could not read full header from {source_filepath}. Skipping.")
                    fh.close()
                    continue
                
                if first_valid_header_data is None:
                    first_valid_header_data = header_bytes

                file_handles.append(fh)
                
                next_item = read_next_record_with_timestamp(fh, record_size)
                if next_item:
                    timestamp, record_bytes = next_item
                    heapq.heappush(min_heap, (timestamp, record_bytes, fh))
            except IOError as e:
                print(f"Error opening or reading initial data from {source_filepath}: {e}. Skipping.")
                if 'fh' in locals() and fh and not fh.closed:
                    fh.close()
                continue
        
        if not min_heap or first_valid_header_data is None:
            print(f"No data records found to merge for {file_type_suffix} for symbol {symbol}.")
            return

        total_records_merged = 0
        with open(merged_filepath, 'wb') as merged_file_handle:
            merged_file_handle.write(b'\0' * HEADER_SIZE)

            while min_heap:
                timestamp, record_bytes, source_fh = heapq.heappop(min_heap)
                merged_file_handle.write(record_bytes)
                total_records_merged += 1

                next_item = read_next_record_with_timestamp(source_fh, record_size)
                if next_item:
                    new_timestamp, new_record_bytes = next_item
                    heapq.heappush(min_heap, (new_timestamp, new_record_bytes, source_fh))
            
            merged_file_handle.seek(0)
            header_values = list(struct.unpack(HEADER_FULL_STRUCT_FORMAT, first_valid_header_data))
            header_values[2] = total_records_merged
            updated_header_bytes = struct.pack(HEADER_FULL_STRUCT_FORMAT, *header_values)
            merged_file_handle.write(updated_header_bytes)

        if total_records_merged > 0:
            print(f"Successfully merged records from {len(file_handles)} source file(s) into: {merged_filepath}")
            print(f"Total records in merged file: {total_records_merged}")
        else:
            if os.path.exists(merged_filepath):
                os.remove(merged_filepath)
            print(f"No records were ultimately merged into {merged_filepath}.")

    except IOError as e:
        print(f"An IO error occurred during merging for {file_type_suffix} of {symbol}: {e}")
    finally:
        for fh in file_handles:
            if not fh.closed:
                fh.close()
    
    if total_records_merged == 0 and os.path.exists(merged_filepath) and os.path.getsize(merged_filepath) <= HEADER_SIZE :
         if os.path.getsize(merged_filepath) == HEADER_SIZE and open(merged_filepath, 'rb').read() == (b'\0' * HEADER_SIZE):
            try:
                os.remove(merged_filepath)
                print(f"Cleaned up empty or placeholder merged file: {merged_filepath}")
            except OSError as e_rem:
                print(f"Error removing empty merged file {merged_filepath}: {e_rem}")


def main():
    date, symbol = get_user_input()

    base_date_path = f"/home/vir/{date}"
    merged_output_folder = os.path.join(base_date_path, "mergedbooks")

    if not os.path.isdir(base_date_path):
        print(f"Error: Date directory '{base_date_path}' does not exist.")
        return

    try:
        os.makedirs(merged_output_folder, exist_ok=True)
        print(f"Ensured output directory exists: {merged_output_folder}")
    except OSError as e:
        print(f"Error creating output directory '{merged_output_folder}': {e}")
        return

    venue_folders = find_venue_folders(base_date_path)
    if not venue_folders:
        print(f"No venue folders found in '{base_date_path}'.")
        return
    
    print(f"Found venue folders: {', '.join(venue_folders)}")

    # Merge fills files by timestamp
    print(f"\nProcessing 'book_fills' for symbol {symbol} (merging by timestamp)...")
    merge_files_for_symbol_by_timestamp(base_date_path, venue_folders, symbol, "book_fills", merged_output_folder)

    # Merge tops files by timestamp
    print(f"\nProcessing 'book_tops' for symbol {symbol} (merging by timestamp)...")
    merge_files_for_symbol_by_timestamp(base_date_path, venue_folders, symbol, "book_tops", merged_output_folder)

    print("\nScript finished.")


if __name__ == "__main__":
    main()
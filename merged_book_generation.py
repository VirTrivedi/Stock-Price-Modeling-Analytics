import os
import struct
import heapq
import re

# --- Constants ---
HEADER_SIZE = 24
RECORD_COUNT_OFFSET = 12
RECORD_COUNT_STRUCT_FORMAT = '<I'
HEADER_FULL_STRUCT_FORMAT = '<QIIQ'

FILLS_RECORD_STRUCT_FORMAT = '<QQQ?qIQIIQ?qIqII'
FILLS_RECORD_SIZE = struct.calcsize(FILLS_RECORD_STRUCT_FORMAT)

TOPS_RECORD_STRUCT_FORMAT = '<QQqqq qqqIII III'
TOPS_RECORD_SIZE = struct.calcsize(TOPS_RECORD_STRUCT_FORMAT)

def get_user_input_date():
    """Prompts the user for the date."""
    date = input("Enter the date (e.g., YYYYMMDD): ").strip()
    return date

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
    (This function is largely the same as in merged_book.py)
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
        return

    file_handles = []
    first_valid_header_data = None
    min_heap = [] 
    total_records_merged = 0

    try:
        for source_filepath in source_files_to_process:
            try:
                fh = open(source_filepath, 'rb')
                header_bytes = fh.read(HEADER_SIZE) 
                if len(header_bytes) < HEADER_SIZE:
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
                if 'fh' in locals() and fh and not fh.closed:
                    fh.close()
                continue
        
        if not min_heap or first_valid_header_data is None:
            return 

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
            print(f"  Successfully merged {file_type_suffix} for {symbol} into: {merged_filepath} ({total_records_merged} records)")

    except IOError as e:
        print(f"  An IO error occurred during merging {file_type_suffix} for {symbol}: {e}")
    finally:
        for fh in file_handles:
            if not fh.closed:
                fh.close()
    
    if total_records_merged == 0 and os.path.exists(merged_filepath) and os.path.getsize(merged_filepath) <= HEADER_SIZE :
         if os.path.getsize(merged_filepath) == HEADER_SIZE and open(merged_filepath, 'rb').read() == (b'\0' * HEADER_SIZE):
            try:
                os.remove(merged_filepath)
            except OSError as e_rem:
                print(f"  Error removing empty merged file {merged_filepath}: {e_rem}")

def extract_symbols_from_all_venues(base_date_path, venue_folders):
    """
    Extracts unique stock symbols from book_fills and book_tops files
    across all specified venue folders.
    """
    symbols_set = set()
    symbol_pattern = re.compile(r'^[A-Z0-9_-]+\.(?:book_fills|book_tops)\.([A-Z0-9_]+)\.bin$', re.IGNORECASE)

    for venue in venue_folders:
        books_folder_path = os.path.join(base_date_path, venue, "books")
        if not os.path.isdir(books_folder_path):
            continue
        
        for filename in os.listdir(books_folder_path):
            match = symbol_pattern.match(filename)
            if match:
                symbols_set.add(match.group(1).upper())
                
    return sorted(list(symbols_set))


def main():
    """
    Main function to drive the batch merging process for all symbols for a given date.
    """
    date = get_user_input_date()

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

    all_symbols = extract_symbols_from_all_venues(base_date_path, venue_folders)
    if not all_symbols:
        print(f"No symbols found across any venues in '{base_date_path}'.")
        return
    print(f"Found {len(all_symbols)} unique symbols to process: {', '.join(all_symbols)}")

    for i, symbol in enumerate(all_symbols):
        print(f"\n[{i+1}/{len(all_symbols)}] Processing symbol: {symbol}")
        
        # Merge fills files by timestamp
        merge_files_for_symbol_by_timestamp(base_date_path, venue_folders, symbol, "book_fills", merged_output_folder)

        # Merge tops files by timestamp
        merge_files_for_symbol_by_timestamp(base_date_path, venue_folders, symbol, "book_tops", merged_output_folder)

    print("\nBatch merging script finished.")


if __name__ == "__main__":
    main()
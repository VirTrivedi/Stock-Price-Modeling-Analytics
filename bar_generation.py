import os
import subprocess
from parse_book_fills import process_file as process_fills_file

def process_to_books(folder_path):
    """Processes raw .bin files into books and stores them in the books/ folder."""
    input_folder = folder_path
    output_folder = os.path.join(folder_path, "books/")
    
    os.makedirs(output_folder, exist_ok=True)  # Ensure the books folder exists

    if not os.path.isdir(input_folder):
        print(f"Error: {input_folder} is not a valid directory.")
        return

    # Loop through all .bin files in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".bin") and "book_events" in file_name:
            input_file_path = os.path.join(input_folder, file_name)
            
            print(f"Processing raw file into book: {file_name}")
            cpp_executable = "/home/vir/histbook/build/bin/HistBook"  # Path to the compiled C++ executable for processing raw files
            try:
                subprocess.run(
                    [cpp_executable, "--outputpath", output_folder, "--inputpath", input_file_path],
                    check=True
                )
            except subprocess.CalledProcessError as e:
                print(f"Error processing raw file {file_name} with C++ code: {e}")

def process_to_bars(folder_path, date, feed):
    """Processes all .bin files in the given folder using the appropriate script."""
    input_folder = os.path.join(folder_path, "books/")
    output_folder = os.path.join(folder_path, "bars/")
    
    os.makedirs(output_folder, exist_ok=True)  # Ensure the output folder exists

    if not os.path.isdir(input_folder):
        print(f"Error: {input_folder} is not a valid directory.")
        return

    # Loop through all .bin files in the input folder
    for file_name in os.listdir(input_folder):
        if file_name.endswith(".bin"):
            symbol = file_name.split(".")[2]            
            
            # Determine if the file is a fills or tops file based on its name
            if "fills" in file_name.lower():
                print(f"Processing fills file: {file_name}")
                process_fills_file(
                    date=date,
                    feed=feed,
                    symbol=symbol,
                    plot=False)
            elif "tops" in file_name.lower():
                print(f"Processing tops file: {file_name}")
                # Call the C++ executable for processing tops files
                cpp_executable = "./process_tops"  # Path to the compiled C++ executable
                try:
                    subprocess.run(
                        [cpp_executable, date, feed, symbol],
                        check=True
                    )
                except subprocess.CalledProcessError as e:
                    print(f"Error processing tops file {file_name} with C++ code: {e}")            
            else:
                print(f"Skipping unrecognized file: {file_name}")

    print(f"Processing complete. Processed files are saved in {output_folder}.")

if __name__ == "__main__":
    date = input("Enter file date (yearMonthDay): ")
    feed = input("Enter file feed: ")

    folder_path = f"/home/vir/{date}/{feed.lower()}/"

    if os.path.isdir(folder_path):
        # Step 1: Process raw files into books
        process_to_books(folder_path)
        # Step 2: Process books into bars
        process_to_bars(folder_path, date, feed)
    else:
        print(f"Error: {folder_path} is not a valid directory.")
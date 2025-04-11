import os
from parse_book_fills import process_file as process_fills_file
from parse_book_tops import process_file as process_tops_file

def process_folder(folder_path, date, feed):
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
            file_path = os.path.join(input_folder, file_name)
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
                process_tops_file(
                    date=date,
                    feed=feed,
                    symbol=symbol,
                    plot=False)
            else:
                print(f"Skipping unrecognized file: {file_name}")

    print(f"Processing complete. Processed files are saved in {output_folder}.")

if __name__ == "__main__":
    date = input("Enter file date (yearMonthDay): ")
    feed = input("Enter file feed: ")

    folder_path = f"/home/vir/{date}/{feed.lower()}/"

    if os.path.isdir(folder_path):
        process_folder(folder_path, date, feed)
    else:
        print(f"Error: {folder_path} is not a valid directory.")
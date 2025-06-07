import os
import subprocess
import argparse
import re
import sys

def main():
    parser = argparse.ArgumentParser(
        description="Batch process merged_tops files using the process_merged_tops.py script."
    )
    parser.add_argument(
        "--input-folder",
        required=True,
        help="Path to the folder containing merged_tops.SYMBOL.bin files."
    )
    parser.add_argument(
        "--output-folder",
        required=True,
        help="Path to the folder where processed_tops.SYMBOL.bin files will be saved."
    )
    parser.add_argument(
        "--script-path",
        default="process_merged_tops.py",
        help="Path to the process_merged_tops.py script."
    )
    args = parser.parse_args()

    input_folder = args.input_folder
    output_folder = args.output_folder
    python_script_path = args.script_path

    if not os.path.isdir(input_folder):
        print(f"Error: Input folder not found: {input_folder}")
        return

    if not os.path.isfile(python_script_path):
        batch_script_dir = os.path.dirname(os.path.abspath(__file__))
        potential_path = os.path.join(batch_script_dir, os.path.basename(python_script_path))
        if os.path.isfile(potential_path):
            python_script_path = potential_path
        else:
            print(f"Error: Python script not found at '{python_script_path}' or '{potential_path}'. "
                  "Please provide the correct path using --script-path.")
            return
    
    python_executable = sys.executable

    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder: {os.path.abspath(output_folder)}")

    file_pattern = re.compile(r"^merged_tops\.([a-zA-Z0-9_]+)\.bin$")

    processed_count = 0
    skipped_count = 0

    print(f"\nProcessing files from: {os.path.abspath(input_folder)}")
    print(f"Using Python script: {os.path.abspath(python_script_path)}")
    print(f"With Python interpreter: {python_executable}")

    for filename in os.listdir(input_folder):
        match = file_pattern.match(filename)
        if match:
            symbol = match.group(1)
            input_filepath = os.path.join(input_folder, filename)
            output_filename = f"processed_tops.{symbol}.bin"
            output_filepath = os.path.join(output_folder, output_filename)

            print(f"\nFound matching file: {filename}")
            print(f"  Input: {input_filepath}")
            print(f"  Output: {output_filepath}")

            command = [
                python_executable,
                python_script_path,
                "--input-file",
                input_filepath,
                "--output-file",
                output_filepath
            ]

            try:
                print(f"  Executing: {' '.join(command)}")
                result = subprocess.run(command, check=True, capture_output=True, text=True)
                if result.stdout and result.stdout.strip():
                    print(f"  Stdout from {os.path.basename(python_script_path)}:\n{result.stdout.strip()}")
                if result.stderr and result.stderr.strip():
                     print(f"  Stderr from {os.path.basename(python_script_path)}:\n{result.stderr.strip()}")
                print(f"  Successfully processed {filename}")
                processed_count += 1
            except subprocess.CalledProcessError as e:
                print(f"  Error processing {filename} with {os.path.basename(python_script_path)}:")
                print(f"    Return code: {e.returncode}")
                if e.stdout and e.stdout.strip():
                    print(f"    Stdout:\n{e.stdout.strip()}")
                if e.stderr and e.stderr.strip():
                    print(f"    Stderr:\n{e.stderr.strip()}")
                skipped_count += 1
            except Exception as e:
                print(f"  An unexpected error occurred while trying to run {os.path.basename(python_script_path)} for {filename}: {e}")
                skipped_count += 1
        else:
            pass

    print(f"\nBatch processing complete.")
    print(f"Successfully processed: {processed_count} files.")
    print(f"Skipped or failed: {skipped_count} files.")

if __name__ == "__main__":
    main()
import pandas as pd
import sys
import os

def convert_csv_to_parquet(csv_file):
    # Check if the file exists
    if not os.path.exists(csv_file):
        print(f"File '{csv_file}' not found.")
        return

    # Read CSV file using pandas
    data = pd.read_csv(csv_file)

    # Create the output directory if it doesn't exist
    output_dir = '../parquet/'
    os.makedirs(output_dir, exist_ok=True)

    # Create output file name by replacing the extension to .parquet and specify the output directory
    parquet_file = os.path.join(output_dir, os.path.splitext(os.path.basename(csv_file))[0] + '.parquet')

    # Write the DataFrame to a Parquet file
    data.to_parquet(parquet_file)
    print(f"Converted '{csv_file}' to '{parquet_file}'.")

if __name__ == "__main__":
    # Check if there are arguments provided
    if len(sys.argv) < 2:
        print("Please provide the CSV file(s) to convert.")
        sys.exit(1)

    # Iterate through each provided file
    for csv_file_arg in sys.argv[1:]:
        convert_csv_to_parquet(csv_file_arg)

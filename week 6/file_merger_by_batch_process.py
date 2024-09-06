import pandas as pd
import os

__author__ = 'Darshan Dhanani'

ROOT_DIR = 'D:/jainam/nifty_data/1/1'
WRITE_FILE_NAME = 'data.csv'
CHUNK_SIZE = 100  # Number of files to process in a batch

def read_file_data(file_path):
    return pd.read_csv(file_path)

def write_file(dataframe):
    mode = 'a' if os.path.isfile(WRITE_FILE_NAME) else 'w'
    header = not os.path.isfile(WRITE_FILE_NAME)
    dataframe.to_csv(WRITE_FILE_NAME, mode=mode, header=header, index=None)
    file_stats = os.stat(WRITE_FILE_NAME)
    return file_stats.st_size

def process_files_in_batches(file_paths):
    batch = []
    for file_path in file_paths:
        batch.append(read_file_data(file_path))
        if len(batch) == CHUNK_SIZE:
            combined_df = pd.concat(batch, ignore_index=True)
            file_size = write_file(combined_df)
            print(f"Processed {len(batch)} files, current file size: {file_size} bytes")
            if file_size > 2500000000:
                return True  # Stop processing if file size exceeds limit
            batch = []
    if batch:
        combined_df = pd.concat(batch, ignore_index=True)
        file_size = write_file(combined_df)
        print(f"Processed {len(batch)} files, current file size: {file_size} bytes")
        if file_size > 2500000000:
            return True  # Stop processing if file size exceeds limit
    return False

def main():
    for root, dirs, files in os.walk(ROOT_DIR):
        if os.path.basename(root) == 'BANKNIFTY':
            file_paths = [os.path.join(root, file) for file in files]
            stop_processing = process_files_in_batches(file_paths)
            if stop_processing:
                break

if __name__ == "__main__":
    main()

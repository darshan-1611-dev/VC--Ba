import pandas as pd
import os 

__author__ = 'Darshan Dhanani'

ROOT_DIR = 'D:/jainam/nifty_data/1/1'
WRITE_FILE_NAME = 'data.csv'

def read_file_data(file_path):
    return pd.read_csv(file_path)

def write_file(dataframe):
    if not os.path.isfile(WRITE_FILE_NAME):
        dataframe.to_csv(WRITE_FILE_NAME, index=None)
    else: # else it exists so append without writing the header
        dataframe.to_csv(WRITE_FILE_NAME, mode='a', header=False, index=None)
    
    file_stats = os.stat(WRITE_FILE_NAME)
    
    return file_stats.st_size

    
for root, dirs, files in os.walk(ROOT_DIR):
        if os.path.basename(root) == 'BANKNIFTY':
            for file in files:
                file_path = root + '/' + file
                file_data = read_file_data(file_path)
                file_size = write_file(file_data)
                print(file_size)
                if file_size > 2500000000: # if file size more than 2.5GB than break the process
                    break
            else:
                continue  # only executed if the inner loop did NOT break
            break     
                
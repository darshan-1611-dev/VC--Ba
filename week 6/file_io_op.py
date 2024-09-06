import dask.dataframe as dd
import time
import os
import yaml

__author__ = 'Darshan Dhanani'

FILE_NAME = "data.csv"

start_time = time.time()

def column_name_modification(df):
    df.columns.tolist()

    new_column_list = []

    for string  in df.columns.tolist():
        new_column_list.append(''.join(e for e in string if e.isalnum()).lower())

    df.columns = new_column_list
    
    return df
    
    
df = dd.read_csv(FILE_NAME)  # dask 

df = column_name_modification(df)  # change column name 

# write in yml file
d = df.columns.tolist()
with open('file.yaml', 'w') as yaml_file:
    yaml.dump(d, yaml_file, default_flow_style=False)
    

execution_time = time.time() - start_time
print("---------------------------------------------")
print(f"Execution Time: {execution_time:.4f} seconds")

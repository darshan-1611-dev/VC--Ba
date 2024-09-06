import numpy as np
import pandas as pd
from sklearn.datasets import load_wine

BASE_LOCATION = 'dataset'  # Base file location

'''
convert data to pandas DataFrame
'''
def convert_to_df_and_download(data, file_name):
    data1 = pd.DataFrame(data= np.c_[data['data'], data['target']],
                     columns= data['feature_names'] + ['target'])
    
    data1.to_csv(f'{BASE_LOCATION}/{file_name}')  # download data      
                     
                     
data = load_wine() # load wine data from sklearn 
file_name = "wine_data.csv"

convert_to_df_and_download(data, file_name)

print(f"Data Download Sucessfully at {BASE_LOCATION}/{file_name}")
          

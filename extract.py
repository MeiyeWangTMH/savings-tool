#Import libraries
import csv
import pandas as pd
from pandas import *
import numpy as np
import yaml
import glob
import dateutil.parser
from datetime import datetime, timedelta
import time
import os

#Input paths
PATH_INPUT = 'data/input'
PATH_OUTPUT = 'data/output'

#Optional selection, if not all data should be extracted
filter_cols = False #False = "all data is extracted"
cols = ['meter_values_timestamp','charge_current','charge_offer','charge_power','charger_id','connector_id']

#Functions
def read_input(path):
    """
    Read data and delete all unnecessary columns of site index
    """
    filepath = glob.glob(f'{path}/{PATH_INPUT}/*'+'.csv')
    df = pd.read_csv(filepath[0], delimiter = ",", doublequote = True , encoding = "utf-8")

    #formatting: df['timestamp'] to datetime + converting the timezone
    #df['@timestamp'] = pd.to_datetime(df['@timestamp'], errors = 'coerce')
    #df['@timestamp'] = df['@timestamp'].dt.tz_convert('Europe/Berlin')
    #df['@timestamp'] = df['@timestamp'].dt.tz_localize(None)


    #override_max_limit to MW
    if 'override_max_limit' in df.columns:
        df["override_max_limit"] = pd.to_numeric(df["override_max_limit"], errors = 'coerce')

    #Removing the unnecessary last two columns to decrease the size of the dataframe
    if 'total_power' in df.columns:
        df.drop('total_power', axis=1, inplace=True)
    if 'total_unmanaged_power' in df.columns:
        df.drop('total_unmanaged_power', axis=1, inplace=True)
    if 'override_max_limit' in df.columns:
        df.drop('override_max_limit', axis=1, inplace=True)
    
    #filerting the data
    df1 = df[df['evs'] != '[]']
    df1 = df1[df1['evs'].notna()]
    df1 = df1[df1['evs'] != 'evs']

    return df1

def extraction(df):
    """
    In the raw data, extracts the tupel in the field "evs"
    """
    print("Extract column '"'evs'"''...")
    e = [yaml.load(x, Loader = yaml.FullLoader) for x in df['evs']]
    return e

def range_of_data(df,date_start,date_end):
    """
    Optional: filter range to save computing time
    """
    df1 = df[df['@timestamp'] >= date_start]
    df1 = df1[df1['@timestamp'] < date_end]

    return df1

def merge_stationen(e):
    stationen = pd.concat([pd.DataFrame(item) for item in e])
    return stationen

def create_cp_id(stationen):
    """
    Creates new data point "charge_point_id", specific identifier for each charge point
    """
    stationen['connector_id'] = np.where(pd.isnull(stationen['connector_id']),1.0,stationen['connector_id']) #Old data did have "connector_id", therefore it is '1.0' instead of null
    stationen['connector_id'] = stationen['connector_id'].apply(str).astype(float)
    stationen['charge_point_id']=stationen['charger_id']+'_'+stationen['connector_id'].apply(str)
    return stationen


def create_csv(stationen, filter_cols, cols):
    """
    Creates extracted output file for each charge point
    """
    print("Save charge point files...")
    a = stationen['charge_point_id'].unique()

    #creating individual csv-files - for each station
    for item in a:
        a = stationen[stationen['charge_point_id'] == item]
        a = a.sort_values('meter_values_timestamp')
        if filter_cols == True:
            a = a[cols]
            
        a.to_csv('Ladepunkt_' + str(item) + '_2020.csv', sep = ';', decimal = ',', index = False)
        
    return a


def define_result_folder_name():
    """
    Define folder name: output + datatime
    """
    
    timestamp = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')

    
    folder = f'output_{timestamp}'
    
    return folder

def make_result_folder():
    """
    Make directory
    """
    
    if not os.path.exists(PATH_OUTPUT):
        os.makedirs(PATH_OUTPUT)

    folder = define_result_folder_name()
    os.mkdir(f'{PATH_OUTPUT}/{folder}')
    
    return folder

def save_results(stationen):
    """
    Convert tool results into dataframe and save all
    data in new defined folder
    """
    
    print("Save charge point files...")
    
    a = stationen['charge_point_id'].unique()
    
    key = 0
    
    folder = make_result_folder()

    #creating individual csv-files - for each station
    for item in a:
        df = stationen[stationen['charge_point_id'] == item]
        df = df.sort_values('meter_values_timestamp')
        if filter_cols == True:
            df = df[cols]
              
        df.to_csv(f'{PATH_OUTPUT}/{folder}/charge_point_{str(item)}.csv',sep=';',decimal=',', index = False)
        
    
    return folder


def master(path, filtering,date_start,date_end):
    """
    Extracts and saves all available data raw data index file. Data is stored charge point specific.
    """
    df = read_input(path)
    if filtering == True:
        df = range_of_data(df,date_start,date_end)
        
    e = extraction(df)
    stationen = merge_stationen(e)
    stationen =  create_cp_id(stationen)
    folder = save_results(stationen)

        
    return folder
    

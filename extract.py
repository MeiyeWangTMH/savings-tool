#Import libraries
import pandas as pd
import numpy as np
import yaml
import glob
import os
import os
import fnmatch


#Input paths
# PATH_INPUT = 'data/input/'
# PATH_OUTPUT = 'data/output/'


#Functions
def read_input(df,columnToUnpack):
    """
    Read data and delete all unnecessary columns of site index
    """

    #formatting: df['timestamp'] to datetime + converting the timezone
    #df['@timestamp'] = pd.to_datetime(df['@timestamp'], errors = 'coerce')
    #df['@timestamp'] = df['@timestamp'].dt.tz_convert('Europe/Berlin')
    #df['@timestamp'] = df['@timestamp'].dt.tz_localize(None)

    #override_max_limit to MW
    if 'override_max_limit' in df.columns:
        df["override_max_limit"] = pd.to_numeric(df["override_max_limit"], errors = 'coerce')

    #Removing the unnecessary last two columns to decrease the size of the dataframe
    # if 'total_power' in df.columns:
       # df.drop('total_power', axis=1, inplace=True)
    if 'total_unmanaged_power' in df.columns:
        df.drop('total_unmanaged_power', axis=1, inplace=True)
    # if 'override_max_limit' in df.columns:
    #    df.drop('override_max_limit', axis=1, inplace=True)

    #filerting the data
    df1 = df[df[columnToUnpack] != '[]']
    df1 = df1[df1[columnToUnpack].notna()]
    df1 = df1[df1[columnToUnpack] != columnToUnpack]

    df1 = df1.reset_index(drop=True)

    return df1

def extraction(df,columnToUnpack,otherColumn):
    """
    In the raw data, extracts the tuple in the field "evs"
    """
    print("Extract column " + columnToUnpack + " ...")
    #e = [yaml.load(x, Loader = yaml.FullLoader) for x in df['evs']]
    if columnToUnpack == "request":
        df['request'] = '[ ' + df['request'].astype(str) + ']'
    e = [pd.DataFrame(yaml.load(x, Loader=yaml.FullLoader)) for x in df[columnToUnpack]]
    for i in range(len(e)):
        e[i] = pd.DataFrame(e[i])
        for col in otherColumn:
            e[i][col] = df.loc[i, col]
        # e[i]['gridlimit_kW'] = df.loc[i, 'override_max_limit']
        # e[i]['total_power_site'] = df.loc[i, 'total_power']

    return e

def range_of_data(df,date_start,date_end):
    """
    Optional: filter range to save computing time
    """
    df1 = df[df['@timestamp'] >= date_start]
    df1 = df1[df1['@timestamp'] < date_end]

    return df1

def merge_stationen(e):
    stationen = pd.concat([item for item in e])

    #Convert gridlimit from Ampere to kW
    # stationen['gridlimit_kW'] = pd.to_numeric(stationen['gridlimit_kW'])
    # stationen['gridlimit_kW'] = stationen['gridlimit_kW']/1000*230*3
    return stationen

def create_cp_id(stationen,filter_replace_id =False ,to_rep=dict()):
    """
    Creates new data point "charge_point_id", specific identifier for each charge point
    """
    if filter_replace_id:

        stationen['charge_point_id'] = stationen['charger_id']
        stationen.replace({'charge_point_id': to_rep}, inplace=True)
        stationen = stationen.copy(deep=True)

    else:
        stationen['connector_id'] = np.where(pd.isnull(stationen['connector_id']), 1.0, stationen[
            'connector_id'])  # Old data did have "connector_id", therefore it is '1.0' instead of null
        stationen['connector_id'] = stationen['connector_id'].apply(str).astype(float)
        stationen['charge_point_id'] = stationen['charger_id'] + '_' + stationen['connector_id'].apply(str)
    return stationen




def define_result_folder_name():
    """
    Define folder name: output + datatime
    """
    
    timestamp = pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')

    
    folder = 'output_'+str(timestamp)
    
    return folder

def make_result_folder(PATH_OUTPUT):
    """
    Make directory
    """
    
    if not os.path.exists(PATH_OUTPUT):
        os.makedirs(PATH_OUTPUT)

    folder = define_result_folder_name()
    path = os.path.join(PATH_OUTPUT, folder)
    os.mkdir(path)
    
    return folder

def save_results(stationen,PATH_OUTPUT,typ,site,filter_cols=False,cols=[], filter_aggregated=False,format='csv',projectName=""):
    """
    Convert tool results into dataframe and save all
    data in new defined folder
    """
    
    print("Save charge point files...")
    key = 0
    
    folder = make_result_folder(PATH_OUTPUT)
    path = os.path.join(PATH_OUTPUT, folder)
    date = pd.Timestamp.now().strftime('%Y-%m-%d')
    if filter_aggregated:

        df = stationen
        df = df.sort_values('@timestamp')
        if filter_cols == True:
            df = df[cols]
        if format == 'excel':
            df.to_excel(path + '/'+ typ +' - ' +  site + ' - '+ date +'.xlsx', index=False)
        else:
            df.to_csv(path + '/'+ typ +' - ' +  site + ' - '+ date +'.csv', sep=';', decimal='.', index=False)


    else:
        a = stationen['charge_point_id'].unique()
    #creating individual csv-files - for each station
        for item in a:
            df = stationen[stationen['charge_point_id'] == item]
            df = df.sort_values('@timestamp')
            if filter_cols == True:
                df = df[cols]
              
            df.to_csv(path+'/Charging Data - '+str(item)+'.csv',sep=';',decimal='.', index = False)

    
    return folder


def master(path,PATH_INPUT,PATH_OUTPUT,
           typeList,
           filter_cols,cols,
           filter_time,date_start,date_end,
           filter_replace_id, chargeridDic,
           filter_aggregated, format, projectName):
    """
    Extracts and saves all available data raw data index file. Data is stored charge point specific.
    """
    for typ in typeList:
        print(typ)
        if typ == "Error Messages":
            columnToUnpack = "request"
            otherColumn = ['charger_id','action','@timestamp']

            for file in os.listdir(path + PATH_INPUT):

                if fnmatch.fnmatch(file, typ + '*.csv'):
                    # print(file)
                    site = file.split('_')[1]
                    df = pd.read_csv(path + PATH_INPUT + file, delimiter=",", doublequote=True, encoding="utf-8")

                    df = read_input(df, columnToUnpack)
                    if filter_time == True:
                        df = range_of_data(df, date_start, date_end)

                    e = extraction(df, columnToUnpack, otherColumn)
                    stationen = merge_stationen(e)
                    if filter_replace_id == True:
                        to_rep = chargeridDic[site]
                    else :
                        to_rep = {}
                    stationen = create_cp_id(stationen, filter_replace_id, to_rep)
                    folder = save_results(stationen, PATH_OUTPUT, typ, site, filter_cols, cols, filter_aggregated,
                                          format, projectName)

        elif typ == "Charging Data":
            columnToUnpack = "evs"
            otherColumn = ['override_max_limit','@timestamp']
            for file in os.listdir(path + PATH_INPUT):

                if fnmatch.fnmatch(file, typ + '*.csv'):
                    # print(file)
                    site = file.split('_')[1]
                    df = pd.read_csv(path + PATH_INPUT + file, delimiter=",", doublequote=True, encoding="utf-8")

                    df = read_input(df, columnToUnpack)
                    if filter_time == True:
                        df = range_of_data(df, date_start, date_end)

                    e = extraction(df, columnToUnpack, otherColumn)
                    stationen = merge_stationen(e)
                    if filter_replace_id == True:
                        to_rep = chargeridDic[site]
                    else :
                        to_rep = {}

                    stationen = create_cp_id(stationen, filter_replace_id, to_rep)
                    folder = save_results(stationen, PATH_OUTPUT, typ, site, filter_cols, cols, filter_aggregated,
                                          format, projectName)

        else:
            print('typ not in default list, choose between "Error Messages" and "Charging Data"')
    return folder
    

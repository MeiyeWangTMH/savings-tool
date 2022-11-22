#Import libraries
import time
import os

from extract import *
from prep import *
try:
    from analyse import*
except:
    pass
import parsejason

# # name the input file with "Charging Data _ Site Name _ Date.csv" or "Error Messages _ Site Name _ Date .csv"
# For St. Louis, SiteName ="St. Louis", JsonParse = True, change the typeList, filter_aggregated= True, format = 'excel' ,filter_time = False ,
# filter_replace_id = True, filter_cols = False
#Input
SiteName= "St. Louis"
# SiteName = "Ocean View"
# SiteName = "KAT"

# date = pd.Timestamp.now().strftime('%Y-%m-%d')
date="2021-12-09"
# SiteName = 'OceanView-June'

# path =r'C:\Users\Lap\OneDrive - The Mobility House GmbH\Dokumente\tmh-site-data-preprocessing'
# path ='/Users/sarahwoogen/PycharmProjects/tmh-site-data-preprocessing/'
path = '/Users/meiyewang/Documents/Tools/Savings Model/'
os.chdir(path)

#Specify Paths
PATH_INPUT = 'data/'+ SiteName + '/' + date  + '/Input/'
PATH_OUTPUT = 'data/'+ SiteName+ '/' + date +'/Output/'

jsonParse = True

# typeList = ['Error Messages','Charging Data']
typeList = ['Charging Data']

#Setting up optional tool function
filter_aggregated= True
format = 'excel'

#Should the range of the data be adapted
filter_time = False #Not applied if false
date_start = "2020-01-01"
date_end   = "2021-08-01" #this date will be first day to not be included

#timezone
timezone = 'US/Pacific' #options e.g. : 'UTC' or 'Europe/Berlin'

# replace charge id with Charge Pilot ID
filter_replace_id = True
if not filter_replace_id:
    chargeridDict = {}

#Optional selection, if not all data should be extracted
filter_cols = False #False = "all data is extracted"
cols = ['meter_values_timestamp','charge_current','charge_offer','charge_power','charger_id','connector_id']


if not os.path.exists(path + 'data/'+SiteName):
    os.mkdir(path +'data/' + SiteName)
    os.mkdir(path +'data/'+ SiteName + '/' + date)
    os.mkdir(path +PATH_INPUT)
    os.mkdir(PATH_OUTPUT)

if not os.path.exists(path +'data/'+ SiteName + '/' + date):
    os.mkdir(path +'data/' + SiteName + '/' + date)
    os.mkdir(path +PATH_INPUT)
    os.mkdir(path +PATH_OUTPUT)


#Revising the data - Should missing Logs be calculated #Not applied if false
reviseData = False

#Cleaning up the data - Should data be cleaned #Not applied if false
cleanData = False
#Delete all charging events, who do not exceed these limits:
minimum_plugin_duration = 5.0   # in Min
minimum_charge_power = 100.0      # in W
minimum_energy = 100.0          # in Wh

#Creation discrete data
resolution = 15 #Resolution of interval in min

#Create Optimization Input
optimization = False #Not applied if false

#Analyzing data and create graphs and file output
analyse = False #Not applied if false

#Main mehtod
#Algorithm is split up in separate sections
start = time.time()

# Specify
if SiteName == "St. Louis":
    siteidDict = {"a47969b8-2074-48a0-b5d4-4d4f4746f5d7":"St Louis North Area",
                        "e0d907d9-4161-48d3-9646-acbf2e23bc15":"St Louis South Area",
                         "13e62ca5-995b-4cbb-b88e-ebd5b433f44d": "St Louis North Broadway",
                         "5e8b6303-04e1-45d9-af1d-7eb9e2a415b6":"St Louis Main Depot CNG"
                        }

    chargeridDict = {
        "St Louis North Broadway":  dict(zip(['HVC150-US1-5020-742', 'HVC150-US1-5020-743', 'HVC150-US1-5120-955'],
                                             ['OVH Charger 1', 'OVH Charger 2', 'OVH Charger 3'])),
        "St Louis South Area": dict(zip(['HVC150-US1-3720-011', 'HVC150-US1-3820-199',
                                         'HVC150-US1-3720-012','HVC150-US1-3620-826',
                                         'HVC150-US1-3520-626','HVC150-US1-3620-827',
                                         'HVC150-US1-3420-264','HVC150-US1-3520-483',
                                         'HVC150-US1-3520-484'],
                                        ['C01', 'CO2', 'C03','C04','C05','C06','C07','C08','C09'])),
        "St Louis North Area": dict(zip(['HVC150-US1-3220-091', 'HVC150-US1-3520-625', 'HVC150-US1-3820-151',
                                         'HVC150-US1-3820-200','HVC150-US1-3820-150','HVC150-US1-3820-153',
                                         'HVC150-US1-3820-202','HVC150-US1-3220-092','HVC150-US1-3720-981'],
                                        ['C10', 'C11', 'C12','C13','C14','C15','C16','C17','C18'])),
        "St. Louis Main Depot CNG": dict(zip(['HVC150-US1-3020-685', 'HVC150-US1-2030-686'],
                                             ['CNG1', 'CNG2']))
    }

elif SiteName == "Ocean View":
    siteidDict = {"813b8ba7-a6f9-4cfb-a0a0-8969febf69b0":"Ocean View",
                    # "913a175b-92e5-4fde-aed1-3819426d7397":"Stockton"
                        }
    chargeridDict = {
        "Ocean View":  dict(zip(['MH0005', 'MH0006', 'MH0007','MH0018'],
                                             ['Charger 1', 'Charger 2', 'Charger 3','Charger 4'])),

    }

elif SiteName == "KAT" :
    siteidDict = {"3a696ed8-a8a8-49f4-87fa-6fde9593217e": SiteName}
    chargeridDict = { # device id : name
    "KAT": dict(zip(['0001870604E5', '000187060512', '00018706050F', '00018706093E','00018705F4AA',
                     '00018705C963','00018705F4FE','00018705F9DB','00018705C91B','00018705B214',
                     '0001870604E8','00018705F4D1'],
                           ['KAT 1_1', 'KAT 1_2', 'KAT 2_1', 'KAT 2_2', 'KAT 3_1', 'KAT 3_2',
                            'KAT 4_1', 'KAT 4_2', 'KAT 5_1', 'KAT 5_2','KAT 6_1', 'KAT 6_2'
                            ]))}



if jsonParse  == True:
    if SiteName == "St. Louis":
        inputDir= '/Users/meiyewang/The Mobility House GmbH/The Mobility House - USA/Customers/Sold' \
              '/St Louis Metro/Operations/Ad-hoc Data Requests/Data Query from Kibana/'
        inputpath = inputDir + SiteName + '/' + date
        outputpath = 'data/' + SiteName + '/' + date + '/'
    if SiteName == "Ocean View":
        inputDir = '/Users/meiyewang/The Mobility House GmbH/The Mobility House - USA/Customers/Sold/'\
                   'Ocean View School District/Operations/Data Reporting/SCE CR/Raw Data'
        inputpath = inputDir + '/' + date
        outputpath = 'data/'+ SiteName + '/' + date  + '/'
    if SiteName == "KAT":
        inputDir = '/Users/meiyewang/The Mobility House GmbH/The Mobility House - USA/Customers/Sold'\
                   '/Knoxville Area Transit/Operations/Kibana'
        inputpath = inputDir + '/' + date
        outputpath = 'data/'+ SiteName + '/' + date  + '/'
        inputpath = inputDir + '/' + date
        outputpath = 'data/' + SiteName + '/' + date + '/'
    parsejason.main(inputpath,outputpath,siteidDict,typeList )
    os.chdir(path)
    
#Data extraction
print("")
print("Start data extraction...")
folder,df = master(path,PATH_INPUT,PATH_OUTPUT,
                typeList,
                filter_cols,cols,
                filter_time, date_start, date_end,
                filter_replace_id, chargeridDict,
                filter_aggregated,format,SiteName)#`    `q21    `

# print(df)

import numpy as np

from datetime import datetime
import pandas as pd
import warnings
 
from datetime import timedelta

   
warnings.filterwarnings('ignore')



def getSessionAndInterval(df,year,monthStart,monthEnd):
    timezone = 'US/Pacific'
    df['meter_values_timestamp'] = pd.to_datetime(df['meter_values_timestamp'], errors='coerce',utc=True)
    # df['meter_values_timestamp'] = df['meter_values_timestamp'].dt.tz_localize("UTC")
    df['meter_values_timestamp'] = df['meter_values_timestamp'].dt.tz_convert(timezone)
    df['meter_values_timestamp']= df['meter_values_timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")

    df['plugin_time'] = pd.to_datetime(df['plugin_time'], utc=True)
    # df['plugin_time'] = df['plugin_time'].dt.tz_localize("UTC")
    df['plugin_time'] = df['plugin_time'].dt.tz_convert(timezone)
    df['plugin_time'] = df['plugin_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
    df['plugin_time'] = pd.to_datetime(df['plugin_time'])
    # print(df['plugin_time'])
    # print(df['meter_values_timestamp'])

    input_df = df[['plugin_time','charge_power','meter_values_timestamp','charger_id','rfid','session_energy_consumed','status','charge_point_id']]

    input_df.replace({',':'.'}, regex=True,inplace=True)


    # input_df.head(5)
    # print(input_df.shape)
    # input_df['plugin_time']=input_df['plugin_time']+'+00:00'
    input_df["charge_power"] = pd.to_numeric(input_df["charge_power"])
    input_df["session_energy_consumed"] = pd.to_numeric(input_df["session_energy_consumed"])

    input_group=input_df.groupby('plugin_time').max()
    input_online=input_df.groupby(['plugin_time','status']).max()

    input_result=input_group.merge(input_online, on='plugin_time', how='right')[['charge_power_y','meter_values_timestamp_x','charger_id_x','rfid_x','session_energy_consumed_x','meter_values_timestamp_y']]
    input_result['SessionStartDateTime']=pd.to_datetime(input_result.index)
    input_result['SessionEndDateTime']=pd.to_datetime(input_result['meter_values_timestamp_x'])
    input_result['ChargeEndDateTime']=pd.to_datetime(input_result['meter_values_timestamp_y'])

    input_result.index=range(input_result.shape[0])
    input_result=input_result.drop(columns=['meter_values_timestamp_x','meter_values_timestamp_y'])


    # create a new column to store month
    input_result['SessionConnectionTime']=(input_result['SessionEndDateTime']-input_result['SessionStartDateTime'])
    input_result['Duration_StateC']=(input_result['ChargeEndDateTime']-input_result['SessionStartDateTime'])

    # input_result

    input_result=input_result.groupby('SessionStartDateTime').agg({'Duration_StateC': min, 'charge_power_y':max,'charger_id_x':max,'session_energy_consumed_x':max,'SessionEndDateTime':max,'ChargeEndDateTime':max,'SessionConnectionTime': max}).reset_index()


    output_df=pd.read_csv('Data Input/Data Portal Session Data Template.csv')
    # output_df



    # copying data from input file to output file
    output_df['SessionStartDateTime'] = input_result['SessionStartDateTime']
    output_df['SessionEndDateTime'] = input_result['SessionEndDateTime']
    output_df['SessionID']=input_result.index+1
    output_df['SessionConnectionTime']=input_result['SessionConnectionTime']
    output_df['Duration_StateC']=input_result['Duration_StateC']
    output_df['SessionMaxDemandKW']=input_result['charge_power_y']/1000
    output_df['VendorID']='CR_Mobilityhouse'
    output_df['PortID']='1'
    output_df['EVSEModelNbr']='EVP-2001-70-P-0001'
    output_df['SessionKWH']=input_result['session_energy_consumed_x']/1000
    output_df['LocationID']='CRT-2019-0052'
    output_df['EVSEID']=input_result['charger_id_x']
    output_df['EVSENbrOfPorts']=1
    output_df['SessionSaleAmount']='$0.00'

    # replacing TMH charger ID with SCE EVSE ID
    to_rep = dict(zip(['MH0005','MH0006', 'MH0007', 'MH0018'],['EVC-012920-4191C','EVC-012920-4186C','EVC-012920-4184C','EVC-091620-4773C']))




    output_df.replace({'EVSEID':to_rep}, inplace = True)

    output_df_sessionID=output_df.copy(deep=True)
    # output_df.head(5)
    # output_df_sessionID.head(5)


    # modify the format for datetime data and time difference data
    SCT=[]
    SST=[]
    SET=[]
    DT=[]

    for i in range(output_df.shape[0]):
        if output_df['SessionConnectionTime'][i].total_seconds()<0:
            SCT.append(0)
        else:
            SCT.append(int(output_df['SessionConnectionTime'][i].total_seconds()))

        if  output_df['Duration_StateC'][i].total_seconds()<0:
            DT.append(0)
        else:
            DT.append(int(output_df['Duration_StateC'][i].total_seconds()))
        SST.append(datetime.strftime(output_df['SessionStartDateTime'][i], '%m/%d/%y %H:%M:%S'))
        SET.append(datetime.strftime(output_df['SessionEndDateTime'][i], '%m/%d/%y %H:%M:%S'))


    output_df['SessionConnectionTime']=SCT
    output_df['SessionStartDateTime']=SST
    output_df['SessionEndDateTime']=SET
    output_df['Duration_StateC']=DT


    # calculate session average demand
    output_df['SessionAverageDemandKW']=output_df['SessionKWH']/output_df['Duration_StateC']*3600
    output_df.replace([np.inf, -np.inf], 0, inplace=True)
#     output_df=unmanaged_session(output_df)# adding a new column 'SessionEndDateTime_un'
   

        ################Get interval data
    # output_df_sessionID.rename(columns={"SessionStartDateTime":""})
    input_df['plugin_time']=pd.to_datetime(input_df['plugin_time'])
    input_df['meter_values_timestamp']=pd.to_datetime(input_df['meter_values_timestamp'])

    # input_df.head()

    output_interval_df=pd.read_csv('Data Input/Data Portal Interval Template.csv')
    interval_df=pd.merge(output_df_sessionID,input_df,left_on='SessionStartDateTime',right_on='plugin_time',how='right').reset_index()[['SessionID','VendorID','LocationID','EVSEID','PortID','DREventCalled','SessionStartDateTime','SessionEndDateTime','DREventParticipated','meter_values_timestamp','charge_power','session_energy_consumed']]
    interval_result=interval_df.groupby('SessionID').resample("15Min", on='meter_values_timestamp').max()

    from datetime import timedelta
    interval_result['IntervalStartDateTime']=interval_result.index.get_level_values(1)
    interval_result['IntervalEndDateTime']=interval_result['IntervalStartDateTime']+timedelta(minutes=15)
#     interval_result['IntervalEndDateTime_un']=interval_result['IntervalStartDateTime']+timedelta(minutes=15)
    interval_result['IntervalKWH']=np.nan
    interval_result['IntervalID']=np.nan
    interval_result=interval_result.drop(columns=['SessionID','meter_values_timestamp'])
    interval_result=interval_result.reset_index()
    # interval_result


    result=pd.DataFrame(columns=interval_result.columns)

    i=0
    SCT=[]
#     SCT_un=[]
    SST=[]
    SET=[]
#     SET_un=[]
    interval_result['SessionConnectionTime']=interval_result['SessionEndDateTime']-interval_result['SessionStartDateTime']
#     interval_result['SessionConnectionTime_un']=interval_result['SessionEndDateTime_un']-interval_result['SessionStartDateTime']
    
    for name, group in interval_result.groupby('SessionID'):

        group.index=range(group.shape[0])

        for i in range(group.shape[0]):
            group['IntervalID'][i]=i+1
            SST.append(datetime.strftime(group['IntervalStartDateTime'][i], '%m/%d/%y %H:%M:%S'))
            SET.append(datetime.strftime(group['IntervalEndDateTime'][i], '%m/%d/%y %H:%M:%S'))
#             SET_un.append(datetime.strftime(group['IntervalEndDateTime_un'][i], '%m/%d/%y %H:%M:%S'))

            if group['SessionConnectionTime'][i].total_seconds()<0:
                SCT.append(0)
            else:
                SCT.append(group['SessionConnectionTime'][i].total_seconds())
            
#             if group['SessionConnectionTime_un'][i].total_seconds()<0:
#                 SCT_un.append(0)
#             else:
#                 SCT_un.append(group['SessionConnectionTime_un'][i].total_seconds())

    #         group['session_energy_consumed'][i]
            if i==0:
                group['IntervalKWH'][i]=group['session_energy_consumed'][i]/1000
            else:
                group['IntervalKWH'][i]=group['session_energy_consumed'][i]/1000-group['session_energy_consumed'][i-1]/1000
        result=pd.concat([result,group])
        
    result['charge_power']=result['charge_power']/1000
    result=result.rename(columns={'charge_power':'IntervalMaxDemandKW'})
    result['SessionConnectionTime']=SCT
#     result['IntervalStartDateTime']=SST
#     result['IntervalEndDateTime']=SET
    result['IntervalAverageDemandKW']=result['IntervalKWH']/result['SessionConnectionTime']*3600
    result=result.drop(columns=['meter_values_timestamp','session_energy_consumed','SessionStartDateTime','SessionEndDateTime','SessionConnectionTime'])
    result.replace([np.inf, -np.inf], 0, inplace=True)
    result['IntervalID']=result['IntervalID'].astype(int)
    result = result.reindex(columns=output_interval_df.columns)
    # result
    
    date = pd.Timestamp.now().strftime('%Y%m%d')

    monthRange=range(monthStart,monthEnd+1)

    for month in monthRange:

        output_df['month']=pd.to_datetime(output_df['SessionStartDateTime']).dt.month
        output_month=output_df[output_df['month']==month]
        output_month= output_month.drop(columns='month')
        output_month.to_csv('Data Output/'+str(date) +'_TheMobilityHouse_Session(' +str(year)+'-0'+str(month)+'_CRT-2019-0052_session.csv',index=False)

        result['month']=pd.to_datetime(result['IntervalStartDateTime']).dt.month
        result_month=result[result['month']==month]
        result_month= result_month.drop(columns='month')
        result_month.to_csv('Data Output/'+str(date) +'_TheMobilityHouse_Interval(' +str(year)+'-0'+str(month)+'_CRT-2019-0052_interval.csv',index=False)
        print('Results saved to Data Output for year ' +str(year)+' month '+str(month))

    return output_month, result_month

# year=2021
# monthStart=11
# monthEnd=11
# session, interval = getSessionAndInterval(df,year,monthStart,monthEnd)
import numpy as np

from datetime import datetime
import pandas as pd
import warnings
 
from datetime import timedelta

   
warnings.filterwarnings('ignore')

def unmanaged_session(session,chargerPower):
    session['ChargePowerKW']=chargerPower
    charge_efficiency=0.93
    session['chargetimeMin_un']=session['SessionKWH']/session['ChargePowerKW']/charge_efficiency*60 #60min/h, change from 

    for index, row in session.iterrows():
        session['SessionEndDateTime'][index]=datetime.strftime(datetime.strptime(row['SessionStartDateTime'],'%m/%d/%y %H:%M:%S')+timedelta(minutes=row['chargetimeMin_un']), '%m/%d/%y %H:%M:%S')

    return session

def getUnmanagedSessionAndInterval(df,year,monthStart,monthEnd):
    timezone = 'US/Pacific'
    df['meter_values_timestamp'] = pd.to_datetime(df['meter_values_timestamp'], errors='coerce',utc=True)
    # df['meter_values_timestamp'] = df['meter_values_timestamp'].dt.tz_localize("UTC")
    df['meter_values_timestamp'] = df['meter_values_timestamp'].dt.tz_convert(timezone)
    df['meter_values_timestamp']= df['meter_values_timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S")

    df['plugin_time'] = pd.to_datetime(df['plugin_time'], utc=True)
    # df['plugin_time'] = df['plugin_time'].dt.tz_localize("UTC")
    df['plugin_time'] = df['plugin_time'].dt.tz_convert(timezone)
    df['plugin_time'] = df['plugin_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
    df['plugin_time'] = pd.to_datetime(df['plugin_time'])
    # print(df['plugin_time'])
    # print(df['meter_values_timestamp'])

    input_df = df[['plugin_time','charge_power','meter_values_timestamp','charger_id','rfid','session_energy_consumed','status','charge_point_id']]

    input_df.replace({',':'.'}, regex=True,inplace=True)


    # input_df.head(5)
    # print(input_df.shape)
    # input_df['plugin_time']=input_df['plugin_time']+'+00:00'
    input_df["charge_power"] = pd.to_numeric(input_df["charge_power"])
    input_df["session_energy_consumed"] = pd.to_numeric(input_df["session_energy_consumed"])

    input_group=input_df.groupby('plugin_time').max()
    input_online=input_df.groupby(['plugin_time','status']).max()

    input_result=input_group.merge(input_online, on='plugin_time', how='right')[['charge_power_y','meter_values_timestamp_x','charger_id_x','rfid_x','session_energy_consumed_x','meter_values_timestamp_y']]
    input_result['SessionStartDateTime']=pd.to_datetime(input_result.index)
    input_result['SessionEndDateTime']=pd.to_datetime(input_result['meter_values_timestamp_x'])
    input_result['ChargeEndDateTime']=pd.to_datetime(input_result['meter_values_timestamp_y'])

    input_result.index=range(input_result.shape[0])
    input_result=input_result.drop(columns=['meter_values_timestamp_x','meter_values_timestamp_y'])


    # create a new column to store month
    input_result['SessionConnectionTime']=(input_result['SessionEndDateTime']-input_result['SessionStartDateTime'])
    input_result['Duration_StateC']=(input_result['ChargeEndDateTime']-input_result['SessionStartDateTime'])

    # input_result

    input_result=input_result.groupby('SessionStartDateTime').agg({'Duration_StateC': min, 'charge_power_y':max,'charger_id_x':max,'session_energy_consumed_x':max,'SessionEndDateTime':max,'ChargeEndDateTime':max,'SessionConnectionTime': max}).reset_index()


    output_df=pd.read_csv('Data Input/Data Portal Session Data Template.csv')
    # output_df

    chargerPower=150

    # copying data from input file to output file
    output_df['SessionStartDateTime'] = input_result['SessionStartDateTime']
    output_df['SessionEndDateTime'] = input_result['SessionEndDateTime']
    output_df['SessionID']=input_result.index+1
    output_df['SessionConnectionTime']=input_result['SessionConnectionTime']
    output_df['Duration_StateC']=input_result['Duration_StateC']
    output_df['SessionMaxDemandKW']=chargerPower
    output_df['VendorID']='CR_Mobilityhouse'
    output_df['PortID']='1'
    output_df['EVSEModelNbr']='EVP-2001-70-P-0001'
    output_df['SessionKWH']=input_result['session_energy_consumed_x']/1000
    output_df['LocationID']='CRT-2019-0052'
    output_df['EVSEID']=input_result['charger_id_x']
    output_df['EVSENbrOfPorts']=1
    output_df['SessionSaleAmount']='$0.00'

    # replacing TMH charger ID with SCE EVSE ID
    to_rep = dict(zip(['MH0005','MH0006', 'MH0007', 'MH0018'],['EVC-012920-4191C','EVC-012920-4186C','EVC-012920-4184C','EVC-091620-4773C']))




    output_df.replace({'EVSEID':to_rep}, inplace = True)

    output_df_sessionID=output_df.copy(deep=True)
    # output_df.head(5)
    # output_df_sessionID.head(5)


    # modify the format for datetime data and time difference data
    SCT=[]
    SST=[]
    SET=[]
    DT=[]

    for i in range(output_df.shape[0]):
        if output_df['SessionConnectionTime'][i].total_seconds()<0:
            SCT.append(0)
        else:
            SCT.append(int(output_df['SessionConnectionTime'][i].total_seconds()))

        if  output_df['Duration_StateC'][i].total_seconds()<0:
            DT.append(0)
        else:
            DT.append(int(output_df['Duration_StateC'][i].total_seconds()))
        SST.append(datetime.strftime(output_df['SessionStartDateTime'][i], '%m/%d/%y %H:%M:%S'))
        SET.append(datetime.strftime(output_df['SessionEndDateTime'][i], '%m/%d/%y %H:%M:%S'))


    output_df['SessionConnectionTime']=SCT
    output_df['SessionStartDateTime']=SST
    output_df['SessionEndDateTime']=SET
    output_df['Duration_StateC']=DT


    # calculate session average demand
    output_df['SessionAverageDemandKW']=output_df['SessionKWH']/output_df['Duration_StateC']*3600
    output_df.replace([np.inf, -np.inf], 0, inplace=True)
    
    output_df=unmanaged_session(output_df,chargerPower)# adding a new column 'SessionEndDateTime_un'
   

        ################Get interval data
    # output_df_sessionID.rename(columns={"SessionStartDateTime":""})
    input_df['plugin_time']=pd.to_datetime(input_df['plugin_time'])
    input_df['meter_values_timestamp']=pd.to_datetime(input_df['meter_values_timestamp'])

    # input_df.head()

    output_interval_df=pd.read_csv('Data Input/Data Portal Interval Template.csv')
    interval_df=pd.merge(output_df_sessionID,input_df,left_on='SessionStartDateTime',right_on='plugin_time',how='right').reset_index()[['SessionID','VendorID','LocationID','EVSEID','PortID','DREventCalled','SessionStartDateTime','SessionEndDateTime','DREventParticipated','meter_values_timestamp','charge_power','session_energy_consumed']]
    interval_result=interval_df.groupby('SessionID').resample("15Min", on='meter_values_timestamp').max()

    from datetime import timedelta
    interval_result['IntervalStartDateTime']=interval_result.index.get_level_values(1)
    interval_result['IntervalEndDateTime']=interval_result['IntervalStartDateTime']+timedelta(minutes=15)
#     interval_result['IntervalEndDateTime_un']=interval_result['IntervalStartDateTime']+timedelta(minutes=15)
    interval_result['IntervalKWH']=np.nan
    interval_result['IntervalID']=np.nan
    interval_result=interval_result.drop(columns=['SessionID','meter_values_timestamp'])
    interval_result=interval_result.reset_index()
    # interval_result


    result=pd.DataFrame(columns=interval_result.columns)

    i=0
    SCT=[]
#     SCT_un=[]
    SST=[]
    SET=[]
#     SET_un=[]
    interval_result['SessionConnectionTime']=interval_result['SessionEndDateTime']-interval_result['SessionStartDateTime']
#     interval_result['SessionConnectionTime_un']=interval_result['SessionEndDateTime_un']-interval_result['SessionStartDateTime']
    
    for name, group in interval_result.groupby('SessionID'):

        group.index=range(group.shape[0])

        for i in range(group.shape[0]):
            group['IntervalID'][i]=i+1
            SST.append(datetime.strftime(group['IntervalStartDateTime'][i], '%m/%d/%y %H:%M:%S'))
            SET.append(datetime.strftime(group['IntervalEndDateTime'][i], '%m/%d/%y %H:%M:%S'))
#             SET_un.append(datetime.strftime(group['IntervalEndDateTime_un'][i], '%m/%d/%y %H:%M:%S'))

            if group['SessionConnectionTime'][i].total_seconds()<0:
                SCT.append(0)
            else:
                SCT.append(group['SessionConnectionTime'][i].total_seconds())
            
#             if group['SessionConnectionTime_un'][i].total_seconds()<0:
#                 SCT_un.append(0)
#             else:
#                 SCT_un.append(group['SessionConnectionTime_un'][i].total_seconds())

    #         group['session_energy_consumed'][i]
            if i==0:
                group['IntervalKWH'][i]=group['session_energy_consumed'][i]/1000
            else:
                group['IntervalKWH'][i]=group['session_energy_consumed'][i]/1000-group['session_energy_consumed'][i-1]/1000
        result=pd.concat([result,group])
        
    result['charge_power']=chargerPower
    result=result.rename(columns={'charge_power':'IntervalMaxDemandKW'})
    result['SessionConnectionTime']=SCT
#     result['IntervalStartDateTime']=SST
#     result['IntervalEndDateTime']=SET
    result['IntervalAverageDemandKW']=result['IntervalKWH']/result['SessionConnectionTime']*3600
    result=result.drop(columns=['meter_values_timestamp','session_energy_consumed','SessionStartDateTime','SessionEndDateTime','SessionConnectionTime'])
    result.replace([np.inf, -np.inf], 0, inplace=True)
    result['IntervalID']=result['IntervalID'].astype(int)
    result = result.reindex(columns=output_interval_df.columns)
    # result
    
    date = pd.Timestamp.now().strftime('%Y%m%d')

    monthRange=range(monthStart,monthEnd+1)

    for month in monthRange:

        output_df['month']=pd.to_datetime(output_df['SessionStartDateTime']).dt.month
        output_month=output_df[output_df['month']==month]
        output_month= output_month.drop(columns='month')
        output_month.to_csv('Data Output/'+str(date) +'_TheMobilityHouse_Session(' +str(year)+'-0'+str(month)+'_CRT-2019-0052_session.csv',index=False)

        result['month']=pd.to_datetime(result['IntervalStartDateTime']).dt.month
        result_month=result[result['month']==month]
        result_month= result_month.drop(columns='month')
        result_month.to_csv('Data Output/'+str(date) +'_TheMobilityHouse_Interval(' +str(year)+'-0'+str(month)+'_CRT-2019-0052_interval.csv',index=False)
        print('Results saved to Data Output for year ' +str(year)+' month '+str(month))

    return output_month, result_month

# year=2021
# monthStart=11
# monthEnd=11
# session_un, interval_un = getUnmanagedSessionAndInterval(df,year,monthStart,monthEnd)

import tou_optimizer as opt
import fm_opt_simul as simul
import sys
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime, timedelta
import time as timedate
import numpy as np
import pandas as pd
# import plotter as plot
import locale ; locale.setlocale(locale.LC_ALL, 'en_US')
import os

Folder = '/Users/meiyewang/Documents/Tools/SCSimulation-master/us_lm/Project/St.Louis'
filename = 'Input_File_StLouis_South_1'
year=2021
monthStart=11
monthEnd=11

user_filename = Folder+'/'+filename


#USER energy iteration kW steps desired - only used for non-TOU rates
'''This will eventually be part of input file'''
iter_steps = 10

input_file = pd.ExcelFile(user_filename+'.xlsx')
input_df = pd.read_excel(input_file,sheet_name=0,index_col=0,usecols="A:B")
input_df = input_df.dropna()

utility = input_df.loc['Utility'][0]

rate_array = input_df[input_df.index.str.contains('Rate ')].values

if len(rate_array) == 1:
    rate_name = rate_array[0][0]
else:
    rate_name = [i[0] for i in np.array(rate_array).tolist()]

season = input_df.loc['Season'][0]
desired_fulfillment = input_df.loc['Desired Fulfillment'][0]
kW_max = input_df.loc['kW_max Guess'][0]
hard_max = input_df.loc['Grid Max (kW)'][0]

weekdaysetting = input_df.loc['Weekday Setting'][0]
# the simulation is for 2 days, we will assume a 30 day month
if weekdaysetting == "Only Weekdays":
    days = 11
elif weekdaysetting == "Weekdays And Weekends":
    days = 15
elif weekdaysetting == "Only Weekends":
    days = 4
elif weekdaysetting == 'Only One Day':
    days = 1
else:
    print("weekdaysetting doesn't exit, please check again")

twodaycount = 2 * 96
onedaycount = 96

read_file = pd.ExcelFile("TariffDictionary_v10.xlsx") #needs to be kept up to date with tariff dictionary version
read_data = pd.read_excel(read_file, sheet_name=0,index_col=0)
tariff_df = read_data[read_data.Utility == utility]

def convert(interval):
# have to seperate chargers, or add up charging value from the same 
    interval['IntervalKWH'] = interval['IntervalKWH'].replace(np.nan, 0)
#     # interval[interval['IntervalKWH']==NaN]
#     interval['IntervalKWH'].isnull().sum()
    energyList=[0]*31*24*4 #30days, 24 hours, 4 15mins blocks.
    powerList=[0]*31*24*4
    for index, row in interval.iterrows():
#         print(row)
        time=row['IntervalStartDateTime']
        index= int(time.minute/15 + time.hour*4 + (time.day-1)*24*4)         
        energyList[index]+=row['IntervalKWH']
        powerList[index]=row['IntervalMaxDemandKW']
#         print(row['IntervalKWH'])
    return energyList,max(powerList)


# charged_energy,kw = convert(interval) # charged_power is the site wide 15mins 


def define_tariff(name,tariff_df,season, weekdaysetting ):

    tariff_df = tariff_df[tariff_df['Tariff Name'] == name]
    # print(tariff_df)
    '''This will eventually be part of input file to define what version, for now we will default to the most recent version'''
    recent_version = tariff_df['Effective Date Start'].max()
    tariff_df = tariff_df[tariff_df['Effective Date Start'] == recent_version]

    rate_dict = tariff_df[tariff_df['Season'] == season]

    if len(rate_dict) == 0:
        print("User set a season that does not exist, resetting season")
        rate_dict = tariff_df[tariff_df['Season'] == "-"]


    if weekdaysetting == "Only Weekdays":
        rate_dict = rate_dict[rate_dict['Applicable Days\n(1-7)'] != '6,7']
        rate_dict = rate_dict[rate_dict['Applicable Days\n(1-7)'] != '7,']
    elif weekdaysetting ==  "Only Weekends":
        rate_dict = rate_dict[rate_dict['Applicable Days\n(1-7)'] == '6,7']

    if len(rate_dict) == 1:
        tou = 0
    elif len(rate_dict) > 1:
        tou = 1
    else:
        print('No rate dictionary and tou defined, check inputs')

    return rate_dict,tou

def get_unmanaged(kw,rate_dict):
    onedaycount = 96
    count = 'unmanaged'

    #set temporary unrealistic gridmax
    grid_max = kw*100000

    temp_gridmax = [grid_max] * onedaycount
    a_gridmax = temp_gridmax + temp_gridmax

    a_result = simul.main(filename, a_gridmax,count,Folder,season)
#     print(a_result[2])
    #Get total load - charging + siteload
    total_load = [x + y for x, y in zip(a_result[2], a_result[3])]
    max_kW = max(total_load)

    if tou == 0:
        cost,rate = get_nontou_cost(max_kW,rate_dict)
    elif tou == 1:
        cost,rate = get_tou_cost(a_result[2],total_load,rate_dict) # a_result[2] is the unmanaged charging power
    
    a_result.append(rate)

    return cost, a_result



def get_nontou_cost(kw,rate_dict):# only calculates demand cost

    if isinstance(rate_dict,list):
        min_kw_restrictions = []
        for i in rate_dict:
            min_kw_restrictions.append(i['Rate kW Minimum Restriction'].min())
        min_kw_restrictions.sort()
        min_kW = 0
        for n in min_kw_restrictions:
            if kw > n:
                min_kW = n
        for i in rate_dict:
            if min_kW == i['Rate kW Minimum Restriction'].min():
                temp_dict = i
        rate_dict = temp_dict

    demand_rate = rate_dict['Demand Charges\n($/kW)'].values[0]

    #We do not take energy costs into account for non-TOU rates as there is no change
    total_cost = demand_rate * kw

    return total_cost, rate_dict['Tariff Name'].unique()[0]


def get_tou_cost_month_data(charged_energy,max_kW,rate_dict):

    if isinstance(rate_name,list):
        min_kw_restrictions = []
        for i in rate_dict:
            min_kw_restrictions.append(i['Rate kW Minimum Restriction'].min())
        min_kw_restrictions.sort()
        min_kW = 0
        for n in min_kw_restrictions:
            if max_kW > n:
                min_kW = n
        for i in rate_dict:
            if min_kW == i['Rate kW Minimum Restriction'].min():
                temp_dict = i
        rate_dict = temp_dict

        print('used rate ' + str(rate_dict['Tariff Name'].unique()[0]) + 'for max ' + str(max_kW))

    rate_dict['Start Interval'] = rate_dict['TOU Start - local time\n(incl)'].apply(lambda x: (x.hour + (x.minute / 60))*4)
    rate_dict['End Interval'] = rate_dict['TOU End - local time\n(excl)'].apply(lambda x: (x.hour + (x.minute / 60))*4)

    #set default costs as 0
    energy_rate = [0 for i in range(onedaycount)]
    demand_cost = 0
    customer_charge = 0

    for index,values in rate_dict.iterrows():
        start = int(values['Start Interval'] - 1)
        end = int(values['End Interval'] - 1)
       
        if end == -1:
            end = 96
        if start == -1:
            start = 0

        energy = values['Energy Charges\n($/kWh)']
        demand = values['Demand Charges\n($/kW)']
        customer_charge = values['Customer Charges\n($ / Month - per meter)']
        if start < end:
            energy_rate[start:end] = [energy] * len(energy_rate[start:end])

            max_demand = max_kW
            demand_cost += max_demand * demand
        else:
            energy_rate[start:] = [energy] * len(energy_rate[start:])
            energy_rate[:end] = [energy] * len(energy_rate[:end])
            d1_max = max_kW # first day
            d2_max = max_kW # second day
            max_demand = max_kW
            demand_cost += max_demand * demand


    full_energy_rate = energy_rate + energy_rate
#     print("energy rate for real charging")

    a_energy_cost = [a * b for a, b in zip(full_energy_rate, charged_energy)]

    energy_cost = sum(a_energy_cost)

    peak_kW = max_kW

    #Calculate max demand/facilities related demand
    '''The below also takes into account block rates
        Can be changed to read from tariff dictionary if we see a lot of rates like this, right now we only have PG&E'''
    block = 0
    if rate_dict['Tariff Name'].unique()[0] == 'BEV-1':
        block = 10
    elif rate_dict['Tariff Name'].unique()[0] == 'BEV-2-S Secondary' or rate_dict['Tariff Name'].unique()[0] == 'BEV-2-P Primary':
        block = 50

    if block == 0:
        max_rate = rate_dict['Max Demand\n($/kW)'].max()
        if np.isnan(max_rate):
            max_rate = 0
        peak_charge = max_rate*peak_kW
    else:
        subscription = peak_kW // block
        overage = peak_kW % block
        max_rate = rate_dict['Max Demand\n($/kW)'].max()
        overage_rate = rate_dict['Overage fee ($/kW)'].max()

        peak_charge_1 = (subscription * block * max_rate) + (overage * overage_rate)
        peak_charge_2 = ((subscription * block) + block) * max_rate

        peak_charge = min(peak_charge_1,peak_charge_2)

    total_cost = energy_cost + demand_cost + customer_charge + peak_charge

    return total_cost, rate_dict['Tariff Name'].unique()[0]

if __name__ == '__main__':

    if season == 'All':
        seasons = ['Winter','Summer']#,'March,April']

    else:
        seasons = [season]
    for s in seasons:
        season = s
        print(season)

        #Determine tariff to be used for optimization:
        if isinstance(rate_name,list):
            rate_dict = []
            for i in rate_name:
                temp_dict, tou = define_tariff(i,tariff_df,season, weekdaysetting)
                rate_dict.append(temp_dict)
        else:
            rate_dict, tou = define_tariff(rate_name, tariff_df, season, weekdaysetting)

#         print(rate_dict)
        
        # calculate real cost
        session, interval = getSessionAndInterval(df,year,monthStart,monthEnd)
        session_un,interval_un=getUnmanagedSessionAndInterval(df,year,monthStart,monthEnd)
        
        charged_energy,kw = convert(interval) # charged_power is the site wide 15mins 
        charged_energy_un,kw_un= convert (interval_un)
        
        if tou == 0:
            real_cost,rate = get_nontou_cost(kw,rate_dict)
            unmanaged_cost, rate_un=get_nontou_cost(kw_un,rate_dict)
        elif tou == 1:
            real_cost,rate = get_tou_cost_month_data(charged_energy,kw,rate_dict) # a_result[2] is the unmanaged charging power 1*192 list
            unmanaged_cost,rate_un = get_tou_cost_month_data(charged_energy_un,kw_un,rate_dict)
        #
#         unmanaged_cost, unmanaged_result = get_unmanaged(kw,rate_dict)
        savings = unmanaged_cost - real_cost
        print("monthly unmanaged cost is $" + str(unmanaged_cost) + ", real cost is $"+ str(real_cost) + ", saving $" + str(savings))
#         print()
   
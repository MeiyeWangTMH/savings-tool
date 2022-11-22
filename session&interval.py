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

    chargerPower=450

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
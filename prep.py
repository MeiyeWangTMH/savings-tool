#Import libraries
import pandas as pd
from pandas import *
import numpy as np
import glob



#Input paths
PATH_INPUT = 'data/input'
PATH_OUTPUT = 'data/output'

#Selection, which parameter should be maintainend on the site level (Additional adaption in function necessary)
#discrete
cols_site = ['meter_values_timestamp','charge_power', 'event_max',"L1_current","L2_current","L3_current","L1_offer","L2_offer","L3_offer", "total_energy_consumed_reset"]
#optimization
cols_site_opt =['meter_values_timestamp','power_trading_max_mw', 'soe_min_mwh','soe_max_mwh','power_actual_in_mw']

#Functions
def csv2df(file):
    df = pd.read_csv(file, delimiter = ";",decimal = ",", doublequote = True , encoding = "utf-8" )
    
    ids = df['id'].nunique()
    pts = df['plugin_time'].nunique()

    print ('Different charging-ids in the original data: ' + str(ids))
    print ('Different plugin_times in the original data: ' + str(pts))
    return df

def error_duplicated_logs(df):
    """
    Check for multiple ids or plugin-times
    """
    times = df['plugin_time'].unique()
    c = 0
    print('Finding plugin times with more than one id...')
    for item in times:
        ids = df[df['plugin_time'] == item]['id'].nunique()
        if ids > 1:
            print('Time: '+ str(item) + ': ' + str(ids))
            c += ids - 1      
    if c > 0:
        print('Too many ids: '+ str(c))
    else:
        print('Number ids ok')
    
    i = df['id'].unique()
    d = 0
    print('Finding ids with more than one plugin_time...')
    for item in i:
        ts = df[df['id'] == item]['plugin_time'].nunique()
        if ts > 1:
            print('id: '+ str(item) + ': ' + str(ts))
            d += ts - 1      
    if d > 0:
        print('Too many plugin_times: '+ str(d))
    else:
        print('Number plugin_times ok')
    
def error_wrong_events(df_event):
    """
    Check for double events, which should be probably a single event --> No reset of session energy consumed indicates this problem
    """
    df_event['last energy'] = df_event['session_energy_consumed'].shift()

    df_e = df_event[df_event['first energy'] > df_event['last energy']]
    df_e = df_e[df_e['first energy'] > 50]

    ratio = (1-(len(df_e.axes[0])/len(df_event.axes[0]))) *100


    print(station)
    print('Rows df_event: ', len(df_event.axes[0]))
    print('Rows "wrong" df_event: ', len(df_e.axes[0]))

    print('Ratio correct events: ' + str(ratio) + '%')

def format_timestamp(df, timezone):
    df['meter_values_timestamp'] = pd.to_datetime(df['meter_values_timestamp'], errors = 'coerce')
    df['meter_values_timestamp'] = df['meter_values_timestamp'].dt.tz_convert(timezone)
    df['meter_values_timestamp'] = df['meter_values_timestamp'].dt.tz_localize(None)
    df['plugin_time'] = pd.to_datetime(df['plugin_time'], utc = True)
    df['plugin_time'] = df['plugin_time'].dt.tz_convert(timezone)
    df['plugin_time'] = df['plugin_time'].dt.tz_localize(None)
    df['plugin_time'] = df['plugin_time'].dt.strftime("%Y-%m-%d %H:%M:%S")
    df['plugin_time'] = pd.to_datetime(df['plugin_time'])
    return df

def adapt_plugin(df):
    """
    Adapt the Plugin-Time: each id is assigned only one plugin time
    One Plugintime for multiple ids, but energy is decreasing between ids --> Create new plugin-times
    """
    df = df.reset_index()
    m = df['energy_diff'].lt(0).groupby(df['id']).transform('first')
    df.loc[m, 'plugin_time'] = df.groupby('id')['meter_values_timestamp'].transform('min')
    df['plugin_time'] = pd.to_datetime(df['plugin_time'])
    return df

def reduce_id(df):
    """
    Reduce id: first id is used if one plugin-time has multiple ids
    """
    df['id'] = df['id'].groupby(df['plugin_time']).transform('first')
    return df

def reduce_plugin(df):
    """
    Reduce plugin: first plugin-time is used if one id has multiple plugin-times
    """
    df['plugin_time'] = df['plugin_time'].groupby(df['id']).transform('first')
    return df

def delete_columns(df):
    #delete parameters not required
    del df['action']
    #delete bidirectional parameters
    del (df['discharge_capability'], df['discharge_current'], df['discharge_power'],
    df['total_energy_produced'], df['discharge_offer'], df['session_energy_produced'])
    #deleted parameters, which could be interesting for the future
    del df['start_charging_time']
    del df['charge_capability']
    return df

def total_energy_consumed_reset(df):
    """
    Reset total energy consumed, so the metering starts from zero in the data
    """
    df = df.sort_values('meter_values_timestamp')
    if not df['total_energy_consumed'].empty:
        df['total_energy_consumed_reset'] = df['total_energy_consumed'] - df['total_energy_consumed'].iloc[0]
    else:
        df['total_energy_consumed_reset'] = df['total_energy_consumed'] - df['total_energy_consumed'].min()
    df['total_energy_consumed_reset'] = df['total_energy_consumed_reset'].sub(df['total_energy_consumed_reset'].shift(), fill_value= 0)
    return df

def transform_current(df):
    """
    Extract current from brackets
    """
    df2 = df[df['charge_current'] != '[]']
    #Transform current-tuples into float columns
    df_temporary=df2['charge_current'].str[1:-1].str.split(',', expand=True).astype(float)
    df2['L1_current']=df_temporary[0].copy()
    df2['L2_current']=df_temporary[1].copy()
    df2['L3_current']=df_temporary[2].copy()
    
    df_temporary=df2['charge_offer'].str[1:-1].str.split(',', expand=True).astype(float)
    df2['L1_offer']=df_temporary[0].copy()
    df2['L2_offer']=df_temporary[1].copy()
    df2['L3_offer']=df_temporary[2].copy()

    del df2['charge_current']
    del df2['charge_offer']
    return df2

def add_sitedata(df,site_max,charger_max):
    """
    Create columns for site, charger and event max charging power (static so far)
    """
    df.loc[:,'site_max']=site_max
    df.loc[:,'charger_max']=charger_max
    return df
    
def event_max(df):
    """
    Event max: highest metered charging power for each
    """
    df['event_max'] = 0
    t = df['id'].unique()
    for item in t:
        df_sub=df[df.id == item]
        x = df_sub.charge_power.max()
        df['event_max'] = np.where(df['id']==item,x,df['event_max'])    
    return df

def delete_zero_total_energy(df):
    """
    Delete lines where metering value is zero, probably metering or data error
    """
    print('Identify lines, where "total_energy_consumed" = 0...')
    zero = 0
    for row in df.index:
        if df['total_energy_consumed'][row] == 0:
            df = df.drop([row])   
            zero += 1
    print(str(zero) + ' lines found')
    if zero > 0:
        print('Deleting these lines')
    return df 

# def delete_zero_ids(df):
#     print('FUNKTION: delete_zero_ids')
#     print('Identify lines, where max "session_energy_consumed"(id) = 0...')
#     x = 0
#     ids = df['id'].unique()
#     if df.groupby('id')['session_energy_consumed'].max() == 0:
#         df = df.drop([row])
#         x += 1
#     print(str(zero) + ' lines found')
#     if zero > 0:
#         print('Deleting these lines')
#     return df

def del_double_timestamps(df):
    """
    Delete double metering values
    """
    df['mvt_shift'] = df['meter_values_timestamp'].shift()
    co = 0
    for row in df.index:
        if df['meter_values_timestamp'][row] == df['mvt_shift'][row]:
            df = df.drop([row])
            co +=1
    del df['mvt_shift']        
    print('Deleted rows (Duplicated timestamp): ' + str(co))
    return df

def session_energy_calculated(df):
    """
    Calculate charged energy from total metering data
    """
    print('Calculating the session_energy_consumed by using total_energy_consumed...')
    df['session_energy'] = df['total_energy_consumed'] - df['total_energy_consumed'].shift()
    df['session_energy'] = df['session_energy'].fillna(df['session_energy_consumed'])
    df['session_energy_calculated'] = df['session_energy'].groupby(df['id']).cumsum()
    df2 = df[df['session_energy_consumed'].between(df['session_energy_calculated']*0.9, df['session_energy_calculated']*1.1)]
    print('Number of session_energy_consumed != session_energy_calculated: ', (len(df2.axes[0])/len(df.axes[0]))*100, '%')
    print('Replace column session_energy_consumed with session_energy_calculated...')
    df['session_energy_consumed'] = df['session_energy_calculated']
    df.drop(['session_energy', 'session_energy_calculated'], axis=1, inplace=True)
    df = df[df['session_energy_consumed'] >= 0]
    return df
    

def prepare_df(df,site_max,charger_max,timezone):
    """
    Combines all prepare functions
    """
    df = format_timestamp(df, timezone)
    df = delete_columns(df)
    df = transform_current(df)
    df = add_sitedata(df,site_max,charger_max)
    df = delete_zero_total_energy(df)
    #df = delete_zero_ids(df)


    df = df.sort_values('meter_values_timestamp')
    df = del_double_timestamps(df)
    #df = df[df['session_energy_consumed'] >0]
 
    df['timedelta'] = df['meter_values_timestamp'] - df['meter_values_timestamp'].shift()
    df['energy_diff'] = df['session_energy_consumed'] - df['session_energy_consumed'].shift()
        
    df = adapt_plugin(df)
    df = reduce_id(df)
    df = reduce_plugin(df)
    df = event_max(df)
    
    
    #df =session_energy_calculated(df)

    return df

def df_event_creation(df,site_max,charger_max):
    """
    Create dataframe df_event, which contains all plugin events
    """
    print('Identify charging events...')
    df_event = pd.DataFrame(columns=['id','charger_id','charge_point_id','connector_id','rfid','plugin_time','plugout_time',
                                     'plugin_duration','charging_time_start','charging_time_stop','charging_duration','max charge_power','mean charge_power event',
                                     'mean charge_power charging','session_energy_consumed','soc_start','soc_stop','charger_max','site_max',
                                     'ev_suspended', 'first energy'])

    #Filling the data for each event
    t = df['id'].unique()
    df_event['id']=t
    #variable for looping: x = row
    x = 0
    for item in t:
        df_sub=df[df.id == item]
        df.groupby(df['id'])['session_energy_consumed'].min()
        df_event['charger_id']=np.where(df_event['id']==item,df_sub.charger_id.max(),df_event['charger_id'])
        df_event['connector_id']=np.where(df_event['id']==item,df_sub.connector_id.max(),df_event['connector_id'])
        df_event['rfid'] = np.where(df_event['id'] == item, df_sub.rfid.max(), df_event['rfid'])
        df_event['charge_point_id']=np.where(df_event['id']==item,df_sub.charge_point_id.max(),df_event['charge_point_id'])
        df_event['max charge_power']=np.where(df_event['id']==item,df_sub.charge_power.max(),df_event['max charge_power'])
        df_event['session_energy_consumed']=np.where(df_event['id']==item,df_sub.session_energy_consumed.max(),
                                                     df_event['session_energy_consumed'])
        df_event['first energy']=np.where(df_event['id']==item,df_sub.session_energy_consumed.min(),df_event['first energy'])
        df_sub = df_sub.sort_values('meter_values_timestamp')
        df_event['ev_suspended']=np.where(df_event['id']==item,df_sub['ev_suspended'].iloc[-1],df_event['ev_suspended'])
        df_event['soc_start'] = np.where(df_event['id'] == item, df_sub['soc'].iloc[0],
                                        df_event['soc_start'])
        df_event['soc_stop'] = np.where(df_event['id'] == item, df_sub['soc'].iloc[-1],
                                            df_event['soc_stop'])

    
    #Calculate plugin_ & plugout_time (np.where doesn't work - dtype-Problems!)
        if df_event.loc[x, 'id'] == item:
            df_event.loc[x, 'plugin_time']= df_sub['plugin_time'].min()
            df_event.loc[x, 'plugout_time'] = df_sub['meter_values_timestamp'].max()
        
        
    #Calculate the charging duration & mean charge_power charging
        df_sub['timestamp_shift'] = df_sub['meter_values_timestamp'].shift()
        df_sub['diff'] = df_sub['meter_values_timestamp'] - df_sub['timestamp_shift']
        df_sub2 = df_sub[df_sub['charge_power']>100]

        df_event['ev_suspended'] = np.where(df_event['id'] == item, df_sub['ev_suspended'].iloc[-1],
                                            df_event['ev_suspended'])

        sum_charging2 = df_sub2['diff'].sum()
        mean_power = df_sub2['charge_power'].mean()

        if df_event.loc[x, 'id'] == item:
            df_event.loc[x, 'plugin_time']= df_sub['plugin_time'].min()
            df_event.loc[x, 'plugout_time'] = df_sub['meter_values_timestamp'].max()
            df_event.loc[x, 'charging_time_start'] = df_sub2['timestamp_shift'].min()
            if len(df_sub2) > 0:
                if pd.isnull(df_sub2['timestamp_shift'].min()):
                    df_event.loc[x, 'charging_time_start'] = df_sub2['meter_values_timestamp'].min()

                else:
                    df_event.loc[x, 'charging_time_start'] = df_sub2['timestamp_shift'].min()

            df_event.loc[x, 'charging_time_stop'] = df_sub2['meter_values_timestamp'].max()
            

        df_event['mean charge_power charging'] = np.where(df_event['id']==item, mean_power, 
                                                            df_event['mean charge_power charging'])
        #(np.where doesn't work - dtype-Problems!)
        if df_event.loc[x, 'id'] == item:
            df_event.loc[x, 'charging_duration'] = sum_charging2
        
        x +=1
      
        
    #Calculate the mean power for the event    
        df_event['mean charge_power event'] = np.where(df_event['id']==item, df_sub['charge_power'].mean(), 
                                                       df_event['mean charge_power event'])

    #Calculate plugin duration
    df_event['plugin_duration']= df_event['plugout_time']- df_event['plugin_time']

    #Add Site-specific Data
    df_event['charger_max']=charger_max
    df_event['site_max']=site_max
    
    #Calculate charging duration in %
    df_event['charge_duration [%]'] = (df_event['charging_duration']/df_event['plugin_duration'])*100
    
    #Fillna & Formating dtype durations
    df_event['charging_duration'] = df_event['charging_duration'].fillna(0)
    df_event['charging_duration']= pd.to_timedelta(df_event['charging_duration'])
    df_event['plugin_duration']= pd.to_timedelta(df_event['plugin_duration'])

    
    #charging_durations in [h]
    df_event['charging_duration [h]'] = df_event['charging_duration']/pd.Timedelta('1h')
    
    #Cross-check session_energy_consumed: Determine whether charged energy volumne can be met with given charge power
    df_event['mean charge_power charging'] = df_event['mean charge_power charging'].fillna(0)
    df_event['energy_calculated'] = df_event['charging_duration [h]'] * df_event['mean charge_power charging']
    del df_event['charging_duration [h]']

    #Allow 5% deviation
    df_event['cross_check_energy 5%'] = df_event['session_energy_consumed'].between((df_event['energy_calculated']*0.95),
                                                                                    (df_event['energy_calculated']*1.05))

    # dtypes
    df_event['plugin_time'] = pd.to_datetime(df_event['plugin_time'])
    df_event['plugout_time'] = pd.to_datetime(df_event['plugout_time'])
    df_event['max charge_power'] = pd.to_numeric(df_event['max charge_power'])
    df_event['mean charge_power event'] = pd.to_numeric(df_event['mean charge_power event'])
    df_event['session_energy_consumed'] = pd.to_numeric(df_event['session_energy_consumed'])
    df_event['first energy'] = pd.to_numeric(df_event['first energy'])

    lines = len(df_event.axes[0])
    pits = df_event['plugin_time'].nunique()
    print('Data has '+str(lines)+' charging events')
    print('Data has '+str(pits)+' plugin times')
    
    return df_event

def clean_original (df, df_event, maxcharge, minduration, minenergy):
    """
    Optional cleaning of data, so each event has to meet the minimum restrictions
    """
    for row in df_event.index:
        if df_event['max charge_power'][row] < maxcharge:
            df = df[df['id'] != df_event['id'][row]]

        if not (isinstance(df_event['plugin_duration'][row], int) or isinstance(df_event['plugin_duration'][row], float)):
            minduration_act = df_event['plugin_duration'][row].seconds   
            minduration_act = minduration_act/60.0
            if minduration_act < minduration: 
                 df = df[df['id'] != df_event['id'][row]]
                    
        if df_event['session_energy_consumed'][row] < minenergy: 
            df = df[df['id'] != df_event['id'][row]]
            
    return df

def resample_data(df,resolution):
    """
    Create timeseries from data in given interval
    """

    #Reset total_energy_consumed
    df = total_energy_consumed_reset(df)

    # Resample the data
    df2 = df.set_index('meter_values_timestamp').resample(str(resolution) + 'T').agg({'L1_current':'mean',
                                                                                      'L2_current':'mean',
                                                                                      'L3_current':'mean',
                                                                                      'L1_offer':'mean',
                                                                                      'L2_offer':'mean',
                                                                                      'L3_offer':'mean',
                                                                                      'charge_power':'mean',
                                                                                      'charger_id':'last',
                                                                                      'connector_id':'last',
                                                                                      'charge_point_id':'last',
                                                                                      'ev_suspended':'last',
                                                                                      'id': 'last',
                                                                                      'plugin_time':'last',
                                                                                      'session_energy_consumed':'last',
                                                                                      'status':'last',
                                                                                      'total_energy_consumed':'last',
                                                                                      'total_energy_consumed_reset': 'sum',
                                                                                      'soc':'last',
                                                                                      'transaction_ongoing':'last',
                                                                                      'event_max':'max',
                                                                                      'charger_max':'last',
                                                                                      'site_max':'last'})

    return df2

def filling_data(df2, resolution):
    """
    Fill empty interval of timeseries
    """
    #scheme for filling the nan-values: if ffill = bfill --> charging, otherwise no charging
    s = df2['plugin_time'].ffill()
    x = df2['plugin_time'].bfill()
    g = df2['plugin_time'].mask(s.eq(x), s)

    #Filling energy
    df2['session_energy_consumed'] = df2['session_energy_consumed'].groupby(g).ffill().fillna(0)

    #Filling plugin_time
    df2['plugin_time'] = df2['plugin_time'].groupby(g).ffill()

    #Filling ID
    df2['id'] = df2['id'].groupby(g).ffill().fillna('-')#.astype(int, errors = 'ignore')

    #Filling event_max
    df2['event_max'] = df2['event_max'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')

    #Filling charger_max
    df2['charger_max'] = df2['charger_max'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')

    #Filling site_max
    df2['site_max'] = df2['site_max'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')

    #Filling charge_current
    df2['L1_current'] = df2['L1_current'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')
    df2['L2_current'] = df2['L2_current'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')
    df2['L3_current'] = df2['L3_current'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')

    #Filling charge_offer
    df2['L1_offer'] = df2['L1_offer'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')
    df2['L2_offer'] = df2['L2_offer'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')
    df2['L3_offer'] = df2['L3_offer'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')


    #Filling total_energy
    df2['total_energy_consumed'] = df2['total_energy_consumed'].mask(s.eq(x), df2['total_energy_consumed'].interpolate(
        limit_direction='both', limit_area='inside'))
    df2['total_energy_consumed'] = df2['total_energy_consumed'].ffill().astype(float, errors = 'ignore')


    #Filling total_energy
    df2['total_energy_consumed_reset'] = df2['total_energy_consumed_reset'].fillna(0).astype(float, errors = 'ignore')
    df2['total_energy_consumed'] = df2['total_energy_consumed_reset'].cumsum()


    #Filling power
    df2['charge_power'] = df2['total_energy_consumed_reset'] * (60 / resolution)
    df2['charge_power'] = df2['charge_power'].groupby(g).ffill().fillna(0).astype(float, errors = 'ignore')

    #Filling soc
    df2['soc'] = df2['soc'].ffill()

    #Filling station
    df2['charger_id'] = df2['charger_id'].ffill()
    df2['connector_id'] = df2['connector_id'].ffill()
    df2['charge_point_id'] = df2['charge_point_id'].ffill()

    #Filling status
    df2['status'] = df2['status'].groupby(g).ffill().fillna('-')

    #Filling transaction_ongoing
    df2['transaction_ongoing'] = df2['transaction_ongoing'].groupby(g).ffill().fillna(False)
    df2['transaction_ongoing'] = df2["transaction_ongoing"].map({1:True, 0: False})

    #Filling ev_suspended
    df2['ev_suspended'] = df2['ev_suspended'].groupby(g).ffill().fillna(False)
    df2['ev_suspended'] = df2["ev_suspended"].map({1:True, 0: False})
    
    return df2


def revised_df(df,resolution):
    """
    Filling the missing logs at the beginning of charging event
    """

    df2 = df.copy()

    plugin_time_bfill = df2['plugin_time'].bfill()
    df2['plugin_time2'] = df2['plugin_time'].mask(df2.index.to_series().ge(plugin_time_bfill), plugin_time_bfill)

    # Filling ID
    id_replace = df2['id'].replace('-', np.nan)
    df2['id2'] = df2['id'].mask((df2['id'] == '-') & (df2['plugin_time2'].notna()), id_replace)
    df2['id2'] = df2['id2'].bfill()

    # Filling ev_suspendend: values aren't cosistent!!!
    df2['ev_suspended2'] = df2['ev_suspended'].mask((df2['ev_suspended'] == False) & (df2['plugin_time2'].notna()),
                                                    True)

    # set status for these logs to 'charging'
    df2['status2'] = df2['status'].mask((df2['status'] == '-') & (df2['plugin_time2'].notna()), 'charging')

    # set transaction_ongoing for these logs to 'True'
    df2['transaction_ongoing2'] = df2['transaction_ongoing'].mask(
        (df2['transaction_ongoing'] == False) & (df2['plugin_time2'].notna()), True)

    # Filling event_max
    event_replace = df2['event_max'].replace(0, np.nan)
    df2['event_max2'] = df2['event_max'].mask((df2['event_max'] == 0) & (df2['plugin_time2'].notna()), event_replace)
    df2['event_max2'] = df2['event_max2'].bfill()

    # Filling charger_max
    charger_replace = df2['charger_max'].replace(0, np.nan)
    df2['charger_max2'] = df2['charger_max'].mask((df2['charger_max'] == 0) & (df2['plugin_time2'].notna()),
                                                  charger_replace)
    df2['charger_max2'] = df2['charger_max2'].bfill()

    # Filling site_max
    site_replace = df2['site_max'].replace(0, np.nan)
    df2['site_max2'] = df2['site_max'].mask((df2['site_max'] == 0) & (df2['plugin_time2'].notna()), site_replace)
    df2['site_max2'] = df2['site_max2'].bfill()

    # Filling session_energy_consumed linear (two steps are needed)
    df2['session_energy_consumed2'] = df2['session_energy_consumed'].mask(
        (df2['plugin_time'].isnull()) & (df2['plugin_time2'].notna()) & (df2.index.to_series().ne(df2['plugin_time2'])),
        np.NaN)
    df2['session_energy_consumed3'] = df2['session_energy_consumed2'].interpolate(limit_direction='both',
                                                                                  limit_area='inside')

    # Calculating the power for the logs
    df2['charge_power2'] = df2['charge_power'].mask((df2['charge_power'] == 0) & (df2['plugin_time2'].notna())
                                                    & ((df2['session_energy_consumed3'] - df2[
        'session_energy_consumed3'].shift()) > 0)
                                                    & ((60 / resolution) * (
                df2['session_energy_consumed3'] - df2['session_energy_consumed3'].shift()) < df2['event_max2']),
                                                    (60 / resolution) * (df2['session_energy_consumed3'] - df2[
                                                        'session_energy_consumed3'].shift()))

    # Filling session_energy_consumed only, when physical poosible
    df2['session_energy_consumed4'] = df2['session_energy_consumed'].mask(
        (df2['session_energy_consumed'] == 0) & (df2['plugin_time2'].notna())
        & ((60 / resolution) * (df2['session_energy_consumed3'] - df2['session_energy_consumed3'].shift()) < df2[
            'event_max2']),
        df2['session_energy_consumed3'])

    # Re-change the columns after calculating the missing values
    df2[['charge_power', 'session_energy_consumed', 'total_energy_consumed', 'total_energy_consumed_reset', 'plugin_time', 'id',
         'event_max', 'charger_max', 'site_max',
         'charger_id', 'ev_suspended', 'status', 'transaction_ongoing',
         'L1_current', 'L2_current', 'L3_current', 'L1_offer', 'L2_offer', 'L3_offer']] = df2[
        ['charge_power2', 'session_energy_consumed4', 'total_energy_consumed', 'total_energy_consumed_reset', 'plugin_time2', 'id2',
         'event_max2', 'charger_max2', 'site_max2',
         'charger_id', 'ev_suspended2', 'status2', 'transaction_ongoing2',
         'L1_current', 'L2_current', 'L3_current', 'L1_offer', 'L2_offer', 'L3_offer']]

    # drop columns which are no longer needed
    df2.drop(['charge_power2', 'session_energy_consumed4', 'session_energy_consumed3', 'session_energy_consumed2',
              'plugin_time2', 'id2',
              'event_max2', 'charger_max2', 'site_max2',
              'ev_suspended2', 'status2', 'transaction_ongoing2'], axis=1, inplace=True)

    return df2

def discrete_df(df,resolution, reviseData):
    """
    Combines all functions to create timeseries out of data. Resolution can be chosen (in min). Optional completion of missing logs in the beginning of the charging event.
    """
    print('Create discrete data in '+str(resolution)+' min resolution...')
    df = resample_data(df,resolution)
    df = filling_data(df, resolution)
    if reviseData == True:
        print('Filling the missing logs at the beginning of an event...')
        df = revised_df(df,resolution)
    
    return df

def optimization_input_cp(path,folder,optimization):
    """
    Create input file for mumop-Tool to run smart sourcing on charge point level (Function is optional)
    """
    if optimization == True:
        #Get all discrete charge point files
        files = glob.glob(f'{path}/{PATH_OUTPUT}/{folder}/*' + "discrete.csv")

        for file in files:
            df_opt = read_csv(file, delimiter=";", decimal=",", doublequote=True, encoding="utf-8",
                              index_col='meter_values_timestamp')
            df_opt.index = pd.to_datetime(df_opt.index)

            df_opt['time'] = df_opt.index
            df_opt['time'] = df_opt['time'].dt.time

            #Binary variable, if charging is allowed
            df_opt['trading_active'] = np.where(df_opt['event_max'] > 0, 1, 0)

            #Transforming in MW
            df_opt['power_trading_max_mw'] = df_opt['event_max'] / 1000000

            df_opt['soe_min_mwh'] = 0
            df_opt['soe_max_mwh'] = 0

            #Calculating soe_min and soe_max in each interval
            t = df_opt['id'].unique()
            counter = 0
            for item in t:
                df_sub = df_opt[df_opt.id == item]
                x = df_sub.total_energy_consumed.min()

                # Index +1 % special case: last entry of df
                y = df_sub.total_energy_consumed.max()

                if counter != len(t) - 1:
                    y = df_opt['total_energy_consumed'][df_sub.index.max() + pd.Timedelta(minutes=15)]

                if counter != 0:
                    x = df_opt['total_energy_consumed'][df_sub.index.min() - pd.Timedelta(minutes=15)]

                df_opt['soe_max_mwh'] = np.where(df_opt['id'] == item, y, df_opt['soe_max_mwh'])

                df_opt['soe_min_mwh'] = np.where(df_opt['id'] == item, x, df_opt['soe_min_mwh'])
                df_opt['soe_min_mwh'] = np.where(df_opt.index == df_sub.index.max(),
                                                     df_opt['soe_max_mwh'],
                                                     df_opt['soe_min_mwh'])
                counter = counter + 1

            #Filling soe limits, when no EV is plugged
            df_opt['soe_min_mwh'] = np.where(df_opt['id'] == "-", df_opt['total_energy_consumed'],
                                             df_opt['soe_min_mwh'])
            df_opt['soe_max_mwh'] = np.where(df_opt['id'] == "-", df_opt['total_energy_consumed'],
                                             df_opt['soe_max_mwh'])


            z = df_opt['id'].unique()
            for item in z:
                df_sub = df_opt[df_opt.id == item]
                df_opt['power_trading_max_mw'] = np.where(df_opt['id'] == item, df_sub['event_max'].max() / 1000000,
                                                          df_opt['power_trading_max_mw'])

                df_opt['power_trading_max_mw'] = np.where(df_opt.soe_min_mwh.shift(-1) > df_opt.soe_max_mwh,
                                                          (df_opt.soe_min_mwh.shift(
                                                              -1) - df_opt.soe_min_mwh) * 4 / 1000000,
                                                          df_opt['power_trading_max_mw'])
                df_opt['soe_max_mwh'] = np.where(df_opt.soe_min_mwh.shift(-1) > df_opt.soe_max_mwh,
                                                 df_opt.soe_max_mwh.shift(-1), df_opt['soe_max_mwh'])

                df_opt['trading_active'] = np.where(df_opt['power_trading_max_mw'] > 0, 1, 0)

                s = df_opt['id'].unique()
                df_opt['soe_min_mwh'] = np.where(df_opt['id'] == s[0], 0, df_opt['soe_min_mwh'])
                df_opt['soe_min_mwh'] = np.where(df_opt.index == df_opt[df_opt['id'] == s[0]].index.max(),
                                                 df_opt['total_energy_consumed'],
                                                 df_opt['soe_min_mwh'])

            #Transforming to MW
            df_opt['soe_min_mwh'] = df_opt['soe_min_mwh'] / 1000000
            df_opt['soe_max_mwh'] = df_opt['soe_max_mwh'] / 1000000


            #Setup index column
            df_opt['t_index'] = 0
            df_index = df_opt['soe_max_mwh']
            df_index = df_index.reset_index()
            df_opt['t_index'] = df_index.index

            #Add historical charged quantities
            df_opt['power_actual_in_mw'] = (df_opt.total_energy_consumed - df_opt.total_energy_consumed.shift(
                    1)) * 4 / 1000000
            df_opt.loc[df_opt.index == df_opt.index.min(),'power_actual_in_mw'] = df_opt.total_energy_consumed * 4 / 1000000

            # earrange and delete columns
            del df_opt['L1_current'], df_opt['L2_current'], df_opt['L3_current'], df_opt['L1_offer'], df_opt[
                'L2_offer'], \
                df_opt['L3_offer']
            del df_opt['charger_id'], df_opt['connector_id'], df_opt['ev_suspended'], df_opt['transaction_ongoing']

            df_opt = df_opt[['time', 't_index', 'trading_active', 'power_trading_max_mw', 'soe_min_mwh', 'soe_max_mwh',
                             'charge_point_id', 'id', 'plugin_time', 'session_energy_consumed', 'total_energy_consumed',
                             'event_max', 'charger_max', 'site_max', 'power_actual_in_mw']]

            #Save optimization file
            charge_point = file.rsplit('\\', 1)[-1]
            charge_point = file.rsplit('/', 1)[-1] #Mac: '/' instead of '\\'
            charge_point = charge_point[:-12]
            df_opt.to_csv(f'{PATH_OUTPUT}/{folder}/' + charge_point + 'optimisation.csv', sep=';', decimal=",",
                          index=True)

    return

def optimization_input_site(path,folder,optimization,resolution):
    """
    Create input file for mumop-Tool to run smart sourcing on site level (Function is optional)
    """
    if optimization == True:
        #Get all optimisation files on charge point level
        filenames = glob.glob(f'{path}/{PATH_OUTPUT}/{folder}/*' + '_optimisation.csv')

        #read all files
        dct_df = {}
        i = 0
        for filename in filenames:
            df = pd.read_csv(filename, delimiter=";", decimal=",", doublequote=True, encoding="utf-8", index_col=None)
            df.meter_values_timestamp = pd.to_datetime(df.meter_values_timestamp)
            dct_df[i] = df
            i = i + 1

        dct_df_concat = dct_df.copy()

        df_all = []

        # concat all files to one
        key = 0

        for key in range(len(dct_df_concat) - 1):
            df_sub = pd.concat([dct_df_concat[key][cols_site_opt], dct_df_concat[key + 1][cols_site_opt]]).groupby(
                ['meter_values_timestamp']).sum()
            dct_df_concat[key + 1] = df_sub
            dct_df_concat[key + 1] = dct_df_concat[key + 1].reset_index()
            df_all = dct_df_concat[key + 1].copy()
            key = key + 1


        #Binary variable
        df_all['trading_active'] = np.where(df_all['power_trading_max_mw'] > 0, 1, 0)

        df_all.set_index('meter_values_timestamp', inplace=True)

        #Complete timeseries if there is a gap in timeseries
        df_all = df_all.resample(str(resolution) + 'T').agg({'trading_active': 'mean',
                                                             'power_trading_max_mw': 'mean',
                                                             'soe_min_mwh': 'mean',
                                                             'soe_max_mwh': 'mean',
                                                             'power_actual_in_mw': 'mean'})



        #Filling the completed timeseries
        # Filling trading_active
        df_all['trading_active'] = df_all['trading_active'].fillna(0).astype(float, errors='ignore')

        # Filling power
        df_all['power_trading_max_mw'] = df_all['power_trading_max_mw'].fillna(0).astype(float, errors='ignore')
        df_all['power_actual_in_mw'] = df_all['power_actual_in_mw'].fillna(0).astype(float, errors='ignore')

        # Filling soe
        df_all['soe_min_mwh'] = df_all['soe_min_mwh'].ffill().astype(float, errors='ignore')
        df_all['soe_max_mwh'] = df_all['soe_max_mwh'].ffill().astype(float, errors='ignore')

        #Setup index and time column
        df_index = df_all['soe_max_mwh']
        df_index = df_index.reset_index()
        df_all['t_index'] = df_index.index
        df_all['time'] = df_all.index
        df_all['time'] = df_all['time'].dt.time

        #Rearrange columns
        df_all = df_all[['time', 't_index', 'trading_active', 'power_trading_max_mw', 'soe_min_mwh', 'soe_max_mwh',
                         'power_actual_in_mw']]

        # Save site file
        df_all.to_csv(f'{PATH_OUTPUT}/{folder}/site_optimisation.csv', sep=';', decimal=',', index=True)
    return


def data_preparation(path,folder, site_max,charger_max,cleanData,minimum_charge_power,minimum_plugin_duration,minimum_energy,resolution,reviseData, timezone):
    """
    Combines all function and loop to perform data preparation for all available charge points in the output folder
    """
    files = glob.glob(f'{path}/{PATH_OUTPUT}/{folder}/*'+".csv")

    #Iterate through all charge points
    for file in files:
        print("")
        charge_point = file.rsplit('\\', 1)[-1]
        charge_point = file.rsplit('/', 1)[-1]  # Mac: '/' instead of '\\'
        print(charge_point[:-4])
        print("")
        df = csv2df(file)
        df = prepare_df(df,site_max,charger_max,timezone)

        df_event = df_event_creation(df,site_max,charger_max)

        #Cleaning data (optional)
        if cleanData == True:
            df2 = clean_original(df,df_event,minimum_charge_power,minimum_plugin_duration,minimum_energy)
            df_event2 = df_event_creation(df2,site_max,charger_max)
            df3 = discrete_df(df2, resolution, reviseData)
            error_duplicated_logs(df3)
        else:
            df3 = discrete_df(df, resolution, reviseData)
            error_duplicated_logs(df3)
            df_event2 = df_event

        #Save files and check if still data exists after cleaning
        if not df3.empty:
            df3.to_csv(f'{PATH_OUTPUT}/{folder}/charge_point_' + df3['charge_point_id'][0] + '_discrete.csv', sep = ';', decimal=",", index = True)
        if not df_event2.empty:
            df_event2.to_csv(f'{PATH_OUTPUT}/{folder}/charge_point_' + df_event2['charge_point_id'][0] + '_event.csv', sep = ';', decimal=",", index = True)
    return


def add_site_data_discrete(path, folder, resolution):
    """
    Create discrete file on site level out of all charge points. Then add this site data to single charge point files
    """

    #Get all discrete charge point files
    filenames = glob.glob(f'{path}/{PATH_OUTPUT}/{folder}/*'+'_discrete.csv')


    #read all files
    dct_df = {}
    i = 0
    for filename in filenames:  
        df = pd.read_csv(filename, delimiter = ";",decimal = ",", doublequote = True , encoding = "utf-8", index_col=None)
        df.meter_values_timestamp = pd.to_datetime(df.meter_values_timestamp)
        dct_df[i] = df 
        i = i+1

    dct_df_concat = dct_df.copy()

    dct_df_concat_cp = dct_df.copy()

    df_all = []

    #concat all files to one
    key=0
    
    for key in range(len(dct_df_concat)-1):    
        df_sub = pd.concat([dct_df_concat[key][cols_site], dct_df_concat[key+1][cols_site]]).groupby(['meter_values_timestamp']).sum()
        dct_df_concat[key+1] = df_sub
        dct_df_concat[key+1] = dct_df_concat[key+1].reset_index()
        df_all = dct_df_concat[key+1].copy()
        key = key +1
    # rename columns of df_data
    df_all.columns = ['meter_values_timestamp','charge_power_site', 'evs_max', 'L1_current_site', 'L2_current_site', 'L3_current_site','L1_offer_site', 'L2_offer_site', 'L3_offer_site', "total_energy_consumed_site"]
    df_all.set_index('meter_values_timestamp', inplace=True)

    df_all['active_charge_points'] = 0


    #Complete timeseries if there is a time gap between charge point files
    df_all = df_all.resample(str(resolution) + 'T').agg({'L1_current_site':'mean',
                                                                                      'L2_current_site':'mean',
                                                                                      'L3_current_site':'mean',
                                                                                      'L1_offer_site':'mean',
                                                                                      'L2_offer_site':'mean',
                                                                                      'L3_offer_site':'mean',
                                                                                      'charge_power_site':'mean',
                                                         'evs_max':'mean',
                                                         'total_energy_consumed_site':'last',
                                                         'active_charge_points':'mean',})


    #Filling the completed timeseries
    # Filling event_max
    df_all['evs_max'] = df_all['evs_max'].fillna(0).astype(float, errors='ignore')


    # Filling charge_current
    df_all['L1_current_site'] = df_all['L1_current_site'].fillna(0).astype(float, errors='ignore')
    df_all['L2_current_site'] = df_all['L2_current_site'].fillna(0).astype(float, errors='ignore')
    df_all['L3_current_site'] = df_all['L3_current_site'].fillna(0).astype(float, errors='ignore')

    # Filling charge_offer
    df_all['L1_offer_site'] = df_all['L1_offer_site'].fillna(0).astype(float, errors='ignore')
    df_all['L2_offer_site'] = df_all['L2_offer_site'].fillna(0).astype(float, errors='ignore')
    df_all['L3_offer_site'] = df_all['L3_offer_site'].fillna(0).astype(float, errors='ignore')

    # Filling power
    df_all['charge_power_site'] = df_all['charge_power_site'].fillna(0).astype(float, errors='ignore')

    # Filling total_energy
    df_all['total_energy_consumed_site'] = df_all['total_energy_consumed_site'].fillna(0).astype(float, errors = 'ignore')

    # Filling power
    df_all['active_charge_points'] = df_all['active_charge_points'].fillna(0).astype(float, errors='ignore')


    #Total energy consumed cumsum
    df_all['total_energy_consumed_site'] = df_all['total_energy_consumed_site'].cumsum()

    #Setup extra columns for each charge point in site file, which contain the ids of charging event
    cp = 0
    for cp in range(len(dct_df_concat_cp)):

        df_sub = dct_df_concat_cp[cp].copy()
        newname = df_sub['charge_point_id'][0]
        df_sub.rename(columns={'id':newname}, inplace=True)

        df_sub.set_index('meter_values_timestamp', inplace=True)
        df_sub = df_sub[[newname]]

        df_all = pd.merge(df_all,df_sub, how='left', on=['meter_values_timestamp'])

        df_all[newname] = np.where(df_all[newname] == "-",None, df_all[newname])

        df_all['active_charge_points'] = np.where(pd.isnull(df_all[newname]), df_all['active_charge_points'],
                                                      df_all['active_charge_points'] + 1)


        cp = cp + 1

    # save site file
    df_all.to_csv(f'{PATH_OUTPUT}/{folder}/site_discrete.csv', sep = ';', decimal = ',', index = True)

    # add total values to single charge point files
    key=0
    for key in range(len(dct_df)): 
        #dct_df[key] = pd.merge_asof(dct_df[key], df_all, left_on='meter_values_timestamp')
        dct_df[key] = dct_df[key].merge(df_all, how = 'left', on=['meter_values_timestamp'])
        key = key +1

    # save data to csv
    i = 0
    for filename in filenames:
        dct_df[i] = dct_df[i][['meter_values_timestamp','L1_current','L2_current','L3_current','L1_offer','L2_offer','L3_offer','charge_power','charger_id','connector_id','charge_point_id','ev_suspended','id','plugin_time','session_energy_consumed','total_energy_consumed','status','transaction_ongoing','event_max','charger_max','site_max','charge_power_site','L1_current_site','L2_current_site','L3_current_site','L1_offer_site','L2_offer_site','L3_offer_site']]
        dct_df[i].to_csv(filename, sep = ';', decimal = ',', index = False)
        i = i+1

    return



def add_site_data_event(path, folder):
    """
    Create event site file out of all charge point event files
    """
    filenames = glob.glob(f'{path}/{PATH_OUTPUT}/{folder}/*' + '_event.csv')

    # read all files
    dct_df = {}
    i = 0
    for filename in filenames:
        df = pd.read_csv(filename, delimiter=";", decimal=",", doublequote=True, encoding="utf-8", index_col=0)
        dct_df[i] = df
        i = i + 1

    dct_df_concat = dct_df.copy()

    df_all = []

    # concat all files to one
    key = 0

    for key in range(len(dct_df_concat) - 1):
        df_sub = pd.concat([dct_df_concat[key], dct_df_concat[key + 1]])
        dct_df_concat[key + 1] = df_sub
        df_all = dct_df_concat[key + 1].copy()
        key = key + 1

    # sort and set index
    df_all.sort_values('plugin_time')
    df_all = df_all.reset_index(drop=True)


    # save site file
    df_all.to_csv(f'{PATH_OUTPUT}/{folder}/site_event.csv', sep=';', decimal=',', index=True)

    return

def add_site_data(path, folder, resolution, optimization):
    """
    Combines function to create site file for discrete, event and optimisation file
    """
    add_site_data_discrete(path,folder,resolution)
    add_site_data_event(path,folder)
    optimization_input_site(path, folder, optimization, resolution)

    return
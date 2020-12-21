#Import libraries
import csv
import pandas as pd
from pandas import *
import numpy as np
import yaml
import glob
import dateutil.parser
import math
from datetime import timezone, datetime, timedelta
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from curlyBrace import curlyBrace
import locale
import plotly.graph_objects as go
from plotly.subplots import make_subplots

#Input paths
PATH_INPUT = 'data/input'
PATH_OUTPUT = 'data/output'


#Functions

def plot_garage_statistics(site_discrete, folder):
    """
    Parameters
    ----------
    site_discrete
    folder (could be used to save images/csv, not yet implemented)

    Returns
    -------
    per weekday plots of charging power and energy demand with relating quantiles
    """

    def unstack(df):
        # unstack multiindex, sort weekdays and drop multiindex column
        df = df.unstack(level=0)
        df = df.sort_index(axis=1, key=lambda x: pd.Series(pd.Categorical(x, categories=days, ordered=True)))
        df.columns = df.columns.droplevel(level=0)
        return df

    def plot_energy(df_list, days, df_list_qty):
        fig = make_subplots(rows=2, cols=1, start_cell='top-left', subplot_titles=['Histogram', 'Statistical parameters'])
        colors = ['#081F47', '#133561', '#22588C', '#327bab', '#669cc0', '#6D94AC', '#B8CCDE']
        colors_qty = ['#038993', '#0F9B99', '#56BCB5', '#83CAC9']
        names_qty = ['60% quantile', '80% quantile', 'average']

        for df, day, color in zip(df_list, days, colors):
            fig.add_trace(go.Histogram(x=df, histnorm='percent', name=day, marker_color=color, opacity=0.75),
                          row=1, col=1)

        for df_qty, color, name in zip(df_list_qty, colors_qty, names_qty):
            df_qty = df_qty.sort_index(axis=0, key=lambda x: pd.Series(pd.Categorical(x, categories=days, ordered=True)))
            fig.add_trace(go.Bar(x=df_qty.index, y=df_qty['energy_demand_site'], name=name, marker_color=color), row=2, col=1)

        fig.update_layout(
            title='Energy demand',
            xaxis_title='Energy demand in kWh',
            yaxis_title='Count (days)', yaxis2_title='Energy demand in kWh',
            bargap=0.2,  # gap between bars of adjacent location coordinates
            bargroupgap=0.1  # gap between bars of the same location coordinates
        )

        fig.show()

    def plot_power(df_list, days):
        fig = make_subplots(rows=2, cols=3, start_cell='top-left', subplot_titles=days)
        names = ['average<br>(max_evs)', '60% quantile<br>(max_evs)', '40% quantile<br>(max_evs)', 'average', '60% quantile', '40% quantile']
        colors = ['#327bab', '#22588C', '#133561', '#b8d5cd', '#8abaae', '#2e856e']
        show_legend = ['legendonly', True, True, 'legendonly', True, True]
        group_legend = ['group_soc', 'group_soc', 'group_soc', 'group_soc', 'group_soc', 'group_soc']
        rows = [1, 1, 1, 2, 2, 2]
        cols = [1, 2, 3, 1, 2, 3]
        for day, row, col in zip(days, rows, cols):
            for df, name, color, legend, group in zip(df_list, names, colors, show_legend, group_legend):
                # iterate df
                fig.add_trace(go.Scatter(x=df.index, y=df[day], line_shape='hv', fill='tozeroy', line_color=color,
                                         name=name, visible=legend, legendgroup=name, showlegend=True if day == days[0] else False),
                              row=row, col=col)
        fig.update_layout(title='Charging power and max event-based power with quantiles in kW')

        fig.show()

    # Grouping site_discrete data
    site_discrete['day_name'] = site_discrete.index.day_name(locale=None)
    site_discrete['time'] = site_discrete.index.time
    site_discrete['date'] = site_discrete.index.date
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    power_quantile = [0.4, 0.6]
    energy_quantile = [0.6, 0.8]


    df_wkd_power_avg = unstack(site_discrete.groupby(['day_name', 'time']).agg({'charge_power_site': lambda x: x.mean()/1000}))
    [df_wkd_power_qty_40, df_wkd_power_qty_60] = [site_discrete.groupby(['day_name', 'time']).agg
                                                  ({'charge_power_site': lambda x: x.quantile(i)/1000}) for i in power_quantile]  # [x>0]
    [df_wkd_power_qty_40, df_wkd_power_qty_60] = [unstack(i) for i in [df_wkd_power_qty_40, df_wkd_power_qty_60]]

    df_wkd_power_max_avg = unstack(site_discrete.groupby(['day_name', 'time']).agg({'evs_max': lambda x: x.mean() / 1000}))
    [df_wkd_power_max_qty_40, df_wkd_power_max_qty_60] = [site_discrete.groupby(['day_name', 'time']).agg
                                                          ({'evs_max': lambda x: x.quantile(i) / 1000}) for i in power_quantile]  # [x>0]
    [df_wkd_power_max_qty_40, df_wkd_power_max_qty_60] = [unstack(i) for i in [df_wkd_power_max_qty_40, df_wkd_power_max_qty_60]]

    df_wkd_energy = site_discrete.groupby('date').agg({'charge_power_site': lambda x: x.sum()/4/1000, 'day_name': 'first'})
    df_wkd_energy.rename(columns={'charge_power_site': 'energy_demand_site'}, inplace=True)
    df_wkd_energy_avg = df_wkd_energy.groupby('day_name').agg({'energy_demand_site': 'mean'})
    df_wkd_energy_qty = [df_wkd_energy.groupby(['day_name']).agg({'energy_demand_site': lambda x: x.quantile(i)}) for i in energy_quantile]
    df_wkd_energy_qty.append(df_wkd_energy_avg)

    # calling plot functions
    plot_power([df_wkd_power_max_avg,  df_wkd_power_max_qty_60, df_wkd_power_max_qty_40,
                df_wkd_power_avg, df_wkd_power_qty_60, df_wkd_power_qty_40], days)
    plot_energy([df_wkd_energy[df_wkd_energy['day_name'] == i]['energy_demand_site'] for i in days],
                days, df_wkd_energy_qty)



def prepare_discrete(path, folder):
    """
    Get discrete site file and transform timestamp
    """
    file = glob.glob(f'{path}/{PATH_OUTPUT}/{folder}/site_discrete.csv')

    site_discrete = read_csv(file[0], delimiter=";", decimal=",", doublequote=True, encoding="utf-8")
    site_discrete['meter_values_timestamp'] = pd.to_datetime(site_discrete['meter_values_timestamp'])
    site_discrete = site_discrete.set_index('meter_values_timestamp')

    return site_discrete

def prepare_event(path, folder, df_analyse):
    """
    Get event site file and transform timestamp
    """
    file = glob.glob(f'{path}/{PATH_OUTPUT}/{folder}/site_event.csv')

    site_event = read_csv(file[0], delimiter=";", decimal=",", doublequote=True, encoding="utf-8")

    site_event = site_event.sort_values('plugin_time')
    site_event = site_event.reset_index(drop=True)
    del site_event['Unnamed: 0']

    site_event['plugin_time'] = pd.to_datetime(site_event['plugin_time'])
    site_event['plugout_time'] = pd.to_datetime(site_event['plugout_time'])
    site_event['plugin_duration'] = pd.to_timedelta(site_event['plugin_duration'])
    site_event['charging_duration'] = pd.to_timedelta(site_event['charging_duration'])
    site_event = site_event[site_event['plugin_duration'] > timedelta(0)]
    x = int(site_event.shape[0])

    df_analyse.loc[0, 'Number charging events'] = x

    # Only analyse events which meet cross-check
    site_event = site_event[site_event['cross_check_energy 5%'] == True].copy()
    del site_event['cross_check_energy 5%']
    del site_event['energy_calculated']

    y = int(site_event.shape[0])
    df_analyse.loc[0, 'Number charging events without log error'] = y

    z = (x - y) / x
    df_analyse.loc[0, 'Log-error in %'] = round(z, 4) * 100

    return site_event


def boxplot_Ladedauer(site_event, df_analyse, folder):
    """
    Boxplot graphic creation for plugin duration
    """
    site_event['plugin_duration'] = site_event['plugin_duration'].dt.total_seconds() / 3600

    medianprops = dict(color='black')
    plt.rcParams.update({'font.size': 18})

    box = plt.boxplot(site_event['plugin_duration'], labels=(['plugin duation']), medianprops=medianprops, zorder=3)
    plt.ylabel('plugin duration')
    plt.grid(axis='y')
    plt.tight_layout
    plt.savefig(f'{PATH_OUTPUT}/{folder}/boxplot_charging_duration_original.pdf', bbox_inches='tight')

    # !!!  Req. debugging --- IndexError: index 2 is out of bounds for axis 0 with size 2
    # if len(box["fliers"][0].get_data()[1]) >= 1:
    #     plt.plot([1], [box["fliers"][0].get_data()[1][0]], 'b+')
    #     plt.plot([1], [box["fliers"][0].get_data()[1][1]], 'r+')
    #     plt.plot([1], [box["fliers"][0].get_data()[1][2]], 'b+')
    #     plt.plot([1], [box["fliers"][0].get_data()[1][3]], 'r+')
    # plt.clf()
    # !!!

    top_points = box['fliers'][0].get_data()[1]
    #     #bottom_points = box["fliers"][2].get_data()[1]
    #     point_min = top_points.min()
    #     print(point_min)
    #     point_max = box['fliers'][0].get_data()[1][1]
    #     print(box['fliers'][0].get_data()[1])
    #     print(box['fliers'][0].get_data()[1][0])
    #     print(box['fliers'][0].get_data()[1][1])
    #     print(box['fliers'][0].get_data()[1][2])
    #     print(box['fliers'][0].get_data())
    #     #print(box['fliers'][0].get_data()[0][1])
    #     print(point_max)
    #     point_max = top_point
    outlier = round(len(top_points) / site_event.shape[0], 4) * 100

    df_analyse.loc[0, 'Numbers_outlier_plugin_duration'] = int(len(top_points))
    df_analyse.loc[0, 'share_outliers_in_%'] = outlier

    plt.grid(axis='y', zorder=0)
    box2 = plt.boxplot(site_event['plugin_duration'], labels=(['Plugin duration']), medianprops=medianprops, zorder=3,
                       showfliers=False)
    plt.rcParams.update({'font.size': 18})
    plt.ylabel('Plugin duration [in h]')
    plt.tight_layout

    for item, row in site_event.iterrows():
        for points in top_points:
            if row['plugin_duration'] == points:
                site_event = site_event.drop([item])

    site_event = site_event.reset_index()

    #     plt.grid(axis = 'y', zorder = 0)
    #     box2 = plt.boxplot(site_event['plugin_duration'], labels = (['Ansteckzeit']), medianprops = medianprops, zorder = 3)
    #     plt.rcParams.update({'font.size': 18})
    #     plt.ylabel('Ansteckzeit in h')
    #     plt.tight_layout
    #     plt.savefig('Ladedauer_bereinigt.pdf', bbox_inches='tight')
    #     plt.show()

    site_event['plugin_duration'] = pd.to_timedelta(site_event['plugin_duration'] * 3600, unit='s')

    plt.savefig(f'{PATH_OUTPUT}/{folder}/boxplot_charging_duration_cleaned.pdf', bbox_inches='tight')

    plt.clf()

    return site_event

def E_histogramm_site(site_event, folder):
    """
    Histogram for charged energy and number of charging events
    """
    plt.grid(axis='y', zorder=0)
    energy_kwh = site_event['session_energy_consumed'] / 1000
    plt.hist(energy_kwh, bins=range(0, int(energy_kwh.max()), 1),
             edgecolor='black', color='grey', zorder=3)
    plt.xlim(0, int(energy_kwh.max()) + 1)
    plt.xlabel("charged energy [in kWh]")
    plt.ylabel("Number charging events")
    plt.tight_layout

    plt.savefig(f'{PATH_OUTPUT}/{folder}/E_histogramm_site.pdf', bbox_inches='tight')
    plt.clf()
    #return plt.show()

def charging_ratio_site(site_event, folder, resolution):
    """
    Histogram for ratio charging time to plugin time
    """
    plt.grid(axis='y', zorder=0)
    plt.hist(site_event['charge_duration [%]'], bins=range(0, 100, 10),
             edgecolor='black', color='grey', zorder=3)
    plt.xlim(0, 100)
    plt.xlabel("Ratio of charging time to plugin time [in %]")
    plt.ylabel("Number charging events")
    plt.tight_layout

    plt.savefig(f'{PATH_OUTPUT}/{folder}/charging_ratio_site.pdf', bbox_inches='tight')
    plt.clf()
    #return plt.show()

def plugged_in_ratio_site(site_discrete, folder, resolution):
    """
    Graphic: Plugged-in ratio for single charge point
    """
    site_discrete['plugged_in'] = np.where(site_discrete['active_charge_points'] > 0, 1, 0)
    site_discrete['day_name'] = site_discrete.index.day_name(locale=None)
    site_discrete['time'] = site_discrete.index.time
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df2 = site_discrete.groupby(['day_name', 'time']).agg({'plugged_in': 'count'})
    df2['plugged_ins'] = site_discrete.groupby(['day_name', 'time']).agg({'plugged_in': 'sum'})
    df2['ratio'] = df2['plugged_ins'] / df2['plugged_in'] * 100
    df2['x'] = 0
    df2 = df2.reindex(days, level=0)
    df2.index = [str(i) for i in (df2.index.map('{0[0]}, {0[1]}'.format))]


    plt.grid(axis='y', zorder=0)
    plt.plot(df2.index, df2['ratio'], color='grey', zorder=3)
    plt.ylabel('Plugin probability [in %]')
    plt.xticks(np.arange(0, len(df2.index), step=(720 / resolution)), rotation=90)
    plt.tight_layout

    plt.savefig(f'{PATH_OUTPUT}/{folder}/plugged_in_ratio_site_one_charge_point.pdf', bbox_inches='tight')
    plt.clf()

    del site_discrete['plugged_in']
    del site_discrete['day_name']
    del site_discrete['time']
    #return plt.show()

def utilization_site(site_discrete, folder, resolution):
    """
    Graphic: Utilization of site infrastructure
    """
    charge_points = site_discrete.iloc[:, 10:].shape[1]

    site_discrete['day_name'] = site_discrete.index.day_name(locale=None)
    site_discrete['time'] = site_discrete.index.time
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df2 = site_discrete.groupby(['day_name', 'time']).agg({'active_charge_points': 'mean'})
    df2['ratio'] = df2['active_charge_points'] / charge_points * 100

    df2 = df2.reindex(days, level=0)
    df2.index = [str(i) for i in (df2.index.map('{0[0]}, {0[1]}'.format))]


    plt.grid(axis='y', zorder=0)
    plt.plot(df2.index, df2['ratio'], color='grey', zorder=3)
    plt.ylabel('Utilization of infrastructure in %')
    plt.xticks(np.arange(0, len(df2.index), step=(720 / resolution)), rotation=90)
    plt.tight_layout


    plt.savefig(f'{PATH_OUTPUT}/{folder}/utilization_site.pdf', bbox_inches='tight')
    plt.clf()
    del site_discrete['day_name']
    del site_discrete['time']
    #return plt.show()

def plugged_in_energy_duration_site(site_discrete, site_event, folder, resolution):
    """
    Graphic: Average plugin duration and charging quantities on weekly profile
    """
    site_discrete['day_name'] = site_discrete.index.day_name(locale=None)
    site_discrete['time'] = site_discrete.index.time
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df2 = site_discrete.groupby(['day_name', 'time']).agg({'charge_power_site': 'count'})
    df2 = df2.reindex(days, level=0)
    df2.index = [str(i) for i in (df2.index.map('{0[0]}, {0[1]}'.format))]

    site_event2 = site_event[['plugin_time', 'plugin_duration', 'session_energy_consumed']].copy(deep=True)

    site_event2['plugin_time'] = site_event2['plugin_time'].dt.round(str(resolution) + 'T')
    site_event2['day_name'] = site_event2['plugin_time'].dt.day_name(locale=None)
    site_event2['time'] = site_event2['plugin_time'].dt.time

    site_event2['session_energy_consumed'] = pd.to_numeric(site_event2['session_energy_consumed'])

    site_event2['plugin_duration'] = site_event2['plugin_duration'].dt.total_seconds()

    site_event3 = site_event2.groupby(['day_name', 'time']).agg({'session_energy_consumed': 'mean'})
    site_event3['plugin_duration'] = site_event2.groupby(['day_name', 'time']).agg({'plugin_duration': 'mean'})
    site_event3['plugin_duration'] = pd.to_timedelta(site_event3['plugin_duration'], unit='s')

    site_event3 = site_event3.reindex(days, level=0)
    site_event3.index = [str(i) for i in (site_event3.index.map('{0[0]}, {0[1]}'.format))]

    df3 = pd.concat([df2, site_event3], axis=1)
    del df3['charge_power_site']
    df3['session_energy_consumed'] = df3['session_energy_consumed'].fillna(0)
    df3['plugin_duration'] = df3['plugin_duration'].fillna(timedelta(hours=0))

    plt.plot(df3.index, df3['session_energy_consumed'], color='grey', zorder=3)
    plt.ylabel('Average charged energy [in Wh]')
    plt.xticks(np.arange(0, len(df3.index), step=(720 / resolution)), rotation=90)
    plt.grid(axis='y', zorder=0)
    plt.tight_layout

    plt.savefig(f'{PATH_OUTPUT}/{folder}/weekly_profile_charging_volume_site.pdf', bbox_inches='tight')
    plt.clf()


    df3['plugin_duration'] = (df3['plugin_duration'].dt.total_seconds()) / 3600
    plt.plot(df3.index, df3['plugin_duration'], color='grey', zorder=3)
    plt.ylabel('Average plugin duration [in h]')
    plt.xticks(np.arange(0, len(df3.index), step=(720 / resolution)), rotation=90)
    df3['plugin_duration'] = pd.to_timedelta(df3['plugin_duration'], unit='H')
    plt.grid(axis='y', zorder=0)
    plt.tight_layout

    plt.savefig(f'{PATH_OUTPUT}/{folder}/weekly_profile_charging_duration_site.pdf', bbox_inches='tight')
    plt.clf()

    site_discrete = site_discrete.drop(columns=['day_name', 'time'])
    #return plt.show()


def plugged(site_discrete, df_analyse, folder, resolution):
    """
    Graphic: Plugged-in ratio on weekly profile
    """
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    colors = ['gainsboro', 'silver', 'grey', 'dimgrey', 'black', 'cornflowerblue', 'royalblue', 'blue', 'mediumblue',
              'navy']

    charge_points = site_discrete.iloc[:, 10:].shape[1]

    df_analyse['Number charge points'] = charge_points

    for i in range(charge_points):
        i += 1
        site_discrete[i] = np.where(site_discrete['active_charge_points'] >= i, 1, 0)

    site_discrete['day_name'] = site_discrete.index.day_name(locale=None)
    site_discrete['time'] = site_discrete.index.time

    df2 = site_discrete.groupby(['day_name', 'time']).agg({'active_charge_points': 'count'})
    df2['x'] = 0
    for i in range(charge_points):
        i += 1

        df2[str(i) + 'x EV'] = site_discrete.groupby(['day_name', 'time']).agg({i: 'sum'})
        df2['ratio' + str(i)] = (df2[str(i) + 'x EV'] / df2['active_charge_points']) * 100

    df2 = df2.reindex(days, level=0)
    df2.index = [str(i) for i in (df2.index.map('{0[0]}, {0[1]}'.format))]

    for i in range(charge_points):
        i += 1
        del site_discrete[i]

    for i in range(charge_points):
        i += 1
        if df2['ratio' + str(i)].sum() > 10:
            plt.plot(df2.index, df2['ratio' + str(i)], label=str(i) + 'x EV', color=colors[i - 1], zorder=3)
            plt.fill_between(df2.index, 0, df2['ratio' + str(i)], color=colors[i - 1], zorder=3)
    plt.ylabel('Average plugin probability [in %]')
    plt.xticks(np.arange(0, len(df2.index), step=(720 / resolution)), rotation=90)
    plt.grid(axis='y', zorder=0)
    plt.tight_layout
    plt.legend(bbox_to_anchor=(1.04, 1), loc="upper left")


    plt.savefig(f'{PATH_OUTPUT}/{folder}/plugged_in_ratio_site.pdf', bbox_inches='tight')
    del site_discrete['day_name']
    del site_discrete['time']
    plt.clf()

    #return plt.show()

def power_ratio_site(site_discrete, folder, resolution):
    """
    Graphic: Charging power on weekly profile
    """
    site_discrete['plugged_in'] = np.where(site_discrete['active_charge_points'] > 0, 1, 0)

    site_discrete['charge_power_site'] = site_discrete['charge_power_site'] / 1000
    site_discrete['day_name'] = site_discrete.index.day_name(locale=None)
    site_discrete['time'] = site_discrete.index.time
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    df2 = site_discrete.groupby(['day_name', 'time']).agg({'plugged_in': 'sum'})
    df2['plugged_ins'] = site_discrete.groupby(['day_name', 'time']).agg({'plugged_in': 'count'})

    df2['1'] = site_discrete.groupby(['day_name', 'time']).agg({'charge_power_site': 'mean'})

    df2['av_charge_power_site'] = np.where(df2['plugged_in'] > 0,
                                               df2['1'],
                                               0)
    del df2['1']
    df2 = df2.reindex(days, level=0)
    df2.index = [str(i) for i in (df2.index.map('{0[0]}, {0[1]}'.format))]

    plt.grid(axis='y', zorder=0)
    plt.plot(df2.index, df2['av_charge_power_site'], color='black', zorder=3)
    plt.ylabel('Average charge power [in kW]')
    plt.xticks(np.arange(0, len(df2.index), step=(720 / resolution)), rotation=90)
    plt.tight_layout

    site_discrete['charge_power_site'] = site_discrete['charge_power_site'] * 1000

    plt.savefig(f'{PATH_OUTPUT}/{folder}/power_ratio_site.pdf', bbox_inches='tight')
    del site_discrete['day_name']
    del site_discrete['time']
    plt.clf()

    #return plt.show()


def simultaneity(site_discrete, df_simultaneity):
    """
    Graphic: Charge points simultaneous active at a site
    """
    charge_points = site_discrete.iloc[:, 10:].shape[1]

    for i in range(charge_points):
        i += 1
        site_discrete[i] = np.where(site_discrete['active_charge_points'] >= i, 1, 0)

    for i in range(charge_points):
        i += 1
        x = len(site_discrete[site_discrete[i] > 0])
        y = len(site_discrete)
        z = round(x / y * 100, 2)
        df_simultaneity[str(i) + 'x evs'] = z

    for i in range(charge_points):
        i += 1
        del site_discrete[i]


def energy(site_discrete, site_event, df_days, folder, resolution):
    """
    Graphic and output-file of depot analysis on daily basis
    """
    site_discrete['day_name'] = site_discrete.index.day_name(locale=None)
    # site_discrete['time'] = site_discrete.index.time
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df2 = site_discrete.groupby(['day_name']).agg({'charge_power_site': 'count'})
    df2 = df2.reindex(days)

    site_event2 = site_event[['plugin_time', 'plugin_duration', 'session_energy_consumed', 'charging_duration',
                              'mean charge_power event', 'mean charge_power charging']].copy(deep=True)

    site_event2['plugin_time'] = site_event2['plugin_time'].dt.round(str(resolution) + 'T')
    site_event2['day_name'] = site_event2['plugin_time'].dt.day_name(locale=None)
    # site_event2['time'] = site_event2['plugin_time'].dt.time

    site_event2['session_energy_consumed'] = pd.to_numeric(site_event2['session_energy_consumed'])

    site_event2['plugin_duration'] = site_event2['plugin_duration'].dt.total_seconds()
    site_event2['charging_duration'] = site_event2['charging_duration'].dt.total_seconds()

    site_event3 = site_event2.groupby(['day_name']).agg({'session_energy_consumed': 'mean'})
    site_event3['session_energy_consumed_std'] = site_event2.groupby(['day_name']).agg(
        {'session_energy_consumed': 'std'})

    site_event3['plugin_duration'] = site_event2.groupby(['day_name']).agg({'plugin_duration': 'mean'})
    site_event3['plugin_duration'] = pd.to_timedelta(site_event3['plugin_duration'], unit='s')
    site_event3['plugin_duration_std'] = site_event2.groupby(['day_name']).agg({'plugin_duration': 'std'})
    site_event3['plugin_duration_std'] = pd.to_timedelta(site_event3['plugin_duration_std'], unit='s')

    site_event3['charging_duration'] = site_event2.groupby(['day_name']).agg({'charging_duration': 'mean'})
    site_event3['charging_duration'] = pd.to_timedelta(site_event3['charging_duration'], unit='s')
    site_event3['charging_duration_std'] = site_event2.groupby(['day_name']).agg({'charging_duration': 'std'})
    site_event3['charging_duration_std'] = pd.to_timedelta(site_event3['charging_duration_std'], unit='s')

    site_event3['mean charge_power event'] = site_event2.groupby(['day_name']).agg({'mean charge_power event': 'mean'})
    site_event3['mean charge_power event_std'] = site_event2.groupby(['day_name']).agg(
        {'mean charge_power event': 'std'})

    site_event3['mean charge_power charging'] = site_event2.groupby(['day_name']).agg(
        {'mean charge_power charging': 'mean'})
    site_event3['mean charge_power charging_std'] = site_event2.groupby(['day_name']).agg(
        {'mean charge_power charging': 'std'})

    site_event3 = site_event3.reindex(days)

    df3 = pd.concat([df2, site_event3], axis=1)
    del df3['charge_power_site']
    df3['session_energy_consumed'] = df3['session_energy_consumed'].fillna(0)
    df3['session_energy_consumed'] = df3['session_energy_consumed'] / 1000
    df3['session_energy_consumed_std'] = df3['session_energy_consumed_std'].fillna(0)
    df3['session_energy_consumed_std'] = df3['session_energy_consumed_std'] / 1000

    df3['plugin_duration'] = df3['plugin_duration'].fillna(timedelta(hours=0))
    df3['plugin_duration_std'] = df3['plugin_duration_std'].fillna(timedelta(hours=0))

    df3['charging_duration'] = df3['charging_duration'].fillna(timedelta(hours=0))
    df3['charging_duration_std'] = df3['charging_duration_std'].fillna(timedelta(hours=0))

    df3['mean charge_power event'] = df3['mean charge_power event'].fillna(0)
    df3['mean charge_power event'] = df3['mean charge_power event'] / 1000
    df3['mean charge_power event_std'] = df3['mean charge_power event_std'].fillna(0)
    df3['mean charge_power event_std'] = df3['mean charge_power event_std'] / 1000

    df3['mean charge_power charging'] = df3['mean charge_power charging'].fillna(0)
    df3['mean charge_power charging'] = df3['mean charge_power charging'] / 1000
    df3['mean charge_power charging_std'] = df3['mean charge_power charging_std'].fillna(0)
    df3['mean charge_power charging_std'] = df3['mean charge_power charging_std'] / 1000

    # print('Durchschnittlich geladene Energiemenge eines Ladeevents in kWh:')
    df3['plugin_duration'] = (df3['plugin_duration'].dt.total_seconds()) / 3600
    df3['plugin_duration_std'] = (df3['plugin_duration_std'].dt.total_seconds()) / 3600
    df3['charging_duration'] = (df3['charging_duration'].dt.total_seconds()) / 3600
    df3['charging_duration_std'] = (df3['charging_duration_std'].dt.total_seconds()) / 3600

    df_days['av_energy_charged_kWh'] = round(df3['session_energy_consumed'], 2)
    df_days['standard_deviation_energy_charged_kWh'] = round(df3['session_energy_consumed_std'], 2)

    df_days['av_plugin_duration_h'] = round(df3['plugin_duration'], 2)
    df_days['standard_deviation_pluging_duration_h'] = round(df3['plugin_duration_std'], 2)

    df_days['av_charging_duration_h'] = round(df3['charging_duration'], 2)
    df_days['standard_deviation_charging_duration_h'] = round(df3['charging_duration_std'], 2)

    df_days['av_charge_power_event_kW'] = round(df3['mean charge_power event'], 2)
    df_days['standard_deviation_charge_power_event_kW'] = round(df3['mean charge_power event_std'], 2)

    df_days['av_charge_power_charging_kW'] = round(df3['mean charge_power charging'], 2)
    df_days['tandard_deviation_charge_power_charging_kW'] = round(df3['mean charge_power charging_std'], 2)

    x = np.arange(len(df3.index))  # the label locations
    width = 0.2  # the width of the bars

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    rect1 = ax1.bar(x - 5 * width / 3, df3['session_energy_consumed'], width,
                    label='Average energy charged per event [in kWh]',
                    color='dimgrey', zorder=3)
    rect2 = ax1.bar(x - 2 * width / 3, df3['session_energy_consumed_std'], width,
                    label='Standard deviation energy charged per event [in kWh]',
                    color='silver', zorder=3)

    ax1.set_ylabel('Energie in kWh')

    rect3 = ax2.bar(x + 2 * width / 3, df3['plugin_duration'], width, label='Average plugin duration per event [in h]',
                    color='darkblue', zorder=3)
    rect4 = ax2.bar(x + 5 * width / 3, df3['plugin_duration_std'], width,
                    label='Standard deviation plugin duration per event [in h]',
                    color='blue', zorder=3)
    ax2.set_ylabel('Plugin-Dauer in h')

    ax1.set_xticks(x)
    ax1.set_xticklabels(df3.index)
    # plt.xticks(rotation = 30)
    fig.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    df3['plugin_duration'] = pd.to_timedelta(df3['plugin_duration'], unit='H')
    df3['charging_duration'] = pd.to_timedelta(df3['charging_duration'], unit='H')
    del site_discrete['day_name']

    df_days.to_csv(f'{PATH_OUTPUT}/{folder}/site_daily_analysis.csv', sep=';', decimal=',', index=True)

    plt.savefig(f'{PATH_OUTPUT}/{folder}/daily_average_site.pdf', bbox_inches='tight')
    plt.clf()

    #return plt.show()

def show_graphs_site(site_discrete, site_event, df_analyse, df_simultaneity,df_days, folder, resolution):
    """
    Combines analysis outputs in a single function.
    """
    mpl.rcParams.update(mpl.rcParamsDefault)
    E_histogramm_site(site_event, folder)
    charging_ratio_site(site_event, folder, resolution)
    plugged_in_ratio_site(site_discrete, folder, resolution)
    plugged(site_discrete, df_analyse, folder, resolution)

    power_ratio_site(site_discrete, folder, resolution)

    utilization_site(site_discrete, folder, resolution)
    plugged_in_energy_duration_site(site_discrete, site_event, folder, resolution)

    simultaneity(site_discrete, df_simultaneity)
    energy(site_discrete, site_event, df_days, folder, resolution)



def timeflex(site_event):
    """
    Input file creation for flexbar
    """
    df_timeflex = site_event[['plugin_duration', 'charging_duration', 'session_energy_consumed',
                              'mean charge_power event', 'mean charge_power charging', 'max charge_power']].copy(
        deep=True)
    df_timeflex['charging_duration'] = pd.to_timedelta(df_timeflex['charging_duration'])

    df_timeflex['idle_time'] = df_timeflex['plugin_duration'] - df_timeflex['charging_duration']

    df_timeflex['charging_time P_av'] = df_timeflex['session_energy_consumed'] / df_timeflex[
        'mean charge_power charging']
    df_timeflex['charging_time P_av'] = pd.to_timedelta(df_timeflex['charging_time P_av'], unit='H')
    df_timeflex['flex P_av'] = df_timeflex['plugin_duration'] / df_timeflex['charging_time P_av']

    df_timeflex['P_fix'] = 3000
    df_timeflex['charging_time P_fix'] = df_timeflex['session_energy_consumed'] / df_timeflex['P_fix']
    df_timeflex['charging_time P_fix'] = pd.to_timedelta(df_timeflex['charging_time P_fix'], unit='H')
    df_timeflex['flex P_fix'] = df_timeflex['plugin_duration'] / df_timeflex['charging_time P_fix']

    df_timeflex['charging_time P_max'] = df_timeflex['session_energy_consumed'] / df_timeflex['max charge_power']
    df_timeflex['charging_time P_max'] = pd.to_timedelta(df_timeflex['charging_time P_max'], unit='H')
    df_timeflex['flex P_max'] = df_timeflex['plugin_duration'] / df_timeflex['charging_time P_max']

    df_timeflex = df_timeflex.reset_index(drop=True)
    return df_timeflex


def flexbar_av(df_timeflex, folder):
    """
    Graphic: Flexbar-Plot - with the mean-values
    """
    #Visualization
    fig = plt.figure()
    ax = fig.add_axes([0,0,1,1])
    p = patches.Rectangle((0,0), 1, 1, color= 'grey', alpha=0.2)

    x1 = df_timeflex['charging_time P_av'].mean()/df_timeflex['plugin_duration'].mean()
    y1 = df_timeflex['mean charge_power charging'].mean()/df_timeflex['max charge_power'].mean()
    p1 = patches.Rectangle((0,0), x1, y1, color= 'grey', alpha=0.8, label = '$ΔE_{av}$')

    x2 = df_timeflex['charging_time P_max'].mean()/df_timeflex['plugin_duration'].mean()
    y2 = df_timeflex['max charge_power'].mean()/df_timeflex['max charge_power'].mean()
    p2 = patches.Rectangle((0,0), x2, y2, color= 'grey', alpha=0.5, label = '$ΔE_{max}$')

    ax.add_patch(p)
    ax.add_patch(p1)
    ax.add_patch(p2)

    #td
    pe_a = [0.0, -0.2]
    pe_b = [1, -0.2]

    #td1
    pe_1a = [0.0, -0.1]
    pe_1b = [x1, -0.1]

    #td2
    pe_2a = [0.0, 0.0]
    pe_2b = [x2, 0.0]

    # fontdict for curly bracket 1 text
    font = {'family': 'serif',
            'color':  'k',
            'weight': 'bold',
            'style': 'italic',
            'size': 10,
            }

    # coefficient for curly
    k_r1 = 0.01
    k_r2 = 0.05

    # td - Brace
    curlyBrace.curlyBrace(fig, ax, pe_b, pe_a, k_r1, bool_auto=True, str_text='plugin duration', color='black', lw=1, int_line_num=1, fontdict=font)
    curlyBrace.curlyBrace(fig, ax, pe_1b, pe_1a, k_r1, bool_auto=True, str_text='$P_{av}-charging-time$', color='black', lw=1, int_line_num=1, fontdict=font)
    curlyBrace.curlyBrace(fig, ax, pe_2b, pe_2a, k_r1, bool_auto=True, str_text='$P_{max}-charging-time$', color='black', lw=1, int_line_num=1, fontdict=font)

    # numb1
    h_1a = [0.0, y1]
    h_1b = [0.0, 0,0]

    # numb2
    h_2a = [-0.07, y2]
    h_2b = [-0.07, 0,0]

    # numb - Brace
    curlyBrace.curlyBrace(fig, ax, h_1b, h_1a, k_r2, bool_auto=True, str_text='$P_{av}$', color='black', lw=1, int_line_num=1, fontdict=font)
    curlyBrace.curlyBrace(fig, ax, h_2b, h_2a, k_r2, bool_auto=True, str_text='$P_{max}$', color='black', lw=1, int_line_num=1, fontdict=font)

    ax.set_axis_off()
    ax.legend(bbox_to_anchor=(1.04,1), loc="upper left")


    #Numbers
    Flex_P_av = round(df_timeflex['flex P_av'].mean(), 2)
    Flex_P_max = round(df_timeflex['flex P_max'].mean(), 2)

    plt.savefig(f'{PATH_OUTPUT}/{folder}/flexbar_av.pdf', bbox_inches='tight')

    plt.clf()

    #return  plt.show()

def idle_time_histogramm(df_timeflex, df_analyse, folder):
    """
    Graphic: Idle time distribution
    """
    #Visualization
    #print('Histogramm Idle-Time:')
    df_timeflex['idle_time'] = df_timeflex['idle_time'].dt.total_seconds()/60
    plt.hist(df_timeflex['idle_time'], bins=np.arange(0, max(df_timeflex['idle_time']) + 15, 15),
             edgecolor = 'black', color = 'grey')
    plt.xlabel("idle time [in Min]")
    plt.ylabel("Number events")
    plt.xticks(np.arange(0, max(df_timeflex['idle_time']), 240))
    plt.xlim(0,  max(df_timeflex['idle_time']))
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout

    x = round((df_timeflex['idle_time'] > 120).value_counts(True)[True], 4)*100

    #Numbers for §14a
    df_analyse.loc[0, 'Idle_time_>_2h_in_%'] = x

    #print('Idle-Time > 2h: ' + str(x) + '%')
    df_timeflex['idle_time'] = pd.to_timedelta(df_timeflex['idle_time'], unit='T')
    y = df_timeflex['idle_time'].mean().round('s')
    df_analyse.loc[0, 'average_idle_time'] = y


    plt.savefig(f'{PATH_OUTPUT}/{folder}/idle_time_histogram.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def P_av_Flex_histogramm(df_timeflex, df_analyse, folder):
    """
    Graphic: Histogram flexibility with average power
    """
    # Data P_av:
    x = df_timeflex['charging_time P_av'].mean().round('s')
    y = round(df_timeflex['mean charge_power charging'].mean()/1000, 2)
    z = round(df_timeflex['flex P_av'].mean(), 2)

    df_analyse.loc[0, 'average_charge_dutation_P_av'] = x
    df_analyse.loc[0, 'average_power_P_av_kW'] = y
    df_analyse.loc[0, 'average_flex_P_av'] = z

    #Visualization
    plt.hist(df_timeflex['flex P_av'], bins=np.arange(0, max(df_timeflex['flex P_av']) + 1, 1),
             edgecolor = 'black', color = 'grey')
    plt.xlabel("Shifting flexibility with " + '$P_{av}$')
    plt.ylabel("Number events")
    plt.xlim(0,  max(df_timeflex['flex P_max'])+1)
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout


    plt.savefig(f'{PATH_OUTPUT}/{folder}/P_av_Flex_histogram.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def P_max_Flex_histogramm(df_timeflex, df_analyse, folder):
    """
    Graphic: Histogram flexibility with maximum power
    """
    # Data for  P_max:
    x = df_timeflex['charging_time P_max'].mean().round('s')
    y = round(df_timeflex['max charge_power'].mean()/1000, 2)
    z = round(df_timeflex['flex P_max'].mean(), 2)

    df_analyse.loc[0, 'average_charge_dutation_P_max'] = x
    df_analyse.loc[0, 'average_power_P_max_kW'] = y
    df_analyse.loc[0, 'average_flex_P_max'] = z

    #Visualization
    plt.hist(df_timeflex['flex P_max'], bins=np.arange(0, max(df_timeflex['flex P_max']) + 1, 1),
             edgecolor = 'black', color = 'grey')
    plt.xlabel("Shifting flexibility with " + '$P_{max}$')
    plt.ylabel("Number events")
    plt.xlim(0,  max(df_timeflex['flex P_max'])+1)
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout

    plt.savefig(f'{PATH_OUTPUT}/{folder}/P_max_Flex_histogram.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def boxplot_idle_time(df_timeflex, folder):
    """
    Graphic: boxplot idle time distribution
    """
    df_timeflex['idle_time'] = df_timeflex['idle_time'].dt.total_seconds()/3600

    medianprops = dict( color='black')

    box = plt.boxplot(df_timeflex['idle_time'], labels = (['Idle-Time']), medianprops = medianprops)
    plt.ylabel('Idle time [in h]')
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout

    df_timeflex['idle_time'] = pd.to_timedelta(df_timeflex['idle_time']*3600, unit='s')


    plt.savefig(f'{PATH_OUTPUT}/{folder}/boxplot_idle_time.pdf', bbox_inches='tight')

    plt.clf()
    #return plt.show()

def idle_time_histogramm_small(df_timeflex, folder):
    """
    Graphic: Histogram idle time distribution (small)
    """
    #Visualization
    df_timeflex['idle_time'] = df_timeflex['idle_time'].dt.total_seconds()/60
    plt.hist(df_timeflex['idle_time'], bins=np.arange(0, max(df_timeflex['idle_time']) + 15, 15),
             edgecolor = 'black', color = 'grey')
    plt.xlabel("Idle time [in min]")
    plt.ylabel("Number events")
    plt.xticks(np.arange(0, max(df_timeflex['idle_time']), 60))
    plt.xlim(0,  300)
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout

    df_timeflex['idle_time'] = pd.to_timedelta(df_timeflex['idle_time'], unit='T')


    plt.savefig(f'{PATH_OUTPUT}/{folder}idle_time_histogram_small.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def boxplot_Flex_P_av(df_timeflex, folder):
    """
    Graphic: boxplot shifitng flexibility
    """
    medianprops = dict( color='black')

    box = plt.boxplot(df_timeflex['flex P_av'], labels = (['$P_{av}$']), medianprops = medianprops)
    plt.ylabel('Shifting flexibility')
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout


    plt.savefig(f'{PATH_OUTPUT}/{folder}/boxplot_Flex_P_av.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def P_av_Flex_histogramm_small(df_timeflex, folder):
    """
    Graphic: Histrogram flexibility with average power (small)
    """
    #Visualization
    plt.hist(df_timeflex['flex P_av'], bins=np.arange(0, max(df_timeflex['flex P_av']) + 1, 1),
             edgecolor = 'black', color = 'grey')
    plt.xlabel("Shifting flexibility with " + '$P_{av}$')
    plt.ylabel("Number events")
    plt.xlim(0,  30)
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout


    plt.savefig(f'{PATH_OUTPUT}/{folder}/P_av_Flex_histogram_small.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def boxplot_Flex_P_max(df_timeflex, folder):
    """
    Graphic: boxplot flexibility with max power
    """
    medianprops = dict( color='black')

    box = plt.boxplot(df_timeflex['flex P_max'], labels = (['$P_{max}$']), medianprops = medianprops)
    plt.ylabel('Shifting flexibility')
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout


    plt.savefig(f'{PATH_OUTPUT}/{folder}/boxplot_Flex_P_max.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def P_max_Flex_histogramm_small(df_timeflex, folder):
    """
    Graphic: histogram flexibility with max power (small)
    """
    #Visualization
    plt.hist(df_timeflex['flex P_max'], bins=np.arange(0, max(df_timeflex['flex P_max']) + 1, 1),
             edgecolor = 'black', color = 'grey')
    plt.xlabel("Shifting flexibilty with " + '$P_{max}$')
    plt.ylabel("Number events")
    plt.xlim(0,  30)
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout


    plt.savefig(f'{PATH_OUTPUT}/{folder}/P_max_Flex_histogram_small.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def boxplot_Flex_P(df_timeflex, folder):
    """
    Graphic: boxplot flexibility with max power
    """
    medianprops = dict( color='black')

    box = plt.boxplot([df_timeflex['flex P_av'], df_timeflex['flex P_max']], labels = (['$P_{av}$', '$P_{max}$']), medianprops = medianprops)
    plt.ylabel('Shifting flexibility')
    plt.grid(axis = 'y', zorder = 0)
    plt.tight_layout


    plt.savefig(f'{PATH_OUTPUT}/{folder}/boxplot_Flex_P.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def timeflex_analyse(df_timeflex, df_analyse, folder):
    """
    Combines several function for the flexibility analysis
    """
    mpl.rcParams.update(mpl.rcParamsDefault)
    flexbar_av(df_timeflex, folder)
    idle_time_histogramm(df_timeflex, df_analyse, folder)
    boxplot_idle_time(df_timeflex, folder)
    P_av_Flex_histogramm(df_timeflex, df_analyse, folder)
    boxplot_Flex_P_av(df_timeflex, folder)
    P_max_Flex_histogramm(df_timeflex, df_analyse, folder)
    boxplot_Flex_P_max(df_timeflex, folder)
    boxplot_Flex_P(df_timeflex, folder)
#     idle_time_histogramm_small(df_timeflex, folder)
#     P_av_Flex_histogramm_small(df_timeflex, folder)
#     P_max_Flex_histogramm_small(df_timeflex, folder)

def datetime_site(site_discrete):
    """
    Extraction and transformation of datatime information of site discrete for following code
    """
    #site_discrete: datetime --> year, month, day, time
    site_discrete['plugged_in'] = np.where(site_discrete['active_charge_points'] > 0, 1, 0)
    site_discrete['day_name'] = site_discrete.index.day_name(locale = None)
    site_discrete['month_name'] = site_discrete.index.month_name(locale = None)
    site_discrete['year'] = site_discrete.index.year
    site_discrete['time'] = site_discrete.index.time
    return site_discrete

def datetime_site_event(site_event):
    """
    Extraction and transformation of datatime information of site event for following code
    """
    #site_event: datetime --> year, month
    site_event['plugin_time'] = pd.to_datetime(site_event['plugin_time'])
    site_event['month_name'] = site_event['plugin_time'].dt.month_name(locale = None)
    site_event['year'] = site_event['plugin_time'].dt.year
    return site_event

def del_col_site(site_discrete):
    """
    Delete previously added datetime columns
    """
    site_discrete.drop(['plugged_in', 'day_name', 'month_name', 'year', 'time'], axis=1, inplace=True)
    return site_discrete

def del_col_site_event(site_event):
    """
    Delete previously added datetime columns
    """
    site_event.drop(['month_name', 'year'], axis=1, inplace=True)
    return site_event

def df_year_creation(site_discrete, site_event, folder):
    """
    Fill output file for monthly analysis
    """

    site_event['plugin_time'] = pd.to_datetime(site_event['plugin_time'])
    site_event['plugout_time'] = pd.to_datetime(site_event['plugout_time'])
    site_event['plugin_duration'] = pd.to_timedelta(site_event['plugin_duration'])
    site_event['charging_duration'] = pd.to_timedelta(site_event['charging_duration'])


    #Empty df
    df_year = pd.DataFrame()

    #total energy per Month
    df_year['total_energy_month [kWh]'] = (site_discrete.groupby(['year','month_name'])['total_energy_consumed_site'].last() -
                                     site_discrete.groupby(['year','month_name'])['total_energy_consumed_site'].first())/1000

    #Events
    df_year['Number Events'] = site_event.groupby(['year', 'month_name'])['month_name'].count()
    df_year['Number Events'] = df_year['Number Events'].fillna(0)

    # Average Energy per Event
    df_year['av_energy_event [kWh]'] = round(df_year['total_energy_month [kWh]'] / df_year['Number Events'], 2)
    df_year['std_energy_event [kWh]'] = round(
        (site_event.groupby(['year', 'month_name'])['session_energy_consumed'].std()) / 1000, 2)

    # Average Power per Month
    # df_year['av_power_month [W]'] = site_discrete.groupby(['year','month_name'])['charge_power_site'].mean()

    # Average Power for Events
    df2 = site_discrete[site_discrete['charge_power_site'] > 100]
    df_year['av_power_month_charging [W]'] = round(df2.groupby(['year', 'month_name'])['charge_power_site'].mean(), 2)
    df_year['av_power_month_charging [W]'] = df_year['av_power_month_charging [W]'].fillna(0)

    # Data for plugin_time
    site_event['plugin_duration'] = site_event['plugin_duration'].dt.total_seconds()
    df_year['av_plugin'] = site_event.groupby(['year', 'month_name'])['plugin_duration'].mean()
    df_year['max_plugin'] = site_event.groupby(['year', 'month_name'])['plugin_duration'].max()
    df_year['min_plugin'] = site_event.groupby(['year', 'month_name'])['plugin_duration'].min()
    df_year['std_plugin'] = site_event.groupby(['year', 'month_name'])['plugin_duration'].std()
    site_event['plugin_duration'] = pd.to_timedelta(site_event['plugin_duration'], unit='s')
    df_year['av_plugin'] = pd.to_timedelta(df_year['av_plugin'], unit='s').dt.round('s')
    df_year['max_plugin'] = pd.to_timedelta(df_year['max_plugin'], unit='s').dt.round('s')
    df_year['min_plugin'] = pd.to_timedelta(df_year['min_plugin'], unit='s').dt.round('s')
    df_year['std_plugin'] = pd.to_timedelta(df_year['std_plugin'], unit='s').dt.round('s')

    # Data for charging_time
    site_event['charging_duration'] = site_event['charging_duration'].dt.total_seconds()
    df_year['av_charging'] = site_event.groupby(['year', 'month_name'])['charging_duration'].mean()
    df_year['max_charging'] = site_event.groupby(['year', 'month_name'])['charging_duration'].max()
    df_year['min_charging'] = site_event.groupby(['year', 'month_name'])['charging_duration'].min()
    df_year['std_charging'] = site_event.groupby(['year', 'month_name'])['charging_duration'].std()
    site_event['charging_duration'] = pd.to_timedelta(site_event['charging_duration'], unit='s')
    df_year['av_charging'] = pd.to_timedelta(df_year['av_charging'], unit='s').dt.round('s')
    df_year['max_charging'] = pd.to_timedelta(df_year['max_charging'], unit='s').dt.round('s')
    df_year['min_charging'] = pd.to_timedelta(df_year['min_charging'], unit='s').dt.round('s')
    df_year['std_charging'] = pd.to_timedelta(df_year['std_charging'], unit='s').dt.round('s')

    # Fillna
    df_year = df_year.fillna(timedelta(hours=0))

    # Sort df_year
    months = site_discrete['month_name'].unique()
    df_year = df_year.reindex(months, level=1)

    df_year.to_csv(f'{PATH_OUTPUT}/{folder}/site_monthly_analysis.csv', sep=';', decimal=',', index=True)

    return df_year

def create_df_year(site_discrete, path, folder):
    """
    Create output file for monthly analysis
    """
    file = glob.glob(f'{path}/{PATH_OUTPUT}/{folder}/site_event.csv')

    site_event = read_csv(file[0], delimiter=";", decimal=",", doublequote=True, encoding="utf-8")

    site_discrete = datetime_site(site_discrete)
    site_event = datetime_site_event(site_event)
    df_year = df_year_creation(site_discrete, site_event, folder)
    site_discrete = del_col_site(site_discrete)
    site_event = del_col_site_event(site_event)
    return df_year

def av_PI_E_diagramm(df_year, folder):
    """
    Graphic: monthly energy charged and plugin duration (average values)
    """
    x = np.arange(len(df_year.groupby(level=1)))  # the label locations
    width = 0.35  # the width of the bars

    df_year['av_plugin'] = df_year['av_plugin'].dt.total_seconds() / 60

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    rect1 = ax1.bar(x - width / 2, df_year["av_plugin"], width, label="Average plugin duration",
                    color='dimgrey', zorder=3)
    rect2 = ax2.bar(x + width / 2, df_year["av_energy_event [kWh]"], width,
                    label="Average energy charged per event",
                    color='silver', zorder=3)
    ax1.set_ylabel("Average plugin duration [in min]")
    ax2.set_ylabel("Average energy charged [in kWh]")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df_year.index.get_level_values(1))
    fig.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    df_year['av_plugin'] = pd.to_timedelta(df_year['av_plugin'], unit='T').dt.round('s')

    plt.savefig(f'{PATH_OUTPUT}/{folder}/av_PI_E_diagram.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()


def PI_E_diagramm(df_year, folder):
    """
    Graphic: monthly energy charged and events (total values)
    """
    x = np.arange(len(df_year.groupby(level=1)))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    rect1 = ax1.bar(x - width / 2, df_year["Number Events"], width, label="Number events",
                    color='dimgrey', zorder=3)
    rect2 = ax2.bar(x + width / 2, df_year["total_energy_month [kWh]"], width, label="Total energy charged",
                    color='silver', zorder=3)
    ax1.set_ylabel("Number events")
    ax2.set_ylabel("Total energy charged [in kWh]")
    ax1.set_xticks(x)
    ax1.set_xticklabels(df_year.index.get_level_values(1))
    fig.legend(bbox_to_anchor=(1.04, 1), loc="upper left")
    plt.gcf().autofmt_xdate()
    plt.tight_layout()

    plt.savefig(f'{PATH_OUTPUT}/{folder}/PI_E_diagram.pdf', bbox_inches='tight')

    plt.clf()

    #return plt.show()

def graphs_year(df_year, folder):
    """
    Combines the monthly analysis functions
    """
    PI_E_diagramm(df_year, folder)
    av_PI_E_diagramm(df_year, folder)

def analyse_site(path, folder, resolution):
    """
    Combines all the functions of the analysis and bundles them in a single function
    """
    df_analyse = pd.DataFrame()
    df_days = pd.DataFrame()
    df_simultaneity = pd.DataFrame(index=['Simultaneity [in %]'])

    site_discrete1 = prepare_discrete(path, folder)
    site_event1 = prepare_event(path, folder, df_analyse)
    site_event2 = boxplot_Ladedauer(site_event1, df_analyse, folder)

    ### Note ### boolean value garageExPost to be included in main.py to enable/disable
    garageExPost = True
    if garageExPost:
        plot_garage_statistics(site_discrete1, folder)
        
    show_graphs_site(site_discrete1, site_event2, df_analyse, df_simultaneity, df_days, folder, resolution)

    df_timeflex = timeflex(site_event2)
    timeflex_analyse(df_timeflex, df_analyse, folder)
    df_year = create_df_year(site_discrete1, path, folder)
    graphs_year(df_year, folder)

    df_analyse.to_csv(f'{PATH_OUTPUT}/{folder}/site_data.csv', sep=';', decimal=',', index=False)
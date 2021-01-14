#Import libraries
import csv
import pandas as pd
from pandas import *
import numpy as np
import yaml
import glob
import dateutil.parser
from datetime import timezone, datetime, timedelta
import time
import os

from extract import *
from prep import *
try:
    from analyse import*
except:
    pass

#Input paths

PATH_INPUT = 'data/input'
PATH_OUTPUT = 'data/output'

path =r'C:\Users\Lap\Documents\tmh-site-data-preprocessing'

#Setting up optional tool function

#Should the range of the data be adapted
filter_time = False #Not applied if false
date_start = "2020-01-01"
date_end   = "2020-03-01" #this date will be first day to not be included

#Input Parameter (static at the moment)
#max power on site available for charging limited through ChargePilot [in W]
site_max = 31868
#max power of the charger [in W]
charger_max = 11200

#timezone
timezone = 'UTC' #options e.g. : 'UTC' or 'Europe/Berlin'

#Revising the data - Should missing Logs be calculated #Not applied if false
reviseData = True

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

#Data extraction
print("")
print("Start data extraction...")
folder = master(path,filter_time, date_start, date_end)

#Data preparation
print("________________")
print("")
print("Start data preparation for single charge point...")
data_preparation(path,folder, site_max,charger_max,cleanData,minimum_charge_power,minimum_plugin_duration,minimum_energy,resolution,reviseData, timezone)
optimization_input_cp(path,folder,optimization)

#Aggregate all to site data
print("________________")
print("")
print("Start data preparation for site...")
add_site_data(path, folder, resolution, optimization)
if analyse == True:
    print("________________")
    print("")
    print("Start analyzing site...")
    analyse_site(path, folder, resolution)
print("________________")
print("")
print("All files are saved.")
end = time.time()
if round((end - start)<60,2):
    print("Runtime: " + str(round((end - start),2))+" seconds")

elif  round((end - start)/60 < 60,2):
    print("Runtime: " + str(round((end - start)/60,2))+" minutes")  

else:
    print("Runtime: " + str(round((end - start)/3600,2))+" hours")
# Import libraries

import time
from extractErrorMessage import *



#Input
ProjectName = 'USA-2718'

#Specify Paths
PATH_INPUT = 'data/'+ProjectName+'/Input/'
PATH_OUTPUT = 'data/'+ProjectName+'/Output/'

# path =r'C:\Users\Lap\OneDrive - The Mobility House GmbH\Dokumente\tmh-site-data-preprocessing'
# path ='/Users/sarahwoogen/PycharmProjects/tmh-site-data-preprocessing/'
path ='/Users/meiyewang/Documents/Tools/tmh-site-data-preprocessing/'


#Setting up optional tool function
# Optional selection, if not all data should be extracted
filter_cols = True  # False = "all data is extracted"
cols = ['timestamp', 'charge_point_id', 'action', 'status', 'error_code', 'vendor_error_code', 'vendor_id', "id_tag"]

# Optional selection, if true the output will be one excel sheet
filter_aggregated=True

#Should the range of the data be adapted
filter_time = False #Not applied if false
date_start = "2020-01-01"
date_end   = "2021-08-01" #this date will be first day to not be included

#timezone
timezone = 'US/Pacific' #options e.g. : 'UTC' or 'Europe/Berlin'

# replace charge id with Charge Pilot ID
filter_replace_id=True
to_rep = dict(zip(
    ['HVC150-US1-5020-742', 'HVC150-US1-5020-743', 'HVC150-US1-5120-955'],
    ['OVH Charger 1', 'OVH Charger 2', 'OVH Charger 3']))

#Data extraction
print("")
print("Start data extraction...")
folder = master(path,PATH_INPUT,PATH_OUTPUT,
                filter_time, date_start, date_end,
                filter_replace_id,to_rep,
                filter_aggregated,
                filter_cols,cols)#`    `q21    `



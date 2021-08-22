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
# SiteName = "St. Louis"
date = pd.Timestamp.now().strftime('%Y-%m-%d')
SiteName = 'OceanView-June'

# path =r'C:\Users\Lap\OneDrive - The Mobility House GmbH\Dokumente\tmh-site-data-preprocessing'
# path ='/Users/sarahwoogen/PycharmProjects/tmh-site-data-preprocessing/'
path = '/Users/meiyewang/Documents/Tools/tmh-site-data-preprocessing/'
os.chdir(path)

#Specify Paths
PATH_INPUT = 'data/'+ SiteName + '/' + date  + '/Input/'
PATH_OUTPUT = 'data/'+ SiteName+ '/' + date +'/Output/'

jsonParse = False
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
filter_replace_id = False
if not filter_replace_id:
    chargeridDict = {}

#Optional selection, if not all data should be extracted
filter_cols = False #False = "all data is extracted"
cols = ['meter_values_timestamp','charge_current','charge_offer','charge_power','charger_id','connector_id']


if not os.path.exists(path + 'data/'+SiteName):
    print(path + 'data/'+SiteName + " does not exist")
    print(PATH_OUTPUT)
    os.mkdir(path +'data/' + SiteName)
    os.mkdir(path +'data/'+ SiteName + '/' + date)
    os.mkdir(path +PATH_INPUT)
    os.mkdir(PATH_OUTPUT)

if not os.path.exists(path +'data/'+ SiteName + '/' + date):
    print(path +'data/'+ SiteName + '/' + date + " does not exist")
    print(PATH_OUTPUT)
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



if jsonParse  == True:
    inputDir= '/Users/meiyewang/The Mobility House GmbH/The Mobility House - USA/Customers/Sold' \
              '/St Louis Metro/Operations/Ad-hoc Data Requests/Data Query from Kibana/'
    inputpath = inputDir + SiteName + '/' + date
    outputpath = 'data/'+ SiteName + '/' + date  + '/'
    parsejason.main(inputpath,outputpath,siteidDict)
    os.chdir(path)
    
#Data extraction
print("")
print("Start data extraction...")
folder = master(path,PATH_INPUT,PATH_OUTPUT,
                typeList,
                filter_cols,cols,
                filter_time, date_start, date_end,
                filter_replace_id, chargeridDict,
                filter_aggregated,format,SiteName)#`    `q21    `

#Data preparation
print("________________")
print("")
print("Start data preparation for single charge point...")

data_preparation(path,folder,cleanData,minimum_charge_power,minimum_plugin_duration,minimum_energy,resolution,reviseData, timezone)
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
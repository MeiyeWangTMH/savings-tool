#Import libraries
import time


from extract import *
from prep import *
try:
    from analyse import*
except:
    pass

#Input
ProjectName = 'Stockton'

#Specify Paths
PATH_INPUT = 'data/'+ProjectName+'/Input/'
PATH_OUTPUT = 'data/'+ProjectName+'/Output/'

# path =r'C:\Users\Lap\OneDrive - The Mobility House GmbH\Dokumente\tmh-site-data-preprocessing'
# path ='/Users/sarahwoogen/PycharmProjects/tmh-site-data-preprocessing/'
path ='/Users/meiyewang/Documents/Tools/tmh-site-data-preprocessing/'

#Setting up optional tool function

#Should the range of the data be adapted
filter_time = False #Not applied if false
date_start = "2020-01-01"
date_end   = "2021-08-01" #this date will be first day to not be included

#timezone
timezone = 'US/Pacific' #options e.g. : 'UTC' or 'Europe/Berlin'

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

#Data extraction
print("")
print("Start data extraction...")
folder = master(path,PATH_INPUT,PATH_OUTPUT,filter_time, date_start, date_end)#`    `q21    `

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
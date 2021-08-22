import json
import csv
import os
import fnmatch
import glob
import pandas as pd
# Opening JSON file and loading the data

def parse(inputpath,outputpath,siteidDic):

    count = 0
    siteSet = set ()
    for file in os.listdir(inputpath):

        if fnmatch.fnmatch(file, 'error*.txt'):

            typ = "Error Messages"
            fieldname = ["@timestamp", "charger_id", "action", "request"]

            siteid = file.split("_")[1]

            site = siteidDic[siteid]
            siteSet.add(site)
            with open(inputpath + "/" + file) as j:
                jsondata = json.load(j)
                hits = jsondata['hits']

            outputfile = outputpath + typ + "_" + site + "_" + str(count) + "_input.csv"
            with open(outputfile, 'w') as c:
                writer = csv.DictWriter(c, fieldnames=fieldname)
                writer.writeheader()
                for data in hits:
                    writer.writerow(data['_source'])

        if fnmatch.fnmatch(file, 'charge*.txt'):

            typ = "Charging Data"
            fieldname = ["@timestamp", "override_max_limit", "evs"]

            siteid = file.split("_")[1]

            site = siteidDic[siteid]
            with open(inputpath + "/" + file) as j:
                jsondata = json.load(j)
                hits = jsondata['hits']

            outputfile = outputpath + typ + "_" + site + "_" + str(count) + "_input.csv"
            with open(outputfile, 'w') as c:
                writer = csv.DictWriter(c, fieldnames=fieldname)
                writer.writeheader()
                for data in hits:
                    writer.writerow(data['_source'])

        count += 1
    return siteSet

def combinecsv(outputpath,siteSet):
    os.chdir(outputpath )
    extension = 'csv'
    for site in siteSet:
        for typ in ["Error Messages", "Charging Data"]:
            all_filenames = [i for i in glob.glob(typ + "*" + site + "*".format(extension))]
            # combine all files in the list
            combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
            # export to csv
            combined_csv.to_csv( "Input/" + typ + "_" + site + "_comp.csv", index=False, encoding='utf-8-sig')

def main(inputpath, outputpath, siteidDic):
    siteSet = parse(inputpath, outputpath, siteidDic)
    combinecsv(outputpath, siteSet)

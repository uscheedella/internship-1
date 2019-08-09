#!/usr/bin/python3

import getopt
import sys
import pandas as p
import numpy as np
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import datetime

client = InfluxDBClient(host="eu1-cnws001.int.mblox.com")
clientd = InfluxDBClient(host="eu1-cnws001.int.mblox.com")

filesuc = ""
filecomp = ""

def main(argv):
    suc = ''
    comp = ''

    try:
        opts, args = getopt.getopt(argv, "hs:c:", ["successfile=", "completionfile="])
    except getopt.GetoptError:
        print('rome.py -s <successfile>.csv -c <completionfile>.csv')
        print('Note: Files must be in this directory!')
        sys.exit(2)
    for opt, arg in opts:
        if opt == 'h':
            print('rome.py -s <successfile>.csv -c <completionfile>.csv')
            sys.exit()
        elif opt in ("-s", "--successfile"):
            suc = arg
        elif opt in ("-c", "--completionfile"):
            comp = arg
        print('File with success rate: %s'%suc)
        print('File with completion rate: %s'%comp)
    
    return suc, comp

if __name__ == "__main__":
    filesuc,filecomp = main(sys.argv[1:])
    

dfgraf = p.read_csv(filesuc, header=0, sep=';', parse_dates = ['Time'])
dfcomp = p.read_csv(filecomp, header=0, parse_dates = ['_time'])

print('SUCCESS%')
dfgraf.rename(columns={"country": "Country"}, inplace=True)
dfgraf['success%'].replace('-', '0', inplace=True)
dfgraf['success%'] = p.to_numeric(dfgraf['success%'])
print(dfgraf.head())

print("")

print('COMPLETION RATE')
dfcomp.rename(columns={'_time': 'Time'}, inplace=True)
print(dfcomp.head())

print("")

print('MERGED')
dfmerge = p.merge(dfgraf, dfcomp, on=['Time','Country'])
dfmerge['C-S'] = abs(dfmerge['CompletionRate'] - dfmerge['success%'])
dfmerge['Time' = p.to_datetime(dfmerge['Time'], format='%Y-%m-%d %H:%M:%S.%f')
print(dfmerge.head())

dfmerge = dfmerge.set_index('Time')
clientd.write_points(dfmerge, 'merged', protocol='json', database='rome')

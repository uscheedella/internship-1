#!/usr/bin/python3

import getopt
import sys
import pandas as p
import numpy as np
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
import datetime

client = InfluxDBClient(host="eu1-cnws001.int.mblox.com")
clientd = DataFrameClient(host="eu1-cnws001.int.mblox.com")

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
    
dfgraf = p.read_csv(filesuc, header=0, delimiter=';', parse_dates = ['Time'])
print(dfgraf.columns)
dfcomp = p.read_csv(filecomp, header=0, sep=',', parse_dates = ['_time'], engine='python')

#TESTING
print(type(dfgraf['DR'][0]))
dfgraf['DR'].replace('-', '0', inplace=True)
dfgraf['DR'] = p.to_numeric(dfgraf['DR'])
print(type(dfgraf['DR']))
dfgraf['Successful'].replace('-', '0', inplace=True)
dfgraf['Successful'] = p.to_numeric(dfgraf['Successful'])

print('SUCCESS%')
dfgraf.rename(columns={"country": "Country"}, inplace=True)
dfgraf['success%'].replace('-', '0', inplace=True)
dfgraf['success%'] = p.to_numeric(dfgraf['success%'])
dfgraf.rename(columns={'success%': 'SuccessRate'}, inplace=True)
print(dfgraf.head())

print("")

print('COMPLETION RATE')
dfcomp.rename(columns={'_time': 'Time'}, inplace=True)
print(dfcomp.head())

print("")

print('MERGED')
dfmerge = p.merge(dfgraf, dfcomp, on=['Time','Country'])
dfmerge['C-S'] = abs(dfmerge['CompletionRate'] - dfmerge['SuccessRate'])
dfmerge['Time'] = p.to_datetime(dfmerge['Time'], format='%Y-%m-%d %H:%M:%S.%f')
print(dfmerge.head())
print(dfmerge.columns)

dfmerge = dfmerge.set_index('Time')
# dfmerge = dfmerge.reindex(columns=dfmerge['Country'])

ctrycol = dfmerge['Country']
succol = dfmerge['SuccessRate']
compcol = dfmerge['CompletionRate']
diffcol = dfmerge['C-S']

srdf = p.DataFrame({'SuccessRate':succol, 'Country':ctrycol})
srtags = dict(sorted(srdf.values.tolist()))
print(srdf.head())

crdf = p.DataFrame({'Country':ctrycol, 'CompletionRate':compcol})
crtags = dict(sorted(crdf.values.tolist()))

diffdf = p.DataFrame({'Country':ctrycol, 'Difference':diffcol})
difftags = dict(sorted(diffdf.values.tolist()))

statsdf = p.DataFrame({'Country':ctrycol, 'MT':dfmerge['MT'], 'Supplier':dfmerge['Supplier'], 'supplier%':dfmerge['supplier%'], 'DR':dfmerge['DR'], 'Successful':dfmerge['Successful'], 'SuccessRate':succol, 'SMSSent':dfmerge['SMSSent'], 'SMSValidated':dfmerge['SMSValidated'], 'CompletionRate':compcol, 'Difference':diffcol})
#stags = p.Series(dfmerge.CompletionRate.values, index=dfmerge.Country).to_dict()
#ctags = {dfmerge['Country']:dfmerge['CompletionRate']}
#dtags = {dfmerge['Country']:dfmerge['C-S']}
#srdf = dfmerge.set_index('Country')

tags = {'Country':srdf[['Country']]}
sucdf = p.DataFrame({'SuccessRate':succol})
clientd.write_points(statsdf, 'totalstats', tag_columns=['Country'], protocol='json', database='rome', batch_size=1000)
#clientd.write_points(crdf, 'crate', tags=crtags, protocol='json', database='rome')
#clientd.write_points(diffdf, 'diff', tags=difftags, protocol='json', database='rome')


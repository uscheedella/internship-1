#!/usr/bin/python3

import datetime
import os
import gzip
import glob
import re
from influxdb import InfluxDBClient
from threading import Thread
from multiprocessing import Pool
import multiprocessing
import concurrent.futures

client = InfluxDBClient(host="eu1-cnws001.int.mblox.com")

# get yesterday's date
today = datetime.datetime.now()
date = (today - datetime.timedelta(4)).strftime('%Y-%m-%d')

dirs = ['us1-cnqoslte001', 'us1-cnqoslte002', 'us2-cnqoslte001', 'us2-cnqoslte002', 'eu1-cnqoslte001', 'eu1-cnqoslte002', 'eu2-cnqoslte001', 'eu2-cnqoslte002']

# global lists for final smpp and xml values in each directory
svalues = [0, 0, 0, 0, 0, 0, 0, 0]
xvalues = [0, 0, 0, 0, 0, 0, 0, 0]
mvalues = [0, 0, 0, 0, 0, 0, 0, 0]

dummy = ["hello"]

def main(dum):
    dumdum = dum

    scount = 0
    xcount = 0
    mcount = 0
    svals = [0, 0, 0, 0, 0, 0, 0, 0]
    xvals = [0, 0, 0, 0, 0, 0, 0, 0]
    mvals = [0, 0, 0, 0, 0, 0, 0, 0]

    #loop through directories
    for i, j in enumerate(dirs):
        # change into working directory
        os.chdir("/") 
        os.chdir('/mnt/QOS-LOGS-S3/%s/logs/ipro/msip'%j)
        print(os.getcwd())

        # list of all carriers with date files
        datedirs = list(glob.glob('*/%s.txt.gz'%(date)))
        
        # loop through each line in date files and increase smpp and xml counts if keywords are there
        for y in datedirs:
            print(y)
            try:
                with gzip.open('%s'%y, 'r') as f:
                    for line in f:
                        if re.search("SubmitReq", str(line)):
                            if re.search('Interface=smpp', str(line)):
                                scount = scount + 1
                            elif re.search('Interface=xml', str(line)):
                                xcount = xcount + 1
                            else:
                                mcount = mcount + 1
            except:
                print("Skipped Permission Denied")
                continue 
            # save final values in local list
            svals[i] = scount
            xvals[i] = xcount
            mvals[i] = mcount

            print("date: %s"%(date))
            print("index: %s / host: %s"%(i,j))
            print("smppnum: %s"%svals[i])
            print("xmlnum: %s"%xvals[i])
            print("msipnum: %s"%mvals[i])
   
        # reset values for next directory
        scount = 0
        xcount = 0
        mcount = 0   
 
    return svals, xvals, mvals

if __name__ == "__main__":
    from multiprocessing.pool import ThreadPool
    pool = ThreadPool(2)
    results = pool.map(main, dummy)
    pool.close()
    pool.join()

    print("Threading Results: %s"%results)
    svalues = (results[0])[0]
    xvalues = (results[0])[1]
    mvalues = (results[0])[2]
    
    for i in range(len(dirs)):
    
        json_body = [
        {
            "measurement": "qoscur_%s"%i,
            "tags": {
                "date": "%s"%date,
                "host": "%s"%dirs[i]
            },
            "fields": {
                "smpp": svalues[i],
                "xml": xvalues[i],
                "msip": mvalues[i]
            }
        }
        ]

        if i < 4:
            json = [
            {
                "measurement": "qosus",
                "tags": {
                    "date": "%s"%date,
                    "host": "%s"%dirs[i]
                },
                "fields": {
                    "smpp": svalues[i],
                    "xml": xvalues[i],
                    "msip": mvalues[i]
                }
            }
            ]
        else:
            json = [
            {
                "measurement": "qoseu",
                "tags": {
                    "date": "%s"%date,
                    "host": "%s"%dirs[i]
                },
                "fields": {
                    "smpp": svalues[i],
                    "xml": xvalues[i],
                    "msip": mvalues[i]
                }
            }
            ]


        client.write_points(json_body, database='legacy')
        client.write_points(json, database='legacy')
        
        print('INFLUX DATA ENTERED')
 
 #        with open('/home/sriya.cheedella/qoslite.csv','a') as fd:
 #            fd.write("%s, %s, %d, %d, %d\n"%(date, dirs[i], svalues[i], xvalues[i], mvalues[i]))
 #        
 #        print('CSV DATA ENTERED')
    

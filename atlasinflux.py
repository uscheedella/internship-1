#!/usr/bin/python3

import os
import datetime
from influxdb import InfluxDBClient

client = InfluxDBClient(host="eu1-cnws001.int.mblox.com")

files = 'Atlas_Daily_Volume_Report_yes.csv'

date = 'date'
count = 0

def main():
    locdate = 'date'
    loccount = 0
    num = 0

    print("file: %s"%files)
    with open('%s'%files, 'r') as f:
        for line in f:
            strlist = line.split(";")
            locdate = strlist[0]
            loccount = int(strlist[2])

            print("date: %s"%locdate)
            print("count: %s"%loccount)

            if num == 0:
                json_body = [
                {
                    "measurement": "atl_uscounts",
                    "tags": {
                        "date": "%s"%locdate
                    },
                    "fields": {
                        "count": loccount
                    }
                 }
                 ]
                client.write_points(json_body, database='legacy')        
                print("Data entered for atl_uscounts")
                num = num + 1
                print("num: %s"%num)
            elif num == 1:
                json_body = [
                {
                    "measurement": "atl_eucounts",
                    "tags": {
                        "date": "%s"%locdate
                    },
                    "fields": {
                        "count": loccount
                    }
                 }
                 ]
                client.write_points(json_body, database='legacy')        
                print("Data entered for atl_eucounts")
                num = 0
        

if __name__ == "__main__":
    main()


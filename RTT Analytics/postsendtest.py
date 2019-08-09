#!/usr/bin/python3
# -*- coding: utf-8 -*-

##put this in a cron job to do it every 5 mins in 5 hours.
import requests
import pymysql
import json
import sys
import datetime
from dateutil import parser
import time

# GENERAL

conn = pymysql.connect('localhost', 'uscheedella', 'pass123', 'python_db')

cursor = conn.cursor()

headers = {
        'Authorization': 'Bearer 96e2eba86a2547e1af3e2b8482c84b27',
        'Content-Type': 'application/json',
        }
data = '    {         "from": "28444",         "to": [         "14046682795",        "14707259582",        "17033109854",        "14046268163"],          "body": "Testing with ${carrier}.",          "parameters": {            "carrier": {                "14046682795": "Sprint",                "14707259582": "Verizon",                "17033109854": "T-Mobile",               "14046268163": "ATT",                "default": "there"            }        },          "delivery_report": "full",          "callback_url": "https://webhook.site/49a9f99a-d918-4bd7-8d82-aa0dc8425ced"     }'


nums = ['14046682795', '14707259582', '17033109854', '14046268163']

dell_list = []
att_list = []

response1 = requests.post('https://api.clxcommunications.com/xms/v1/CLX_Test/batches', headers=headers, data=data)

dumps = json.dumps(response1.json()) # str
dumps2 = json.loads(dumps)

print(dumps2['created_at'])

# POST

for i in range(len(nums)):

    sqlid = 'INSERT INTO python_db.posttab (batchid, nums, deltime) VALUES (%s, %s, %s)'
    numid = dumps2['id']
    numstr = (dumps2['to'])[i]
    delivery = dumps2['created_at']
    dell = parser.parse(delivery)
    msisdn = nums[i]

    cursor.execute(sqlid, (numid, numstr, dell))

time.sleep(10)

# GET

rowsq = "select count(*) from posttab"
cursor.execute(rowsq)
rowsnum = str(cursor.fetchall())[2:-4]
print(rowsnum)
rowsnumm =  int(rowsnum) - len(nums)

if rowsnumm < 0:
    rowsnumm = 0

query = "SELECT batchid FROM posttab LIMIT %s,4" 
cursor.execute(query, rowsnumm)
ids = cursor.fetchall()

for j in range(0, len(ids), len(nums)):
    print(ids[j])
    for i in range(len(nums)):
        print(str(nums[i]))
        url = 'https://api.clxcommunications.com/xms/v1/CLX_Test/batches/' + str(ids[j])[2:-3] + '/delivery_report/' + str(nums[i])
        response2 = requests.get(url, headers=headers)
        print(response2)
        dumps = json.dumps(response2.json()) # str
        dumps2 = json.loads(dumps)
        print(dumps2)

        at = dumps2['at']
        delr = dumps2['status']
        numid = ids[j];
        codey = str(dumps2['code'])
        
        att = parser.parse(at)
        print(i)
        dell = dell_list[i]
        msisdn = nums[i]

        sqlat = 'INSERT INTO python_db.gettab (attime, delrec, batchid, code, nums) VALUES (%s, %s, %s, %s, %s)'
        cursor.execute(sqlat, (att, delr, numid, codey, msisdn))

# PARSING RTT
        print(att)
        print(dell)
        opp = ''
        if delr == 'Delivered':
            rtt = att-dell
            rtt = rtt.total_seconds()
            opp = dumps2['operator_status_at']
        else:
            rtt = 0

        rttq = 'INSERT INTO python_db.kpi (rpi,opstatus, nums, batchid) VALUES (%s,%s,%s, %s)'
        cursor.execute(rttq, (rtt,opp, msisdn, numid))

conn.commit()

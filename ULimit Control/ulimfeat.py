#!/usr/bin/python3

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot
import os
import sys
import getopt
import requests
import pymysql
import subprocess
import plotly.plotly as py
import pandas as pd
import plotly.graph_objs as go
from pylab import *
from scipy.stats import wilcoxon

# FOR EACH TIME
# graph (pandas, plotly)
# see if it's significant (stats or comparison)


# MYSQL CONNECT

conn = pymysql.connect('localhost', 'uscheedella', 'pass123', 'ulimit')

cursor = conn.cursor()

# FETCHING USERNAME

user = ""
sig = False
hrtf = False
dtf = False
wtf = False
mtf = False

def prim():
    n = 0
    if hrtf is True:
        n = 60/15
    if dtf is True:
        n = (60/15) * 24
    if wtf is True:
        n = ((60/15) * 24) * 7
    if mtf is True:
        n = (((60/15) * 24) * 7) * 30
   
    print(n)
    print(hrtf)
    print(dtf)
    print(wtf)
    print(mtf)

    rowsq = "select count(*) from ulimtab"
    cursor.execute(rowsq)
    rowsnum = int(cursor.fetchall()[0][0])
    print(rowsnum)
    rowsnumm =  rowsnum - n
    
    if rowsnumm < 0:
        rowsnumm = 0

    sqlper = "SELECT percent FROM ulimtab LIMIT %s,%s"
    cursor.execute(sqlper, (rowsnumm, n))
    pers = cursor.fetchall()
    sqltim = "SELECT date from ulimtab LIMIT %s,%s"
    cursor.execute(sqltim, (rowsnumm, n))
    tims = cursor.fetchall()
    
    if rowsnum == 0:
        print("Nothing to compare value to, run script again!")
        return
    else:
        maxp = int(max(pers)[0])
        print("maxp: %s"%maxp)
        minp = int(min(pers)[0])
        print("minp: %s"%minp)
        curr = int((pers[-1])[0])
    if hrtf:
        if (curr <= maxp+5 or curr >= minp-5):
            print("Nothing significant, it is within range")
        else:
            print("Percentage is significant!")
            sig = True
    else:
        pdf = pd.DataFrame( [[ij for ij in i] for i in str(pers)]   )
        stat, p = wilcoxon(pdf, curr)
        print('Stat=%.2f, %.2f'%(stat,p))

        if p < 0.05:
            print("Current number of open files is significant!")
            sig = True
        else:
            print("Current number of open files is not significant, don't worry.")

    fig = figure()
    fig.set_size_inches(10,4,forward=True)
    ax = subplot(111)
    box = ax.get_position()
    ax.set_position([box.x0,box.y0,box.width,box.height*.80])
    line = ax.plot(tims,pers)
    show()
    
    if hrtf is True:
        savefig('hourline')
        sqlhrg = "insert into graphstab(images) values(load_file('/home/uscheedella/ulimit/hourline.png'))"
        cursor.execute(sqlhrg)
        os.remove('/home/uscheedella/ulimit/hourline.png')
    if dtf is True:
        savefig('dayline')
        sqldg = "insert into graphstab(images) values(load_file('/home/uscheedella/ulimit/dayline.png'))"
        cursor.execute(sqldg)
        os.remove('/home/uscheedella/ulimit/dayline.png')
    if wtf is True:
        savefig('weekline')
        sqlwg = "insert into graphstab(images) values(load_file('/home/uscheedella/ulimit/weekline.png'))"
        cursor.execute(sqlwg)
        os.remove('/home/uscheedella/ulimit/weekline.png')
    if mtf is True:
        sqlmg = "insert into graphstab(images) values(load_file('/home/uscheedella/ulimit/monthline.png'))"
        cursor.execute(sqlmg)
        os.remove('/home/uscheedella/ulimit/monthline.png')
        savefig('monthline')

# general variables

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hu:rdwm", ["username=", "hour", "day", "week", "month"])
    except getopt.GetoptError:
        print('<filename> -u <username> -<time tag (optional)>')
        sys.exit(2)
    hrb = False
    db = False
    wb = False
    mb = False
    for opt, arg in opts:
        if opt == '-h':
            print('./<filename> -u <username>')
            print('./<filename> -u <username> -r to compare value within past hour')
            print('./<filename> -u <username> -d to compare value within past day')
            print('./<filename> -u <username> -w to compare value within past week')
            print('./<filename> -u <username> -m to compare value within past month')
            sys.exit()
        elif opt in ("-u", "--username"):
            username = arg
        elif opt in ("-r", "--hour"):
            hrb = True
            prim()
        elif opt in ("-d", "--day"):
            db = True
            prim()
        elif opt in ("-w", "--week"):
            wb = True
            prim()
        elif opt in ("-m", "--month"):
            mb = True
            prim()

    print('Username is %s'%(username))

    return username,hrb,db,wb,mb

if __name__ == "__main__":
    user,hrtf,dtf,wtf,mtf = main(sys.argv[1:])

try:
# max
    maxf = int(str(subprocess.check_output(["su", "%s"%user, "--shell", "/bin/bash", "-c", "ulimit -n"]))[2:-3])
    print("The # of max files for %s is %s"%(user,maxf))
    
    # open
    foo = subprocess.Popen(('lsof', '-u', '%s'%user), stdout=subprocess.PIPE)
    openf = int(str(subprocess.check_output(('wc', '-l'), stdin=foo.stdout))[2:-3])
    print("The # of open files for %s is %s"%(user,openf))
    
    # percent
    percent = round((openf * 100) / maxf, 2)
    print('The user %s is using %s %% of their open file capacity.'%(user,percent))
    
    sqlinfo = "insert into ulimit.ulimtab (user,openf,maxf,percent) values (%s, %s, %s, %s)"
    cursor.execute(sqlinfo, (user,openf, maxf, percent))
    
    conn.commit()
    
    # SLACK NOTIFICATION
    if (percent >= 75 or sig):
        headers = {
                'Content-type': 'application/json',
                }
        data = '{"text":"User '+str(user)+' is using '+str(percent)+'% of their open file capacity which is more than 75%"}'
        response = requests.post('https://hooks.slack.com/services/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', headers=headers, data=data)
    
    sig, hrtf, dtf, wtf, mtf = False
    
except:
    print("Oops, username not found! Try another username.")


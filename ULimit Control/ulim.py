#!/usr/bin/python3

import sys
import getopt
import requests
import pymysql
import subprocess

# FETCHING USERNAME

user = ""

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hu:", "username=");
    except getopt.GetoptError:
        print('./<filename> -u <username>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('./<filename> -u <username>')
            sys.exit()
        elif opt in ("-u", "--username"):
            username = arg
    return username
if __name__ == "__main__":
    user = main(sys.argv[1:])

# EXECUTION

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

    # SLACK NOTIFICATION
    
    if (percent >= 75):
        headers = {
                    'Content-type': 'application/json',
                    }
        data = '{"text":"User '+str(user)+' is using '+str(percent)+'% of their open file capacity which is more than 75%"}'
        response = requests.post('https://hooks.slack.com/services/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', headers=headers, data=data)

except:
    print("Oops, username not found! Try another username.")    

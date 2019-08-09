#!/bin/sh

PREV_DATE=$(date +%Y-%m-%d -d "yesterday")
SQL_QUERY_FILE="/home/sriya.cheedella/Atlas_Daily_Volume_Report.sql"
QUERY_RES_FILE="/home/sriya.cheedella/Atlas_Daily_Volume_Report_S.csv"

SQL_QUERY="select left(a.messagereceivetime,10) as log_day, b.routename,count(a.messageid) from messages as a join routes as b on a.routeid=b.routeid and a.messagereceivetime between '"$PREV_DATE" 00:00:00' and '"$PREV_DATE" 23:59:59' and b.routename ilike 'NOVA%' group by b.routename,log_day;"

echo $SQL_QUERY > $SQL_QUERY_FILE

/usr/bin/psql -h eu1-redshift001.int.mblox.com -d adwhdb -U serviceops  -p 5439 -F\; -f $SQL_QUERY_FILE -a -E -A -o $QUERY_RES_FILE

CLEAN_RES_FILE="/home/sriya.cheedella/Atlas_Daily_Volume_Report_yes.csv"

cat $QUERY_RES_FILE | tail -n 3 > $CLEAN_RES_FILE

sed -i '$d' $CLEAN_RES_FILE

python3 atlasinflux.py

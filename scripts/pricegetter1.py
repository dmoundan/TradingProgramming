#!/usr/bin/env python3

import requests
import sqlite3
import sys
import getopt
import datetime as dt
import os
import pandas as pd
import pandas_datareader.data as web
import numpy as np
import quandl
from progressbar import *               # just a simple progress bar


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return None

def process_file(fn,name_list):
    with open(fn) as f:
        name_list += f.read().splitlines() #same as readlines() but removes the \n from each line

def main(argv):
    
    list_file=""
    db_name=""
    start_date=""
    end_date=""
    feed="yahoo"
    update_flag=False
    name_list=[]

    try:
        opts, args = getopt.getopt(argv,"hl:d:s:e:f:u",["help", "list_file=","db_name=","start_date=","end_date=","feed=","update"])
    except getopt.GetoptError:
        print ("""earningsdb.py   -l <file containing list of stocks> 
                -d <name of the database>
                -s <start_date in YYYY-MM-DD format>
                -e <end_date in YYYY-MM-DD format> 
                -u (update, do not replace)
                -f <feed currently yahoo or quandl>                
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print ("""earningsdb.py   -l <file containing list of stocks> 
            -d <name of the database>
            -s <start_date in YYYY-MM-DD format>
            -e <end_date in YYYY-MM-DD format>
            -u (update, do not replace) 
            -f <feed currently yahoo or quandl>  
            """)    
            sys.exit()
        elif opt in("-l","--list_file"):
            list_file=arg
        elif opt in("-d","--db_name"):
            db_name=arg
        elif opt in("-s","--start_date"):
            start_date=arg
        elif opt in("-e","--end_date"):
            end_date=arg
        elif opt in("-f","--feed"):
            feed=arg
        elif opt in("-u","--update"):
            update_flag=True   
        

    process_file(list_file, name_list)  
    str1=start_date.split('-')
    start = dt.datetime(int(str1[0]),int(str1[1]),int(str1[2]))
    str2=end_date.split('-') 
    end = dt.datetime(int(str2[0]),int(str2[1]),int(str2[2]))

    conn=create_connection(db_name)  
    cur=conn.cursor() 

    df=pd.DataFrame()


    count=0
    widgets = ['Test: ', Percentage(), ' ', Bar(marker='*',left='[',right=']'),
               ' ', ETA(), ' ', FileTransferSpeed()] #see docs for other options
    pbar = ProgressBar(widgets=widgets, maxval=len(name_list))
    pbar.start()

    for ticker in name_list:
        #print(ticker)
        table_name=ticker.lower()
        if feed=="yahoo":
            df = web.DataReader(ticker, 'yahoo', start, end)
            df.index=df.index.astype('datetime64[ns]')
        elif feed == "googl":
            df = web.DataReader(ticker, 'google', start, end)
            df.index=df.index.astype('datetime64[ns]')
        elif feed == "quandl":
            quandl.ApiConfig.api_key = "ZqaDsXtwQZgz-M8i-A42"
            df = quandl.get("WIKI/" + ticker, start_date=start, end_date=end)
        if update_flag==False:
            df.to_sql(table_name, conn, if_exists="replace")
        else:
            df.to_sql(table_name, conn, if_exists="append")
        #print(df.tail())
        count+=1
        """
        period = 10
        query=f"SELECT count(*) FROM {table_name}"
        cur.execute(query)
        rows=cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df1=pd.read_sql_query(query, conn)
        df1=df1.set_index('Date')
        df1.index=df1.index.astype('datetime64[ns]')
        print(df1)
        """


    conn.close()
    pbar.finish

if __name__ == "__main__":
    main(sys.argv[1:])   
#!/usr/bin/env python3

import sqlite3
import pandas as pd
from datetime import date
import sys
from tabulate import tabulate
import numpy as np
import getopt

class DataBase:
    
    def __init__(self, name):
        self.name=name
        try:
            self.conn = sqlite3.connect(self.name)
        except Error as e:
            print(e)
        self.cur=self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()
    
    def find_pivot_points(self, ticker, sample_size, window):
        table_name=ticker.lower()
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-sample_size
        query=f"SELECT * FROM {table_name} LIMIT {sample_size} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #df=df.set_index('Date')
        #df.index=df.index.astype('datetime64[ns]')
        print(df.tail())

        l=df.shape[0]   # this gives the number of rows
        peaks=[]
        bottoms=[]
        for i in range(window+1, l-window,1):
            valueh=(df.loc[[i],['High']]).values[0][0]
            max_before=(df.loc[i-window:i-1,['High']]).values.max()
            max_after=(df.loc[i+1:i+window,['High']]).values.max()
            if valueh > max_before and valueh > max_after:
                peaks.append(i)
            valuel=(df.loc[[i],['Low']]).values[0][0]
            min_before=(df.loc[i-window:i-1,['Low']]).values.min()
            min_after=(df.loc[i+1:i+window,['Low']]).values.min()
            if valuel < min_before and valuel < min_after:
                bottoms.append(i)

        peaks_df_list=[]
        for item in peaks:
            peaks_df_list.append(df.loc[[item],['High', 'Low', 'Open', 'Close','Date', 'Volume']])
        peaks_df=pd.concat(peaks_df_list, axis=0)
        peaks_df.index = pd.RangeIndex(len(peaks_df.index))
        bottoms_df_list=[]
        for item in bottoms:
            bottoms_df_list.append(df.loc[[item],['High', 'Low', 'Open', 'Close','Date', 'Volume']])
        bottoms_df=pd.concat(bottoms_df_list, axis=0)
        bottoms_df.index = pd.RangeIndex(len(bottoms_df.index))
        print(peaks_df)
        print("------")
        print(bottoms_df)
        
        

def process_file(fn,name_list):
    with open(fn) as f:
        for elem in f.read().splitlines():
            name_list.add(elem)



def main(argv):
    list_file=""
    name_list=set()
    db_name=""
    
    
    
    try:
        opts, args = getopt.getopt(argv,"hd:l:",["db_name=","list_file="])
    except getopt.GetoptError:
        print ("""scanner.py   -l <file containing list of stocks> 
                -d <name of the database>
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""scanner.py   -l <file containing list of stocks> 
            -d <name of the database>
            """)    
            sys.exit()
        elif opt in("-l","--list_file"):
            list_file=arg
        elif opt in("-d","--db_name"):
            db_name=arg
        
        
        
    process_file(list_file, name_list)
    for ticker in name_list:
        db=DataBase(db_name)
        db.find_pivot_points(ticker, 150, 4)

if __name__ == "__main__":
    main(sys.argv[1:])         
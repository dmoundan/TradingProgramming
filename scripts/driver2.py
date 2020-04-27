#!/usr/bin/env python3

from yahoofinancials import YahooFinancials
import sqlite3
from progressbar import *               # just a simple progress bar
import sys
import getopt
import pandas as pd
import time

"""
To get the update for a full week, we need to run with a date after the friday of the week that closed (i.e. Saturday or Sunday). This will update the Monday values to be correct for the week, but it will also include the
Friday values in the DB and that needs to be removed before running anything on the weekly figures. I am suuming the same for monthly as well. This needs to be addressed here.
The way to address it for weeks, is after you get the values over the weekend to run this:
./driver2.py -d stock_values.db -l ../DB_backups/stocks.list -t weekly -c
However, at the moment for weekly and monthly we do not do updates, we run it form beginning of time every (i.e. 01/01/1998) every time. We do use update for daily values.


To do updates we need to give one day passsed the current day as the end point. Start point should be the current day.
For months and weeks the start point would have to be the first day of the week and month for updates.
When doing weeks from scratch we need to provide a Monday as the start point.

./driver2.py -d stock_values1.db -l options_stock.csv -s 1988-01-01 -e 2018-12-25  -t daily
./driver2.py -d stock_values1.db -l options_stock.csv -s 1988-01-05 -e 2018-12-25  -t weekly
./driver2.py -d stock_values1.db -l options_stock.csv -s 1988-01-01 -e 2018-12-25  -t monthly

./driver2.py -d etf_values1.db -l options_etf.csv -s 1988-01-01 -e 2019-01-04  -t daily
./driver2.py -d etf_values1.db -l options_etf.csv -s 1988-01-01 -e 2019-01-04  -t monthly
./driver2.py -d etf_values1.db -l options_etf.csv -s 1988-01-05 -e 2019-01-04  -t weekly
"""


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


def find_next_index(conn, table_name):
    cursor=conn.cursor()
    query=f"select count(*) from {table_name}"
    cursor.execute(query)
    results=cursor.fetchone()
    return results[0]


def process_file(fn,name_list):
    with open(fn) as f:
        name_list += f.read().splitlines() #same as readlines() but removes the \n from each line

def main(argv):
    list_file=""
    db_name=""
    name_list=[]
    start_date=""
    end_date=""
    time_frame=""
    update_flag=False
    add_flag=False
    correct_flag=False
    try:
        opts, args = getopt.getopt(argv,"hl:d:s:e:t:uac",["list_file=","db_name=","start_date=","end_date=","time_frame=","update","add","correct"])
    except getopt.GetoptError:
        print ("""driver2.py   -l <file containing list of stocks> 
                -d <name of the database>
                -s <start_date in YYYY-MM-DD format>
                -e <end_date in YYYY-MM-DD format>
                -t <timeframe: daily or weekly or monthly>
                -u (update, do not replace)
                -a (add additional instruments)
                -c (correct the weekly DB)
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""driver2.py   -l <file containing list of stocks> 
            -d <name of the database>
            -s <start_date in YYYY-MM-DD format>
            -e <end_date in YYYY-MM-DD format>  
            -t <timeframe: daily or weekly or monthly>
            -u (update, do not replace) 
            -a (add additional instruments)
            -c (correct the weekly DB)
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
        elif opt in("-t","--time_frame"):
            time_frame=arg
        elif opt in("-u","--update"):
            update_flag=True
        elif opt in("-a","--add"):
            add_flag=True   
        elif opt in("-c","--correct"):
            correct_flag=True    
        

    process_file(list_file, name_list)
    #print(len(name_list))
  
    count=0
    widgets = ['Test: ', Percentage(), ' ', Bar(marker='*',left='[',right=']'),
               ' ', ETA(), ' ', FileTransferSpeed()] #see docs for other options
    pbar = ProgressBar(widgets=widgets, maxval=len(name_list))
    pbar.start()

    conn=create_connection(db_name)


    for stock in name_list:
        if correct_flag==True:
            table_name=""
            stock1=stock.replace("-","_")
            if time_frame =="weekly":
                table_name=stock1.lower()+"_values_weekly"
            elif time_frame=="monthly":
                table_name=stock1.lower()+"_values_monthly"  
            query=f"SELECT count(*) FROM {table_name}"
            cur=conn.cursor()
            cur.execute(query)
            rows=cur.fetchone()
            number=rows[0]
            period=1
            offset=number-period
            query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
            df=pd.read_sql_query(query,conn)
            dt=df['Date'].values[0]
            query=f"DELETE  FROM {table_name} WHERE Date = \"{dt}\""
            cur=conn.cursor()
            cur.execute(query)
            conn.commit()
            pbar.update(count)
            count+=1
            continue

        #print(stock)
        td={'Date':[], 'Open':[], 'Close':[], 'High':[], 'Low':[], 'Volume': [], 'Adj Close':[]}
        yahoo_financials = YahooFinancials(stock)
        result=yahoo_financials.get_historical_price_data(start_date, end_date, time_frame)
        guard=set()
        for elem in result[stock]['prices']:
            if elem['formatted_date'] not in guard:
                td['Date'].append(elem['formatted_date'])
                td['Open'].append(elem['open'])
                td['Close'].append(elem['close'])
                td['High'].append(elem['high'])
                td['Low'].append(elem['low'])
                td['Adj Close'].append(elem['adjclose'])
                td['Volume'].append(elem['volume'])
                guard.add(elem['formatted_date'])
        table_name=""
        
        #print(td)

        stock1=stock.replace("-","_")
        if time_frame=="daily":
            table_name=stock1.lower()+"_values"
        elif time_frame =="weekly":
            table_name=stock1.lower()+"_values_weekly"
        elif time_frame=="monthly":
            table_name=stock1.lower()+"_values_monthly"
        if update_flag==True:
            for dt in td['Date']:
                query=f"DELETE  FROM {table_name} WHERE Date = \"{dt}\""
                cur=conn.cursor()
                cur.execute(query)
                #print(query)
            if time_frame == "daily":
                ml=[];
                l=len(td['Open'])
                num=find_next_index(conn, table_name)
                #print(num)
                for i in range(l):
                    ml.append(num)
                    num+=1
                df=pd.DataFrame(td, index=ml)
                df.to_sql(table_name, conn, if_exists="append") 
        else:
            df=pd.DataFrame(td)
            if add_flag == False:
                df.to_sql(table_name, conn, if_exists="replace")
            else:
                df.to_sql(table_name, conn, if_exists="replace")    
        pbar.update(count)
        count+=1
        #time.sleep(1)

    pbar.finish
    conn.close()

if __name__ == "__main__":
    main(sys.argv[1:])   
#!/usr/bin/env python3

from yahoo_earnings_calendar import YahooEarningsCalendar
from datetime import datetime, timezone
import sys
import getopt
import calendar
import sqlite3
import json
import requests
import pandas as pd
from io import StringIO
from tabulate import tabulate
import time

"""  Another way to get earnings
def get_earnings_date(ticker=''):
    
    This function gets the earnings date for the given ticker symbol. It performs a request to the
    nasdaq url and parses the response to find the earnings date.
    :param ticker: The stock symbol/ticker to use for the lookup
    :return: String containing the earnings date
    
    try:
        earnings_url = 'http://www.nasdaq.com/earnings/report/' + ticker.lower()
        request = requests.get(earnings_url)
        soup = bs4.BeautifulSoup(request.text, 'html.parser')
        tag = soup.find(text=re.compile('Earnings announcement*'))
        return tag[tag.index(':') + 1:].strip()
    except:
        return 'No Data Found'

"""

#  ./coming_earnings.py -l options_stock.csv  -s "Jan 14 2019" -e "Jan 16 2019" -f
# ./coming_earnings.py -l DB_backups/stocks.list  -s "Jan 25 2019" -e "Jan 25 2019" -f -m
#  ./coming_earnings.py -l test.csv  -d test.db -s "Apr 01 2017" -e "Apr 30 2017"
# ./coming_earnings.py -l options_stock.csv  -d earnings.db -s "Aug  01 2017" -e "Dec 31 2017" -u

earnings_url="https://api.earningscalendar.net/?date={date}"
width=85
day_dict={0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
month_dict={"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11, "Dec":12}

def process_file(fn,name_list):
    with open(fn) as f:
        for elem in f.read().splitlines():
            name_list.add(elem)
        

def print_future_earnings(el,name_list):
    print("".ljust(width,"="))
    print("|","Ticker".center(10), "Name".center(40),"Date".center(15),"When".center(13),"|")
    print("".ljust(width,"="))
    
    for elem in el:
        if elem['ticker'] in name_list:
            str1=elem['startdatetime']
            l1=str1.split("T")
            date=l1[0]
            str2=elem['startdatetimetype']
            type=""
            if str2 == "BMO" or str2 == "AMC":
                type=str2
            else:
                l2=l1[1].split(":")
                if int(l2[0]) <= 16:
                    type = "BMO"
                else: type="AMC"        
            l2=date.split("-")
            day=day_dict[calendar.weekday(int(l2[0]),int(l2[1]),int(l2[2]))]            
            print("|",elem['ticker'].ljust(10),elem['companyshortname'].ljust(40),day.ljust(4),date.ljust(15),type.ljust(8),"|")
            print("".ljust(width,"-"))

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
"""
def obtain_historical_earnings_data(db_name, el, name_list):
    conn=create_connection(db_name)
    ed={}
    for elem in el:
        if elem['ticker'] in name_list:
            ticker=elem['ticker']
            str1=elem['startdatetime']
            l1=str1.split("T")
            date=l1[0]
            str2=elem['startdatetimetype']
            type=""
            if str2 == "BMO" or str2 == "AMC":
                type=str2
            else:
                l2=l1[1].split(":")
                if int(l2[0]) <= 16:
                    type = "BMO"
                else: type="AMC" 
            if name not in ed:
                ed[name]=[]
                ed[name].append({"date":date, "when":type})
            else:
                ed[name].append({"date":date, "when":type})
    print(ed)            
"""
def find_next_index(conn, table_name):
    cursor=conn.cursor()
    query=f"select count(*) from {table_name}"
    cursor.execute(query)
    results=cursor.fetchone()
    return results[0]

def does_table_exist(conn, table_name):
    cursor=conn.cursor()
    query=f"select name from sqlite_master where name='{table_name}'"
    cursor.execute(query)
    return bool(cursor.fetchone())



def populate_database(db_name, results_dict, update_flag):
    conn=create_connection(db_name)
    table_name=""
    for key, value in results_dict.items():
        stock=key.replace("-","_")
        table_name=stock.lower()+"_earnings"
        if update_flag==True:
            ml=[];
            l=len(value['date'])
            if does_table_exist(conn, table_name):
                num=find_next_index(conn, table_name)
                #print(num)
                for i in range(l):
                    ml.append(num)
                    num+=1
                df=pd.DataFrame(value, index=ml)
                df.to_sql(table_name, conn, if_exists="append")
            else: 
                df=pd.DataFrame(value)
                df.to_sql(table_name, conn, if_exists="replace")  
        else:    
            df=pd.DataFrame(value)
            df.to_sql(table_name, conn, if_exists="replace")  

def obtain_historical_earnings_data(l1,l2,name_list, db_name, update_flag):
    results_dict={}
    start_year=l1[2]
    start_month=month_dict[l1[0]]
    start_day=l1[1]
    end_year=l2[2]
    end_month=month_dict[l2[0]]
    end_day=l2[1]
    year=int(start_year)
    while (year <= int(end_year)):
        month=start_month
        if (year == int(end_year)):
            limit_month=end_month
        else:
            limit_month = 12    
        while (month <= limit_month):
            day=int(start_day)
            limit_day=31
            if (year == int(end_year) and month == end_month):
                limit_day=int(end_day)
            elif month == 4 or month == 6 or month == 9 or month == 11:
                limit_day=30
            elif month == 2:
                if calendar.isleap(int(year)):
                    limit_day=29
                else:
                    limit_day=28  
            #print(limit_day)          
            while (day <= limit_day):
                req_date=str(year)+str(month).zfill(2)+str(day).zfill(2)
                req_date1=str(year)+"-"+str(month).zfill(2)+"-"+str(day).zfill(2)
                print("\n",req_date)
                session = requests.Session()
                url=earnings_url.format(date=req_date)
                #print(url)
                response = session.get(url)
                response.raise_for_status()
                df=pd.read_json(StringIO(response.text))
                for index, row in df.iterrows():
                    if row['ticker'] in name_list:
                        if row['ticker'] in results_dict:
                            results_dict[row['ticker']]['date'].append(req_date1)
                            results_dict[row['ticker']]['when'].append(row["when"].upper())
                        else:
                            results_dict[row['ticker']]={}
                            results_dict[row['ticker']]['date']=[] 
                            results_dict[row['ticker']]['when']=[]   
                            results_dict[row['ticker']]['date'].append(req_date1)
                            results_dict[row['ticker']]['when'].append(row["when"].upper())
                        #print (row["ticker"]," ", row["when"])
                        print(".", end=" ")
                #print(tabulate(df, headers='keys', tablefmt='psql'))
                day+=1  
                time.sleep(1)  
                #here need to store result in a dict of dicts, then create a data frame for each contained dict
                #and store it as a table per stock in the DB. I should also provide the capability of updating
                #the DB
            month+=1
        year+=1
    print("\n")    
    populate_database(db_name, results_dict, update_flag)
    
def future_earnings_method2(start_date, end_date, name_list):
    l1=start_date.split()
    l2=end_date.split()
    results_dict={}
    start_year=l1[2]
    start_month=month_dict[l1[0]]
    start_day=l1[1]
    end_year=l2[2]
    end_month=month_dict[l2[0]]
    end_day=l2[1]
    year=int(start_year)
    company_names={}


    print("".ljust(width,"="))
    print("|","Ticker".center(10), "Name".center(40),"Date".center(15),"When".center(13),"|")
    print("".ljust(width,"="))

    with open('../DB_backups/company_names.json', 'r') as f:
        company_names = json.load(f)
    while (year <= int(end_year)):
        month=start_month
        if (year == int(end_year)):
            limit_month=end_month
        else:
            limit_month = 12    
        while (month <= limit_month):
            day=int(start_day)
            limit_day=31
            if (year == int(end_year) and month == end_month):
                limit_day=int(end_day)
            elif month == 4 or month == 6 or month == 9 or month == 11:
                limit_day=30
            elif month == 2:
                if calendar.isleap(int(year)):
                    limit_day=29
                else:
                    limit_day=28  
            #print(limit_day)          
            while (day <= limit_day):
                req_date=str(year)+str(month).zfill(2)+str(day).zfill(2)
                req_date1=str(year)+"-"+str(month).zfill(2)+"-"+str(day).zfill(2)
                #print("\n",req_date)
                session = requests.Session()
                url=earnings_url.format(date=req_date)
                #print(url)
                response = session.get(url)
                response.raise_for_status()
                df=pd.read_json(StringIO(response.text))
                for index, row in df.iterrows():
                    #print (row['ticker'])
                    if row['ticker'] in name_list:
                        company_name=company_names[row['ticker']][0]
                        when=row["when"].upper()
                        l10=req_date1.split("-")
                        day1=day_dict[calendar.weekday(int(l10[0]),int(l10[1]),int(l10[2]))]            
                        print("|",(row['ticker']).ljust(10),company_name.ljust(40),day1.ljust(4),req_date1.ljust(15),when.ljust(8),"|")
                        print("".ljust(width,"-"))
                day+=1  
                time.sleep(1) 
            month+=1
        year+=1
                        

def main(argv):
    list_file=""
    name_list=set()
    start_date=""
    end_date=""
    future_flag=False
    db_name=""
    update_flag=False
    method_flag=False
    
    try:
        opts, args = getopt.getopt(argv,"hd:l:s:e:uafm",["db_name=","list_file=","start_date=","end_date=","update","add","future","method"])
    except getopt.GetoptError:
        print ("""driver2.py   -l <file containing list of stocks> 
                -d <name of the database>
                -s <start_date in YYYY-MM-DD format>
                -e <end_date in YYYY-MM-DD format>
                -u (update, do not replace)
                -a (add additional instruments)
                -f (this is for coming earnings)
                -m (use laternative method for future earnings)
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""driver2.py   -l <file containing list of stocks> 
            -s <start_date in YYYY-MM-DD format>
            -e <end_date in YYYY-MM-DD format>  
            -d <name of the database>
            -u (update, do not replace)
            -a (add additional instruments)
            -f (this is for coming earnings)
            -m (use laternative method for future earnings)
            """)    
            sys.exit()
        elif opt in("-l","--list_file"):
            list_file=arg
        elif opt in("-s","--start_date"):
            start_date=arg
        elif opt in("-e","--end_date"):
            end_date=arg
        elif opt in("-f","--future"):
            future_flag=True
        elif opt in("-u","--update"):
            update_flag=True    
        elif opt in("-m","--method"):
            method_flag=True    
        elif opt in("-d","--db_name"):
            db_name=arg
    
    process_file(list_file, name_list)

    date_from = datetime.strptime(
        start_date, '%b %d %Y')   
    date_to = datetime.strptime(
        end_date, '%b %d %Y') 

    if future_flag == True:
        if method_flag == False:
            yec = YahooEarningsCalendar()
            el= yec.earnings_between(date_from, date_to) 
            print_future_earnings(el,name_list)
        else:
            future_earnings_method2(start_date, end_date, name_list)
    else:
        l1=start_date.split()
        l2=end_date.split()
        obtain_historical_earnings_data(l1,l2,name_list, db_name, update_flag)

if __name__ == "__main__":
    main(sys.argv[1:])          
#!/usr/bin/env python3

import time
import requests
from bs4 import BeautifulSoup
import sqlite3
from progressbar import *               # just a simple progress bar
import sys
import getopt
import pandas as pd
import json

#./finviz.py -l options_stock.csv -d info.db
#./finviz.py -l options_etf.csv -d info.db -e

def process_file(fn,name_list):
    with open(fn) as f:
        for elem in f.read().splitlines():
            name_list.add(elem)

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

def sanitize_string(str):
    #print(str)
    if str != "-":
        s1=str.replace("%","")
        return float(s1)
    else:
        return str

def sanitize_string1(str):
    #print(str)
    if str != "-":
        return float(str)
    else:
        return str

def main(argv):
    list_file=""
    name_list=set()
    db_name=""
    etf_flag=False
    gather_flag=False
    analyze_flag=False
    company_names={}
    sectors={}
    industries_set=set()
    
    
    try:
        opts, args = getopt.getopt(argv,"hd:l:ega",["db_name=","list_file=","etf","gather","analyze"])
    except getopt.GetoptError:
        print ("""driver2.py   -l <file containing list of stocks> 
                -d <name of the database>
                -e (this is for ETFs)
                -g (gather information in files)
                -a (dump a csv file with stock info)
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""driver2.py   -l <file containing list of stocks> 
            -d <name of the database>
            -e (this is for ETFs)
            -g (gather information in files)
            -a (dump a csv file with stock info)
            """)    
            sys.exit()
        elif opt in("-l","--list_file"):
            list_file=arg
        elif opt in("-d","--db_name"):
            db_name=arg
        elif opt in("-e","--etf"):
            etf_flag=True
        elif opt in("-g","--gather"):
            gather_flag=True
        elif opt in("-a","--analyze"):
            analyze_flag=True

    
    process_file(list_file, name_list)
    conn=create_connection(db_name)
    if etf_flag == False:
        table_name="stock_info"
    else: 
        table_name= "etf_info"

    td1={'Ticker':[], 'Company':[], 'Sector':[], 'Industry':[], 'Country':[], 'StockIndex':[], 'PerfWeek': [],
         'PerfMonth':[], 'ShortFloat': [], 'PerfQuarter': [], 'PerfHalfY':[], 'TargetPrice':[],
         'PerfYear':[], 'Range52W':[], 'PerfYTD':[], 'High52W':[], 'Low52W':[], 'ATR':[], 'RSI14':[],
         'Volatility':[], 'RelVolume':[], 'Earnings':[], 'AvgVolume':[], 'Price':[], 'Volume':[], 'Change':[],
         'SMA20':[], 'SMA50':[], 'SMA200':[], 'Price':[], 'PrevClose':[] }

    count=0
    widgets = ['Test: ', Percentage(), ' ', Bar(marker='*',left='[',right=']'),
               ' ', ETA(), ' ', FileTransferSpeed()] #see docs for other options
    pbar = ProgressBar(widgets=widgets, maxval=len(name_list))
    pbar.start()

    finviz_url="https://finviz.com/quote.ashx?t={ticker}"

    for stock in name_list:
        #print(stock)
        url=finviz_url.format(ticker=stock)
        req = requests.get(url)
        if req.status_code == requests.codes.ok:
            soup = BeautifulSoup(req.content, 'html.parser')
            table = soup.find_all(lambda tag: tag.name=='table')
            rows = table[6].findAll(lambda tag: tag.name=='tr')
            out=[]
            for i in range(len(rows)):
                td=rows[i].find_all('td')
                out=out+[x.text for x in td]
            #print(out)   
            td1['Ticker'].append(stock)
            td1['Company'].append(out[1]) 
            
            l1=out[2].split("|")
            td1['Sector'].append(l1[0])  
            td1['Industry'].append(l1[1])
            td1['Country'].append(l1[2])
            if gather_flag == True:
                company_names[stock]=[out[1], l1[0], l1[1]]
                if l1[1] not in industries_set:
                    industries_set.add(l1[1])
                    if l1[0] in sectors:
                        sectors[l1[0]].append(l1[1])
                    else:
                        sectors[l1[0]]=[]
                        sectors[l1[0]].append(l1[1])
            rows = table[8].findAll(lambda tag: tag.name=='tr')
            out=[]
            for i in range(len(rows)):
                td=rows[i].find_all('td')
                out=out+[x.text for x in td]
            count1=0
            for elem in out:
                if elem == 'Index' :
                    td1['StockIndex'].append(out[count1+1])
                elif elem == 'Perf Week':
                    td1['PerfWeek'].append(sanitize_string(out[count1+1]))
                elif elem == 'Perf Month':
                    td1['PerfMonth'].append(sanitize_string(out[count1+1]))
                elif elem ==  'Short Float' :
                    td1['ShortFloat'].append(sanitize_string(out[count1+1]))
                elif elem ==  'Perf Quarter':
                    td1['PerfQuarter'].append(sanitize_string(out[count1+1]))
                elif elem ==  'Perf Half Y':
                    td1['PerfHalfY'].append(sanitize_string(out[count1+1]))
                elif elem == 'Target Price' :
                    td1['TargetPrice'].append(sanitize_string1(out[count1+1]))
                elif elem ==  'Perf Year':
                    td1['PerfYear'].append(sanitize_string(out[count1+1]))
                elif elem ==  '52W Range':
                    td1['Range52W'].append(out[count1+1])
                elif elem == 'Perf YTD':
                    td1['PerfYTD'].append(sanitize_string(out[count1+1]))
                elif elem == '52W High':
                    td1['High52W'].append(sanitize_string(out[count1+1]))
                elif elem == '52W Low':
                    td1['Low52W'].append(sanitize_string(out[count1+1]))
                elif elem == 'ATR':
                    td1['ATR'].append(sanitize_string1(out[count1+1]))
                elif elem == 'RSI (14)':
                    td1['RSI14'].append(sanitize_string1(out[count1+1]))
                elif elem == 'Volatility':
                    td1['Volatility'].append(out[count1+1])
                elif elem == 'Rel Volume':
                    td1['RelVolume'].append(sanitize_string1(out[count1+1]))
                elif elem == 'Earnings' :
                    td1['Earnings'].append(out[count1+1])
                elif elem == 'Avg Volume':
                    td1['AvgVolume'].append(out[count1+1])
                elif elem == 'Price':
                    td1['Price'].append(sanitize_string1(out[count1+1]))
                elif elem == 'SMA20':
                    td1['SMA20'].append(sanitize_string(out[count1+1]))
                elif elem == 'SMA50':
                    td1['SMA50'].append(sanitize_string(out[count1+1]))
                elif elem == 'SMA200':
                    td1['SMA200'].append(sanitize_string(out[count1+1]))
                elif elem == 'Volume':
                    td1['Volume'].append(out[count1+1])
                elif elem == 'Change':
                    td1['Change'].append(sanitize_string(out[count1+1]))
                elif elem ==  'Price':
                    td1['Price'].append(sanitize_string1(out[count1+1]))
                elif elem ==  'Prev Close':
                    td1['PrevClose'].append(sanitize_string1(out[count1+1]))
                
                count1+=1
            #print(out)    
        pbar.update(count)
        count+=1
        time.sleep(0.5)


    if gather_flag == True:
        with open('company_names.json', 'w') as fp:
            json.dump(company_names, fp)
        with open('sectors.json', 'w') as fp:
            json.dump(sectors, fp)

    df=pd.DataFrame(td1)
    df.to_sql(table_name, conn, if_exists="replace")  

    if analyze_flag == True:
        csv_filename="stock_info.csv"
        if etf_flag == True:
            csv_filename="etf_info.csv"
        df.to_csv(csv_filename, encoding='utf-8', index=False)
           

if __name__ == "__main__":
    main(sys.argv[1:])          
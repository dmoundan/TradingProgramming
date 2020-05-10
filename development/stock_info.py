#!/usr/bin/env python3


#stock_info.py -d 2020-05-04

import finviz
import pdfkit as pdf
#import weasyprint as wep
import pandas as pd
from datetime import datetime, timedelta
import calendar
import getopt
import sys
import os

file1="collateral/top_interest.csv"
file2="collateral/top_interest2.csv"

#-- Helper Functions
def process_file(fn,name_list):
    with open(fn) as f:
        for elem in f.read().splitlines():
            name_list.add(elem)


def find_weekdays(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    start_of_week = date_obj - timedelta(days=date_obj.weekday())  # Monday
    #end_of_week = start_of_week + timedelta(days=4)  # Sunday
    rlist=[]
    kset=set()
    dict1={}
    for i in range(5):
        d={}
        new_day= start_of_week + timedelta(days=i)
        mstr=calendar.month_abbr[new_day.month]
        dstr=new_day.day
        dstr1="0"+str(dstr) if dstr < 10 else str(dstr)
        key=mstr+" "+dstr1
        d["key"]=key
        kset.add(key)
        dict1[key]=new_day.date()
        d['day']=new_day.strftime("%a")
        rlist.append(d)
    return (dict1,kset)


info={ 'Ticker' :[],
       'Company': [],
       'Industry': [],
       'Earnings' : [],
       'Time' : [],
       'Short Float' : [],
       'ATR' : [],
       'Beta' : [] 
    }



def main(argv):
    start_date=""
    try:
        opts, args = getopt.getopt(argv,"hd:",["start_date="])
    except getopt.GetoptError:
        print ("""main.py   -d <start_date> 
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""main.py   -d <start_date> 
            """)    
            sys.exit()
        elif opt in("-d","--start_date"):
            start_date=arg

    los=set()

    process_file(file1, los)
    process_file(file2, los)

    dict1={}
    kset=set()
    (dict1,kset)=find_weekdays(start_date)


    for stock in los:
        d=finviz.get_stock(stock)
        earnings=d['Earnings']
        lst=earnings.split(" ")
        if len(lst) < 3:
            continue
        key=lst[0]+" "+lst[1]
        tm=lst[2]
        if key in kset:
            info['Ticker'].append(stock)
            info['Company'].append(d['Company'])
            info['Industry'].append(d['Industry'])
            info['Earnings'].append(dict1[key])
            info['Time'].append(tm)
            info['Short Float'].append(d['Short Float'])
            info['ATR'].append(d['ATR'])
            info['Beta'].append(d['Beta'])
        else:
            continue

    df=pd.DataFrame(info)
    df.sort_values(by='Earnings', ascending=True, inplace=True)
    #print(df)
    
    temp_html="earnings_"+start_date+".html"
    df.to_html(temp_html, index=False)
    out_file="earnings_"+start_date+".pdf"
    pdf.from_file(temp_html,out_file)
    os.remove(temp_html)
    #wep.HTML(temp_html).write_pdf(out_file)
    


if __name__ == "__main__":
    main(sys.argv[1:])         
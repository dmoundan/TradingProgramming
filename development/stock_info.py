#!/usr/bin/env python3

import finviz
import pandas as pd
from datetime import datetime, timedelta
import calendar


def find_weekdays(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    start_of_week = date_obj - timedelta(days=date_obj.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=4)  # Sunday
    print(start_of_week)
    print (start_of_week.year)
    print (start_of_week.month)
    print (start_of_week.day)
    print(start_of_week.strftime("%A"))
    print(end_of_week)   
    #calendar.month_abbr[month_val]


file1="collateral/top_interest.csv"
file2="collateral/top_interest2.csv"

#-- Helper Functions
def process_file(fn,name_list):
    with open(fn) as f:
        for elem in f.read().splitlines():
            name_list.add(elem)


los=set()

process_file(file1, los)
#process_file(file2, los)

info={ 'Ticker' :[],
       'Company': [],
       'Industry': [],
       'Earnings' : [],
       'Short Float' : [],
       'ATR' : [],
       'Beta' : [] 
    }

"""
for stock in los:
    d=finviz.get_stock(stock)
    info['Ticker'].append(stock)
    info['Company'].append(d['Company'])
    info['Industry'].append(d['Industry'])
    earnings=d['Earnings']
    info['Earnings'].append(earnings)
    info['Short Float'].append(d['Short Float'])
    info['ATR'].append(d['ATR'])
    info['Beta'].append(d['Beta'])
    
df=pd.DataFrame(info)
print(df)
"""

find_weekdays('2020-05-11')
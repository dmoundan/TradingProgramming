#!/usr/bin/env python3

# I will be putting everything in here for now and maybe split it later

import sys
import getopt
import sqlite3
import pandas as pd
from pandas.plotting import register_matplotlib_converters

import numpy as np

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from mpl_finance import candlestick_ohlc
register_matplotlib_converters()
from matplotlib import style
style.use('ggplot') # another style is fivethirtyeight
#print(plt.style.available)
#print(plt.__file__) To get to the matplotlib location and then in there find the style sheets : mpldata/stylelib



#-- Collateral Information

collateral_dir="/Users/dinos/Trading/Programming/OptionsTrading/DB_backups"
DB_dir="/Users/dinos/Trading/Programming/OptionsTrading/DBs"
list_of_stocks="stocks.list.1"
list_of_etfs="etf.list"
etf_db="etf_values.db"
stock_db="stock_values.db"
timeframes=["daily", "weekly", "monthly"]

#-- Helper Functions
def process_file(fn,name_list):
    with open(fn) as f:
        for elem in f.read().splitlines():
            name_list.add(elem)


#--Classes
class DataBase:
    
    def __init__(self, name):
        self._name=name
        try:
            self._conn = sqlite3.connect(self._name)
        except Error as e:
            print(e)
        self._cur=self._conn.cursor()

    def __del__(self):
        self._cur.close()
        self._conn.close()

    @property
    def cur(self):
        return self._cur

    @property
    def conn(self):
        return self._conn



class Instrument:

    def __init__(self, name, db_name):
        self._name=name
        self._DB=DataBase(db_name)

    def get_values(self,tf,period):
        table_name=""
        name=self._name.replace("-","_")
        if tf == "daily":
            table_name=name.lower()+"_values"
        else:
            table_name=name.lower()+"_values_"+tf
        query=f"SELECT count(*) FROM {table_name}"
        self._DB.cur.execute(query)
        rows=self._DB.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self._DB.conn)
        return df

    def find_swingHL_one(self, val, w):
        t=val.shape
        l=t[0]
        rd={'Date':[], 'SLH' : [], 'Value': []}
        for i in range(0+w, l-w):
            if val.loc[i,'High'] > val.loc[i-w:i-1,'High'].max() and val.loc[i,'High'] > val.loc[i+1:i+w,'High'].max():
                #print(val.loc[i,'Date'], " SH")
                rd['Date'].append(val.loc[i,'Date'])
                rd['SLH'].append("SH")
                rd['Value'].append(val.loc[i,'High'])
            elif val.loc[i,'Low'] < val.loc[i-w:i-1,'Low'].min() and val.loc[i,'Low'] < val.loc[i+1:i+w,'Low'].min():
                #print(val.loc[i,'Date'], " SL")
                rd['Date'].append(val.loc[i,'Date'])
                rd['SLH'].append("SL")
                rd['Value'].append(val.loc[i,'Low'])
        return pd.DataFrame(rd)

    def plot_candles(self, tf, period):
        df=self.get_values(tf, period)
        df.Date = pd.to_datetime(df.Date)
        df = df[['Date', 'Open', 'High', 'Low', 'Adj Close', 'Volume']]
        df["Date"] = df["Date"].apply(mdates.date2num)
        fig=plt.figure()
        ax1=plt.subplot2grid((1,1), (0,0))
        candlestick_ohlc(ax1,df.values, width=0.4, colorup='g', colordown='r')
        ax1.xaxis_date()
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%y-%m-%d'))
        ax1.xaxis.set_major_locator(mticker.MaxNLocator(20))
        ax1.grid(True)
        #ax1.annotate('Bad news!', (df['Date'].values[30], df['Adj Close'].values[30]), xytext=(0.8,0.9), textcoords='axes fraction', arrowprops =dict(facecolor='grey', color='grey'))
        #font_dict= {'family':'serif', 'color' : 'darkred', 'size' : 15}
        #ax1.text(df['Date'].values[10], df['Adj Close'].values[10], 'Prices', fontdict=font_dict)
        #To annotate the right edge of the chart
        #bbox_props=dict(boxstyle='round', fc='w', ec='k', lw=1)
        #ax1.annotate(str(df['Adj Close'].values[-1]), (df['Date'].values[-1], df['Adj Close'].values[-1]), xytext=(df['Date'].values[-1] + 4, df['Adj Close'].values[-1]), bbox=bbox_props)

        #For subplots (method#1)
        #fig=plt.figure()
        #ax1= fig.add_subplot(2,1,1)
        #ax2=fig.add_subplot (2,1,2) First number says that we split horizontally (s subs), 1 means that it takes the whole width, and the last number is the designation plot#1 nad #2
        #x,y=create_plots()
        #ax1.plot(x,y) and have to repeat these two lines for all subplots
        #For subplots (method #2)
        #ax1 = plt.subplot2grid((6,1), (0,0) , rowspan=, colspan=) First gives the size of grid 6 rows one col, second tuple is origin, and then how many rows, columns it spans
        

        plt.xticks(rotation=45)
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.title(self._name)
        plt.subplots_adjust(left=0.11, bottom=0.24, right=0.90, wspace=0.2, top=0.90, hspace=0 )
        plt.show()





##--
def main(argv):
    list_file=""
    name_list=set()

    los=set()
    loe=set()

    process_file(collateral_dir+"/"+list_of_stocks, los)
    process_file(collateral_dir+"/"+list_of_etfs, loe)

    try:
        opts, args = getopt.getopt(argv,"hl:",["list_file="])
    except getopt.GetoptError:
        print ("""main.py   -l <file containing list of stocks> 
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""main.py   -l <file containing list of stocks> 
            """)    
            sys.exit()
        elif opt in("-l","--list_file"):
            list_file=arg
           
    if list_file != "":
        process_file(list_file, name_list)
        for stock in name_list:
            if stock in los:
                print (stock+" is a stock")
                ins=Instrument(stock, DB_dir+"/"+stock_db)
                #df=ins.get_values(timeframes[0], 60)
                #df.set_index('index', inplace=True)
                #df1=df.loc[0:, 'Date':'Adj Close']
                
                #df2=df.loc[0:, ['Date','High','Low']]
                #df2.set_index('Date', inplace=True)
                #print(df2)
                #df3=ins.find_swingHL_one(df2,2)
                #print(df3)
                ins.plot_candles(timeframes[0], 60)
            elif stock in loe:
                print(stock+" is an ETF")
                ins=Instrument(stock, DB_dir+"/"+etf_db)
            else:
                print(stock+" not recognized")    

if __name__ == "__main__":
    main(sys.argv[1:])         
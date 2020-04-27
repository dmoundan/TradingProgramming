#!/usr/bin/env python3

import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
from mpl_finance import candlestick_ohlc  #used to be matplotlib.finance
import matplotlib.dates as mdates
import pandas as pd
#from  pandas_datareader import data as web
import sqlite3
import sys
import os
import bs4 as bs
import pickle
import requests


def save_sp500_tickers():
    resp = requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup=bs.BeautifulSoup(resp.text)
    table=soup.find('table', {'class':'wikitable sortable'})
    tickers=[]
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)
        
    with open('sp500tickers.pickle','wb') as f:
        pickle.dump(tickers,f)
    
    print(tickers)

    return tickers

def get_data_from_yahoo(reload_sp500=False):
    if reload_sp500:
        tickers=save_sp500_tickers()
    else:
        with open('sp500tickers.pickle','rb') as f: 
            tickers=pickle.load(f)

    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')

    start= dt.datetime(1998,1,1)
    end= dt.datetime(2019,8,13)

    for ticker in tickers:
        print (ticker.rstrip())

    """
    for ticker in tickers:
        print(ticker)
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            df=web.DataReader(ticker,'yahoo',start,end)
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        else:
            print('Already have {}'.format(ticker))
    """
            


"""
start = dt.datetime(2019,01,01)
end = dt.datetime(2019,08,09)
df = web.DataReader(ticker, 'yahoo', start, end)

df.to_csv('filename')
df=pd.read_csv('filename', parse_dates=True, index_col=0)
df[['Open','Close']].head()
"""
style.use('ggplot')

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


def main(argv):
    db_name="./DBs/stock_values.db"
    ticker="TSLA"
    column_list="Date, High, Low, Open, Close, Volume"
    start="\'2019-01-01\'"
    end="\'2019-08-09\'"

    """
    conn=create_connection(db_name)  
    cur=conn.cursor() 
    table_name=ticker.lower()+"_values"
    df=pd.DataFrame()
    query=f"SELECT {column_list} FROM {table_name} WHERE Date BETWEEN {start} AND  {end} "
    #query=f"SELECT {column_list} FROM {table_name} WHERE Date >= {start} AND Date <= {end}"
    #query=f"SELECT {column_list} FROM {table_name}"
    df=pd.read_sql_query(query, conn, parse_dates=['Date']) #This is to make the Dates into datetime objects
    df.set_index('Date', inplace=True)
    #print(df.tail())
    #df['Close'].plot()
    #plt.show()
    """

    # How to get SMAs
    """
    df['20SMA']=df['Close'].rolling(window=20).mean()

    df.dropna(inplace=True) 
    For the firat 19 rows we will not have an MA so we will get NaN that we can drop that whole row. Or we can set the min_periods=0 arg above and that will start
    calculating the avg from the beginning even if we do not yet have 20 values.
    """
    # this is plotting using pandas and matplotlib
    """
    df[['Close','20SMA']].plot()
    plt.show()
    """

    #this is graphing using matplotlib directly
    """
    ax1=plt.subplot2grid((6,1),(0,0), rowspan=5, colspan=1)
    ax2=plt.subplot2grid((6,1),(5,0), rowspan=5, colspan=1, sharex=ax1)

    ax1.plot(df.index, df['Close'])
    ax1.plot(df.index,df['20SMA'])
    ax2.bar(df.index,df['Volume'])
    plt.show()
    """

    #Resampling and Plotting Candlesticks
    """
    df_ohlc=df['Close'].resample('10D').ohlc()
    df_volume=df['Volume'].resample('10D').sum()

    df_ohlc.reset_index(inplace=True)  #make Date a regular column
    df_ohlc['Date']=df_ohlc['Date'].map(mdates.date2num)
    #print(df_ohlc.head())
    ax1=plt.subplot2grid((6,1),(0,0), rowspan=5, colspan=1)
    ax2=plt.subplot2grid((6,1),(5,0), rowspan=5, colspan=1, sharex=ax1)
    ax1.xaxis_date()
    candlestick_ohlc(ax1,df_ohlc.values, width=2,colorup='g')
    ax2.fill_between(df_volume.index.map(mdates.date2num), df_volume.values,0)
    plt.show()
    """

    #Getting the S&P 500 List of companies
    """
    save_sp500_tickers()
    """

    #Getting data for all sp500 companies
    get_data_from_yahoo()


if __name__ == "__main__":
    main(sys.argv[1:])   
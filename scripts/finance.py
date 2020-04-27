#!/usr/bin/env python3

import datetime as dt
import os
import matplotlib.pyplot as plt
from matplotlib import style
#from matplotlib.mpl_finance import candlestick_ohlc   This is apparently deprecated
from mpl_finance import candlestick_ohlc  #pip3 install https://github.com/matplotlib/mpl_finance/archive/master.zip
import matplotlib.dates as mdates

import pandas as pd
import pandas_datareader.data as web
import numpy as np

import bs4 as bs
import pickle
import requests



#style.use('ggplot')
"""
start = dt.datetime(1998,1,1)
end = dt.datetime(2019,5,17)

df = web.DataReader('MMM', 'yahoo', start, end)
print(df.head())
print(df.tail())

df.to_csv('tesla.csv')
"""

"""
df=pd.read_csv('tesla.csv', parse_dates=True, index_col=0)
print(df.head())
#df.plot()
#df['Adj Close'].plot()
#df[['Open','Close']].plot()
#df['200MA']=df['Adj Close'].rolling(window=200).mean()
#df.dropna(inplace=True)
#df['200MA']=df['Adj Close'].rolling(window=200, min_periods=0).mean()    #starts calculating even when not enough points yet, to avoid NaN at the beginning

#df[['Adj Close','200MA']].plot()
#plt.show()

#If we wanted to plot directly from matplotlib and not through pandas

ax1=plt.subplot2grid((6,1),(0,0),rowspan=5, colspan=1)
ax2=plt.subplot2grid((6,1),(5,0),rowspan=1, colspan=1,sharex=ax1) #now the two subplots are synchronized on the x axis as they share it
#ax1.plot(df.index, df['Adj Close'])
#ax1.plot(df.index, df['200MA'])
#ax2.plot(df.index, df['Volume'])
#plt.show()

df_ohlc=df['Adj Close'].resample('10D').ohlc()  #can resample weekly, monthly  
df_volume=df['Volume'].resample('10D').sum()

df_ohlc.reset_index(inplace=True)
df_ohlc['Date']=df_ohlc['Date'].map(mdates.date2num)

#print(df_ohlc.head())
ax1.xaxis_date()
candlestick_ohlc(ax1, df_ohlc.values, width=2, colorup='g')
ax2.fill_between(df_volume.index.map(mdates.date2num),df_volume.values,0) #use fill_between instead of having volume bars, this is continuous
plt.show()
"""

def save_sp500_tickers():
    resp=requests.get('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup=bs.BeautifulSoup(resp.text, "lxml")
    table= soup.find('table', {'class':'wikitable sortable'})
    tickers=[]
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        if ticker != "":
            ticker1=ticker.replace(".","-")
            tickers.append(ticker1.strip())

    with open("sp500tickers.pickle","wb") as f:
        pickle.dump(tickers, f)

    print(tickers)
    
    return tickers

#save_sp500_tickers()

def get_data_from_yahoo(reload_sp500=False):
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle","rb") as f:
            tickers=pickle.load(f)
        
    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')

    start = dt.datetime(1998,1,1)
    end = dt.datetime(2019,5,17)

    for ticker in tickers:
        print(ticker)
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            df = web.DataReader(ticker,'yahoo', start,end)
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        else:
            print('Already have {}'.format(ticker))
    

#get_data_from_yahoo()

def compile_data():
    with open("sp500tickers.pickle","rb") as f:
        tickers=pickle.load(f)
    
    main_df= pd.DataFrame()

    for count, ticker in enumerate(tickers):
        df = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
        df.set_index('Date', inplace=True)

        df.rename(columns={"Adj Close": ticker}, inplace=True)
        df.drop(['Open', 'High', 'Low', 'Close', 'Volume'], 1 , inplace=True)

        if main_df.empty:
            main_df= df
        else:
            main_df = main_df.join(df, how='outer')

        if count % 10 == 0:
            print(count)

    print(main_df.head())
    main_df.to_csv('sp500_joined_closes.csv')

#compile_data()

def visualize_data():
    df = pd.read_csv('sp500_joined_closes.csv')
    df_corr = df.corr()

    data=df_corr.values
    fig=plt.figure()
    ax=fig.add_subplot(1,1,1)

    heatmap=ax.pcolor(data, cmap=plt.cm.RdYlGn)
    fig.colorbar(heatmap)
    ax.set_xticks(np.arange(data.shape[0]) + 0.5, minor=False)
    ax.set_yticks(np.arange(data.shape[1]) + 0.5, minor=False)
    ax.invert_yaxis()
    ax.xaxis.tick_top()

    column_labels=df_corr.columns
    row_labels=df_corr.index

    ax.set_xticklabels(column_labels)
    ax.set_yticklabels(row_labels)
    plt.xticks(rotation=90)
    heatmap.set_clim(-1,1)
    plt.tight_layout()
    plt.show()


#visualize_data()

def get_data_from_pickle():
    with open("sp500tickers.pickle","rb") as f:
        tickers=pickle.load(f)
    for ticker in tickers:
        print(ticker)
        

get_data_from_pickle()





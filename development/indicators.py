#!/usr/bin/env python3

import pandas as pd
import numpy as np

class Indicators:
    def atr(self,df,period=14):
        #Would like to do this with an asserion or exception, TODO
        l=df.shape[0]
        if l <= period:
            print("ERROR: The dataframe does not have enough data to calculate the ATR")
        df1=pd.DataFrame(columns=['Date','TrueRange'])
        for i in range(1,l,1):
            curLow=np.asscalar(df.loc[[i],['Low']].values)
            curHigh=np.asscalar(df.loc[[i],['High']].values)
            prevClose=np.asscalar(df.loc[[i],['Adj Close']].values)
            true_range=max((curHigh-curLow), abs(curHigh-prevClose), abs(curLow-prevClose))
            df1.loc[i,'Date']=df.loc[i,'Date']
            df1.loc[i,'TrueRange']=true_range
        df1['ATR']=df1.rolling(window=period)['TrueRange'].mean()
        return(df1)

    def ma(self,df,period,target=0,typ=0):
        """
        typ 0    : Simple Moving Average
        typ 1    : Exponential Moving Average
        target 0 : Using the closing price to calculate the average
        target 1 : Using the high price to calculate the average
        target 2 : Using the low price to calculate the average
        """
        #Would like to do this with an asserion or exception, TODO
        l=df.shape[0]
        if l <= period:
            print("ERROR: The dataframe does not have enough data to calculate the MA")
        if typ == 0:
            if target == 0:
                df['MA']=df.rolling(window=period)['Adj Close'].mean()
            elif target == 1:
                df['MA']=df.rolling(window=period)['High'].mean()
            elif target == 2:
                df['MA']=df.rolling(window=period)['Low'].mean()
            df1 = df[['Date', 'MA']]
            return df1
        elif typ == 1:
            smoothing=2
            #first calculate the sma to begin the process
            sma=0
            for i in range (0, period, 1):
                if target == 0:
                    sma+= np.asscalar(df.loc[[i],['Adj Close']].values)
                elif target == 1:
                    sma+= np.asscalar(df.loc[[i],['High']].values)
                elif target == 2:
                    sma+= np.asscalar(df.loc[[i],['Low']].values)
            sma/=period
            factor=smoothing /(period+1)
            df1=pd.DataFrame(columns=['Date','MA'])
            for i in range(period, l ,1):
                prev = sma if i == period else np.asscalar(df1.loc[[i-1],['MA']].values)
                if target == 0:
                    ema=np.asscalar(df.loc[[i],['Adj Close']].values) * factor +  prev * (1-factor)
                elif target == 1:
                    ema=np.asscalar(df.loc[[i],['High']].values) * factor +  prev * (1-factor)
                elif target == 2:
                    ema=np.asscalar(df.loc[[i],['Low']].values) * factor +  prev * (1-factor)
                df1.loc[i,'Date']=df.loc[i,'Date']
                df1.loc[i,'MA']=ema
            return df1





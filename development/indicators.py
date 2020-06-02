#!/usr/bin/env python3

import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import math

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
            j=0
            for i in range(period, l ,1):
                prev = sma if i == period else np.asscalar(df1.loc[[j-1],['MA']].values)
                if target == 0:
                    ema=np.asscalar(df.loc[[i],['Adj Close']].values) * factor +  prev * (1-factor)
                elif target == 1:
                    ema=np.asscalar(df.loc[[i],['High']].values) * factor +  prev * (1-factor)
                elif target == 2:
                    ema=np.asscalar(df.loc[[i],['Low']].values) * factor +  prev * (1-factor)
                df1.loc[j,'Date']=df.loc[i,'Date']
                df1.loc[j,'MA']=ema
                j+=1
            return df1

    def rsi(self,df, period=14):
        #Would like to do this with an asserion or exception, TODO
        l=df.shape[0]
        if l <= period:
            print("ERROR: The dataframe does not have enough data to calculate the RSI")
        igain=0
        iloss=0
        for i in range (1, period, 1):
            curClose=np.asscalar(df.loc[[i],['Adj Close']].values)
            prevClose=np.asscalar(df.loc[[i-1],['Adj Close']].values)
            if curClose >= prevClose:
                igain+=(curClose-prevClose)
            else:
                iloss+=(prevClose-curClose)
        iagain=igain/period
        ialoss=iloss/period
        if ialoss == 0:
            irsi=100
        else:    
            irs=iagain/ialoss
            irsi=100-(100/(1+irs))
        df1=pd.DataFrame(columns=['Date','RSI','PAG','PAL'])
        df1.loc[0,'Date']=df.loc[period,'Date']
        df1.loc[0,'RSI']=irsi
        df1.loc[0,'PAG']=iagain
        df1.loc[0,'PAL']=ialoss
        j=1
        
        for i in range(period+1, l ,1):
            curClose=np.asscalar(df.loc[[i],['Adj Close']].values)
            prevClose=np.asscalar(df.loc[[i-1],['Adj Close']].values)
            cgain=0
            closs=0
            if curClose >= prevClose:
                cgain=curClose-prevClose
            else:
                closs=prevClose-curClose
            again=((df1.loc[j-1,'PAG']*(period-1))+ (cgain))/period
            aloss=((df1.loc[j-1,'PAL']*(period-1))+ (closs))/period
            if aloss == 0:
                rsi = 100
            else:    
                rs=again/aloss
                rsi=100-(100/(1+rs))
            df1.loc[j,'Date']=df.loc[i,'Date']
            df1.loc[j,'RSI']=rsi
            df1.loc[j,'PAG']=again
            df1.loc[j,'PAL']=aloss
            j+=1
        df2 = df1[['Date', 'RSI']]
        return df2


    def stddev(self,df, period=20):
        #Would like to do this with an asserion or exception, TODO
        l=df.shape[0]
        if l <= period:
            print("ERROR: The dataframe does not have enough data to calculate the RSI")
        df['Mean']=df.rolling(window=period)['Adj Close'].mean()
        k=0
        df1=pd.DataFrame(columns=['Date','STDDEV'])
        for i in range(period-1, l,1):
            mn=np.asscalar(df.loc[[i],['Mean']].values)
            sm=0
            for j in range(i-period+1, i,1):
                close=np.asscalar(df.loc[[j],['Adj Close']].values)
                devsq=(mn-close)**2
                sm+=devsq
            std=math.sqrt(sm/period)
            df1.loc[k,'Date']=df.loc[i,'Date']
            df1.loc[k,'STDDEV']=std
            k+=1
        return df1


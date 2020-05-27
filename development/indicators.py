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
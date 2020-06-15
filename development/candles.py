#!/usr/bin/env python3

import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np

doji_factor=0.0001
hw_factor=2
hmr_factor=0.005
hmr_size=0.38
tzr_factor=0.0005
wnd_factor=0.01

CandleEncoding={ 
                    1 : "Hammer",
                    2 : "Inverted Hammer",
                    3 : "Bull Counter Attack",
                    4 : "Bullish Engulfing",
                    5 : "Bullish Outside",
                    6 : "Bullish Harami",
                    7 : "Bullish Inside",
                    8 : "Piercing Pattern",
                    9 : "Bullish Sash",
                    10: "Bullish Separating Line",
                    11: "Tweezer Bottoms",
                    12: "Rising Window",
                    13: "Norning Star",
                    14: "Three White Soldiers",
                    15: "Rising Three Methods",
                    20: "Doji",
                    21: "High Wave Candle",
                    -1 : "Shooting Star",
                    -2 : "Hanging Man",
                    -3 : "Bear Counter Attack",
                    -4 : "Bearish Engulfing",
                    -5 : "Bearish Outside",
                    -6 : "Bearish Harami",
                    -7 : "Bearish Inside",
                    -8 : "Dark Cloud Cover",
                    -9 : "Bearish Sash",
                    -10: "Bearish Separating Line",
                    -11: "Tweezer Tops",
                    -12: "Falling Window",
                    -13: "EveningStar",
                    -14: "Three Black Crows",
                    -15: "Falling Three Methods"                   
                }

class Candles:
    def isBullish1C(self,df1):
        l=df1.shape[0]
        Close=np.asscalar(df1.loc[[l-1],['Adj Close']].values)
        Open=np.asscalar(df1.loc[[l-1],['Open']].values)
        High=np.asscalar(df1.loc[[l-1],['High']].values)
        Low=np.asscalar(df1.loc[[l-1],['Low']].values)
        
        #Hammer
        if (Close > Open and (Close -Open) < hmr_size*(High-Low) and (High-Close) <= hmr_factor*(High-Low)):
            return 1
        elif (Close <  Open and (Open - Close) < hmr_size*(High-Low) and (High-Open) <= hmr_factor*(High-Low)):
            return 1
        #Inverted Hammer
        elif (Close > Open and (Close -Open) < hmr_size*(High-Low) and (Open-Low) <= hmr_factor*(High-Low)):
            return 2
        elif (Close <  Open and (Open - Close) < hmr_size*(High-Low) and (Close-Low) <= hmr_factor*(High-Low)):
            return 2
        else:
            return 0

    def isBullish2C(self,df1):
        l=df1.shape[0]
        Close=np.asscalar(df1.loc[[l-1],['Adj Close']].values)
        Open=np.asscalar(df1.loc[[l-1],['Open']].values)
        High=np.asscalar(df1.loc[[l-1],['High']].values)
        Low=np.asscalar(df1.loc[[l-1],['Low']].values)
        Close_1=np.asscalar(df1.loc[[l-2],['Adj Close']].values)
        Open_1=np.asscalar(df1.loc[[l-2],['Open']].values)
        High_1=np.asscalar(df1.loc[[l-2],['High']].values)
        Low_1=np.asscalar(df1.loc[[l-2],['Low']].values)
        #Bullish Engulfing
        if Close_1 < Open_1 and Close > Open and Close > Open_1 and Open < Close_1:
            return 4
        #Bullish Outside
        elif Close_1 < Open_1 and Close > Open and High > High_1 and Low < Low_1:
            return 5
        #Bullish Harami
        elif Close_1 < Open_1 and Close > Open and Close < Open_1 and Open > Close_1:
            return 6
        #Bullish Inside
        elif Close_1 < Open_1 and Close > Open and High < High_1 and Low > Low_1:
            return 7
        #Piercing Pattern
        elif Close_1 < Open_1 and Close > Open and Open < Low_1 and Close >= Close-1 +0.5* (Open_1-Close_1):
            return 8
        #Bullish Counter Attack
        elif Close_1 < Open_1 and Close > Open and Open < Low_1 and Close <= Close-1:
            return 3
        #Tweezer Bottoms
        elif Close_1 < Open_1 and Close > Open and abs(Low_1-Low) < tzr_factor*max(Low,Low_1):
            return 11
        #Rising Window
        elif Low-High_1 > wnd_factor * Close_1:
            return 12
        #Bullish Sash
        elif Close_1 < Open_1 and Close > Open and Close > High_1 and Open > Open_1 - 0.5*(Open_1 - Close_1) and Open < Open_1:
            return 9
        #Bullish Separating Line
        elif Close_1 < Open_1 and Close > Open and Close > High_1 and Open >= Open_1 :
            return 10
        else:
            return 0


    def isBullish3C(self,df1):
            l=df1.shape[0]
            Close=np.asscalar(df1.loc[[l-1],['Adj Close']].values)
            Open=np.asscalar(df1.loc[[l-1],['Open']].values)
            High=np.asscalar(df1.loc[[l-1],['High']].values)
            Low=np.asscalar(df1.loc[[l-1],['Low']].values)
            Close_1=np.asscalar(df1.loc[[l-2],['Adj Close']].values)
            Open_1=np.asscalar(df1.loc[[l-2],['Open']].values)
            High_1=np.asscalar(df1.loc[[l-2],['High']].values)
            Low_1=np.asscalar(df1.loc[[l-2],['Low']].values)
            Close_2=np.asscalar(df1.loc[[l-3],['Adj Close']].values)
            Open_2=np.asscalar(df1.loc[[l-3],['Open']].values)
            High_2=np.asscalar(df1.loc[[l-3],['High']].values)
            Low_2=np.asscalar(df1.loc[[l-3],['Low']].values)
            #Three White Soldiers
            if Low_1 > Low_2 and High_1 > High_2 and Close_1 > Close_2 and High > High_1 and Low > Low_1 and Close > Close_1:
                return 14
            #Morning Star
            elif Close_2 < Open_2 and Close > Open and Low_1 < Low_2 and Low_1 < Low:
                return 13
            else:
                return 0


    def isBearish1C(self, df1):
        l=df1.shape[0]
        Close=np.asscalar(df1.loc[[l-1],['Adj Close']].values)
        Open=np.asscalar(df1.loc[[l-1],['Open']].values)
        High=np.asscalar(df1.loc[[l-1],['High']].values)
        Low=np.asscalar(df1.loc[[l-1],['Low']].values)
        #Hanging Man
        if (Close > Open and (Close -Open) < hmr_size*(High-Low) and (High-Close) <= hmr_factor*(High-Low)):
            return -2
        elif (Close <  Open and (Open - Close) < hmr_size*(High-Low) and (High-Open) <= hmr_factor*(High-Low)):
            return -2
        #Shooting Star
        if (Close > Open and (Close -Open) < hmr_size*(High-Low) and (Open-Low) <= hmr_factor*(High-Low)):
            return -1
        elif (Close <  Open and (Open - Close) < hmr_size*(High-Low) and (Close-Low) <= hmr_factor*(High-Low)):
            return -1
        else:
            return 0

    def isBearish2C(self, df1):
        l=df1.shape[0]
        Close=np.asscalar(df1.loc[[l-1],['Adj Close']].values)
        Open=np.asscalar(df1.loc[[l-1],['Open']].values)
        High=np.asscalar(df1.loc[[l-1],['High']].values)
        Low=np.asscalar(df1.loc[[l-1],['Low']].values)
        Close_1=np.asscalar(df1.loc[[l-2],['Adj Close']].values)
        Open_1=np.asscalar(df1.loc[[l-2],['Open']].values)
        High_1=np.asscalar(df1.loc[[l-2],['High']].values)
        Low_1=np.asscalar(df1.loc[[l-2],['Low']].values)
        #Bearish Engulfing
        if Close_1 > Open_1 and Close < Open and Close < Open_1 and Open > Close_1:
            return -4
        #Bearish Outside
        elif Close_1 > Open_1 and Close < Open and High > High_1 and Low < Low_1:
            return -5
        #Bearish Harami
        elif Close_1 > Open_1 and Close < Open and Close > Open_1 and Open < Close_1:
            return -6
        #Bearish Inside
        elif Close_1 > Open_1 and Close < Open and High < High_1 and Low > Low_1:
            return -7
        #Dark Cloud Cover
        elif Close_1 > Open_1 and Close < Open and Open > High_1 and Close <= Close_1 - 0.5* (Open_1-Close_1):
            return -8
        #Bearish Counter Attack
        elif Close_1 > Open_1 and Close < Open and Open > High_1 and Close >= Close_1 :
            return -3
        #Tweezer Tops
        elif Close_1 > Open_1 and Close < Open and abs(High_1-High) < tzr_factor*max(High,High_1):
            return -11
        #Falling Window
        elif Low_1-High > wnd_factor * Close_1:
            return -12
        #Bearish Sash
        elif Close_1 > Open_1 and Close < Open and Close < Low_1 and Open < Open_1 + 0.5*(Open_1 - Close_1) and Open > Open_1:
            return -9
        #Bearish Separating Line
        elif Close_1 > Open_1 and Close < Open and Close < Low_1 and Open <= Open_1 :
            return -10
        else:
            return 0


    def isBearish3C(self,df1):
            l=df1.shape[0]
            Close=np.asscalar(df1.loc[[l-1],['Adj Close']].values)
            Open=np.asscalar(df1.loc[[l-1],['Open']].values)
            High=np.asscalar(df1.loc[[l-1],['High']].values)
            Low=np.asscalar(df1.loc[[l-1],['Low']].values)
            Close_1=np.asscalar(df1.loc[[l-2],['Adj Close']].values)
            Open_1=np.asscalar(df1.loc[[l-2],['Open']].values)
            High_1=np.asscalar(df1.loc[[l-2],['High']].values)
            Low_1=np.asscalar(df1.loc[[l-2],['Low']].values)
            Close_2=np.asscalar(df1.loc[[l-3],['Adj Close']].values)
            Open_2=np.asscalar(df1.loc[[l-3],['Open']].values)
            High_2=np.asscalar(df1.loc[[l-3],['High']].values)
            Low_2=np.asscalar(df1.loc[[l-3],['Low']].values)
            #Three Black Crows
            if Low_1 < Low_2 and High_1 < High_2 and Close_1 < Close_2 and High < High_1 and Low < Low_1 and Close < Close_1:
                return -14
            #Evening Star
            elif Close_2 > Open_2 and Close <  Open and High_1 > High_2 and High_1 > High:
                return -13
            else:
                return 0

    def isNeutral(self, df1):
        l=df1.shape[0]
        Close=np.asscalar(df1.loc[[l-1],['Adj Close']].values)
        Open=np.asscalar(df1.loc[[l-1],['Open']].values)
        High=np.asscalar(df1.loc[[l-1],['High']].values)
        Low=np.asscalar(df1.loc[[l-1],['Low']].values)
        #Doji
        if (Close >= Open and Close <= (1+doji_factor) * Open) or  (Close <=  Open and Close >= (1-doji_factor) * Open):
            return 20
        #High Wave Candle
        elif (Close > Open and High - Close >= hw_factor * (Close-Open) and Open - Low >= hw_factor * (Close-Open)) or (Close < Open and Close - Low >= hw_factor * (Open-Close) and High - Open >= hw_factor * (Open-Close)):
            return 21
        else:
            return 0 

    def HeikinAshi(self,df):
        l=df.shape[0]
        dfr=pd.DataFrame(columns=['Date','HA_Open','HA_Close','HA_Low','HA_High'])
        for i in range(0,l):
            Close=np.asscalar(df.loc[[i],['Adj Close']].values)
            Open=np.asscalar(df.loc[[i],['Open']].values)
            High=np.asscalar(df.loc[[i],['High']].values)
            Low=np.asscalar(df.loc[[i],['Low']].values)
            Date=np.asscalar(df.loc[[i],['Date']].values)
            dfr.loc[i,'Date']=Date
            if i == 0:
                dfr.loc[i,'HA_High']=High
                dfr.loc[i,'HA_Low']=Low
                dfr.loc[i,'HA_Open']= (Open+Close)/2
                dfr.loc[i,'HA_Close'] =(Open+Close+High+Low)/4
            else:
                ha_close=(Open+Close+High+Low)/4
                pha_open=np.asscalar(dfr.loc[[i-1],['HA_Open']].values)
                pha_close=np.asscalar(dfr.loc[[i-1],['HA_Close']].values)
                ha_open=(pha_open+pha_close)/2
                dfr.loc[i,'HA_High']=max(High, ha_open,ha_close)
                dfr.loc[i,'HA_Low']=min(Low, ha_open,ha_close)
                dfr.loc[i,'HA_Open']= ha_open
                dfr.loc[i,'HA_Close'] = ha_close
        return dfr

        



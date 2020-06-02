#!/usr/bin/env python3

from instrument import *
import pandas as pd
import numpy as np
import pdfkit as pdf
import helpers as hlp
import os

class Strategies:
    
    def __init__(self, dictc, etfs, stocks, tf=0):
        self._dictc=dictc
        self._etfs=etfs
        self._stocks=stocks
        self._timeframe=tf

    def TigerSharkMomentumStrategy(self, names, atr=1.0, rvol=1.2, rsi2f=2):
        period=15
        """ Looking for a stock that closes very strong within 90% of the range for longs, or very weak at lower 10% of the range.
            Trigger is the high/low of day
            Discard trade if it pullbacks or retraces 38.2%
            Target1 61.8%, target2 100%
            Stop Loss 1:2 Risk Reward based on Target1
            TO DO: I need to add some filter based on ATR and RVOL on this *****
        """    
        outdict={'Ticker': [],
                 'LongShort' : [],
                 'Trigger' :[],
                 'Discard' : [],
                 'Target1' : [],
                 'Target2' : [],
                 'StopLoss' : []
                 }
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
            df=ins.get_values(hlp.timeframes[self._timeframe], period)
            df1 = df[['Date', 'High', 'Low', 'Adj Close', 'Volume']]
            df2=ins.ind.atr(df1)
            curOpen=np.asscalar(df.loc[[period-1],['Open']].values)
            curLow=np.asscalar(df.loc[[period-1],['Low']].values)
            curHigh=np.asscalar(df.loc[[period-1],['High']].values)
            curClose=np.asscalar(df.loc[[period-1],['Adj Close']].values)
            df['AvgVolume']=df.rolling(window=10)['Volume'].mean()

            if((curHigh-curLow) >= atr * (df2.loc[period-1,'ATR']) and df.loc[period-1,'Volume'] > rvol * df.loc[period-1,'AvgVolume']):
                if curClose > curOpen and (curHigh - curClose) < 0.1*(curHigh-curLow) :
                    outdict['Ticker'].append(stock)
                    outdict['LongShort'].append("Long")
                    outdict['Trigger'].append(curHigh)
                    outdict['Discard'].append(curHigh-0.382*(curHigh-curLow))
                    outdict['Target1'].append(curHigh+0.618*(curHigh-curLow))
                    outdict['Target2'].append(curHigh+(curHigh-curLow))
                    outdict['StopLoss'].append(curHigh - 0.5*(0.618*(curHigh-curLow)))
                elif curClose < curOpen and (curClose - curLow) < 0.1*(curHigh-curLow) :
                    outdict['Ticker'].append(stock)
                    outdict['LongShort'].append("Short")
                    outdict['Trigger'].append(curLow)
                    outdict['Discard'].append(curLow+0.382*(curHigh-curLow))
                    outdict['Target1'].append(curLow-0.618*(curHigh-curLow))
                    outdict['Target2'].append(curLow-(curHigh-curLow))
                    outdict['StopLoss'].append(curLow + 0.5*(0.618*(curHigh-curLow)))
        outdf=pd.DataFrame(outdict)
        temp_html="str.html"
        outdf.to_html(temp_html, index=False)
        out_file="str.pdf"
        pdf.from_file(temp_html,out_file)
        os.remove(temp_html)      

    def RSI2Strategy(self, names, rsi2f=2, atr=1.0, rvol=1.2):
        period=250
        obght=100-rsi2f
        osld=rsi2f
        print(osld)
        j=0
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
            df=ins.get_values(hlp.timeframes[self._timeframe], period)
            if df.shape[0] < 200:
                continue
            df1 = df[['Date', 'High', 'Low', 'Adj Close', 'Volume']]
            df2=ins.ind.ma(df1,200,typ=1)
            df3=ins.ind.rsi(df1,2)
            l1=df1.shape[0]
            l2=df2.shape[0]
            l3=df3.shape[0]
            ma200=np.asscalar(df2.loc[[l2-1],['MA']].values)
            rsi2=np.asscalar(df3.loc[[l3-1],['RSI']].values)
            curClose=np.asscalar(df.loc[[l1-1],['Adj Close']].values)
            curDate=np.asscalar(df.loc[[l1-1],['Date']].values)
            dfr=pd.DataFrame(columns=['Date','RSI','MA','Close'])
            if  curClose > ma200 and rsi2 <= osld:
                dfr.loc[j,'Date']=curDate
                dfr.loc[j,'Close']=curClose
                dfr.loc[j,'MA']=ma200
                dfr.loc[j,'RSI']=rsi2
                j+=1
            elif curClose < ma200 and rsi2 >= obght:
                dfr.loc[j,'Date']=curDate
                dfr.loc[j,'Close']=curClose
                dfr.loc[j,'MA']=ma200
                dfr.loc[j,'RSI']=rsi2
                j+=1
        temp_html="str.html"
        dfr.to_html(temp_html, index=False)
        out_file="str.pdf"
        pdf.from_file(temp_html,out_file)
        os.remove(temp_html)      





    def TestStrategy(self, names, atr=1.0, rvol=1.2, rsi2f=2):
        period=250
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
            df=ins.get_values(hlp.timeframes[self._timeframe], period)
            df1 = df[['Date', 'High', 'Low', 'Adj Close', 'Volume']]
            #df2=ins.ind.ma(df1, 20, typ=0)
            #df2=ins.ind.rsi(df1,2)
            df2=ins.ind.stddev(df1)
            print(df2)



    tr_strategies = { 0 : TigerSharkMomentumStrategy,
                      1 : RSI2Strategy,
                     10 : TestStrategy   }

    def run(self, strategy, names, **kwargs):
        self.tr_strategies[strategy](self,names,**kwargs)
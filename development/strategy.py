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
        dfr=pd.DataFrame(columns=['Ticker','Date','RSI','MA','Close'])
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
            if  curClose > ma200 and rsi2 <= osld:
                dfr.loc[j,'Ticker']=stock
                dfr.loc[j,'Date']=curDate
                dfr.loc[j,'Close']=curClose
                dfr.loc[j,'MA']=ma200
                dfr.loc[j,'RSI']=rsi2
                j+=1
            elif curClose < ma200 and rsi2 >= obght:
                dfr.loc[j,'Ticker']=stock
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


    def StdDevZonesStrategy(self, names, rsi2f=2, atr=1.0, rvol=1.2):
        period=30
        window=20
        k=0
        dfr=pd.DataFrame(columns=['Ticker','Date','STDDEV','MA','Close','B23','B3'])
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
            df=ins.get_values(hlp.timeframes[self._timeframe], period)
            if df.shape[0] < 30:
                continue
            df1 = df[['Date', 'High', 'Low', 'Adj Close', 'Volume']]
            df2=ins.ind.ma(df1,window,typ=0)
            df3=ins.ind.stddev(df1,window)
            l1=df1.shape[0]
            l2=df2.shape[0]
            l3=df3.shape[0]
            curClose=np.asscalar(df1.loc[[l1-1],['Adj Close']].values)
            curDate=np.asscalar(df1.loc[[l1-1],['Date']].values)
            curma=np.asscalar(df2.loc[[l2-1],['MA']].values)
            curstddev=np.asscalar(df3.loc[[l3-1],['STDDEV']].values)
            std2=2*curstddev
            std3=3*curstddev
            if curClose > curma + std2 and (ins.candle.isBearish2C(df) or ins.candle.isBearish3C(df) ):
                dfr.loc[k,'Ticker']=stock
                dfr.loc[k,'Date']=curDate
                dfr.loc[k,'Close']=curClose
                dfr.loc[k,'MA']=curma
                dfr.loc[k,'STDDEV']=curstddev
                if curClose > curma + std3:
                    dfr.loc[k,'B3']="+"
                else:
                    dfr.loc[k,'B23']="+"
                k+=1
            elif  curClose < curma - std2 and (ins.candle.isBullish2C(df) or ins.candle.isBullish3C(df) ):
                dfr.loc[k,'Date']=curDate
                dfr.loc[k,'Close']=curClose
                dfr.loc[k,'MA']=curma
                dfr.loc[k,'STDDEV']=curstddev
                if curClose < curma - std3:
                    dfr.loc[k,'B3']="-"
                else:
                    dfr.loc[k,'B23']="-"
                k+=1  
        temp_html="str.html"
        dfr.to_html(temp_html, index=False)
        out_file="str.pdf"
        pdf.from_file(temp_html,out_file)
        os.remove(temp_html)       


    def JBEquivStrategy(self, names, rsi2f=2, atr=1.0, rvol=1.2):
        period=30
        window1=2
        window2=5
        window31=8
        window32=20
        k=0
        dfr=pd.DataFrame(columns=['Ticker','Date','MA2','MA5','MA20','Close',"Dir"])
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
            df=ins.get_values(hlp.timeframes[self._timeframe], period)
            if df.shape[0] < 30:
                continue
            df1 = df[['Date', 'High', 'Low', 'Adj Close', 'Volume']]
            df2=ins.ind.ma(df1,window1,typ=0)
            df3=ins.ind.ma(df1,window2,typ=0)
            df4=ins.ind.ma(df1,window31,typ=0)
            df5=ins.ind.ma(df1,window32,typ=0)
            l1=df1.shape[0]
            l2=df2.shape[0]
            l3=df3.shape[0]
            l4=df4.shape[0]
            l5=df5.shape[0]
            curClose=np.asscalar(df1.loc[[l1-1],['Adj Close']].values)
            curDate=np.asscalar(df1.loc[[l1-1],['Date']].values)
            cur2=np.asscalar(df2.loc[[l2-1],['MA']].values)
            prev2=np.asscalar(df2.loc[[l2-2],['MA']].values)
            cur5=np.asscalar(df3.loc[[l3-1],['MA']].values)
            prev5=np.asscalar(df3.loc[[l3-2],['MA']].values)
            cur8=np.asscalar(df4.loc[[l4-1],['MA']].values)
            cur20=np.asscalar(df5.loc[[l5-1],['MA']].values)
            if cur2 > cur5 and prev2 < prev5 and curClose >= cur20:
                dfr.loc[k,'Ticker']=stock
                dfr.loc[k,'Date']=curDate
                dfr.loc[k,'Close']=curClose
                dfr.loc[k,'MA2']=cur2
                dfr.loc[k,'MA5']=cur5
                dfr.loc[k,'MA20']=cur20
                dfr.loc[k,'Dir']="Up"
            elif cur2 < cur5 and prev2 > prev5 and curClose <= cur8:
                dfr.loc[k,'Ticker']=stock
                dfr.loc[k,'Date']=curDate
                dfr.loc[k,'Close']=curClose
                dfr.loc[k,'MA2']=cur2
                dfr.loc[k,'MA5']=cur5
                dfr.loc[k,'MA20']=cur20
                dfr.loc[k,'Dir']="Down"
            else:
                continue
            k+=1
        temp_html="str.html"
        dfr.to_html(temp_html, index=False)
        out_file="str.pdf"
        pdf.from_file(temp_html,out_file)
        os.remove(temp_html)       

    def HAStrategy(self, names, atr=1.0, rvol=1.2, rsi2f=2):
        period=50
        window=21
        k=0
        dfr=pd.DataFrame(columns=['Ticker','Date',"Close","Dir"])
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
            df=ins.get_values(hlp.timeframes[self._timeframe], period)
            df1 = df[['Date', 'Open', 'High', 'Low', 'Adj Close', 'Volume']]
            df2 = ins.candle.HeikinAshi(df1)
            df3=ins.ind.ma(df1,window,typ=0)
            l1=df1.shape[0]
            l2=df2.shape[0]
            l3=df3.shape[0]
            curma=np.asscalar(df3.loc[[l3-1],['MA']].values)
            curClose=np.asscalar(df1.loc[[l1-1],['Adj Close']].values)
            curhaopen=np.asscalar(df2.loc[[l2-1],['HA_Open']].values)
            curhaclose=np.asscalar(df2.loc[[l2-1],['HA_Close']].values)
            prevhaopen=np.asscalar(df2.loc[[l2-2],['HA_Open']].values)
            prevhaclose=np.asscalar(df2.loc[[l2-2],['HA_Close']].values)
            curDate=np.asscalar(df1.loc[[l1-1],['Date']].values)
            if curClose > curma and curhaopen < curhaclose and prevhaopen > prevhaclose:
                dfr.loc[k,'Ticker']=stock
                dfr.loc[k,'Date']=curDate
                dfr.loc[k,'Close']=curClose
                dfr.loc[k,'Dir']="Up"
            elif curClose  < curma and curhaopen > curhaclose and prevhaopen < prevhaclose:
                dfr.loc[k,'Ticker']=stock
                dfr.loc[k,'Date']=curDate
                dfr.loc[k,'Close']=curClose
                dfr.loc[k,'Dir']="Down"
            else:
                continue
            k+=1
        temp_html="str.html"
        dfr.to_html(temp_html, index=False)
        out_file="str.pdf"
        pdf.from_file(temp_html,out_file)
        os.remove(temp_html)               


    def MomentumStrategy(self, names, atr=1.0, rvol=1.2, rsi2f=2):
        period=90
        window=28
        window1=60
        k=0
        dfr=pd.DataFrame(columns=['Ticker','Date',"Close","Dir",'Target','Trigger'])
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
            df=ins.get_values(hlp.timeframes[self._timeframe], period)
            df1 = df[['Date', 'Open', 'High', 'Low', 'Adj Close', 'Volume']]
            df11=df1.iloc[-window1:-1]
            df11.columns=df11.columns.str.replace(' ','_')
            df2 = ins.ind.momentum(df1)
            df22=df2.iloc[-window1:-1]
            df1minclose=df11[df11.Adj_Close == df11.Adj_Close.min()]
            df1maxclose=df11[df11.Adj_Close == df11.Adj_Close.max()]
            mincloseDate=np.asscalar(df1minclose.loc[[0],['Date']].values)
            maxcloseDate=np.asscalar(df1maxclose.loc[[0],['Date']].values)            
            df2minmomentum=df22[df22.Momentum == df22.Momentum.min()]
            df2maxmomentum=df22[df22.Momentum == df22.Momentum.max()]
            minmomentumDate=np.asscalar(df2minmomentum.loc[[0],['Date']].values)
            maxmomentumDate=np.asscalar(df2maxmomentum.loc[[0],['Date']].values)
            if mincloseDate != minmomentumDate and minmomentumDate < mincloseDate:
                df1minmomentumclose=df11[df11.Date == minmomentumDate]
                minmomentum=np.asscalar(df1minmomentumclose.loc[[0],['Adj_Close']].values)
                df2corrmomentum=df22[df22.Date == mincloseDate]
                corrmomentum=np.asscalar(df2corrmomentum.loc[[0],['Momentum']].values)
                if minmomentum < corrmomentum:
                    df111=df11[df11.Date >= minmomentumDate & df11.Date >= mincloseDate]
                    df222=df22[df22.Date >= minmomentumDate & df22.Date >= mincloseDate]
                    trigger=df222['Momentum'].max()
                    hh=df111['Adj_Close'].max()
                    minclose=np.asscalar(df1minclose.loc[[0],['Adj_Close']]).values
                    target=hh-minclose
                    dfr.loc[k,'Ticker']=stock
                    dfr.loc[k,'Date']=mincloseDate
                    dfr.loc[k,'Close']=minclose
                    dfr.loc[k,'Dir']="Up"
                    dfr.loc[k,'Target']=target
                    dfr.loc[k,'Trigger']=trigger
                    k+=1
#            elif :
        temp_html="str.html"
        dfr.to_html(temp_html, index=False)
        out_file="str.pdf"
        pdf.from_file(temp_html,out_file)
        os.remove(temp_html)         

    def PinBarStrategy(self, names, rsi2f=2, atr=1.0, rvol=1.2):
        period=60
        window=50
        k=0
        dfr=pd.DataFrame(columns=['Ticker','Date','MA','Close','Dir'])
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
            df=ins.get_values(hlp.timeframes[self._timeframe], period)
            if df.shape[0] < period:
                continue
            df1 = df[['Date', 'High', 'Low', 'Open', 'Adj Close', 'Volume']]
            df2=ins.ind.ma(df1,window,typ=1)
            l1=df1.shape[0]
            l2=df2.shape[0]
            curClose=np.asscalar(df1.loc[[l1-1],['Adj Close']].values)
            curDate=np.asscalar(df1.loc[[l1-1],['Date']].values)
            cur50=np.asscalar(df2.loc[[l2-1],['MA']].values)
            if curClose > cur50 and ins.candle.isBullish1C(df1):
                dfr.loc[k,'Ticker']=stock
                dfr.loc[k,'Date']=curDate
                dfr.loc[k,'Close']=curClose
                dfr.loc[k,'MA']=cur50
                dfr.loc[k,'Dir']="Up"
            elif curClose < cur50 and ins.candle.isBearish1C(df1):
                dfr.loc[k,'Ticker']=stock
                dfr.loc[k,'Date']=curDate
                dfr.loc[k,'Close']=curClose
                dfr.loc[k,'MA']=cur50
                dfr.loc[k,'Dir']="Down"
            else:
                continue
            k+=1
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
            #df2=ins.ind.stddev(df1)
            df2=ins.ind.momentum(df1,2)
            print(df2)



    tr_strategies = { 0 : TigerSharkMomentumStrategy,
                      1 : RSI2Strategy,
                      2 : StdDevZonesStrategy,
                      3 : JBEquivStrategy,
                      4 : HAStrategy,
                      5 : MomentumStrategy,
                      6: PinBarStrategy,
                     10 : TestStrategy   }

    def run(self, strategy, names, **kwargs):
        self.tr_strategies[strategy](self,names,**kwargs)
#!/usr/bin/env python3

from instrument import *
import pandas as pd

class BackTester:
    
    def __init__(self, dictc, etfs, stocks):
        self._dictc=dictc
        self._etfs=etfs
        self._stocks=stocks

    def BoundedMorningGapStrategy(self, names, **kwargs):
        strategy_params = {
                            'timeframe' : kwargs.get("timeframe", "daily"),
                            'period'    : kwargs.get("period", 800),
                            'gp'        : kwargs.get("gp", 0.05)
                          }
        
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
            df=ins.get_values(strategy_params["timeframe"], strategy_params["period"])
            GapUp=0
            GapDown=0
            Gap=0
            ClosedGapUp=0
            ClosedGapDown=0
            ClosedGap=0
            limit=strategy_params["period"]
            gp=strategy_params["gp"]
            #print(df.dtypes)
            
            for i in range(1,limit):
                curOpen=df.loc[[i],['Open']].values
                curLow=df.loc[[i],['Low']].values
                curHigh=df.loc[[i],['High']].values
                prevClose=df.loc[[i-1],['Adj Close']].values
                """
                if df.loc[[i],['Open']] > df.loc[[i-1],['Adj Close']] and (df.loc[[i],['Open']] - df.loc[[i-1],['Adj Close']]) <=  gp*df.loc[[i-1],['Adj Close']]:
                    GapUp+=1
                    Gap+=1
                    if df.loc[[i],['Low']] <= df.loc[[i-1],['Adj Close']]:
                        ClosedGapUp+=1
                        ClosedGap+=1
                elif df.loc[[i],['Open']] < df.loc[[i-1],['Adj Close']] and (df.loc[[i-1],['Adj Close']]-df.loc[[i],['Open']]) <=  gp*df.loc[[i-1],['Adj Close']]:
                    GapDown+=1
                    Gap+=1
                    if df.loc[[i],['High']] >= df.loc[[i-1],['Adj Close']]:
                        ClosedGapDown+=1
                        ClosedGap+=1
                """
                if curOpen > prevClose and (curOpen - prevClose) <=  gp*prevClose:
                    GapUp+=1
                    Gap+=1
                    if curLow <= prevClose:
                        #print(df.loc[[i],['Date']])
                        ClosedGapUp+=1
                        ClosedGap+=1
                elif curOpen < prevClose and (prevClose-curOpen) <=  gp*prevClose:
                    GapDown+=1
                    Gap+=1
                    if curHigh >= prevClose:
                        
                        ClosedGapDown+=1
                        ClosedGap+=1
            print("Gaps: "+str(Gap)+" Closed Gaps "+str(ClosedGap))
            print("Gaps Up: "+str(GapUp)+" Closed Gaps Up "+str(ClosedGapUp))
            print("Gaps Down: "+str(GapDown)+" Closed Gaps Down "+str(ClosedGapDown))

    strategies = {0: BoundedMorningGapStrategy,}

    def run(self, strategy, names, **kwargs):
        self.strategies[strategy](self,names,**kwargs)
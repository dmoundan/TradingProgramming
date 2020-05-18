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
                            'period'    : kwargs.get("period", 500),
                            'gp'        : kwargs.get("gp", 0.02)
                          }
        
        for stock in names:
            if stock in self._stocks:
                print (stock+" is a stock")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["stock_db"])
            elif stock in self._etfs:
                print(stock+" is an ETF")
                ins=Instrument(stock, self._dictc["DB_dir"]+"/"+self._dictc["etf_db"])
                df=ins.get_values(strategy_params["timeframe"], strategy_params["period"])
                print(df.tail())

    strategies = {0: BoundedMorningGapStrategy,}

    def run(self, strategy, names, **kwargs):
        self.strategies[strategy](self,names,**kwargs)
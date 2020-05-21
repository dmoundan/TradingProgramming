#!/usr/bin/env python3

from instrument import *
import pandas as pd
import numpy as np
import pdfkit as pdf
import os

class Strategies:
    
    def __init__(self, dictc, etfs, stocks):
        self._dictc=dictc
        self._etfs=etfs
        self._stocks=stocks

    def TigerSharkMomentumStrategy(self, names):
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
            df=ins.get_values("daily", 1)
            curOpen=np.asscalar(df.loc[[0],['Open']].values)
            curLow=np.asscalar(df.loc[[0],['Low']].values)
            curHigh=np.asscalar(df.loc[[0],['High']].values)
            curClose=np.asscalar(df.loc[[0],['Adj Close']].values)
            
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

    tr_strategies = {0: TigerSharkMomentumStrategy,}

    def run(self, strategy, names, **kwargs):
        self.tr_strategies[strategy](self,names,**kwargs)
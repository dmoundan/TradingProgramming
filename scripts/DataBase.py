import sqlite3
import pandas as pd
from datetime import date
import sys
from tabulate import tabulate
import numpy as np

from talib import STOCH
from talib import SMA
from talib import EMA
from talib import RSI
from talib import MACD
from talib import ADOSC
from talib import ATR
from talib import ADX
from talib import MINUS_DI
from talib import PLUS_DI
from talib import BBANDS

from talib import CDLCLOSINGMARUBOZU
from talib import CDLDOJI
from talib import CDLDOJISTAR
from talib import CDLDRAGONFLYDOJI
from talib import CDLGRAVESTONEDOJI
from talib import CDLHAMMER
from talib import CDLHANGINGMAN
from talib import CDLHIGHWAVE
from talib import CDLINVERTEDHAMMER
from talib import CDLLONGLEGGEDDOJI
from talib import CDLLONGLINE
from talib import CDLMARUBOZU
from talib import CDLRICKSHAWMAN
from talib import CDLSHOOTINGSTAR
from talib import CDLSHORTLINE
from talib import CDLSPINNINGTOP
from talib import CDLTASUKIGAP
from talib import CDL2CROWS
from talib import CDLBELTHOLD
from talib import CDLCOUNTERATTACK
from talib import CDLDARKCLOUDCOVER
from talib import CDLENGULFING
from talib import CDLHARAMI
from talib import CDLHARAMICROSS
from talib import CDLHOMINGPIGEON
from talib import CDLINNECK
from talib import CDLKICKING
from talib import CDLKICKINGBYLENGTH
from talib import CDLMATCHINGLOW
from talib import CDLONNECK
from talib import CDLPIERCING
from talib import CDLSEPARATINGLINES
from talib import CDLTHRUSTING
from talib import CDLUPSIDEGAP2CROWS
from talib import CDL3BLACKCROWS
from talib import CDL3INSIDE
from talib import CDL3OUTSIDE
from talib import CDL3STARSINSOUTH
from talib import CDL3WHITESOLDIERS
from talib import CDLABANDONEDBABY
from talib import CDLADVANCEBLOCK
from talib import CDLEVENINGDOJISTAR
from talib import CDLEVENINGSTAR
from talib import CDLGAPSIDESIDEWHITE
from talib import CDLIDENTICAL3CROWS
from talib import CDLMORNINGDOJISTAR
from talib import CDLMORNINGSTAR
from talib import CDLSTICKSANDWICH
from talib import CDLTASUKIGAP
from talib import CDLTRISTAR
from talib import CDLUNIQUE3RIVER
from talib import CDLXSIDEGAP3METHODS
from talib import CDL3LINESTRIKE
from talib import CDLBREAKAWAY
from talib import CDLCONCEALBABYSWALL
from talib import CDLHIKKAKE
from talib import CDLHIKKAKEMOD
from talib import CDLLADDERBOTTOM
from talib import CDLMATHOLD
from talib import CDLRISEFALL3METHODS
from talib import CDLSTALLEDPATTERN

"""
from talib import *
"""

class DataBase:

    def __init__(self, name):
        self.name=name
        try:
            self.conn = sqlite3.connect(self.name)
        except Error as e:
            print(e)
        self.cur=self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()


class YFDB(DataBase):
    def __init__(self, name):
        super(YFDB, self).__init__(name)

    def ScanForDojisHammers(self, openv, close,high, low, doji_body_factor):
        if openv == close or abs(close - openv) <= doji_body_factor * close:
            if (openv >= close and (close -low) <= doji_body_factor * close ) or (openv <= close and (openv - low) <= doji_body_factor * close ):
                return "gravestone_doji"
            elif (openv >= close and (high -openv) <= doji_body_factor * close ) or (openv <= close and (high - close) <= doji_body_factor * close ):
                return "dragonfly_doji"
            else:
                return "doji"
        elif abs(openv-close) <= (1/3)*(abs(high-low)):
            if (openv > close and high-openv <= doji_body_factor * close) or (openv  < close and high-close <= doji_body_factor * close):
                return "hammer"
            elif (openv > close and close-low <= doji_body_factor * close) or (openv  < close and openv-low <= doji_body_factor * close):
                return "inverted_hammer"
            else:
                return "no_1candle_pattern"
        else:
            return "no_1candle_pattern"

    def ScanFor2C(self, openv, close,high, low, doji_body_factor):
        if (openv[0] >  close[0] and openv[1] < close[1] and openv[1] <  close[0] and close[1] > openv[0]):
            return "bullish_engulfing"
        elif (openv[0] <  close[0] and openv[1] > close[1] and openv[1] >  close[0] and close[1] < openv[0]):
            return "bearish_engulfing"
        elif (openv[0] >  close[0] and openv[1] < close[1] and openv[1] < low[0]  and close[1] >= (1/2*(openv[0]-close[0])+close[0])):
            return "piercing_pattern"
        elif (openv[0] < close[0] and openv[1] > close[1] and openv[1] > high[0]  and close[1] <= (close[0] - 1/2*(close[0]-openv[0]))):
            return "dark_cloud"
        elif (openv[0] >  close[0] and openv[1] < close[1] and ((low[1] > close[0] and high[1] < openv[0]) or (low[1] > low[0] and high[1] < high[0]))):
            return "bullish_inside_day"    
        elif (openv[0] <  close[0] and openv[1] >  close[1] and ((low[1] > openv[0] and high[1] < close[0]) or (low[1] > low[0] and high[1] < high[0]))):
            return "bearish_inside_day"    
        elif (openv[0] >  close[0] and openv[1] < close[1] and openv[1] > close[0] and  close[1] < openv[0]):
            return "bullish_harami"
        elif (openv[0] <  close[0] and openv[1] > close[1] and openv[1] < close[0]  and  close[1] > openv[0]):
            return "bearish_harami"
        elif (openv[0] <  close[0] and openv[1] > close[1] and (high[1] == high[0] or openv[0] == openv[1])):
            return "twizzer_top"
        elif (openv[0] >  close[0] and openv[1] < close[1] and (low[1] == low[0] or openv[0] == openv[1])):
            return "twizzer_bottom"
        elif (openv[0] >  close[0] and openv[1] < close[1] and openv[1] >= openv[0]):
            return "bullish_kicker"
        elif (openv[0] <  close[0] and openv[1] > close[1] and openv[1] <= openv[0]):
            return "bearish_kicker"
        else:
            return "no_2candle_pattern"
        
    
    def ScanFor1CSPatternsTALib(self, ticker, time_frame):
        period = 20
        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        i1 = CDLCLOSINGMARUBOZU(openv, high, low, close)
        i2 = CDLDOJI(openv, high, low, close)
        i3 = CDLDOJISTAR(openv, high, low, close)
        i4 = CDLDRAGONFLYDOJI(openv, high, low, close)
        i5 = CDLGRAVESTONEDOJI(openv, high, low, close)
        i6 = CDLHAMMER(openv, high, low, close)
        i7 = CDLHANGINGMAN(openv, high, low, close)
        i8 = CDLHIGHWAVE(openv, high, low, close)
        i9 = CDLINVERTEDHAMMER(openv, high, low, close)
        i10 = CDLLONGLEGGEDDOJI(openv, high, low, close)
        i11 = CDLLONGLINE(openv, high, low, close)
        i12 = CDLMARUBOZU(openv, high, low, close)
        i13 = CDLRICKSHAWMAN(openv, high, low, close)
        i14 = CDLSHOOTINGSTAR(openv, high, low, close)
        i15 = CDLSHORTLINE(openv, high, low, close)
        i16 = CDLSPINNINGTOP(openv, high, low, close)
        i17 = CDLTASUKIGAP(openv, high, low, close)
        result=(i1[-1],i2[-1],i3[-1],i4[-1],i5[-1],i6[-1],i7[-1],i8[-1],i9[-1],i10[-1],i11[-1],i12[-1],i13[-1],i14[-1],i15[-1],i16[-1],i17[-1])
        print(ticker," ",result)

    def ScanFor2CSPatternsTALib(self, ticker, time_frame):
        period = 20
        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        str1=""
        i1 = CDL2CROWS(openv, high, low, close)
        if i1[-1] != 0:
            str1+="-"+"2CROWS"  
        i2 = CDLBELTHOLD(openv, high, low, close)
        """
        if i2[-1] != 0:
            str1+="-"+"BELTHOLD"
        """
        i3 = CDLCOUNTERATTACK(openv, high, low, close)
        if i3[-1] != 0:
            str1+="-"+"COUNTERATTACK"
        i4 = CDLDARKCLOUDCOVER(openv, high, low, close, penetration=0)
        if i4[-1] != 0:
            str1+="-"+"DARKCLOUDCOVER"
        i5 = CDLENGULFING(openv, high, low, close)
        if i5[-1] != 0:
            str1+="-"+"ENGULFING"
        i6 = CDLHARAMI(openv, high, low, close)
        if i6[-1] != 0:
            str1+="-"+"HARAMI"
        i7 = CDLHARAMICROSS(openv, high, low, close)
        if i7[-1] != 0:
            str1+="-"+"HARAMICROSS"
        i8 = CDLHOMINGPIGEON(openv, high, low, close)
        if i8[-1] != 0:
            str1+="-"+"HOMINGPIGEON"
        i9 = CDLINNECK(openv, high, low, close)
        if i9[-1] != 0:
            str1+="-"+"KICKING"
        i10 = CDLKICKING(openv, high, low, close)
        if i10[-1] != 0:
            str1+="-"+"KICKING"
        i11 = CDLKICKINGBYLENGTH(openv, high, low, close)
        if i11[-1] != 0:
            str1+="-"+"KICKINGBYLENGTH" 
        i12 = CDLMATCHINGLOW(openv, high, low, close)
        if i12[-1] != 0:
            str1+="-"+"MATCHINGLOW"
        i13 = CDLONNECK(openv, high, low, close)
        if i13[-1] != 0:
            str1+="-"+"ONNECK"
        i14 = CDLPIERCING(openv, high, low, close)
        if i14[-1] != 0:
            str1+="-"+"PIERCING"
        i15 = CDLSEPARATINGLINES(openv, high, low, close)
        if i15[-1] != 0:
            str1+="-"+"SEPARATINGLINES"
        i16 = CDLTHRUSTING(openv, high, low, close)
        if i16[-1] != 0:
            str1+="-"+"THRUSTING"
        i17 = CDLUPSIDEGAP2CROWS(openv, high, low, close)
        if i17[-1] != 0:
            str1+="-"+"UPSIDEGAP2CROWS" 
        """
        result=(i1[-1],i2[-1],i3[-1],i4[-1],i5[-1],i6[-1],i7[-1],i8[-1],i9[-1],i10[-1],i11[-1],i12[-1],i13[-1],i14[-1],i15[-1],i16[-1],i17[-1])
        print(ticker," ",result)
        """
        return(stochk[-1], str1)
    
    def ScanFor3CSPatternsTALib(self, ticker, time_frame):
        period = 20
        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        str1=""
        i1 = CDL3BLACKCROWS(openv, high, low, close)
        if i1[-1] != 0:
            str1+="-"+"3BLACKCROWS"
        i2 = CDL3INSIDE(openv, high, low, close)
        if i2[-1] != 0:
            str1+="-"+"3INSIDE"
        i3 = CDL3OUTSIDE(openv, high, low, close)
        if i3[-1] != 0:
            str1+="-"+"3OUTSIDE"
        i4 = CDL3STARSINSOUTH(openv, high, low, close)
        if i4[-1] != 0:
            str1+="-"+"3STARSINSOUTH"
        i5 = CDL3WHITESOLDIERS(openv, high, low, close)
        if i5[-1] != 0:
            str1+="-"+"3WHITESOLDIERS"
        i6 = CDLABANDONEDBABY(openv, high, low, close, penetration=0)
        if i6[-1] != 0:
            str1+="-"+"ABANDONEDBABY"
        i7 = CDLADVANCEBLOCK(openv, high, low, close)
        if i7[-1] != 0:
            str1+="-"+"ADVANCEBLOCK"
        i8 = CDLEVENINGDOJISTAR(openv, high, low, close, penetration=0)
        if i8[-1] != 0 :
            str1+="-"+"EVENINGDOJISTAR"
        i9 = CDLEVENINGSTAR(openv, high, low, close, penetration=0)
        if i9[-1] != 0:
            str1+="-"+"EVENINGSTAR"
        i10 = CDLGAPSIDESIDEWHITE(openv, high, low, close)
        if i10[-1] != 0:
            str1+="-"+"GAPSIDESIDEWHITE"
        i11 = CDLIDENTICAL3CROWS(openv, high, low, close)
        if i11[-1] != 0:
            str1+="-"+"IDENTICAL3CROWS"
        i12 = CDLMORNINGDOJISTAR(openv, high, low, close, penetration=0)
        if i12[-1] != 0:
            str1+="-"+"MORNINGDOJISTAR"
        i13 = CDLMORNINGSTAR(openv, high, low, close, penetration=0)
        if i13[-1] != 0:
            str1+="-"+"MORNINGSTAR"
        i14 = CDLSTICKSANDWICH(openv, high, low, close)
        if i14[-1] != 0:
            str1+="-"+"STICKSANDWICH"
        i15 = CDLTASUKIGAP(openv, high, low, close)
        if i15[-1] != 0:
            str1+="-"+"TASUKIGAP"
        i16 = CDLTRISTAR(openv, high, low, close)
        if i16[-1] != 0:
            str1+="-"+"TRISTAR"
        i17 = CDLUNIQUE3RIVER(openv, high, low, close)
        if i17[-1] != 0:
            str1+="-"+"UNIQUE3RIVER"
        i18 = CDLXSIDEGAP3METHODS(openv, high, low, close)
        if i18[-1] != 0:
            str1+="-"+"XSIDEGAP3METHODS"
        """
        result=(i1[-1],i2[-1],i3[-1],i4[-1],i5[-1],i6[-1],i7[-1],i8[-1],i9[-1],i10[-1],i11[-1],i12[-1],i13[-1],i14[-1],i15[-1],i16[-1],i17[-1], i18[-1])
        print(ticker," ",result)
        """
        return(stochk[-1], str1)

    def ScanFor4CSPatternsTALib(self, ticker, time_frame):
        period = 20
        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        str1=""
        i1 = CDL3LINESTRIKE(openv, high, low, close)
        if i1[-1] != 0:
            str1+="-"+"3LINESTRIKE"
        i2 = CDLBREAKAWAY(openv, high, low, close)
        if i2[-1] != 0:
            str1+="-"+"BREAKAWAY"        
        i3 = CDLCONCEALBABYSWALL(openv, high, low, close)
        if i3[-1] != 0:
            str1+="-"+"CONCEALBABYSWALL"
        i4 = CDLHIKKAKE(openv, high, low, close)
        """
        if i4[-1] != 0:
            str1+="-"+"HIKKAKE"
        i5 = CDLHIKKAKEMOD(openv, high, low, close)
        if i5[-1] != 0:
            str1+="-"+"HIKKAKEMOD"
        """
        i6 = CDLLADDERBOTTOM(openv, high, low, close)
        if i6[-1] != 0:
            str1+="-"+"LADDERBOTTOM"
        i7 = CDLMATHOLD(openv, high, low, close, penetration=0)
        if i7[-1] != 0:
            str1+="-"+"MATHOLD"
        i8 = CDLRISEFALL3METHODS(openv, high, low, close)
        if i8[-1] != 0:
            str1+="-"+"RISEFALL3METHODS"
        i9 = CDLSTALLEDPATTERN(openv, high, low, close)
        if i9[-1] != 0:
            str1+="-"+"STALLEDPATTERN"
        """
        result=(i1[-1],i2[-1],i3[-1],i4[-1],i5[-1],i6[-1],i7[-1],i8[-1],i9[-1])
        print(ticker," ",result)
        """
        return(stochk[-1], str1)



    def ScanFor1CSPatterns(self, ticker, time_frame):
        period = 20
        doji_body_factor=0.003

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        stoch=STOCH(high, low, close, 12, 3, 0, 3, 0)
        #print(stoch[0][19])
        candle_type=self.ScanForDojisHammers(openv[19], close[19], high[19], low[19], doji_body_factor)
        return (candle_type, stoch[0][19])

    def ScanFor2CSPatterns(self, ticker, time_frame):
        period = 20
        doji_body_factor=0.003

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        stoch=STOCH(high, low, close, 12, 3, 0, 3, 0)
        #print(stoch[0][19])
        s=slice(18,20,1)
        candle_type=self.ScanFor2C(openv[s], close[s], high[s], low[s], doji_body_factor)
        return (candle_type, stoch[0][19])

    def ScanForRSI2Plays(self, ticker, time_frame, method=1):
        period = 400

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        sma200=SMA(close, 200)
        ema50=EMA(close,50)
        ema89=EMA(close,89)
        ema16=EMA(close,16)
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        rsi2=RSI(close,2)
        if method==2:
            if (close[-1] < sma200[-1] and rsi2[-1] > 80 and rsi2[-2] > rsi2[-1] and stochk[-1] > 80): 
                return (sma200[-1], rsi2[-1], "puts")
            elif (close[-1] >  sma200[-1] and rsi2[-1] <  20 and rsi2[-2] < rsi2[-1] and stochk[-1] < 20):
                return (sma200[-1], rsi2[-1], "calls")
            else:
                return (sma200[-1], rsi2[-1], "skip")
        elif method==1:
            if (close[-1] < sma200[-1] and rsi2[-2] > 95 and rsi2[-1] < 95 and rsi2[-1] > 90 and stochk[-1] > 80): 
                return (sma200[-1], rsi2[-1], "puts")
            elif (close[-1] >  sma200[-1] and rsi2[-2] <  5 and rsi2[-1] > 5 and rsi2[-1] < 10 and stochk[-1] < 20):
                return (sma200[-1], rsi2[-1], "calls")
            else:
                return (sma200[-1], rsi2[-1], "skip")
        elif method==3:
            if (close[-1] < ema16[-1] and ema16[-1] < ema50[-1] and ema50[-1] < ema89[-1] and rsi2[-1] > 90): 
                return (sma200[-1], rsi2[-1], "puts")
            elif (close[-1] > ema16[-1] and ema16[-1] > ema50[-1] and ema50[-1] > ema89[-1] and rsi2[-1] < 10):
                return (sma200[-1], rsi2[-1], "calls")
            else:
                return (sma200[-1], rsi2[-1], "skip")

        #print("sma200=",sma200[-1]," rsi2=",rsi2[-1], "close=", close[-1])


    def ScanForHGPlays(self, ticker, time_frame):
        period = 100

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        sma20=SMA(close, 20)
        if (low[-1] < sma20[-1] and close[-1] > sma20[-1] and close[-1] > openv[-1] and (low[-2] > sma20[-2]) and (low[-2] > sma20[-2])):
            return (sma20[-1], "calls")
        elif (high[-1] > sma20[-1] and close[-1] < sma20[-1] and close[-1] < openv[-1] and high[-2] < sma20[-2]):
            return (sma20[-1], "puts")
        else:
            return (sma20[-1], "skip")

    def ScanForMHPlays(self, ticker, time_frame):
        period = 400

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        #print("stochk=",stochk[-1], " stochd=",stockd[-1])
        rsi=RSI(close)
        macd, macdsignal, macdhist = MACD(close)
        #print("macd=",macd[-1]," macdsignal=",macdsignal[-1])
        sma20=SMA(close, 20)
        sma50=SMA(close, 50)
        sma200=SMA(close, 200)
        ema8=EMA(close,8)
        if ((stochk[-1] > 50 and stochk[-2] < stochk[-1]) and stochk[-1] < 80 and (rsi[-1] > 50 and rsi[-2] < rsi[-1]) and (macd[-1] > macdsignal[-1])and close[-1] > sma200[-1] and close[-1] > sma50[-1] and close[-1] > sma20[-1] and sma20[-1] > sma50[-1] and sma50[-1] > sma200[-1]and close[-1] > ema8[-1]):
            return (stochk[-1], rsi[-1], "calls")
        elif ((stochk[-1] < 50 and stochk[-2] > stochk[-1]) and stochk[-1] > 20 and (rsi[-1] < 50 and rsi[-2] > rsi[-1]) and (macd[-1] < macdsignal[-1])and close[-1] < sma200[-1] and close[-1] < sma50[-1] and close[-1] < sma20[-1] and sma20[-1] > sma50[-1] and sma50[-1] > sma200[-1] and close[-1] < ema8[-1]):
            return (stochk[-1], rsi[-1], "puts")
        else:
            return (stochk[-1], rsi[-1], "skip")
        

    def ScanForCrossPlays(self, ticker, time_frame):
        period = 400

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        sma20=SMA(close, 20)
        sma10=SMA(close, 10)
        sma50=SMA(close, 50)
        sma200=SMA(close, 200)
        ema8=EMA(close,8)
        ema20=EMA(close,20)
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        if(close[-1] > sma50[-1] and sma50[-1] > sma200[-1] and ema8[-1] > ema20[-1] and ema8[-2] > ema20[-2] and ema8[-3] < ema20[-3]):
            return (stochk[-1], "calls")
        elif(close[-1] < sma50[-1] and sma50[-1] < sma200[-1] and ema8[-1] < ema20[-1] and ema8[-2] < ema20[-2] and ema8[-3] > ema20[-3]):
            return (stochk[-1], "puts")
        else:
            return (stochk[-1], "skip")


    def ScanForVolPlays(self, ticker, time_frame):
        period = 400

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        volume=df['Volume'].values
        
        sma20=SMA(close, 20)
        sma10=SMA(close, 10)
        sma50=SMA(close, 50)
        sma200=SMA(close, 200)
        ema8=EMA(close,8)
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        osc=ADOSC(high, low, close, volume.astype(float),6,20)
        if(close[-1] > sma20[-1] and sma20[-1] > sma50[-1] and sma50[-1] > sma200[-1] and osc[-1] > 0 and osc[-2] > 0 and close[-1] > ema8[-1]):
            return (stochk[-1], "calls")
        elif(close[-1] < sma20[-1] and sma20[-1] < sma50[-1] and  sma50[-1] < sma200[-1] and osc[-1] < 0 and osc[-2] < 0 and close[-1] < ema8[-1]):
            return (stochk[-1], "puts")
        else:
            return (stochk[-1], "skip")

    def ScanForMMPlays(self, ticker, time_frame, method):
        period = 6

        ticker1=ticker.replace("-","_")
        table_name=ticker1.lower()+"_values"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        volume=df['Volume'].values
        wopen=openv[-6]
        wclose=close[-2]
        wlow=np.min(low[0:5])
        whigh=np.max(high[0:5])
        if method == 1:
            if(low[-1] > whigh):
                return("calls","gap up")
            elif(high[-1] < wlow):
                return("puts", "gap down")
            else:
                return("skip","inside")
        elif method == 2:
            if(openv[-1] > whigh):
                return("calls","gap up")
            elif(openv[-1] < wlow):
                return("puts", "gap down")
            else:
                return("skip","inside")
        

        
        

    def ScanForDojiPlays(self, ticker, time_frame, method):
        period = 400

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        volume=df['Volume'].values
        
        sma20=SMA(close, 20)
        sma10=SMA(close, 10)
        sma50=SMA(close, 50)
        sma200=SMA(close, 200)
        ema8=EMA(close,8)
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        if method == 1:
            if (close[-1] > sma50[-1] and sma50[-1] > sma200[-1] and abs(close[-1] - openv[-1]) < 0.2 * abs(high[-1] - low[-1]) and abs(close[-2] - openv[-2]) < 0.2 * abs(high[-2] - low[-2]) and volume[-1] > volume[-2]):
                return (stochk[-1], "calls")
            elif (close[-1] < sma50[-1] and sma50[-1] < sma200[-1] and abs(close[-1] - openv[-1]) < 0.2 * abs(high[-1] - low[-1]) and abs(close[-2] - openv[-2]) < 0.2 * abs(high[-2] - low[-2]) and volume[-1] > volume[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 2:
            if ((close[-1] - openv[-1]) < 0.2 * (high[-1] - low[-1]) and (close[-2] - openv[-2]) < 0.2 * (high[-2] - low[-2]) and low[-1] > low[-2] and high[-1] > high[-2] and close[-1] > close[-2]  and close[-1] > ema8[-1]):
                return (stochk[-1], "calls")
            elif ((openv[-1] - close[-1]) < 0.2 * (high[-1] - low[-1]) and (openv[-2] - close[-2]) < 0.2 * (high[-2] - low[-2]) and low[-1] < low[-2] and high[-1] < high[-2] and close[-1] < close[-2]  and close[-1] < ema8[-1]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 3:
            if (low[-1] > low[-2] and high[-1] > high[-2] and close[-1] > close[-2] and close[-1] > ema8[-1] and (high[-1] -close[-1]) < 0.15*(high[-1]-low[-1]) and volume[-1] > volume[-2]):
                return (stochk[-1], "calls")
            elif (low[-1] < low[-2] and high[-1] < high[-2] and close[-1] < close[-2] and close[-1] < ema8[-1] and (close[-1] - low[-1]) < 0.15* (high[-1]-low[-1]) and volume[-1] > volume[-2]  ) :
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 4:
            if ( abs(openv[-2] -close[-2]) < 0.1 * abs(high[-2] - low[-2]) and high[-1] < high[-2] and close[-1] < ema8[-1] and close[-2] > ema8[-2] ):
                return (stochk[-1], "puts")
            elif ( abs(openv[-2] -close[-2]) < 0.1 * abs(high[-2] - low[-2]) and low[-1] > low [-2] and close[-1] > ema8[-1] and close[-2]  < ema8[-2]):
                return (stochk[-1], "calls")
            else:
                return (stochk[-1], "skip")
        
        
            

    def MFI(self, close, high,low,volume, period=14):
        size=close.shape[0]
        typ_val=(close+high+low)/3
        money_flow=typ_val*volume
        mfi=[]
        for i in range(size):
            pos_flow=0
            neg_flow=0
            money_ratio=0
            if i <  period:
                mfi.append(-1)
            else:
                for j in range(period):
                    if typ_val[i-j] <= typ_val[i-j-1]:
                        neg_flow+=money_flow[i-j]
                    else:
                        pos_flow+=money_flow[i-j]
                if neg_flow > 0:
                    money_ratio= pos_flow / neg_flow
                else: 
                    money_ratio=pos_flow
                mfi.append(100-(100/(1+money_ratio)))
        return np.array(mfi)


        
    def ScanForMFIPlays(self, ticker, time_frame, method=1):
        period = 400

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        volume=df['Volume'].values
        mfi=self.MFI(close, high, low,volume)
        sma50=SMA(close, 50)
        sma200=SMA(close, 200)
        ema8=EMA(close,8)
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        if method==1:
            if(close[-1] < sma50[-1] and sma50[-1] < sma200[-1] and mfi[-1] > 95):
                return (mfi[-1], stochk[-1], "puts")
            elif(close[-1] > sma50[-1] and sma50[-1] > sma200[-1] and mfi[-1] < 9):
                return (mfi[-1], stochk[-1], "calls")
            else:
                return (mfi[-1], stochk[-1], "skip")
        elif method == 2:
            if (close[-1] > ema8[-1] and mfi[-1] > 40 and mfi[-1] < 50 and mfi[-2] < mfi[-1] and close[-1] > sma50[-1] and sma50[-1] > sma200[-1]):
                return (mfi[-1], stochk[-1], "calls")
            elif (close[-1] < ema8[-1] and mfi[-1] < 70 and mfi[-1] > 60 and mfi[-2] > mfi[-1] and close[-1] < sma50[-1] and sma50[-1] < sma200[-1]):
                return (mfi[-1], stochk[-1], "puts")
            else:
                return (mfi[-1], stochk[-1], "skip")


    def ScanForKeltPlays(self, ticker, time_frame, method=1):
        period = 400

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        volume=df['Volume'].values
        ema20=EMA(close,20)
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        atr=ATR(high,low,close)
        
        if method==1:
            if(close[-1] <= ema20[-1] - (2.5 * atr[-1])):
                return (stochk[-1], "calls")
            elif(close[-1] >= ema20[-1] + (2.5* atr[-1])):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 2:
            lowerkelt_1=ema20[-1] - (2.5 * atr[-1])
            upperkelt_1=ema20[-1] + (2.5 * atr[-1])
            lowerkelt_2=ema20[-2] - (2.5 * atr[-2])
            upperkelt_2=ema20[-2] + (2.5 * atr[-2])
            if(close[-2] <= lowerkelt_2 and close[-1] >= lowerkelt_1 and close[-1] < ema20[-1]):
                return (stochk[-1], "calls")
            elif(close[-2] >= upperkelt_2 and close[-1] <= upperkelt_1 and close[-1] > ema20[-1]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 3:
            lowerkelt_1=ema20[-1] - (2.5 * atr[-1])
            upperkelt_1=ema20[-1] + (2.5 * atr[-1])
            lowerkelt_2=ema20[-2] - (2.5 * atr[-2])
            upperkelt_2=ema20[-2] + (2.5 * atr[-2])
            if(close[-2] <= ema20[-2] and close[-2] > lowerkelt_2 and close[-1] >= ema20[-1] and close[-1] < upperkelt_1):
                return (stochk[-1], "calls")
            elif(close[-2] >= ema20[-2] and close[-2] < upperkelt_2 and close[-1] <= ema20[-1] and close[-1] > lowerkelt_1):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        

    def consecutive_close(self,direction, close, number):
        result=True
        if direction == "up":
            for i in range(-1, -(number+1), -1):
                if close[i] <= close[i-1]:
                    result = False
                    break
        elif direction == "down":
            for i in range(-1, -(number+1), -1):
                if close[i] >= close[i-1]:
                    result = False
                    break
        return result

    def wicks(self, dir, high, low, openv, close, percent):
        if dir == "up":
            if (close  > openv):
                if (openv -low >= percent * (high -low)):
                    return True
                else:
                    return False
            else:
                if (close -low >= percent * (high -low)):
                    return True
                else:
                    return False
        elif dir == "down":
            if (close  > openv):
                if (high -close >= percent * (high -low)):
                    return True
                else:
                    return False
            else:
                if (high -openv >= percent * (high -low)):
                    return True
                else:
                    return False


    def is_engulfed_body(self, bigo, bigc,po,pc,dir):
        if dir=="up":
            if pc > po:
                if pc < bigc and po > bigo:
                    return True
                else:
                    return False
            else:
                if po < bigc and pc > bigo:
                    return True
                else:
                    return False
        elif dir == "down":
            if pc > po:
                if pc < bigo and po > bigc:
                    return True
                else:
                    return False
            else:
                if po < bigo and pc > bigc:
                    return True
                else:
                    return False
           
    def is_engulfed_range(self, bigh, bigl,ph,pl):
            if ph < bigh and pl > bigl:
                return True
            else:
                return False
            


    def ScanForStrategies(self, ticker, time_frame, atr, rv, method=1):
        period = 52
        #print(ticker)
        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        volume=df['Volume'].values
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        if close.shape[0] < 52 :
            return (stochk[-1], "skip")
        

        if method == 1:    
            rsi=RSI(close,8)
            
            maxElement=np.amax(high[0:50])
            resultMaxIndex=np.where(high[0:50] == np.amax(high[0:50]))
            minElement=np.amin(low[0:50])
            resultMinIndex=np.where(low[0:50] == np.amin(low[0:50]))

            if high[50] > maxElement and rsi[resultMaxIndex[0][0]] > 80 and rsi[50] < rsi[resultMaxIndex[0][0]] and high[51] < high[50]:
                return (stochk[-1], "puts")
            elif low[50] < minElement and rsi[resultMinIndex[0][0]] < 20 and rsi[50] >  rsi[resultMinIndex[0][0]] and low[51] > low[50]:
                return (stochk[-1], "calls")
            else:
                return (stochk[-1], "skip")
        elif method ==2 :
            maxElement=np.amax(high[7:47])
            minElement=np.amin(low[7:47])
            if high[46] == maxElement and close[47] < close[46] and close[48] < close[47] and close[49] < close[48] and close[50] > close[49] and close[50] > (low[50] + 0.8 * (high[50]-low[50])) :
                return (stochk[-1], "calls")
            elif low[46] == minElement and close[47] > close[46] and close[48] > close[47] and close[49] > close[48] and close[50] < close[49] and close[50] < (low[50] + 0.2 * (high[50]-low[50])) :
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method ==3 :
            if close[47] < close[46] and close[48] < close[47] and close[49] < close[48] and low[50] > high[49]:
                return (stochk[-1], "calls")
            elif close[47] > close[46] and close[48] > close[47] and close[49] > close[48] and high[50] < low[49] :
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 4:
            rsi=RSI(close,14)
            sma20=SMA(close, 20)

            macd, macdsignal, macdhist = MACD(close)
            if float(rv) > 1 and rsi[-1] > 50 and rsi[-1] < 80 and rsi[-1] > rsi[-2] and stochk[-1] > 50 and stochk[-1] < 80 and stochk[-1] > stochk[-2] and macd[-1] > macdsignal[-1] and macd[-1] > 0 and macd[-1] > macd[-2] and sma20[-1] > sma20[-2] and close[-1] > sma20[-1]:
                return (stochk[-1], "calls")
            elif float(rv) > 1  and rsi[-1] < 50 and rsi[-1] > 20 and rsi[-1] < rsi[-2] and stochk[-1] < 50 and stochk[-1] > 20 and stochk[-1] < stochk[-2] and macd[-1] < macdsignal[-1] and macd[-1] < 0 and macd[-1] < macd[-2] and sma20[-1] < sma20[-2] and close[-1] < sma20[-1]:
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 5: 
            if (close[-1] > openv[-1] and (high[-1] -close[-1]) <= 0.1 *(high[-1]-low[-1]) and float(rv) > 1 and (high[-1] - low[-1]) > float(atr)):
                return (stochk[-1], "calls")
            elif (close[-1] < openv[-1] and (close[-1] -low[-1]) <= 0.1 *(high[-1]-low[-1]) and float(rv) > 1 and (high[-1] - low[-1]) > float(atr)):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 6:
            sma21=SMA(close, 21)
            macd, macdsignal, macdhist = MACD(close)
            HAC_0=0.25*(openv[-1] + close[-1] + high[-1] + low[-1])
            HAC_1=0.25*(openv[-2] + close[-2] + high[-2] + low[-2])
            HAO_0=0.5*(openv[-2] + close[-2])
            HAO_1=0.5*(openv[-3] + close[-3])
            HAH_0=max(high[-1], HAO_0, HAC_0)
            HAH_1=max(high[-2], HAO_1, HAC_1)
            HAL_0=min(low[-1], HAO_0, HAC_0)
            HAL_1=min(low[-2], HAO_1, HAC_1)

            if close[-1] > sma21[-1] and macd[-1] > macdsignal[-1] and sma21[-1] > sma21[-2] and macd[-1] > macd[-2] and HAC_1 < HAO_1 and HAC_0 > HAO_0 and HAL_0 == HAO_0 and HAL_0 < sma21[-1] and HAH_0 > sma21[-1]:
                return (stochk[-1], "calls")
            elif close[-1] < sma21[-1] and macd[-1] < macdsignal[-1] and sma21[-1] < sma21[-2] and macd[-1] < macd[-2] and HAC_1 > HAO_1 and HAC_0 < HAO_0 and HAH_0 == HAO_0 and HAL_0 < sma21[-1] and HAH_0 > sma21[-1]:
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 7:
            sma21=SMA(close, 21)
            macd, macdsignal, macdhist = MACD(close)
            HAC_0=0.25*(openv[-1] + close[-1] + high[-1] + low[-1])
            HAC_1=0.25*(openv[-2] + close[-2] + high[-2] + low[-2])
            HAO_0=0.5*(openv[-2] + close[-2])
            HAO_1=0.5*(openv[-3] + close[-3])
            HAH_0=max(high[-1], HAO_0, HAC_0)
            HAH_1=max(high[-2], HAO_1, HAC_1)
            HAL_0=min(low[-1], HAO_0, HAC_0)
            HAL_1=min(low[-2], HAO_1, HAC_1)

            if macd[-1] > macdsignal[-1]  and macd[-1] > macd[-2] and HAL_0 < sma21[-1] and HAH_0 > sma21[-1] and HAL_0 == HAO_0:
                return (stochk[-1], "calls")
            elif macd[-1] < macdsignal[-1] and  macd[-1] < macd[-2]  and HAL_0 < sma21[-1] and HAH_0 > sma21[-1] and HAH_0 == HAO_0:
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 8:
            maxElement=np.amax(high[0:50])
            minElement=np.amin(low[0:50])
            if (high[-1] > 0.97*maxElement and high[-1] < maxElement and float(rv) > 1):
                return (stochk[-1], "calls")
            elif (low[-1] > minElement and low[-1] < minElement + 0.03*minElement and float(rv) > 1):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 9:
            ema6=EMA(close,6)
            ema8=EMA(close,8)
            ema10=EMA(close,10)
            ema12=EMA(close,12)
            sma20=SMA(close,20)
            if ema6[-1] > ema8[-1] and ema8[-1] > ema10[-1] and ema10[-1] > ema12[-1] and ema6[-1] > sma20[-1] and ema6[-2] < sma20[-2]:
                return (stochk[-1], "calls")
            elif ema6[-1] < ema8[-1] and ema8[-1] < ema10[-1] and ema10[-1] < ema12[-1] and ema6[-1] < sma20[-1] and ema6[-2] > sma20[-2]:
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 10:
            ema6=EMA(close,6)
            ema8=EMA(close,8)
            ema10=EMA(close,10)
            ema12=EMA(close,12)
            sma20=SMA(close,20)
            if ema6[-1] > ema8[-1] and ema8[-1] > ema10[-1] and ema10[-1] > ema12[-1] and ema6[-1] < sma20[-1]:
                return (stochk[-1], "calls")
            elif ema6[-1] < ema8[-1] and ema8[-1] < ema10[-1] and ema10[-1] < ema12[-1] and ema6[-1] > sma20[-1]:
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 11:
            wavgvol=np.average(volume[-21:])
            for i in range(1,16):
                if volume[-i] > 4 * wavgvol:
                    if close[-i] > openv[-i]:
                        return (stochk[-1], "calls")
                    else:
                        return (stochk[-1], "puts")
            return (stochk[-1], "skip")
  










        

    def ScanForSwingPlays(self, ticker, time_frame, method=1):
        period = 400

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        volume=df['Volume'].values
        sma50=SMA(close, 50)
        sma200=SMA(close, 200)
        ema8=EMA(close,8)
        ema6=EMA(close,6)
        ema12=EMA(close,12)
        ema9=EMA(close,9)
        ema20=EMA(close, 20)
        ema3=EMA(close,3)
        sma20=SMA(close,20)

        whigh=np.max(high[-10:])
        wavgvol=np.average(volume[-10:])
        atr=ATR(high,low,close)
        adx=ADX(high, low, close)
        mdi=MINUS_DI(high,low,close)
        pdi=PLUS_DI(high,low,close)
        ema6=EMA(close,6)
        ema12=EMA(close,12)
        ub, mb, lb=BBANDS(close, 20, 2, 2, 0)
        bbdis=np.min(np.subtract(ub[-120:], lb[-120:]))

        wlow=np.min(low[0:10])
        rsi=RSI(close,14)
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        if method==1:
            if(close[-1] > sma50[-1] and sma50[-1] > sma200[-1] and (close[-2] < ema20[-2] or close[-3] < ema20[-3] or close [-4] < ema20[-4]) and close[-1] > ema20[-1] and close[-1] > high[-2]):
                return (stochk[-1], "calls")
            elif(close[-1] < sma50[-1] and sma50[-1] < sma200[-1] and (close[-2] > ema20[-2] or close[-3] > ema20[-3] or close [-4] > ema20[-4]) and close[-1] < ema20[-1] and close[-1] < low[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 2:
            if(close[-1] > sma50[-1] and sma50[-1] > sma200[-1] and (close[-2] < ema20[-2] or close[-3] < ema20[-3] or close [-4] < ema20[-4]) and close[-1] > ema8[-1] and close[-1] > high[-2]):
                return (stochk[-1], "calls")
            elif(close[-1] < sma50[-1] and sma50[-1] < sma200[-1] and (close[-2] > ema20[-2] or close[-3] > ema20[-3] or close [-4] > ema20[-4]) and close[-1] < ema8[-1] and close[-1] < low[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")  
        elif method == 3:
            if(close[-1] > sma50[-1] and sma50[-1] > sma200[-1] and close[-1] > whigh):
                return (stochk[-1], "calls")
            elif(close[-1] < sma50[-1] and sma50[-1] < sma200[-1] and close[-1] < wlow):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")  
        elif method == 4:
            if(close[-1] > sma50[-1] and sma50[-1] > sma200[-1] and rsi[-1] > 50 and volume[-1]> wavgvol):
                return (stochk[-1], "calls")
            elif(close[-1] < sma50[-1] and sma50[-1] < sma200[-1] and rsi[-1] < 50 and volume[-1] > wavgvol):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip") 
        elif method == 5:
            if(close[-1] > sma50[-1] and sma50[-1] > sma200[-1] and rsi[-1] > 50 and volume[-1]> wavgvol and close[-1] > whigh):
                return (stochk[-1], "calls")
            elif(close[-1] < sma50[-1] and sma50[-1] < sma200[-1] and rsi[-1] < 50 and volume[-1] > wavgvol and close[-1] < wlow):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip") 
        elif method == 6:
            if (wavgvol > 2000000 and atr[-1] >= 1 and close[-1] > sma20[-1] and close[-1] > close[-2] and close[-1] > openv[-1] and high[-1] > high[-2] and low[-1] > low[-2]):
                return (stochk[-1], "calls")  
            elif  (wavgvol > 2000000 and atr[-1] >= 1 and close[-1] < sma20[-1] and close[-1] < close[-2] and close[-1] < openv[-1] and high[-1] < high[-2] and low[-1] < low[-2]):
                return (stochk[-1], "puts")   
            else:
                return (stochk[-1], "skip") 
        elif method == 7:
            if (adx[-1] < 25 and adx[-1] > mdi[-1] and mdi[-1] < mdi[-2]):
                return (stochk[-1], "calls") 
            elif (adx[-1] < 25 and adx[-1] > pdi[-1] and pdi[-1] < pdi[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip") 
        elif method == 8:
            if (adx[-1] < 25 and adx[-1] > mdi[-1] and mdi[-1] < mdi[-2] and pdi[-1] > mdi[-1]):
                return (stochk[-1], "calls") 
            elif (adx[-1] < 25 and adx[-1] > pdi[-1] and pdi[-1] < pdi[-2] and pdi[-1] < mdi[-1]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip") 
        elif method == 9:
            if (adx[-1] > 14 and adx[-1] < 25 and pdi[-1] > mdi[-1] and pdi[-2] > mdi[-2]):
                return (stochk[-1], "calls") 
            elif (adx[-1] > 14 and adx[-1] < 25 and pdi[-1] < mdi[-1] and pdi[-2] < mdi[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip") 
        elif method == 10:
            if(close[-1] > sma20[-1] and sma20[-1] > sma50[-1] and sma50[-1] > sma200[-1] and ema9[-1] > ema20[-1] and volume[-1] > wavgvol and volume[-1] > volume[-2] and high[-1] > high[-2] and low[-1] > low[-2] and close[-1] > close[-2]):
                return (stochk[-1], "calls") 
            elif (close[-1] < sma20[-1] and sma20[-1] < sma50[-1] and sma50[-1] < sma200[-1] and ema9[-1] < ema20[-1] and volume[-1] > wavgvol and volume[-1] > volume[-2] and high[-1] < high[-2] and low[-1] < low[-2] and close[-1] < close[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")  
        elif method == 11:
            if (ema3[-1] > ema8[-1] and ema8[-1] > ema20[-1] and ema20[-1] > sma50[-1] and close[-1] > ema3[-1] and ema3[-1] - sma50[-1] <  0.01 * close[-1]):
                return (stochk[-1], "calls")
            elif (ema3[-1] < ema8[-1] and ema8[-1] < ema20[-1] and ema20[-1] < sma50[-1] and close[-1] < ema3[-1] and abs(ema3[-1] - sma50[-1]) <  0.01 * close[-1]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")  
        elif method == 12:
            if (close[-1] > ema8[-1] and ema8[-1] > sma20[-1] and sma20[-1] > sma50[-1] and sma50[-1] > sma200[-1] and close[-1] > openv[-1] and (high[-1] -close[-1]) < 0.05 *(high[-1] - low[-1])and stochk[-1] < 70):
                return (stochk[-1], "calls")
            elif (close[-1] < ema8[-1] and ema8[-1] < sma20[-1] and sma20[-1] < sma50[-1] and sma50[-1] < sma200[-1] and close[-1] < openv[-1] and abs(low[-1] -close[-1]) < 0.05 *(high[-1] - low[-1]) and stochk[-1] > 30):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 13:
            if (close[-1] > sma200[-1] and stochk[-1] <= 10):
                return (stochk[-1], "calls")
            elif(close[-1] < sma200[-1] and stochk[-1] >= 90):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 14:
            if (abs(high[-2] - low[-2]) >= 0.06 * close[-2] and (high[-2] -close[-2]) >= 0.75* (high[-2]-low[-2]) and low[-1] > low[-2] and high[-1] < high[-2] and (close[-1] -low[-1]) <= 0.25 * (high[-1] -low[-1])):
                return (stochk[-1], "calls")
            elif(abs(high[-2] - low[-2]) >= 0.06 * close[-2] and (close[-2] -low[-2]) <= 0.25*(high[-2] -low[-2])  and low[-1] > low[-2] and high[-1] < high[-2] and (high[-1] - close[-1]) <= 0.25 * (high[-1] -low[-1])):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 15:  #Mark's scan
#            if (self.consecutive_close("down", close, 8) or  self.consecutive_close("down", close, 6) or self.consecutive_close("down", close, 5) or self.consecutive_close("down", close, 3)):
            if (self.consecutive_close("down", close, 8) or  self.consecutive_close("down", close, 6)):
        
                return (stochk[-1], "calls")
#            elif(self.consecutive_close("up", close, 8) or  self.consecutive_close("up", close, 6) or self.consecutive_close("up", close, 5) or self.consecutive_close("up", close, 3)):
            elif(self.consecutive_close("up", close, 8) or  self.consecutive_close("up", close, 6)):
    
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 16:   # Mark's scan
            if (close[-1] > ema12[-1] and close[-2] <= ema12[-2] and ema6[-1] > ema8[-1] and ema6[-2] <= ema8[-2]):
                return (stochk[-1], "calls")
            elif (close[-1] < ema12[-1] and close[-2] >= ema12[-2] and ema6[-1] < ema8[-1] and ema6[-2] >= ema8[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 17:
            if(close[-1] > sma50[-1] and close[-1] < close[-2] and close[-2] < close[-3] and close[-1] < openv[-1] and close[-2] < openv[-2] and close[-3] < openv[-3]):
                return (stochk[-1], "calls")
            elif (close[-1] < sma50[-1] and close[-1] > close[-2] and close[-2] > close[-3] and close[-1] > openv[-1] and close[-2] > openv[-2] and close[-3] > openv[-3]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 18:
            if (ema3[-1] > ema8[-1] and ema8[-1] > sma20[-1] and sma20[-1] > sma50[-1] and high[-2] > high[-1] and high[-2] > high[-3]):
                return (stochk[-1], "calls")
            elif (ema3[-1] < ema8[-1] and ema8[-1] < sma20[-1] and sma20[-1] < sma50[-1] and low[-2] < low[-1] and low[-2] < low[-3]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 19:
            if (close[-1] > sma20[-1] and sma20[-1] > sma50[-1] and sma50[-1] > sma200[-1] and stochk[-1] > 40 and stochk[-1] < 80 and stochk[-2] < stochk[-1] and adx[-1] >= 20 and adx[-1] <=30 and (high[-1] -close[-1]) <= 0.25*(high[-1] -low[-1])):
                return (stochk[-1], "calls")
            elif (close[-1] < sma20[-1] and sma20[-1] < sma50[-1] and sma50[-1] < sma200[-1] and stochk[-1] > 20 and stochk[-1] < 60 and stochk[-2] > stochk[-1] and adx[-1] >= 20 and adx[-1] <=30 and (close[-1] - low[-1]) <= 0.25*(high[-1] -low[-1])):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 20:
            if (low[-1] < low[-2] and close[-1] > close[-2] and volume[-1] > wavgvol):
                return (stochk[-1], "calls")
            elif (high[-1] > high[-2] and close[-1] < close[-2] and volume[-1] > wavgvol):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 21:
            if (close[-1] > sma50[-1] and low[-1] < lb[-1] and close[-1] > lb[-1] and close[-1] < ema20[-1] and openv[-1] > close[-1] and (close[-1] - low[-1]) > 0.25*(high[-1] -low[-1])):
                return (stochk[-1], "calls")
            elif (close[-1] < sma50[-1] and high[-1] > ub[-1] and close[-1] < ub[-1] and close[-1] > ema20[-1] and openv[-1] < close[-1] and (high[-1] - close[-1]) > 0.25*(high[-1] -low[-1])):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 22:
            if(ub[-1] - lb[-1] == bbdis):
                return (stochk[-1], "calls")
            else:
                return (stochk[-1], "skip")
        elif method == 23:
            if(sma200[-1] < sma50[-1] and sma50[-1] < sma20[-1] and ((close[-1] > sma50[-1] and close[-1] < sma20[-1]) or (low[-1] > sma50[-1] and low[-1] < sma20[-1]))):
                return (stochk[-1], "calls")
            elif (sma200[-1] > sma50[-1] and sma50[-1] > sma20[-1] and ((close[-1] < sma50[-1] and close[-1] > sma20[-1]) or (high[-1] < sma50[-1] and high[-1] > sma20[-1]))):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 24:
            if( ((close[-1] > sma20[-1] and close[-1] < ema8[-1]) or (low[-1] > sma20[-1] and low[-1] < ema8[-1]))):
                return (stochk[-1], "calls")
            elif (((close[-1] < sma20[-1] and close[-1] > ema8[-1]) or (high[-1] < sma20[-1] and high[-1] > ema8[-1]))):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 25:
            if self.wicks("up", high[-1], low[-1], openv[-1], close[-1], 0.5) and self.wicks("up", high[-2], low[-2], openv[-2], close[-2], 0.5)  and close[-1] > sma50[-1]:
                return (stochk[-1], "calls")
            elif self.wicks("down", high[-1], low[-1], openv[-1], close[-1], 0.5) and  self.wicks("down", high[-2], low[-2], openv[-2], close[-2], 0.5) and close[-1] < sma50[-1] :
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 26:
            if self.wicks("up", high[-1], low[-1], openv[-1], close[-1], 0.5) and close[-1] > sma50[-1] and close[-1] > ema8[-1] and low[-1] < ema8[-1]:
                return (stochk[-1], "calls")
            elif self.wicks("down", high[-1], low[-1], openv[-1], close[-1], 0.5) and close[-1] < sma50[-1] and close[-1] < ema8[-1] and high[-1] > ema8[-1]:
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 27:
            if (close[-1] > openv[-1] and self.is_engulfed_body(openv[-1], close[-1],openv[-2], close[-2], "up") and self.is_engulfed_body(openv[-1], close[-1],openv[-3], close[-3], "up") ):
                return (stochk[-1], "calls")
            elif (close[-1] < openv[-1] and self.is_engulfed_body(openv[-1], close[-1],openv[-2], close[-2], "down") and self.is_engulfed_body(openv[-1], close[-1],openv[-3], close[-3], "down") ):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 28:
            if (close[-1] > openv[-1] and self.is_engulfed_range(high[-1], low[-1],high[-2], low[-2]) and self.is_engulfed_range(high[-1], low[-1],high[-3], low[-3]) ):
                return (stochk[-1], "calls")
            elif (close[-1] < openv[-1] and self.is_engulfed_range(high[-1], low[-1],high[-2], low[-2]) and self.is_engulfed_range(high[-1], low[-1],high[-3], low[-3]) ):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 30:    #RM1
            if(ema6[-1] > ema8[-1] and ema6[-2] < ema8[-2]):
                return (stochk[-1], "calls")
            elif (ema6[-1] < ema8[-1] and ema6[-2] > ema8[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 31:     #RM2
            if(ema6[-1] > ema12[-1] and ema6[-2] < ema12[-2]):
                return (stochk[-1], "calls")
            elif (ema6[-1] < ema12[-1] and ema6[-2] > ema12[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 32:     #RM3
            if (ema6[-1] > ema12[-1] and low[-1] < ema12[-1] and high[-1] > ema6[-1] and close[-1] > openv[-1]):
                return (stochk[-1], "calls")
            elif (ema6[-1] < ema12[-1] and high[-1] > ema12[-1] and low[-1] < ema6[-1] and close[-1] < openv[-1]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 33:    #RM4
            if(ema6[-1] > ema8[-1] and ema6[-2] < ema8[-2] and low[-1] < ema12[-1] and close[-1] > ema12[-1]):
                return (stochk[-1], "calls")
            elif (ema6[-1] < ema8[-1] and ema6[-2] > ema8[-2] and high[-1] > ema12[-1] and close[-1] < ema12[-1]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 34:    #RM5
            if(close[-1] < close[-2] and close[-2] < close[-3] and close[-3] < close[-4] and close[-4] < close[-5] and close[-5] < close[-6]):
                return (stochk[-1], "calls")
            elif (close[-1] > close[-2] and close[-2] > close[-3] and close[-3] > close[-4] and close[-4] > close[-5] and close[-5] > close[-6]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 35:
            if close[-1] > sma20[-1] and low[-1] < sma20[-1] and sma20[-1] > sma200[-1] and sma20[-1] > sma20[-2] and sma20[-2] > sma20[-3] and sma20[-3] > sma20[-4]:
                return (stochk[-1], "calls")
            elif close[-1] < sma20[-1] and high[-1] > sma20[-1] and sma20[-1] < sma200[-1] and sma20[-1] < sma20[-2] and sma20[-2] < sma20[-3] and sma20[-3] < sma20[-4]:
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 36:
            if close[-1] > sma20[-1] and close[-1] < sma20[-1] + 0.01*sma20[-1] and sma20[-1] > sma200[-1] and sma20[-1] > sma20[-2] and sma20[-2] > sma20[-3] and sma20[-3] > sma20[-4]:
                return (stochk[-1], "calls")
            elif close[-1] < sma20[-1] and close[-1] > sma20[-1] - 0.01*sma20[-1] and sma20[-1] < sma200[-1] and sma20[-1] < sma20[-2] and sma20[-2] < sma20[-3] and sma20[-3] < sma20[-4]:
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        

            ###############Hit and Run Strategies ############3

    def CompletelyAboveBelowSMA20(self, high, low, sma20, direction):
        if (direction == 1):
            for i in range(-1,-8,-1):
                if (high[i] < sma20[i] or low[i] < sma20[i]):
                    return False
        elif (direction == -1):
            for i in range(-1,-8,-1):
                if (high[i] > sma20[i] or low[i] > sma20[i]):
                    return False
        return True

    def NR7(self, high, low):
        ranges=[]
        for i in range(-1,-8,-1):
            ranges.append(high[i] - low[i])
        if min(ranges) == ranges[0]:
            return True
        else: 
            return False


    def ScanForHitAndRunPlays(self, ticker, time_frame, method=1):
        period = 400

        ticker1=ticker.replace("-","_")
        if time_frame == "daily":
            table_name=ticker1.lower()+"_values"
        elif time_frame == "weekly":
            table_name=ticker1.lower()+"_values_weekly"
        elif time_frame == "monthly":
            table_name=ticker1.lower()+"_values_monthly"
        query=f"SELECT count(*) FROM {table_name}"
        self.cur.execute(query)
        rows=self.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self.conn)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        close=df['Close'].values
        high=df['High'].values
        low=df['Low'].values
        openv=df['Open'].values
        volume=df['Volume'].values
        stochk, stockd =STOCH(high, low, close, 12, 3, 0, 3, 0)
        wavgvol=np.average(volume[-10:])

        tmh=np.max(high[-45:])
        tml=np.min(low[-45:])
        fdh=np.max(high[-41:])
        fdl=np.min(low[-41:])
        fiftydh=np.max(high[-51:])
        fiftydl=np.min(low[-51:])
        tdl=np.min(low[-10:])
        tdh=np.max(high[-10:])
        todaysrange=high[-1] -low[-1]
        ninedayrangemax=np.max(np.subtract(high[-9:], low[-9:]))
        adx=ADX(high, low, close)
        mdi=MINUS_DI(high,low,close)
        pdi=PLUS_DI(high,low,close)
        sma50=SMA(close, 50)
        sma10=SMA(close, 10)
        sma20=SMA(close, 20)
        sma53=SMA(close, 53)
        sma200=SMA(close, 200)

        ema8=EMA(close,8)
        ema3=EMA(close,3)
        ema20=EMA(close,20)
        ema50=EMA(close,50)
        ema200=EMA(close,200)
        ema34=EMA(close,34)
        rsi2=RSI(close,2)
        rsi8=RSI(close,8)

        if method == 10:     #Expansion Breakouts
            if(high[-1] == tmh and todaysrange >= ninedayrangemax):
                return (stochk[-1], "calls")
            elif (low[-1] == tml and todaysrange >= ninedayrangemax):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 11:   #Gilligan's Island  
            if(openv[-1] <= tml and close[-1] >= openv[-1] and close[-1] > low[-1] + 0.5 * (high[-1] - low[-1])):
                return (stochk[-1], "calls")
            elif(openv[-1] >= tmh and close[-1] <= openv[-1] and close[-1] < high[-1] - 0.5 * (high[-1] - low[-1])):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 12:   #SlingShots
            if(high[-2] == tmh and low[-1] < low[-2]):
                return (stochk[-1], "calls")
            elif (low[-2] == tml and high[-1] > high[-2]):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 13:   #Lizards
            if(close[-1] > low[-1] + 0.75 * (high[-1] - low[-1]) and  openv[-1] > low[-1] + 0.75 * (high[-1] - low[-1]) and low[-1] <= tdl):
                return (stochk[-1], "calls")
            elif (close[-1] < high[-1] - 0.75 * (high[-1] - low[-1]) and  openv[-1] < high[-1] - 0.75 * (high[-1] - low[-1]) and high[-1] >= tdh):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 20: #1-2-3 pullback
            if(adx[-4] >=30 and pdi[-4] > mdi[-4] and (low[-3] <= low[-4] or abs(close[-3] -openv[-3]) <= abs(close[-4]-openv[-4])) and (low[-2] <= low[-3] or abs(close[-2] -openv[-2]) <= abs(close[-3]-openv[-3])) and (low[-1] <= low[2] or abs(close[-1] -openv[-1]) <= abs(close[-2]-openv[-2]))):
                return (stochk[-1], "calls")
            elif (adx[-4] >=30 and pdi[-4] < mdi[-4] and (high[-3] >= high[-4] or abs(close[-3] -openv[-3]) <= abs(close[-4]-openv[-4])) and (high[-2] >= high[-3] or abs(close[-2] -openv[-2]) <= abs(close[-3]-openv[-3])) and (high[-1] >= high[-2] or abs(close[-1] -openv[-1]) <= abs(close[-2]-openv[-2]))):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 21: #Boomers
            if(adx[-3] >=30 and pdi[-3] > mdi[-3] and high[-2] <= high[-3] and low[-2]>= low[-3] and high[-1] <= high[-2] and low[-1] >= low[-2]):
                return (stochk[-1], "calls")
            elif (adx[-3] >=30 and pdi[-4] < mdi[-3] and high[-2] <= high[-3] and low[-2]>= low[-3] and high[-1] <= high[-2] and low[-1] >= low[-2]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 30: #Expansion Pivots
            if(todaysrange >= ninedayrangemax and ((low[-1] < sma50[-1] and close[-1] > sma50[-1]) or (low[-2] < sma50[-2] and close[-1] > sma50[-1]))):
                return (stochk[-1], "calls")
            elif(todaysrange >= ninedayrangemax and ((high[-1] > sma50[-1] and close[-1] < sma50[-1]) or (high[-2] > sma50[-2] and close[-1] < sma50[-1]))):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 40: #180's
            if((close[-2] - low[-2])<= 0.25*(high[-2] - low[-2]) and close[-1] > sma10[-1] and close[-1] > sma50[-1] and (close[-1] - low[-1])>= 0.75*(high[-1] - low[-1])):
                return (stochk[-1], "calls")
            elif((high[-2] - close[-2]) <= 0.25*(high[-2] - low[-2]) and close[-1] < sma10[-1] and close[-1] < sma50[-1] and (high[-1] - close[-1])>= 0.75*(high[-1] - low[-1])):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 41: #Whoops
            if(close[-1] > sma10[-1] and close[-1] > sma50[-1] and close[-2] > openv[-2] and openv[-1] < close[-2]):
                return (stochk[-1], "calls")
            elif(close[-1] < sma10[-1] and close[-1] < sma50[-1] and close[-2] < openv[-2] and openv[-1] > close[-2] ):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 50: #40-3 pullback
            if(high[-5] >= fdh and close[-4] < close[-5] and close[-3] < close[-4] and close[-2] < close[-3] and close[-1] > close[-2] and (high[-1] -close[-1]) > 0.8*(high[-1] - low[-1])):
                return (stochk[-1], "calls")
            elif(low[-5] <= fdl and close[-4] > close[-5] and close[-3] > close[-4] and close[-2] > close[-3] and close[-1] < close[-2] and (close[-1]-low[-1]) < 0.2*(high[-1] - low[-1])):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 51: # 80-20 RSI Strategy
            if(low[-1] <= fiftydl and rsi8[-1] <= 20):
                return (stochk[-1], "calls")
            elif(high[-1] >= fiftydh and rsi8[-1] >= 80):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 60: #Inside bar strategy
            if (close[-1] > sma20[-1] and  close[-1] > openv[-1] and low[-1] > low[-2] and high[-1] < high[-2] ):
                return (stochk[-1], "calls")
            elif (close[-1] < sma20[-1] and  close[-1] <  openv[-1] and low[-1] > low[-2] and high[-1] < high[-2] ):
                return (stochk[-1], "puts")
            else:
                return (stochk[-1], "skip")
        elif method == 61: #Consecutive Days Up/Down
            if  (close[-5] < openv[-5] and close[-4] < openv[-4] and close[-3] < openv[-3] and close[-2] < openv[-2] and close[-1] > openv[-1]):
                return (stochk[-1], "calls")
            elif (close[-5] > openv[-5] and close[-4] > openv[-4] and close[-3] > openv[-3] and close[-2] > openv[-2] and close[-1] < openv[-1]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 62: # 3-bar reversal
            if (close[-3] < openv[-3] and low[-2] < low[-3] and low[-2] < low[-1] and high[-2] < high[-3] and close[-1] > openv[-1] and close[-1] > high[-3]):
                return (stochk[-1], "calls")
            elif (close[-3] > openv[-3] and high[-2] > high[-3] and high[-2] > high[-1] and low[-2] > low[-3] and close[-1] < openv[-1] and close[-1] < low[-3]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 63: # NR7 
            if(self.CompletelyAboveBelowSMA20(high,low,sma20,1) and self.NR7(high, low)):
                return (stochk[-1], "calls")
            elif(self.CompletelyAboveBelowSMA20(high,low,sma20,-1) and self.NR7(high, low)):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 64: # Hikkake
            if(close[-3] < openv[-3] and close[-2] > openv[-2] and low[-2] > low[-3] and high[-2] < high[-3] and high[-1] < high[-2] and low[-1] < low[-2]):
                return (stochk[-1], "calls")
            elif(close[-3] > openv[-3] and close[-2] < openv[-2] and low[-2] > low[-3] and high[-2] < high[-3] and high[-1] > high[-2] and low[-1] > low[-2]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 65: # Trend bar failure setup
            if(close[-3] > openv[-3] and close[-2] < openv[-2] and (openv[-2] - close[-2]) > 0.75*(high[-2] -low[-2]) and close[-1] > openv[-1] and close[-1] > close[-2]):
                return (stochk[-1], "calls")
            elif(close[-3] < openv[-3] and close[-2] > openv[-2] and (close[-2] - openv[-2]) > 0.75*(high[-2] -low[-2]) and close[-1] < openv[-1] and close[-1] < close[-2]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 70: # Quant edge outside bar
            if (close[-1] > openv[-1] and high[-1] > high[-2] and low[-1] < low[-2]):
                return (stochk[-1], "calls")
            elif (close[-1] < openv[-1] and high[-1] > high[-2] and low[-1] < low[-2]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 71: # Quant edge inside bar
            if (close[-1] > openv[-1] and high[-1] < high[-2] and low[-1] > low[-2]):
                return (stochk[-1], "calls")
            elif (close[-1] < openv[-1] and high[-1] < high[-2] and low[-1] > low[-2]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 80: # JWPP
            if ( close[-1] > openv[-1] and (high[-1]- close[-1]) >= 0.80 * (high[-1] -low[-1]) and volume[-1] >= 1.2*wavgvol):
                return (stochk[-1], "calls")
            elif ( close[-1] < openv[-1] and (close[-1] - low[-1]) <= 0.20 * (high[-1] - low[-1]) and  volume[-1] >= 1.2*wavgvol):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 81: #Rubberband Keltner
            atr=ATR(high[-14:],low[-14:],close[-14:])
            ema20=EMA(close,20)
            lk4=ema20[-1] - 4 * atr[-1]
            uk4=ema20[-1] + 4 * atr[-1]
            lk2=ema20[-1] - 2 * atr[-1]
            uk2=ema20[-1] + 2 * atr[-1]
            """
            if(low[-1] < lk4 and high[-1] > lk4 and high[-1] < lk2):
                return (stochk[-1], "calls")
            elif(high[-1] > uk4 and low[-1] < uk4 and low[-1] > uk2):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
            """
            if(low[-1] < lk2 and high[-1] > lk2 and high[-1] < ema20[-1]):
                return (stochk[-1], "calls")
            elif(high[-1] > uk2 and low[-1] < uk2 and low[-1] > ema20[-1]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 82: #Big candle
            #atr=ATR(high[-14:],low[-14:],close[-14:])
            latr=np.average(np.subtract(high[-14:], low[-14:]))

            if (high[-1] - low[-1] > 2.0 * latr and close[-1] > openv[-1]):
                return (stochk[-1], "calls")
            elif (high[-1] - low[-1] > 2.0 * latr and openv[-1] > close[-1]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 83: # NB Long
            if (sma20[-1] > sma50[-1] and close[-1] > sma20[-1] and close[-1] < ema8[-1] and fdh - high[-1] <= 0.05 * fdh) :
                return (stochk[-1], "calls")
            elif (sma20[-1]  < sma50[-1] and close[-1] < sma20[-1] and close[-1] > ema8[-1] and low[-1] -fdl <= 0.05 *fdl):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 84: # Narrow trading range - line
            lmax=np.max(high[-10:])
            lmin=np.min(low[-10:])
            latr=np.average(np.subtract(high[-10:], low[-10:]))
            if(latr <= 0.1*(lmax - lmin) and close[-1] > openv[-1]):
                return (stochk[-1], "calls")
            elif(latr <= 0.1*(lmax - lmin) and close[-1] < openv[-1]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 85: # 50 SMA
            if(sma50[-5] < sma50[-4] and sma50[-4] < sma50[-3] and sma50[-3] < sma50[-2] and sma50[-2] < sma50[-1] and close[-2] <sma50[-2] and close[-1] > sma50[-1]):
                return (stochk[-1], "calls")
            elif(sma50[-5] > sma50[-4] and sma50[-4] > sma50[-3] and sma50[-3] > sma50[-2] and sma50[-2] > sma50[-1] and close[-2] > sma50[-2] and close[-1] < sma50[-1]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 86: # 50 SMA
            if(close[-1] > sma200[-1] and sma20[-2] < sma50[-2] and sma20[-1] > sma50[-1] ):
                return (stochk[-1], "calls")
            elif(close[-1]  < sma200[-1] and sma20[-2] > sma50[-2] and sma20[-1] < sma50[-1] ):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 87 : # 3 bar pattern
            #atr=ATR(high[-14:],low[-14:],close[-14:])
            latr=np.average(np.subtract(high[-14:], low[-14:]))

            #print(latr)
            #print(high[-1] - low[-1])
            #if (high[-2] - low[-2] > 1.5* atr[-2] and close[-3] < openv[-3] and close[-2] > openv[-2] and (high[-1] - low[-1]) <0.5 * (high[-2] -low[-2])):
            if (high[-2] - low[-2] > 2.0* latr and close[-2] > openv[-2] and abs(close[-2] -openv[-2]) > 0.8 *(high[-2] -low[-2]) and (high[-1] - low[-1]) <0.5 * (high[-2] -low[-2])):
            #if (high[-1] - low[-1] > 1.5* latr and close[-1] > openv[-1] ) :    
                return (stochk[-1], "calls")
            #elif (high[-2] - low[-2] > 1.5* atr[-2] and close[-3] > openv[-3] and close[-2] < openv[-2] and (high[-1] - low[-1]) <0.5 * (high[-2] -low[-2])):
            elif (high[-2] - low[-2] > 2.0* latr and close[-2] < openv[-2] and abs(close[-2] -openv[-2]) > 0.8 *(high[-2] -low[-2]) and (high[-1] - low[-1]) <0.5 * (high[-2] -low[-2])):
            #elif (high[-1] - low[-1] > 1.5* latr and close[-1] < openv[-1] ):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 88: # Checklist
            latr=np.average(np.subtract(high[-14:], low[-14:]))
            if(close[-1] > sma20[-1] and volume[-1] > 2000000 and latr > 1 and close[-1] > close[-2] and high[-1] > high[-2] ):
                return (stochk[-1], "calls")
            elif(close[-1] < sma20[-1] and volume[-1] > 2000000 and latr > 1 and close[-1] < close[-2] and low[-1] < low[-2]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 89: # Weekly Outside bar
            if(high[-1] > high[-2] and low[-1]< low[-2] and close[-1] > openv[-1] ):
                return (stochk[-1], "calls")
            elif(high[-1] > high[-2] and low[-1]< low[-2] and close[-1]  < openv[-1]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 90: # EOD
            if(sma20[-1] > sma20[-2] and sma20[-2] > sma20[-3] and sma20[-3] > sma20[-4] and close[-1] > sma20[-1] and close[-1] > openv[-1] and volume[-1] > volume[-2] and (high[-1] - close[-1]) < 0.05* (high[-1] - low[-1]) and (openv[-1] -low[-1]) > 0.5 *(high[-1] -low[-1]) ):
                return (stochk[-1], "calls")
            elif(sma20[-1] < sma20[-2] and sma20[-2] < sma20[-3] and sma20[-3] < sma20[-4] and close[-1] < sma20[-1] and close[-1] < openv[-1] and volume[-1] > volume[-2] and (close[-1] - low[-1] ) < 0.95* (high[-1] - low[-1]) and (high[-1] -openv[-1]) > 0.5 *(high[-1] -low[-1])):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 91: # EOD
            if(sma20[-1] > sma20[-2] and sma20[-2] > sma20[-3] and sma20[-3] > sma20[-4] and close[-1] > sma20[-1] and close[-1] > openv[-1] and volume[-1] > volume[-2] and (high[-1] - close[-1]) < 0.05* (high[-1] - low[-1]) ):
                return (stochk[-1], "calls")
            elif(sma20[-1] < sma20[-2] and sma20[-2] < sma20[-3] and sma20[-3] < sma20[-4] and close[-1] < sma20[-1] and close[-1] < openv[-1] and volume[-1] > volume[-2] and (close[-1] - low[-1] ) < 0.95* (high[-1] - low[-1])):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 92: # EOD
            if(sma20[-1] > sma20[-2] and sma20[-2] > sma20[-3] and sma20[-3] > sma20[-4] and close[-1] > sma20[-1] and close[-1] > openv[-1] and volume[-1] > volume[-2] and (high[-1] - close[-1]) < 0.05* (high[-1] - low[-1]) and close[-2] < sma20[-2] and low[-1] < close[-2]):
                return (stochk[-1], "calls")
            elif(sma20[-1] < sma20[-2] and sma20[-2] < sma20[-3] and sma20[-3] < sma20[-4] and close[-1] < sma20[-1] and close[-1] < openv[-1] and volume[-1] > volume[-2] and (close[-1] - low[-1] ) < 0.95* (high[-1] - low[-1]) and  close[-2] > sma20[-2] and close[-2] < high[-1]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")    
        elif method == 93: # EOD
            if(sma50[-1] > sma50[-2] and sma50[-2] > sma50[-3] and sma50[-3] > sma50[-4] and close[-1] > sma50[-1] and close[-1] > openv[-1] and volume[-1] > volume[-2] and (high[-1] - close[-1]) < 0.05* (high[-1] - low[-1]) and (openv[-1] -low[-1]) > 0.5 *(high[-1] -low[-1]) ):
                return (stochk[-1], "calls")
            elif(sma50[-1] < sma50[-2] and sma50[-2] < sma50[-3] and sma50[-3] < sma50[-4] and close[-1] < sma50[-1] and close[-1] < openv[-1] and volume[-1] > volume[-2] and (close[-1] - low[-1] ) < 0.95* (high[-1] - low[-1]) and (high[-1] -openv[-1]) > 0.5 *(high[-1] -low[-1])):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 94: # EOD
            if(sma50[-1] > sma50[-2] and sma50[-2] > sma50[-3] and sma50[-3] > sma50[-4] and close[-1] > sma50[-1] and close[-1] > openv[-1] and volume[-1] > volume[-2] and (high[-1] - close[-1]) < 0.05* (high[-1] - low[-1]) ):
                return (stochk[-1], "calls")
            elif(sma50[-1] < sma50[-2] and sma50[-2] < sma50[-3] and sma50[-3] < sma50[-4] and close[-1] < sma50[-1] and close[-1] < openv[-1] and volume[-1] > volume[-2] and (close[-1] - low[-1] ) < 0.95* (high[-1] - low[-1])):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")
        elif method == 95: # EOD
            if(sma50[-1] > sma50[-2] and sma50[-2] > sma50[-3] and sma50[-3] > sma50[-4] and close[-1] > sma50[-1] and close[-1] > openv[-1] and volume[-1] > volume[-2] and (high[-1] - close[-1]) < 0.05* (high[-1] - low[-1]) and close[-2] < sma50[-2] and low[-1] < close[-2]):
                return (stochk[-1], "calls")
            elif(sma50[-1] < sma50[-2] and sma50[-2] < sma50[-3] and sma50[-3] < sma50[-4] and close[-1] < sma50[-1] and close[-1] < openv[-1] and volume[-1] > volume[-2] and (close[-1] - low[-1] ) < 0.95* (high[-1] - low[-1]) and  close[-2] > sma50[-2] and close[-2] < high[-1]):
                return (stochk[-1], "puts")
            else: 
                return (stochk[-1], "skip")   
        elif method== 100:
            if(ema20[-1] > ema50[-1] and ema8[-1] > ema20[-1] and ema3[-1] > ema8[-1] and close[-1] < ema3[-1] and close[-1] > ema8[-1]):
                return (stochk[-1], "calls")
            elif (ema20[-1] < ema50[-1] and ema8[-1] < ema20[-1] and ema3[-1] < ema8[-1] and close[-1] > ema3[-1] and close[-1] < ema8[-1]):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")  
        elif method== 101:
            if(close[-1] > ema34[-1] and rsi2[-1] < 10):
                return (stochk[-1], "calls")
            elif ( close[-1] < ema34[-1] and rsi2[-1] > 90):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")     
        elif method == 102:
            #if (close[-1] > ema50[-1] and stochk[-1] < 25 and close[-1] > openv[-1] and openv[-2] > close[-2] and openv[-3] > close[-3] and openv[-4] > close[-4] and close[-1] > openv[-2] and openv[-1] < close[-2]):
            if (close[-1] > ema20[-1] and close[-1] > openv[-1] and openv[-2] > close[-2]  and close[-1] > openv[-2] and openv[-1] < close[-2]):   
                return (stochk[-1], "calls")
            #elif (close[-1] < ema50[-1] and stochk[-1] >75  and close[-1] < openv[-1] and openv[-2] < close[-2] and openv[-3] < close[-3] and openv[-4] < close[-4] and openv[-1] > close[-2] and close[-1] < openv[-2]):
            elif ( close[-1] < ema20[-1] and close[-1] < openv[-1] and openv[-2] < close[-2]  and openv[-1] > close[-2] and close[-1] < openv[-2]):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")   
        elif method==103: #gaps
            if(close[-3] < openv[-3] and (close[-2] < openv[-2] or openv[-2] < close[-3]) and close[-1] > openv[-1] and low[-1] > high[-2]):
                return (stochk[-1], "calls")
            elif (close[-3] > openv[-3] and (close[-2] > openv[-2] or openv[-2] > close[-3]) and close[-1] < openv[-1] and high[-1] < low[-2]):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")    
        elif method== 104:
            if(close[-1] > ema50[-1] and volume[-1] > 2*volume[-2]):
                return (stochk[-1], "calls")
            elif ( close[-1] <  ema50[-1] and volume[-1] > 2*volume[-2]):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")  
        elif method== 105:
            max1=np.max(high[-20:])
            min1=np.min(low[-20:])
            if(sma53[-1] > sma53[-60] and (max1-min1) <= 0.05*max1):
                return (stochk[-1], "calls")
            elif ( sma53[-1] < sma53[-60] and (max1-min1) <= 0.05*max1):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")       
        elif method== 106:
            if(ema50[-1] > ema200[-1] and ema20[-1] > ema200[-1] and ema20[-1] < ema50[-1]):
                return (stochk[-1], "calls")
            elif ( ema50[-1] < ema200[-1] and ema20[-1] < ema200[-1] and ema20[-1] > ema50[-1]):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")       
        elif method== 107:
            if(ema8[-1] > ema20[-1] and ema20[-1] > ema50[-1] and close[-1] > ema8[-1] and openv[-1] < ema8[-1]):
                return (stochk[-1], "calls")
            elif ( ema8[-1] < ema20[-1] and ema20[-1] < ema50[-1] and openv[-1] > ema8[-1] and close[-1] < ema8[-1]):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")       
        elif method== 108:
            if(ema8[-1] > ema20[-1] and ema20[-1] > ema50[-1] and ema50[-1] > ema200[-1] and close[-1] < ema8[-1] and close[-1] > ema20[-1]):
                return (stochk[-1], "calls")
            elif ( ema8[-1] < ema20[-1] and ema20[-1] < ema50[-1] and ema50[-1] < ema200[-1] and close[-1] > ema8[-1] and close[-1] < ema20[-1]):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")       
        elif method== 109:
            if(high[-1] > high[-2] and low[-1] > low[-2]):
                return (stochk[-1], "calls")
            elif ( high[-1] < high[-2] and low[-1] < low[-2]):
                return (stochk[-1], "puts") 
            else: 
                return (stochk[-1], "skip")       









         
        


class YahooFinanceDataBase(DataBase):
    def __init__(self, name, factor):
        super(YahooFinanceDataBase, self).__init__(name)
        self.posFactor=factor

    def NearHOD(self, ticker):
        ticker1=ticker.replace("-","_")
        table_name=ticker1.lower()+"_values"
        today = str(date.today())
        columns="Open,Close,High,Low"
        query=f"SELECT {columns} FROM {table_name} WHERE Date = '{today}'"
        self.cur.execute(query)
        rows=self.cur.fetchall()
        CRange=rows[0][2]-rows[0][3]
        position=self.posFactor*CRange
        if rows[0][1] >= (rows[0][2] - position):
            return True
        else:
            return False

    def NearLOD(self, ticker):
        ticker1=ticker.replace("-","_")
        table_name=ticker1.lower()+"_values"
        today = str(date.today())
        columns="Open,Close,High,Low"
        query=f"SELECT {columns} FROM {table_name} WHERE Date = '{today}'"
        self.cur.execute(query)
        rows=self.cur.fetchall()
        CRange=rows[0][2]-rows[0][3]
        position=self.posFactor*CRange
        if rows[0][1] <= (rows[0][3] + position):
            return True
        else:
            return False


class FinVizDataBase(DataBase):
    def __init__(self, name, type):
        super(FinVizDataBase, self).__init__(name)
        self.type=type
        self.stock_column_list="Ticker,Company,Sector,Industry,Earnings,PerfWeek,Change,RSI14,ATR,RelVolume,SMA20,SMA50,SMA200,High52W,Low52W,Price,PrevClose"
        self.etf_column_list="Ticker,Company,PerfWeek,Change,RSI14,ATR,RelVolume,SMA20,SMA50,SMA200,High52W,Low52W,Price,PrevClose"
        self.sector_industry_list="Sector, Industry"
        if self.type == "stock":
            self.table_name="stock_info"
        else:
            self.table_name="etf_info"

    def execute_query(self, column_list, ticker):
        if self.type == "stock":
            query=f"SELECT {column_list} FROM stock_info WHERE Ticker = \"{ticker}\""
            df=pd.read_sql_query(query, self.conn)
            return df
        else:
            query=f"SELECT {column_list} FROM etf_info WHERE Ticker = \"{ticker}\""
            df=pd.read_sql_query(query, self.conn)
            return df

    def RSI_strategy(self, direction, llimit, ulimit):
        if self.type == "stock":
            #self.cur.execute("SELECT * FROM stock_info WHERE ATR > 5")
            if direction == "up":
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE RSI14 >= {llimit}  AND RSI14 < {ulimit}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE RSI14 <= {ulimit}  AND RSI14 > {llimit}"
                df=pd.read_sql_query(query, self.conn)
                return df
        else:
            #self.cur.execute("SELECT * FROM etf_info WHERE ATR > 5")
            if direction == "up":
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE RSI14 >= {llimit}  AND RSI14 < {ulimit}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE RSI14 <= {ulimit}  AND RSI14 > {llimit}"
                df=pd.read_sql_query(query, self.conn)
                return df
        #rows=self.cur.fetchall()
        #return rows

    def earnings_date(self, ticker):
        query=f"SELECT {self.stock_column_list} FROM stock_info WHERE Ticker = '{ticker.upper()}'"
        df=pd.read_sql_query(query, self.conn)
        return df

    def sectors(self, ticker):
        query=f"SELECT {self.sector_industry_list} FROM stock_info WHERE Ticker = '{ticker.upper()}'"
        df=pd.read_sql_query(query, self.conn)
        return df


    def holly_grail(self,direction,limit):
        if self.type == "stock":
            if direction == "up":
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE SMA20 >= 0  AND SMA20 <= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE SMA20 <= 0  AND SMA20 > {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df
        else:
            if direction == "up":
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE SMA20 >= 0  AND SMA20 <= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE SMA20 <= 0  AND SMA20 > {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df

    def relative_volume(self, limit):
        if self.type == "stock":
            query=f"SELECT {self.stock_column_list} FROM stock_info WHERE RelVolume >=  {limit}"
            df=pd.read_sql_query(query, self.conn)
            return df.sort_values('RelVolume',ascending=False)             
        else:
            query=f"SELECT {self.etf_column_list} FROM etf_info WHERE RelVolume >= {limit}"
            df=pd.read_sql_query(query, self.conn)
            return df.sort_values('RelVolume',ascending=False)  

    def ATR(self, limit):
        if self.type == "stock":
            query=f"SELECT {self.stock_column_list} FROM stock_info WHERE ATR >=  {limit}"
            df=pd.read_sql_query(query, self.conn)
            return df.sort_values('ATR',ascending=False)           
        else:
            query=f"SELECT {self.etf_column_list} FROM etf_info WHERE ATR >= {limit}"
            df=pd.read_sql_query(query, self.conn)
            return df.sort_values('ATR',ascending=False)  

    def Extremes52W(self, direction, limit):
        if self.type == "stock":
            if direction == "up":
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE High52W <= 0  AND High52W >= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df.sort_values('High52W',ascending=False)
            else:
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE Low52W >= 0  AND Low52W <= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df.sort_values('Low52W',ascending=True)
        else:
            if direction == "up":
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE High52W <= 0  AND High52W >= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df.sort_values('High52W',ascending=False)
            else:
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE Low52W >= 0  AND Low52W <= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df.sort_values('Low52W',ascending=True)

    def OverBoughtSold(self, direction,limit):
        if self.type == "stock":
            if direction == "up":
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE RSI14 >= {limit}  AND Change  < 0"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE RSI14  <=  {limit} AND Change > 0"
                df=pd.read_sql_query(query, self.conn)
                return df
        else:
            if direction == "up":
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE RSI14 >= {limit} AND Change  < 0"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE RSI14 <= {limit}  AND Change > 0"
                df=pd.read_sql_query(query, self.conn)
                return df

    def MovingAverages2050200(self, direction,limit):
        if self.type =="stock":
            if direction == "up":
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE SMA20 < SMA50 AND SMA50  < SMA200 AND SMA20 > 0 AND SMA20 <= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE SMA20  < SMA50 AND SMA50 < SMA200 AND SMA200 < 0 AND SMA20 >= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df
        else:
            if direction == "up":
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE SMA20 < SMA50 AND SMA50 < SMA200 AND SMA20 > 0 AND SMA20 <= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE SMA20  < SMA50 AND SMA50 < SMA200 AND SMA200 < 0 AND SMA20 >= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df

    def MovingAverages2050(self, direction,limit):
        if self.type =="stock":
            if direction == "up":
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE SMA20 < SMA50  AND SMA50 > 0 AND SMA20 <= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE SMA20  < SMA50  AND SMA50 < 0 AND SMA20 >= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df
        else:
            if direction == "up":
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE SMA20 <  SMA50  AND SMA50 > 0 AND SMA20 <= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE SMA20  < SMA50 AND SMA50 < 0 AND SMA20 >= {limit}"
                df=pd.read_sql_query(query, self.conn)
                return df

    def SecondDayPlays(self, direction, ValueDBName, atrfactor, relvolfactor, posfactor):
        if self.type =="stock":
            if direction == "up":
                query=f"SELECT Ticker FROM stock_info WHERE (Price - PrevClose) > {atrfactor} * ATR AND RelVolume > {relvolfactor}"
                self.cur.execute(query)
                rows=self.cur.fetchall()
                db=YahooFinanceDataBase(ValueDBName, posfactor)
                str=""
                for elem in rows:
                    if db.NearHOD(elem[0]) == True:
                        str+=" Ticker = '"+elem[0]+"' OR "
                str1=str[0:-3]
                if str == "":
                    print("No results")
                    sys.exit()
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE  {str1}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT Ticker FROM stock_info WHERE (PrevClose - Price) > {atrfactor} * ATR AND RelVolume > {relvolfactor}"
                self.cur.execute(query)
                rows=self.cur.fetchall()
                db=YahooFinanceDataBase(ValueDBName, posfactor)
                str=""
                for elem in rows:
                    if db.NearLOD(elem[0]) == True:
                        str+=" Ticker = '"+elem[0]+"' OR "
                str1=str[0:-3]
                if str == "":
                    print("No results")
                    sys.exit()
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE  {str1}"
                df=pd.read_sql_query(query, self.conn)
                return df
        else:
            if direction == "up":
                query=f"SELECT Ticker FROM etf_info WHERE (Price - PrevClose) > {atrfactor} * ATR AND RelVolume > {relvolfactor}"
                self.cur.execute(query)
                rows=self.cur.fetchall()
                db=YahooFinanceDataBase(ValueDBName, posfactor)
                str=""
                for elem in rows:
                    if db.NearHOD(elem[0]) == True:
                        str+=" Ticker = '"+elem[0]+"' OR "
                str1=str[0:-3]
                if str == "":
                    print("No results")
                    sys.exit()
                query=f"SELECT {self.stock_column_list} FROM etf_info WHERE  {str1}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT Ticker FROM etf_info WHERE (PrevClose - Price) > {atrfactor} * ATR AND RelVolume > {relvolfactor}"
                self.cur.execute(query)
                rows=self.cur.fetchall()
                rows=self.cur.fetchall()
                db=YahooFinanceDataBase(ValueDBName, posfactor)
                str=""
                for elem in rows:
                    if db.NearLOD(elem[0]) == True:
                        str+=" Ticker = '"+elem[0]+"' OR "
                if str == "":
                    print("No results")
                    sys.exit()
                str1=str[0:-3]
                query=f"SELECT {self.stock_column_list} FROM etf_info WHERE  {str1}"
                df=pd.read_sql_query(query, self.conn)
                return df

    def RSIHolyGrail(self, direction, limit1, limit2):
        if self.type == "stock":
            if direction == "up":
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE RSI14 >= {limit1}  AND SMA20 > 0 AND SMA20 < {limit2}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.stock_column_list} FROM stock_info WHERE RSI14 <= {limit1}  AND SMA20 < 0 AND SMA20 > {limit2}"
                df=pd.read_sql_query(query, self.conn)
                return df
        else:
            #self.cur.execute("SELECT * FROM etf_info WHERE ATR > 5")
            if direction == "up":
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE RSI14 >= {limit1}  AND SMA20 > 0 AND SMA20 < {limit2}"
                df=pd.read_sql_query(query, self.conn)
                return df
            else:
                query=f"SELECT {self.etf_column_list} FROM etf_info WHERE RSI14 <= {limit1}  AND SMA20 < 0 AND SMA20 > {limit2}"
                df=pd.read_sql_query(query, self.conn)
                return df
        


    




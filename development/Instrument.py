#!/usr/bin/env python3

from database import *
import pandas as pd

class Instrument:
    
    def __init__(self, name, db_name):
        self._name=name
        self._DB=DataBase(db_name)

    def get_values(self,tf,period):
        table_name=""
        name=self._name.replace("-","_")
        if tf == "daily":
            table_name=name.lower()+"_values"
        else:
            table_name=name.lower()+"_values_"+tf
        query=f"SELECT count(*) FROM {table_name}"
        self._DB.cur.execute(query)
        rows=self._DB.cur.fetchone()
        number=rows[0]
        offset=number-period
        query=f"SELECT * FROM {table_name} LIMIT {period} OFFSET {offset}"
        df=pd.read_sql_query(query, self._DB.conn)
        return df

    def find_swingHL_one(self, val, w):
        t=val.shape
        l=t[0]
        rd={'Date':[], 'SLH' : [], 'Value': []}
        for i in range(0+w, l-w):
            if val.loc[i,'High'] > val.loc[i-w:i-1,'High'].max() and val.loc[i,'High'] > val.loc[i+1:i+w,'High'].max():
                #print(val.loc[i,'Date'], " SH")
                rd['Date'].append(val.loc[i,'Date'])
                rd['SLH'].append("SH")
                rd['Value'].append(val.loc[i,'High'])
            elif val.loc[i,'Low'] < val.loc[i-w:i-1,'Low'].min() and val.loc[i,'Low'] < val.loc[i+1:i+w,'Low'].min():
                #print(val.loc[i,'Date'], " SL")
                rd['Date'].append(val.loc[i,'Date'])
                rd['SLH'].append("SL")
                rd['Value'].append(val.loc[i,'Low'])
        return pd.DataFrame(rd)

    def moving_average(self, values, window):
        weights= np.repeat(1.0, window)/window
        smas= np.convolve(values, weights, 'valid')
        return smas
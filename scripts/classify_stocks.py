#!/usr/bin/env python3

import sqlite3
import json
import requests
import pandas as pd
import sys
from DataBase import FinVizDataBase
from tabulate import tabulate


db_name="info.db"
stock_list="stocks_reduces.list"

def process_file(fn,name_list):
    with open(fn) as f:
        name_list += f.read().splitlines() #same as readlines() but removes the \n from each line

def main(argv):
    name_list=[]
    class_dict={}

    process_file(stock_list, name_list)
    database=FinVizDataBase(db_name,type)

    for elem in name_list:
        print(elem)
        df=database.sectors(elem)
        sector=df.iloc[0]['Sector']
        industry=df.iloc[0]['Industry']
        if sector in class_dict:
            if industry in class_dict[sector]:
                class_dict[sector][industry].append(elem)
            else:
                class_dict[sector][industry]=[]
                class_dict[sector][industry].append(elem)
        else:
            class_dict[sector]={}
            class_dict[sector][industry]=[]
            class_dict[sector][industry].append(elem)
        #print(tabulate(df, headers='keys', tablefmt='psql'))

    with open("classification.json", "w") as fp:
        json.dump(class_dict , fp, indent=4, sort_keys=True)


if __name__ == "__main__":
    main(sys.argv[1:])          
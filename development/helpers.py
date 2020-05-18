#!/usr/bin/env python3

timeframes=["daily", "weekly", "monthly"]
nametype=("ETF", "Stock")


def process_file(fn,name_list):
    with open(fn) as f:
        for elem in f.read().splitlines():
            name_list.add(elem)

def get_collateral_info(fn):
    dict1={}
    with open(fn) as f:
        for line in f.read().splitlines():
            l1=line.split("=")
            dict1[l1[0]]=l1[1]
    return dict1

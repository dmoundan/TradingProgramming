#!/usr/bin/env python3

import sys
import getopt
import helpers as hlp
from backtester import *
from strategy import *



def main(argv):
    collateral_file=""
    list_of_names=""
    strategy=-1
    tr_strategy=-1
    timeframe=-1
    period=-1
    gp=0.0
    atr_factor=1.0
    rvol_factor=1.2
    rsi2_factor=2
    etfs=set()
    stocks=set()
    names=set()

    try:
        opts, args = getopt.getopt(argv,"hf:l:b:t:p:g:s:a:r:i:",["collateral_file=","list_of_names=", "bt_strategy=", "timeframe=", "period=", "gap%=", "tr_startegy=", "atr_factor=","rvol_factor=","rsi2_factor="])
    except getopt.GetoptError:
        print ("""centralpy   -f <file containing  collateral info> 
                            -l <file with list of names to operate on>
                            -b <strategy to be backtested>
                            -s <strategy to be run>
                            -t <timeframe 0-> daily, 1-> weekly, 2 -> monthly>
                            -p <period to examine>
                            -g <gap percentage>
                            -a <atr_factor for strategies>
                            -r <rvol_factor for strategies>
                            -i <rsi2_factor for RSI2 strategy
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""central.py  -f <file containing  collateral info>
                                -l <file with list of names to operate on> 
                                -b <strategy to be backtested>
                                -s <startegy to be run>            
                                -t <timeframe 0-> daily, 1-> weekly, 2 -> monthly>
                                -p <period to examine>
                                -g <gap percentage>
                                -a <atr_factor for strategies>
                                -r <rvol_factor for strategies>
                                -i <rsi2_factor for RSI2 strategy
            """)    
            sys.exit()
        elif opt in("-f","--collateral_file"):
            collateral_file=arg
        elif opt in("-l","--list_of_names"):
            list_of_names=arg
        elif opt in("-b","--bt_strategy"):
            strategy=int(arg)
        elif opt in("-s","--tr_strategy"):
            tr_strategy=int(arg)
        elif opt in("-t","--timeframe"):
            timeframe=int(arg)
        elif opt in("-p","--period"):
            period=int(arg)
        elif opt in("-g","--gap%"):
            gp=float(arg)
        elif opt in("-a","--atr_factor"):
            atr_factor=float(arg)
        elif opt in("-r","--rvol_factor"):
            rvol_factor=float(arg)
        elif opt in("-i","--rsi2_factor"):
            rsi2_factor=float(arg)
    
    dictc=hlp.get_collateral_info(collateral_file)
    etf_files=dictc['list_of_etfs'].split(",")
    stock_files=dictc['list_of_stocks'].split(",")
    for file in etf_files:
        hlp.process_file(dictc['collateral_dir']+"/"+file, etfs)
    for file in stock_files:
        hlp.process_file(dictc['collateral_dir']+"/"+file, stocks)
    hlp.process_file(list_of_names, names)

    if strategy != -1:
        dict1={}
        if timeframe != -1:
            dict1["timeframe"] = hlp.timeframes[timeframe] 
        if period != -1:
            dict1["period"] = period 
        if gp != 0.0:
            dict1["gp"] = gp 
        BT=BackTester(dictc,etfs, stocks)
        #BT.run(strategy, names, timeframe=hlp.timeframes[timeframe], period=period, gp=gp)
        BT.run(strategy, names, **dict1)

    if tr_strategy != -1:
        ST=Strategies(dictc,etfs, stocks, timeframe)
        ST.run(tr_strategy, names, atr=atr_factor, rvol=rvol_factor, rsi2f=rsi2_factor)

if __name__ == "__main__":
    main(sys.argv[1:])        
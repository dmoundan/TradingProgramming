#!/usr/bin/env python3

from DataBase import FinVizDataBase
from tabulate import tabulate
import getopt
import sys


strategies =["RSI_startegy",
             "holly_grail",
             "relative_volume",
             "ATR",
             "Extremes52W",
             "OverBoughtSold",
             "MovingAverages",
             "earnings"]

def main(argv): 
    strategy=0
    limit1=0
    limit2=0
    direction=""
    type=""
    ticker=""
    db_path="./"
    StockValueDBName="stock_values.db"
    ETFValueDBName="etf_values.db"
    InfoDBName="info.db"
    ValueDBName=""
    atrfactor=1
    relvolfactor=1
    posfactor=0.25



    try:
        opts, args = getopt.getopt(argv,"ht:l:m:d:s:i:a:r:p:",["type","limit1=","limit2=","direction=","strategy=", "ticker=","atrfactor=","relvolfactor=","posfactor="])
    except getopt.GetoptError:
        print ("""analyze.py   -l <numerical value to be used as limit> 
                -m <numerical value to be used as limit> 
                -d <direction up|down>
                -t <type stock|etf>
                -i <ticker>
                -a <ATR factor>
                -r <RelVol factor>
                -p <Position Factor>
                -s <strategy 0-10 where the mapping is:
                    0: "RSI_startegy"
                    1: "holly_grail"
                    2: "relative_volume"
                    3: "ATR"
                    4: "Extremes52W"
                    5: "OverBoughtSold"
                    6: "MovingAverages2050200"
                    7: "MovingAverages2050"
                    8: "SecondDayPlays"
                    9: "RSIHolyGrail"
                    10: "earnings"  <-- onluy applies to stocks >
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""analyze.py      -l <numerical value to be used as limit> 
                -m <numerical value to be used as limit> 
                -d <direction up|down>
                -t <type stock|etf>
                -i <ticker>
                -a <ATR factor>
                -r <RelVol factor>
                -p <Position Factor>
                -s <strategy 0-6 where the mapping is:
                    0: "RSI_startegy"
                    1: "holly_grail"
                    2: "relative_volume"
                    3: "ATR"
                    4: "Extremes52W"
                    5: "OverBoughtSold"
                    6: "MovingAverages2050200"
                    7: "MovingAverages2050"
                    8: "SecondDayPlays"
                    9: "RSIHolyGrail"
                    10: "earnings"  <-- onluy applies to stocks >
             """)               
            sys.exit()
        elif opt in("-l","--limit1"):
            limit1=float(arg)
        elif opt in("-m","--limit2"):
            limit2=float(arg)
        elif opt in("-d","--direction"):
            direction=arg
        elif opt in("-t","--type"):
            type=arg
        elif opt in("-s","--strategy"):
            strategy=int(arg)
        elif opt in("-i","--ticker"):
            ticker=arg
        elif opt in("-a","--atrfactor"):
            atrfactor=float(arg)
        elif opt in("-r","--relvolfactor"):
            relvolfactor=float(arg)
        elif opt in("-p","--posfactor"):
            posfactor=float(arg)
        
        
    if strategy == 6:
        type="stock"

    if type == "stock":
        ValueDBName=StockValueDBName
    else:
        ValueDBName=ETFValueDBName

    database=FinVizDataBase(InfoDBName,type)
    if strategy == 0:
        df=database.RSI_strategy(direction, limit1,limit2)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy == 1:
        df=database.holly_grail(direction,limit1)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy == 2:
        df=database.relative_volume(limit1)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy == 3:
        df=database.ATR(limit1)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy == 4:
        df=database.Extremes52W(direction,limit1)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy == 5:
        df=database.OverBoughtSold(direction,limit1)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy == 6:
        df=database.MovingAverages2050200(direction,limit1)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy == 7:
        df=database.MovingAverages2050(direction,limit1)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy == 8:
        df=database.SecondDayPlays(direction, ValueDBName, atrfactor, relvolfactor, posfactor)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy == 9:
        df=database.RSIHolyGrail(direction, limit1,limit2)
        print(tabulate(df, headers='keys', tablefmt='psql'))
    elif strategy== 10:
        df=database.earnings_date(ticker)
        assert(type == "stock")
        print(tabulate(df, headers='keys', tablefmt='psql'))



        


if __name__ == "__main__":
    main(sys.argv[1:])      
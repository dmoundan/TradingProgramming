#!/usr/bin/env python3


from DataBase import YFDB
from DataBase import FinVizDataBase

from progressbar import *               # just a simple progress bar
import sys
import getopt
import pandas as pd
from tabulate import tabulate

"""
StockValueDBName="stock_values.db"
ETFValueDBName="etf_values.db"


"""

def process_file(fn,name_list):
    with open(fn) as f:
        for elem in f.read().splitlines():
            name_list.add(elem)


def main(argv):
    list_file=""
    name_list=set()
    db_name=""
    idb_name=""
    time_frame=""
    typ=""
    scan_typ=""
    table_name=""
    method=1
    candle=1
    stock_column_list="Ticker,Company,Sector,Industry,Earnings,Change,Price,PrevClose"
    etf_column_list="Ticker,Company,Change,Price,PrevClose"
    
    
    
    try:
        opts, args = getopt.getopt(argv,"hd:l:i:t:p:s:m:c:",["db_name=","list_file=","idb_name=", "time_frame=", "type=", "scan=","method=","candle="])
    except getopt.GetoptError:
        print ("""scanner.py   -l <file containing list of stocks> 
                -d <name of the database>
                -i <name of info database>
                -t <timeframe daily, weekly or monthly>
                -p <type stock or etf>
                -s <scan for 1cpattern, 2cpattern, rsi2, hg, mh, talib, cross, vol, mfi, dojis,mm, swing, kelt>
                -m < method when needed, currently just 1,2 or 3>
                -c <ta lib candle 1,2,3,4>
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""scanner.py   -l <file containing list of stocks> 
            -d <name of the database>
            -i <name of info database>
            -t <timeframe daily, weekly or monthly>
            -p <type stock or etf>
            -s <scan for 1cpattern, 2cpattern, rsi2, hg, mh, talib, cross, vol, mfi, dojis, mm, swing, kelt>
            -m < method when needed, currently just 1,2 or 3>
            -c <ta lib candle 1,2,3,4>
            """)    
            sys.exit()
        elif opt in("-l","--list_file"):
            list_file=arg
        elif opt in("-d","--db_name"):
            db_name=arg
        elif opt in("-i","--idb_name"):
            idb_name=arg
        elif opt in("-t","--time_frame"):
            time_frame=arg
        elif opt in("-p","--type"):
            typ=arg
        elif opt in("-s","--scan"):
            scan_typ=arg
        elif opt in("-m","--method"):
            method=int(arg)
        elif opt in("-c","--candle"):
            candle=int(arg)
        
        
    process_file(list_file, name_list)

    count=0
    result1c_dict={}
    result2c_dict={}
    resultrsi2_dict={}
    resulthg_dict={}
    resultmh_dict={}
    resultcross_dict={}
    resultvol_dict={}
    resultmfi_dict={}
    resultdoji_dict={}
    resultmm_dict={}
    resultswing_dict={}
    resultstrat_dict={}
    resulthar_dict={}

    database=FinVizDataBase(idb_name,typ)
    
    widgets = ['Test: ', Percentage(), ' ', Bar(marker='*',left='[',right=']'),
               ' ', ETA(), ' ', FileTransferSpeed()] #see docs for other options
    pbar = ProgressBar(widgets=widgets, maxval=len(name_list))

    if scan_typ == "1cpattern":
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            candle_type, stoch=db.ScanFor1CSPatterns(stock, time_frame)
            result1c_dict[stock]=(candle_type, stoch)
            pbar.update(count)
            count+=1

        pbar.finish
        df_list=[]

        for key, value in result1c_dict.items():
            if value[0] != "no_1candle_pattern" and (value[1] <= 20 or value[1] >= 80):
                df=database.execute_query(stock_column_list, key)
                signal=pd.Series(value[0])
                df = df.assign(signal=signal.values)
                stoch=pd.Series(value[1])
                df = df.assign(stoch=stoch.values)
                df_list.append(df)

        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == " 2cpattern":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            candle_type, stoch=db.ScanFor2CSPatterns(stock, time_frame)
            result2c_dict[stock]=(candle_type, stoch)
            pbar.update(count)
            count+=1

        pbar.finish
        df_list=[]

        for key, value in result2c_dict.items():
            if value[0] != "no_2candle_pattern" and (value[1] <= 20 or value[1] >= 80):
                df=database.execute_query(stock_column_list, key)
                signal=pd.Series(value[0])
                df = df.assign(signal=signal.values)
                stoch=pd.Series(value[1])
                df = df.assign(stoch=stoch.values)
                df_list.append(df)

        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "rsi2":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            sma200, rsi2, action=db.ScanForRSI2Plays(stock, time_frame, method)
            if action != "skip":
                resultrsi2_dict[stock]=(sma200, rsi2, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]

        for key, value in resultrsi2_dict.items():
            df=database.execute_query(stock_column_list, key)
            sma200 = pd.Series(value[0])
            df = df.assign(sma200=sma200.values)
            rsi2 = pd.Series(value[1])
            df = df.assign(rsi2=rsi2.values)
            action= pd.Series(value[2])
            df = df.assign(action=action.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "hg":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            sma20, action=db.ScanForHGPlays(stock, time_frame)
            if action != "skip":
                resulthg_dict[stock]=(sma20, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]

        for key, value in resulthg_dict.items():
            df=database.execute_query(stock_column_list, key)
            sma20 = pd.Series(value[0])
            df = df.assign(sma20=sma20.values)
            action= pd.Series(value[1])
            df = df.assign(action=action.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "mh":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            stoch, rsi, action=db.ScanForMHPlays(stock, time_frame)
            if action != "skip":
                resultmh_dict[stock]=(stoch, rsi, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resultmh_dict.items():
            df=database.execute_query(stock_column_list, key)
            stoch = pd.Series(value[0])
            df = df.assign(stoch=stoch.values)
            rsi = pd.Series(value[1])
            df = df.assign(rsi=rsi.values)
            action= pd.Series(value[2])
            df = df.assign(action=action.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "cross":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            stoch, action=db.ScanForCrossPlays(stock, time_frame)
            if action != "skip":
                resultcross_dict[stock]=(stoch, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resultcross_dict.items():
            df=database.execute_query(stock_column_list, key)
            stoch = pd.Series(value[0])
            df = df.assign(stoch=stoch.values)
            action= pd.Series(value[1])
            df = df.assign(action=action.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "mfi":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            mfi,stoch,action=db.ScanForMFIPlays(stock, time_frame, method)
            if action != "skip":
                resultmfi_dict[stock]=(mfi,stoch, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resultmfi_dict.items():
            df=database.execute_query(stock_column_list, key)
            mfi = pd.Series(value[0])
            df = df.assign(mfi=mfi.values)
            stoch = pd.Series(value[1])
            df = df.assign(stoch=stoch.values)
            action= pd.Series(value[2])
            df = df.assign(action=action.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "kelt":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            stoch,action=db.ScanForKeltPlays(stock, time_frame, method)
            if action != "skip":
                resultmfi_dict[stock]=(stoch, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resultmfi_dict.items():
            df=database.execute_query(stock_column_list, key)
            stoch = pd.Series(value[0])
            df = df.assign(stoch=stoch.values)
            action= pd.Series(value[1])
            df = df.assign(action=action.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "vol":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            stoch, action=db.ScanForVolPlays(stock, time_frame)
            if action != "skip":
                resultvol_dict[stock]=(stoch, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resultvol_dict.items():
            df=database.execute_query(stock_column_list, key)
            stoch = pd.Series(value[0])
            df = df.assign(stoch=stoch.values)
            action= pd.Series(value[1])
            df = df.assign(action=action.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "dojis":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            stoch, action=db.ScanForDojiPlays(stock, time_frame, method)
            if action != "skip":
                resultdoji_dict[stock]=(stoch, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resultdoji_dict.items():
            df=database.execute_query(stock_column_list, key)
            stoch = pd.Series(value[0])
            df = df.assign(stoch=stoch.values)
            action= pd.Series(value[1])
            df = df.assign(action=action.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "mm":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            action, state=db.ScanForMMPlays(stock, time_frame, method)
            if action != "skip":
                resultmm_dict[stock]=(action, state)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resultmm_dict.items():
            df=database.execute_query(stock_column_list, key)
            action= pd.Series(value[0])
            df = df.assign(action=action.values)
            state= pd.Series(value[1])
            df = df.assign(state=state.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "swing":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            stoch, action=db.ScanForSwingPlays(stock, time_frame, method)
            if action != "skip":
                resultswing_dict[stock]=(stoch, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resultswing_dict.items():
            df=database.execute_query(stock_column_list, key)
            stoch = pd.Series(value[0])
            df = df.assign(stoch=stoch.values)
            action= pd.Series(value[1])
            df = df.assign(action=action.values)
            df_list.append(df)   
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "strategies":
        count=0
        idb=FinVizDataBase(idb_name,typ)
        pbar.start()

        for stock in name_list:
            #print(stock)
            column_list="RelVolume,ATR"
            ldf=idb.execute_query(column_list, stock)
            #print(stock," ", ldf.loc[0,"ATR"], " ", ldf.loc[0,"RelVolume"])
            db=YFDB(db_name)
            stoch, action=db.ScanForStrategies(stock, time_frame, ldf.loc[0,"ATR"], ldf.loc[0,"RelVolume"], method)
            if action != "skip":
                resultstrat_dict[stock]=(stoch, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resultstrat_dict.items():
            df=database.execute_query(stock_column_list, key)
            stoch = pd.Series(value[0])
            df = df.assign(stoch=stoch.values)
            action= pd.Series(value[1])
            df = df.assign(action=action.values)
            df_list.append(df)   
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "har":
        count=0
        pbar.start()

        for stock in name_list:
            db=YFDB(db_name)
            stoch, action=db.ScanForHitAndRunPlays(stock, time_frame, method)
            if action != "skip":
                resulthar_dict[stock]=(stoch, action)
            pbar.update(count)
            count+=1
        pbar.finish
        df_list=[]
        for key, value in resulthar_dict.items():
            df=database.execute_query(stock_column_list, key)
            stoch = pd.Series(value[0])
            df = df.assign(stoch=stoch.values)
            action= pd.Series(value[1])
            df = df.assign(action=action.values)
            df_list.append(df)   
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
    elif scan_typ == "talib":
        count=0
        pbar.start()
        l1=[]

        for stock in name_list:
            db=YFDB(db_name)
            if candle == 1:
                db.ScanFor1CSPatternsTALib(stock, time_frame)
            elif candle == 2:
                stoch, str1=db.ScanFor2CSPatternsTALib(stock, time_frame)
                if str1 != "" :
                    l1.append((stock,stoch,str1))
            elif candle == 3:
                stoch, str1=db.ScanFor3CSPatternsTALib(stock, time_frame)
                if str1 != "" :
                    l1.append((stock,stoch,str1))
            elif candle == 4:
                stoch, str1=db.ScanFor4CSPatternsTALib(stock, time_frame)
                if str1 != "" :
                    l1.append((stock,stoch,str1))
                    
            """
            if action != "skip":
                resultmh_dict[stock]=(stoch, rsi, action)
            """
            pbar.update(count)
            count+=1
        pbar.finish
        print("\n\n\n")
        for item in l1:
            if (item[1]> 80 or item[1] < 20):
                print(item[0],"\t",item[1],"\t",item[2])
        """
        df_list=[]
        for key, value in resultmh_dict.items():
            df=database.execute_query(stock_column_list, key)
            stoch = pd.Series(value[0])
            df = df.assign(stoch=stoch.values)
            rsi = pd.Series(value[1])
            df = df.assign(rsi=rsi.values)
            action= pd.Series(value[2])
            df = df.assign(action=action.values)
            df_list.append(df)
        print(tabulate(pd.concat(df_list), headers='keys', tablefmt='psql'))
        """
        

    
    
if __name__ == "__main__":
    main(sys.argv[1:])         
#!/usr/bin/env python3
import PriceGetter as PG
from tabulate import tabulate
import sqlite3
from progressbar import *               # just a simple progress bar
import sys
import getopt


stock_list=["AAPL", "AMZN", "FB", "NFLX", "GOOGL",
            "TSLA", "NVDA", "TWTR", "BABA", "MSFT",
            "INTC", "AMD", "SQ"]



def process_file(fn,name_list):
    with open(fn) as f:
        name_list += f.read().splitlines() #same as readlines() but removes the \n from each line


def main(argv):
# df = PG.YahooFinanceHistory('AAPL', days_back=30).get_quote()
#  print(tabulate(df, headers='keys', tablefmt='psql'))
#  print(df.shape[0])
    list_file=""
    db_name=""
    num_days=0
    name_list=[]
    try:
        opts, args = getopt.getopt(argv,"hl:d:n:",["list_file=","db_name=","num_days="])
    except getopt.GetoptError:
        print ("""driver.py   -l <file containing list of stocks> 
                -d <name of the database>
                -n <number of days to collect prices on> """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""driver.py   -l <file containing list of stocks> 
            -d <name of the database>
            -n <number of days to collect prices on> """)    
            sys.exit()
        elif opt in("-l","--list_file"):
            list_file=arg
        elif opt in("-d","--db_name"):
            db_name=arg
        elif opt in("-n","--num_days"):
            num_days=int(arg)

    process_file(list_file, name_list)
    print(len(name_list))
  
    count=0
    widgets = ['Test: ', Percentage(), ' ', Bar(marker='*',left='[',right=']'),
               ' ', ETA(), ' ', FileTransferSpeed()] #see docs for other options
    pbar = ProgressBar(widgets=widgets, maxval=len(name_list))
    pbar.start()

    conn=sqlite3.connect(db_name)
    for stock in name_list:
        table_name=stock.lower()+"_values"
        df = PG.YahooFinanceHistory(stock, num_days).get_quote()
 #       print(tabulate(df, headers='keys', tablefmt='psql'))
        if num_days==1:
            df.to_sql(table_name, conn, if_exists="append")
        else:
            df.to_sql(table_name, conn, if_exists="replace")    
        pbar.update(count)
        count+=1
    pbar.finish

if __name__ == "__main__":
    main(sys.argv[1:])


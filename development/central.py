#!/usr/bin/env python3

import sys
import getopt
import helpers as hlp




def main(argv):
    collateral_file=""
    list_of_names=""
    etfs=set()
    stocks=set()
    names=set()

    try:
        opts, args = getopt.getopt(argv,"hf:l:",["collateral_file=","list_of_names="])
    except getopt.GetoptError:
        print ("""centralpy   -f <file containing  collateral info> 
                            -l <file with list of names to operate on>
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print ("""central.py  -f <file containing  collateral info>
                                -l <file with list of names to operate on>             
            """)    
            sys.exit()
        elif opt in("-f","--collateral_file"):
            collateral_file=arg
        elif opt in("-l","--list_of_names"):
            list_of_names=arg

    
    dict1=hlp.get_collateral_info(collateral_file)
    etf_files=dict1['list_of_etfs'].split(",")
    stock_files=dict1['list_of_stocks'].split(",")
    for file in etf_files:
        hlp.process_file(dict1['collateral_dir']+"/"+file, etfs)
    for file in stock_files:
        hlp.process_file(dict1['collateral_dir']+"/"+file, stocks)
    hlp.process_file(list_of_names, names)
    print(len(etfs))
    print(len(stocks))
    print(names)

if __name__ == "__main__":
    main(sys.argv[1:])         
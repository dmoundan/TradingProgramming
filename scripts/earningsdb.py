#!/usr/bin/env python3

import bs4 as bs
import pickle
import requests
import sqlite3
import sys
import getopt
import pandas as pd

#./earningsdb.py -l temp1.txt -q Q12019 -d edb.db   This is for initial run per stock.  For subsequent runs we need to use -a switch


months={'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6, 'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12}
month_dict={"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11, "Dec":12}

def alt_date_calculation(ticker):
    finviz_url="https://finviz.com/quote.ashx?t={stock}"
    url=finviz_url.format(stock=ticker)
    req = requests.get(url)
    str1=""
    if req.status_code == requests.codes.ok:
        soup = bs.BeautifulSoup(req.content, 'html.parser')
        table = soup.find_all(lambda tag: tag.name=='table')
        rows = table[8].findAll(lambda tag: tag.name=='tr')
        out=[]
        for i in range(len(rows)):
            td=rows[i].find_all('td')
            out=out+[x.text for x in td]
        count=0
        for elem in out:
            if elem == 'Earnings' :
                str1=out[count+1]
                break
            count+=1
    return(str1)


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return None

def get_data(name_list, quarter, conn,add_flag):
    web_address="https://earningswhispers.com/epsdetails/"
    for ticker in name_list:
        print(ticker)
        results={}
        table_name=ticker.lower()

        str1=web_address+ticker.lower()
        resp=requests.get(str1)
        soup=bs.BeautifulSoup(resp.text, "lxml")
        list1=[]
        table= soup.find('section', {'id':'surprise'})
        for row in table.findAll('div'):
            list1.append(row.text)
        for count,item in enumerate(list1):
            if item == "Reported Earnings":
                results['Reported EPS']=list1[count+1]
            elif item == "Earnings Whisper":
                results['Whisper EPS']=list1[count+1]
            elif item == "Consensus Estimate":
                results['Consensus EPS']=list1[count+1]
            elif item == "Reported Revenue":
                results['Reported Rev']=list1[count+1]
            elif item == "Revenue Estimate":
                results['Estimated Rev']=list1[count+1]

        table=soup.find('section', {'id':'mainbox'})
        for row in table.findAll('div',{'class': 'mbcontent'}):
            str1=str(row.text)
            if str1.find("AM") != -1:
                results['When'] ="BMO"
            elif str1.find("PM") != -1:
                results['When']="AMC"
            else:
                break
            list2=str1.split(",")
            list3=list2[1].strip().split(' ')
            day=list3[1]
            month=months[list3[0]]
            list4=list2[2].strip().split(' ')
            year=list4[0]
            dat=str(month)+"-"+day+"-"+year   
            results['Date']=dat
        results['Quarter']=quarter
        if 'Date' not in results:
            str2=alt_date_calculation(ticker)
            list5=str2.split(' ')
            results['When']=list5[2]
            day=list5[1]
            month=month_dict[list5[0]]
            year=quarter[2:]
            dat=str(month)+"-"+day+"-"+year   
            results['Date']=dat
        print(results)
        df=pd.DataFrame.from_records([results], index='Quarter')
        if add_flag == False:
            df.to_sql(table_name, conn, if_exists="replace")
        else:
            df.to_sql(table_name, conn, if_exists="append")    

def process_file(fn,name_list):
    with open(fn) as f:
        name_list += f.read().splitlines() #same as readlines() but removes the \n from each line

def main(argv):
    list_file=""
    db_name=""
    quarter=""
    add_flag=False
    name_list=[]

    try:
        opts, args = getopt.getopt(argv,"hl:d:q:a",["help", "list_file=","db_name=","quarter=","add"])
    except getopt.GetoptError:
        print ("""earningsdb.py   -l <file containing list of stocks> 
                -d <name of the database>
                -q <current quarter i.e Q12019>
                -a <add new entries for subsequent quarters>
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print ("""earningsdb.py   -l <file containing list of stocks> 
            -d <name of the database>
            -q <current quarter i.e. Q12019>
            -a <add new entries for subsequent quarters>
            """)    
            sys.exit()
        elif opt in("-l","--list_file"):
            list_file=arg
        elif opt in("-d","--db_name"):
            db_name=arg
        elif opt in("-q","--quarter"):
            quarter=arg
        elif opt in("-a","--add"):
            add_flag=True   

    process_file(list_file, name_list)  
    conn=create_connection(db_name)    
    get_data(name_list, quarter, conn, add_flag)
    conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])   






    
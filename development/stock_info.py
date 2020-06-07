#!/usr/bin/env python3


#stock_info.py -d 2020-05-04

from progressbar import *               # just a simple progress bar
from tabulate import tabulate
import finviz
import pdfkit as pdf
#import weasyprint as wep
import pandas as pd
from datetime import datetime, timedelta
import calendar
import getopt
import sys
import os

file1="collateral/top_interest.csv"
file2="collateral/top_interest2.csv"

#-- Helper Functions
def process_file(fn,name_list):
    with open(fn) as f:
        for elem in f.read().splitlines():
            name_list.add(elem)


def find_weekdays(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    start_of_week = date_obj - timedelta(days=date_obj.weekday())  # Monday
    #end_of_week = start_of_week + timedelta(days=4)  # Sunday
    rlist=[]
    kset=set()
    dict1={}
    for i in range(5):
        d={}
        new_day= start_of_week + timedelta(days=i)
        mstr=calendar.month_abbr[new_day.month]
        dstr=new_day.day
        dstr1="0"+str(dstr) if dstr < 10 else str(dstr)
        key=mstr+" "+dstr1
        d["key"]=key
        kset.add(key)
        dict1[key]=new_day.date()
        d['day']=new_day.strftime("%a")
        rlist.append(d)
    return (dict1,kset)


info={ 'Ticker' :[],
       'Company': [],
       'Industry': [],
       'Earnings' : [],
       'Time' : [],
       'Short Float' : [],
       'ATR' : [],
       'Beta' : []
    }

info1={ 'Ticker' :[],
       'Company': [],
       'Industry': [],
       'Earnings' : [],
       'Short Float' : [],
       'ATR' : [],
       'Beta' : [],
       '52W High' : [],
       'Rel Volume' : [] 
    }

info2={ 'Ticker' :[],
       'Company': [],
       'Industry': [],
       'Earnings' : [],
       'Short Float' : [],
       'ATR' : [],
       'Beta' : [],
       '52W High' : [],
       '52W Low' : [],
       'Rel Volume' : [],
       'Method' : [] 
    }

info3={ 'Ticker' :[],
       'Company': [],
       'Industry': [],
       'Earnings' : [],
       'Short Float' : [],
       'ATR' : [],
       'Beta' : [],
       '52W High' : [],
       '52W Low' : [],
       'Rel Volume' : [],
       'Method' : [] 
    }

info4={ 'Ticker' :[],
       'Company': [],
       'Industry': [],
       'Earnings' : [],
       'Short Float' : [],
       'ATR' : [],
       'Beta' : [],
       '52W High' : [],
       '52W Low' : [],
       'Rel Volume' : [],
       'Method' : [] 
    }

info5={ 'Ticker' :[],
       'Company': [],
       'Industry': [],
       'Earnings' : [],
       'Short Float' : [],
       'ATR' : [],
       'Beta' : [],
       '52W High' : [],
       '52W Low' : [],
       'Rel Volume' : [],
       'Float' : [],
       'Method' : [] 
    }

def main(argv):
    start_date=""
    earnings_flag=False
    sfloat_flag=False
    sf_factor=10
    methods_flag=False
    wk52_factor=-5
    x_factor=-5
    atr_factor=1.2
    rvol_factor=1.8
    beta_factor=1.0
    try:
        opts, args = getopt.getopt(argv,"hd:esf:mw:x:a:r:",["help","start_date=","earnings","sfloat","sf_factor=","methods","wk52_factor=","x_factor=", "atr_factor=","rvol_factor="])
    except getopt.GetoptError:
        print ("""stock_info.py   -d <start_date>
                -e (dicsover earnings names for the week starting from start date)  
                -s (discover names with short float > sfloat_factor)
                -f (% of short float we need to be above)
                -m (run strategies)
                -w (% away form 52 week high)
                -x (% away form 52 week high)/low)
                -a (factor for ATR)
                -r (factor for RVOL)
             """)
        sys.exit(2)
    for opt, arg in opts:
        if opt in ('-h', "--help"):
            print ("""stock_info.py   -d <start_date>
                -e (dicsover earnings names for the week starting from start date)     
                -s (discover names with short float > 10%)
                -f (% of short float we need to be above)
                -m (run strategies)
                -w (% away form 52 week high)
                -x (% away form 52 week high)/low)
                -a (factor for ATR)
                -r (factor for RVOL)
            """)    
            sys.exit()
        elif opt in("-d","--start_date"):
            start_date=arg
        elif opt in("-e","--earnings"):
            earnings_flag=True
        elif opt in("-s","--sfloat"):
            sfloat_flag=True
        elif opt in("-f","--sf_factor"):
            sf_factor=int(arg)
        elif opt in("-m","--methods"):
            methods_flag=True
        elif opt in("-w","--wk52_factor"):
            wk52_factor=-(int(arg))
        elif opt in("-x","--x_factor"):
            x_factor=-(int(arg))
        

        
    if earnings_flag == True and start_date == "":
        print(" A start date needs to be provided when the earnings flag is set. Exiting...")
        sys.exit(2)

    los=set()

    process_file(file1, los)
    process_file(file2, los)
    #print(sorted(los))


    dict1={}
    kset=set()
    if earnings_flag == True:
        (dict1,kset)=find_weekdays(start_date)

    count=0
    widgets = ['Test: ', Percentage(), ' ', Bar(marker='*',left='[',right=']'),
               ' ', ETA(), ' ', FileTransferSpeed()] #see docs for other options
    pbar = ProgressBar(widgets=widgets, maxval=len(los))
    pbar.start()

    for stock in los:
        d=finviz.get_stock(stock)
        pbar.update(count)
        count+=1
        if sfloat_flag == True:
            if d['Short Float'] != "-" :
                str1=d['Short Float'].replace('%','')
                if float(str1)  >= sf_factor :
                    info1['Ticker'].append(stock)
                    info1['Company'].append(d['Company'])
                    info1['Industry'].append(d['Industry'])
                    info1['Earnings'].append(d['Earnings'])
                    info1['Short Float'].append(d['Short Float'])
                    info1['ATR'].append(d['ATR'])
                    info1['Beta'].append(d['Beta'])
                    info1['52W High'].append(d['52W High'])
                    info1['Rel Volume'].append(d['Rel Volume'])

        if methods_flag == True:
            # short_float > 10% and within 15% of 52 week high
            if d['Short Float'] != "-" :
                str1=d['Short Float'].replace('%','')
                if float(str1)  >= sf_factor :
                    str2=d['52W High'].replace('%','')
                    if float(str2) > wk52_factor:
                        info2['Ticker'].append(stock)
                        info2['Company'].append(d['Company'])
                        info2['Industry'].append(d['Industry'])
                        info2['Earnings'].append(d['Earnings'])
                        info2['Short Float'].append(d['Short Float'])
                        info2['ATR'].append(d['ATR'])
                        info2['Beta'].append(d['Beta'])
                        info2['52W High'].append(d['52W High'])
                        info2['52W Low'].append(d['52W Low'])
                        info2['Rel Volume'].append(d['Rel Volume'])
                        info2['Method'] = "Squeeze"

            # Within 5% of 52 week low/high
            str3=d['52W High'].replace('%','')
            str4=d['52W Low'].replace('%','')
            if float(str3) > x_factor or float(str4) < -x_factor:
                info3['Ticker'].append(stock)
                info3['Company'].append(d['Company'])
                info3['Industry'].append(d['Industry'])
                info3['Earnings'].append(d['Earnings'])
                info3['Short Float'].append(d['Short Float'])
                info3['ATR'].append(d['ATR'])
                info3['Beta'].append(d['Beta'])
                info3['52W High'].append(d['52W High'])
                info3['52W Low'].append(d['52W Low'])
                info3['Rel Volume'].append(d['Rel Volume'])
                info3['Method'] = "Extremes"

            # 2nd play
            if float(d['Rel Volume']) > rvol_factor and abs(float(d['Price']) -float(d['Prev Close'])) > atr_factor * float(d['ATR']) : 
                info4['Ticker'].append(stock)
                info4['Company'].append(d['Company'])
                info4['Industry'].append(d['Industry'])
                info4['Earnings'].append(d['Earnings'])
                info4['Short Float'].append(d['Short Float'])
                info4['ATR'].append(d['ATR'])
                info4['Beta'].append(d['Beta'])
                info4['52W High'].append(d['52W High'])
                info4['52W Low'].append(d['52W Low'])
                info4['Rel Volume'].append(d['Rel Volume'])
                info4['Method'] = "2ndPlay"

            #Low float stocks
            if d['Beta'] != "-" and float(d['Beta']) >= beta_factor:
                shs_outstd=d['Shs Outstand']
                if shs_outstd.find('M') != -1:
                    info5['Ticker'].append(stock)
                    info5['Company'].append(d['Company'])
                    info5['Industry'].append(d['Industry'])
                    info5['Earnings'].append(d['Earnings'])
                    info5['Short Float'].append(d['Short Float'])
                    info5['ATR'].append(d['ATR'])
                    info5['Beta'].append(d['Beta'])
                    info5['52W High'].append(d['52W High'])
                    info5['52W Low'].append(d['52W Low'])
                    info5['Rel Volume'].append(d['Rel Volume'])
                    info5['Float'].append(d['Shs Outstand'])
                    info5['Method'] = "LowFloat"



        if earnings_flag == True:
            earnings=d['Earnings']
            lst=earnings.split(" ")
            if len(lst) < 3:
                continue
            key=lst[0]+" "+lst[1]
            tm=lst[2]
            if key in kset:
                info['Ticker'].append(stock)
                info['Company'].append(d['Company'])
                info['Industry'].append(d['Industry'])
                info['Earnings'].append(dict1[key])
                info['Time'].append(tm)
                info['Short Float'].append(d['Short Float'])
                info['ATR'].append(d['ATR'])
                info['Beta'].append(d['Beta'])
            else:
                continue
    if earnings_flag == True:
        df=pd.DataFrame(info)
        df.sort_values(by='Earnings', ascending=True, inplace=True)
        #print(df)
        
        temp_html="earnings_"+start_date+".html"
        df.to_html(temp_html, index=False)
        out_file="earnings_"+start_date+".pdf"
        pdf.from_file(temp_html,out_file)
        os.remove(temp_html)
        #wep.HTML(temp_html).write_pdf(out_file)

    if sfloat_flag == True:
        df=pd.DataFrame(info1)
        df.sort_values(by='Short Float', ascending=False, inplace=True)
        #print(df)
        
        temp_html="sfloat.html"
        df.to_html(temp_html, index=False)
        out_file="sfloat.pdf"
        pdf.from_file(temp_html,out_file)
        os.remove(temp_html)
        #wep.HTML(temp_html).write_pdf(out_file)

    if methods_flag == True:
        df1=pd.DataFrame(info2)
        print(tabulate(df1, headers='keys', tablefmt='psql'))
        df2=pd.DataFrame(info3)
        print(tabulate(df2, headers='keys', tablefmt='psql'))
        df3=pd.DataFrame(info4)
        print(tabulate(df3, headers='keys', tablefmt='psql'))
        df4=pd.DataFrame(info5)
        print(tabulate(df4, headers='keys', tablefmt='psql'))

if __name__ == "__main__":
    main(sys.argv[1:])         
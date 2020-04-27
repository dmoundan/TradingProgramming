#!/usr/bin/env python3
import csv
import json
import requests
from bs4 import BeautifulSoup
from collections import defaultdict
from progressbar import *               # just a simple progress bar
import re
from io import StringIO
from datetime import datetime, timedelta
import pandas as pd
from tabulate import tabulate




stock_list=[]
etf_list=[]
sectors=defaultdict(list)
industries=set()


class YahooFinanceHistory:
    timeout = 2
    crumb_link = 'https://finance.yahoo.com/quote/{0}/history?p={0}'
    crumble_regex = r'CrumbStore":{"crumb":"(.*?)"}'
    quote_link = 'https://query1.finance.yahoo.com/v7/finance/download/{quote}?period1={dfrom}&period2={dto}&interval=1d&events=history&crumb={crumb}'

    def __init__(self, symbol, days_back=7):
        self.symbol = symbol
        self.session = requests.Session()
        self.dt = timedelta(days=days_back)

    def get_crumb(self):
        response = self.session.get(self.crumb_link.format(self.symbol), timeout=self.timeout)
        response.raise_for_status()
        match = re.search(self.crumble_regex, response.text)
        if not match:
            raise ValueError('Could not get crumb from Yahoo Finance')
        else:
            self.crumb = match.group(1)

    def get_quote(self):
        if not hasattr(self, 'crumb') or len(self.session.cookies) == 0:
            self.get_crumb()
        now = datetime.utcnow()
        dateto = int(now.timestamp())
        datefrom = int((now - self.dt).timestamp())
        url = self.quote_link.format(quote=self.symbol, dfrom=datefrom, dto=dateto, crumb=self.crumb)
        response = self.session.get(url)
        response.raise_for_status()
        return pd.read_csv(StringIO(response.text), parse_dates=['Date'])



def process_files(filename):
    with open(filename) as f:
        data = f.read().splitlines() #same as readlines() but removes the \n from each line
#process data
    new_data = data[1:-1] # remove the first and last line
    with open(filename, 'w') as f:
        f.write('\n'.join(new_data)) #add a newline character between every line, then write

def read_files(filename, list_name):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            list_name.append(row[0].replace(".","-"))

def get_prices(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/history?period1=1385100000&period2=1542866400&interval=1d&filter=history&frequency=1d"
    resp=requests.get(url)
#http_respone 200 means OK status
    if resp.status_code==200:
        soup=BeautifulSoup(resp.text,'html.parser')
        l=soup.find("table")
        rows=l.find_all("tr",{"class":"BdT Bdc($c-fuji-grey-c) Ta(end) Fz(s) Whs(nw)"})
        print(len(rows))
        for row in rows:
            data=row.find_all("td")
            if len(data) == 7:
                date=data[0].get_text()
                open=data[1].get_text()
                high=data[2].get_text()
                low =data[3].get_text()
                close=data[4].get_text()
                volume=data[6].get_text()
                print(f"{date}")
                print(f"{open}")
    else:
        print("Error")


def get_profile(ticker):
    url=f"https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}"
    resp=requests.get(url)
#http_respone 200 means OK status
    if resp.status_code==200:
#        print(f"Successfully opened the web page for {ticker}")
        soup=BeautifulSoup(resp.text,'html.parser')
        l=soup.find("p",{"class":"D(ib) Va(t)"})
        count=0
        sector=""
        industry=""
        for i in l.findAll("strong"):
            if count == 0 :
                sector = i.text
            elif count == 1:
                industry = i.text
            else:
                break
            count+=1
#        print(f"{ticker} Sector: {sector}  Industry: {industry}")
        if industry not in industries:
            sectors[sector].append(industry)
        industries.add(industry)
    else:
        print("Error")

def main():
    process_files("option_stock.csv")
    process_files("option_etf.csv")

    read_files("option_stock.csv", stock_list)
    read_files("option_etf.csv", etf_list)
    widgets = ['Test: ', Percentage(), ' ', Bar(marker='*',left='[',right=']'),
           ' ', ETA(), ' ', FileTransferSpeed()] #see docs for other options
#    pbar = ProgressBar(widgets=widgets, maxval=len(stock_list))
#    pbar.start()
    df = YahooFinanceHistory('AAPL', days_back=365).get_quote()
#    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#        print(df)
    print(tabulate(df, headers='keys', tablefmt='psql'))
    count=0
    """
    for stock in stock_list:
        get_profile(stock)
#        get_prices(stock)
        pbar.update(count)
        count+=1
    pbar.finish()
    with open("sectors.json", "w") as fp:
       json.dump(sectors , fp, indent=4, sort_keys=True)
"""
main()

#!/usr/bin/env python3

import time
import calendar
import sys
import json
import os
import time
import requests
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
import numpy as np
from tabulate import tabulate
from datetime import datetime
from io import StringIO


from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5 import QtCore, QtWidgets

from functools import partial

"""
from PyQt5.QtWidgets import QMainWindow, QWidget, QPushButton, QAction, QMenu, QProgressBar, QLabel
from PyQt5.QtCore import QSize    
from PyQt5.QtGui import QIcon
"""
from yahoo_earnings_calendar import YahooEarningsCalendar
from datetime import datetime, timezone

################################## Generics  ##################################################
day_dict={0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
month_dict={"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11, "Dec":12}
month_dict_inv={1:"Jan", 2:"Feb", 3:"Mar", 4:"Apr", 5:"May", 6:"Jun", 7:"Jul", 8:"Aug", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dec"}

class pandasModel(QAbstractTableModel):
    
    def __init__(self, data):
        QAbstractTableModel.__init__(self)
        self._data = data

    def rowCount(self, parent=None):
        return self._data.shape[0]

    def columnCount(self, parnet=None):
        return self._data.shape[1]

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.iloc[index.row(), index.column()])
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._data.columns[col]
        return None

################################## Apps      ##################################################

class FutureEarnings:

    def __init__(self, name_list, start_date, end_date):
        self.name_list=name_list
        self.start_date=start_date
        self.end_date=end_date
        #print(start_date)
        #print(end_date)

    def get_future_earnings(self):
        date_from = datetime.strptime(self.start_date, '%b %d %Y')   
        date_to = datetime.strptime(self.end_date, '%b %d %Y') 

        yec = YahooEarningsCalendar()
        el= yec.earnings_between(date_from, date_to) 

        df_list=[]

        for elem in el:
            if elem['ticker'] in self.name_list:
                str1=elem['startdatetime']
                l1=str1.split("T")
                date=l1[0]
                str2=elem['startdatetimetype']
                type=""
                if str2 == "BMO" or str2 == "AMC":
                    type=str2
                else:
                    l2=l1[1].split(":")
                    if int(l2[0]) <= 16:
                        type = "BMO"
                    else: type="AMC"        
                l2=date.split("-")
                day=day_dict[calendar.weekday(int(l2[0]),int(l2[1]),int(l2[2]))]            
                #print("|",elem['ticker'].ljust(10),elem['companyshortname'].ljust(40),day.ljust(4),date.ljust(15),type.ljust(8),"|")
                t=(elem['ticker'], elem['companyshortname'], day, date, type)
                df_list.append(t)
        arr=np.array(df_list)
        df = pd.DataFrame(arr, columns=['Ticker', 'Company', 'Day', 'Date', 'When'])
        model = pandasModel(df)
        #print(tabulate(df, headers='keys', tablefmt='psql'))
        return model


################################## DataBases ##################################################

class DataBase:
    
    def __init__(self, name):
        self.name=name
        try:
            self.conn = sqlite3.connect(self.name)
        except Error as e:
            print(e)
        self.cur=self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()
################################## Detailed Past Earnings DB #################################
class DetailedPastEarningsDB(DataBase):
    name_list=[]

    def __init__(self, name, list_file, quarter, add_flag):        
        super(DetailedPastEarningsDB, self).__init__(name)
        self.list_file=list_file
        self.quarter=quarter
        self.add_flag=add_flag
        self.process_file(self.list_file)


    def process_file(self,fn):
        with open(fn) as f:
            self.name_list += f.read().splitlines() #same as readlines() but removes the \n from each line

    def alt_date_calculation(self, ticker):
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

def get_data(self):
    web_address="https://earningswhispers.com/epsdetails/"
    for ticker in self.name_list:
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
        results['Quarter']=self.quarter
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
        if self.add_flag == False:
            df.to_sql(table_name, self.conn, if_exists="replace")
        else:
            df.to_sql(table_name, self.conn, if_exists="append")       
            
             
################################## Past Earnings DB ##########################################

class PastEarningsDB(DataBase):
    earnings_url="https://api.earningscalendar.net/?date={date}"

    def __init__(self, name):
        super(PastEarningsDB, self).__init__(name)

    def find_next_index(self, table_name):
        query=f"select count(*) from {table_name}"
        self.cur.execute(query)
        results=self.cur.fetchone()
        return results[0]

    def does_table_exist(self, table_name):
        query=f"select name from sqlite_master where name='{table_name}'"
        self.cur.execute(query)
        return bool(self.cur.fetchone())



    def populate_database(self, results_dict, update_flag):
        table_name=""
        for key, value in results_dict.items():
            stock=key.replace("-","_")
            table_name=stock.lower()+"_earnings"
            if update_flag==True:
                ml=[];
                l=len(value['date'])
                if self.does_table_exist( table_name):
                    num=self.find_next_index(table_name)
                    #print(num)
                    for i in range(l):
                        ml.append(num)
                        num+=1
                    df=pd.DataFrame(value, index=ml)
                    df.to_sql(table_name, self.conn, if_exists="append")
                else: 
                    df=pd.DataFrame(value)
                    df.to_sql(table_name, self.conn, if_exists="replace")  
            else:    
                df=pd.DataFrame(value)
                df.to_sql(table_name, self.conn, if_exists="replace")  

    def obtain_historical_earnings_data(self,l1,l2,name_list, update_flag, countChanged):
        results_dict={}
        start_year=l1[2]
        start_month=month_dict[l1[0]]
        start_day=l1[1]
        end_year=l2[2]
        end_month=month_dict[l2[0]]
        end_day=l2[1]
        year=int(start_year)
        count=0
        while (year <= int(end_year)):
            month=start_month
            if (year == int(end_year)):
                limit_month=end_month
            else:
                limit_month = 12    
            while (month <= limit_month):
                day=int(start_day)
                limit_day=31
                if (year == int(end_year) and month == end_month):
                    limit_day=int(end_day)
                elif month == 4 or month == 6 or month == 9 or month == 11:
                    limit_day=30
                elif month == 2:
                    if calendar.isleap(int(year)):
                        limit_day=29
                    else:
                        limit_day=28  
                #print(limit_day)          
                while (day <= limit_day):
                    req_date=str(year)+str(month).zfill(2)+str(day).zfill(2)
                    req_date1=str(year)+"-"+str(month).zfill(2)+"-"+str(day).zfill(2)
                    print("\n",req_date)
                    session = requests.Session()
                    url=self.earnings_url.format(date=req_date)
                    #print(url)
                    response = session.get(url)
                    response.raise_for_status()
                    df=pd.read_json(StringIO(response.text))
                    for index, row in df.iterrows():
                        if row['ticker'] in name_list:
                            if row['ticker'] in results_dict:
                                results_dict[row['ticker']]['date'].append(req_date1)
                                results_dict[row['ticker']]['when'].append(row["when"].upper())
                            else:
                                results_dict[row['ticker']]={}
                                results_dict[row['ticker']]['date']=[] 
                                results_dict[row['ticker']]['when']=[]   
                                results_dict[row['ticker']]['date'].append(req_date1)
                                results_dict[row['ticker']]['when'].append(row["when"].upper())
                            #print (row["ticker"]," ", row["when"])
                            print(".", end=" ")
                    #print(tabulate(df, headers='keys', tablefmt='psql'))
                    day+=1  
                    time.sleep(0.25)  
                    #here need to store result in a dict of dicts, then create a data frame for each contained dict
                    #and store it as a table per stock in the DB. I should also provide the capability of updating
                    #the DB
                month+=1
                countChanged.emit(count)
                count+=1
            year+=1
        self.populate_database(results_dict, update_flag)

################################## Retrieval DB ##############################################

class RetrievalDB(DataBase):
    def __init__(self, name):
        super(RetrievalDB, self).__init__(name)

    def retrieve(self, query):
        return pd.read_sql_query(query, self.conn)




################################## DailyInfo DB ###############################################

class DailyInfoDB(DataBase):

    finviz_url="https://finviz.com/quote.ashx?t={ticker}"

    td1={'Ticker':[], 'Company':[], 'Sector':[], 'Industry':[], 'Country':[], 'StockIndex':[], 'PerfWeek': [],
         'PerfMonth':[], 'ShortFloat': [], 'PerfQuarter': [], 'PerfHalfY':[], 'TargetPrice':[],
         'PerfYear':[], 'Range52W':[], 'PerfYTD':[], 'High52W':[], 'Low52W':[], 'ATR':[], 'RSI14':[],
         'Volatility':[], 'RelVolume':[], 'Earnings':[], 'AvgVolume':[], 'Price':[], 'Volume':[], 'Change':[],
         'SMA20':[], 'SMA50':[], 'SMA200':[], 'Price':[], 'PrevClose':[] }

    

    def __init__(self, name):
        super(DailyInfoDB, self).__init__(name)

    def sanitize_string(self, str):
        #print(str)
        if str != "-":
            s1=str.replace("%","")
            return float(s1)
        else:
            return str

    def sanitize_string1(self, str):
        #print(str)
        if str != "-":
            return float(str)
        else:
            return str

    def createDIDB(self, name_list, table_name, countChanged):
        td1={'Ticker':[], 'Company':[], 'Sector':[], 'Industry':[], 'Country':[], 'StockIndex':[], 'PerfWeek': [],
         'PerfMonth':[], 'ShortFloat': [], 'PerfQuarter': [], 'PerfHalfY':[], 'TargetPrice':[],
         'PerfYear':[], 'Range52W':[], 'PerfYTD':[], 'High52W':[], 'Low52W':[], 'ATR':[], 'RSI14':[],
         'Volatility':[], 'RelVolume':[], 'Earnings':[], 'AvgVolume':[], 'Price':[], 'Volume':[], 'Change':[],
         'SMA20':[], 'SMA50':[], 'SMA200':[], 'Price':[], 'PrevClose':[] }
        count=0
        for stock in name_list:
            print(table_name+" : "+stock)
            url=self.finviz_url.format(ticker=stock)
            req = requests.get(url)
            if req.status_code == requests.codes.ok:
                soup = BeautifulSoup(req.content, 'html.parser')
                table = soup.find_all(lambda tag: tag.name=='table')
                rows = table[6].findAll(lambda tag: tag.name=='tr')
                out=[]
                for i in range(len(rows)):
                    td=rows[i].find_all('td')
                    out=out+[x.text for x in td]
                #print(out)   
                td1['Ticker'].append(stock)
                td1['Company'].append(out[1]) 
            
                l1=out[2].split("|")
                td1['Sector'].append(l1[0])  
                td1['Industry'].append(l1[1])
                td1['Country'].append(l1[2])
            
                rows = table[8].findAll(lambda tag: tag.name=='tr')
                out=[]
                for i in range(len(rows)):
                    td=rows[i].find_all('td')
                    out=out+[x.text for x in td]
                count1=0
                for elem in out:
                    if elem == 'Index' :
                        td1['StockIndex'].append(out[count1+1])
                    elif elem == 'Perf Week':
                        td1['PerfWeek'].append(self.sanitize_string(out[count1+1]))
                    elif elem == 'Perf Month':
                        td1['PerfMonth'].append(self.sanitize_string(out[count1+1]))
                    elif elem ==  'Short Float' :
                        td1['ShortFloat'].append(self.sanitize_string(out[count1+1]))
                    elif elem ==  'Perf Quarter':
                        td1['PerfQuarter'].append(self.sanitize_string(out[count1+1]))
                    elif elem ==  'Perf Half Y':
                        td1['PerfHalfY'].append(self.sanitize_string(out[count1+1]))
                    elif elem == 'Target Price' :
                        td1['TargetPrice'].append(self.sanitize_string1(out[count1+1]))
                    elif elem ==  'Perf Year':
                        td1['PerfYear'].append(self.sanitize_string(out[count1+1]))
                    elif elem ==  '52W Range':
                        td1['Range52W'].append(out[count1+1])
                    elif elem == 'Perf YTD':
                        td1['PerfYTD'].append(self.sanitize_string(out[count1+1]))
                    elif elem == '52W High':
                        td1['High52W'].append(self.sanitize_string(out[count1+1]))
                    elif elem == '52W Low':
                        td1['Low52W'].append(self.sanitize_string(out[count1+1]))
                    elif elem == 'ATR':
                        td1['ATR'].append(self.sanitize_string1(out[count1+1]))
                    elif elem == 'RSI (14)':
                        td1['RSI14'].append(self.sanitize_string1(out[count1+1]))
                    elif elem == 'Volatility':
                        td1['Volatility'].append(out[count1+1])
                    elif elem == 'Rel Volume':
                        td1['RelVolume'].append(self.sanitize_string1(out[count1+1]))
                    elif elem == 'Earnings' :
                        td1['Earnings'].append(out[count1+1])
                    elif elem == 'Avg Volume':
                        td1['AvgVolume'].append(out[count1+1])
                    elif elem == 'Price':
                        td1['Price'].append(self.sanitize_string1(out[count1+1]))
                    elif elem == 'SMA20':
                        td1['SMA20'].append(self.sanitize_string(out[count1+1]))
                    elif elem == 'SMA50':
                        td1['SMA50'].append(self.sanitize_string(out[count1+1]))
                    elif elem == 'SMA200':
                        td1['SMA200'].append(self.sanitize_string(out[count1+1]))
                    elif elem == 'Volume':
                        td1['Volume'].append(out[count1+1])
                    elif elem == 'Change':
                        td1['Change'].append(self.sanitize_string(out[count1+1]))
                    elif elem ==  'Price':
                        td1['Price'].append(self.sanitize_string1(out[count1+1]))
                    elif elem ==  'Prev Close':
                        td1['PrevClose'].append(self.sanitize_string1(out[count1+1]))
                    count1+=1
            countChanged.emit(count)
            count+=1
            time.sleep(0.20)

        df=pd.DataFrame(td1)
        df.to_sql(table_name, self.conn, if_exists="replace")  
        #csv_filename="stock_info.csv"
        #if etf_flag == True:
        #csv_filename="etf_info.csv"
        #df.to_csv(csv_filename, encoding='utf-8', index=False)


#################################### Threads ##################################################

class External(QThread):
    """
    Runs a counter thread.
    """
    countChanged = pyqtSignal(int)

    def __init__(self, dbname, table_name, lst):
        QThread.__init__(self)
        self.dbname=dbname
        self.table_name=table_name
        self.lst=lst

    def run(self):
        didb=DailyInfoDB(self.dbname)
        didb.createDIDB(self.lst,self.table_name, self.countChanged)
        


class External1(QThread):
    """
    Runs a counter thread.
    """
    countChanged = pyqtSignal(int)

    def __init__(self, dbname, lst, start_date, end_date, update_flag):
        QThread.__init__(self)
        self.dbname=dbname
        self.start_date=start_date
        self.end_date=end_date
        self.update_flag=update_flag
        self.lst=lst

    def run(self):
        pedb=PastEarningsDB(self.dbname)
        pedb.obtain_historical_earnings_data(self.start_date.split(), self.end_date.split(), self.lst, self.update_flag, self.countChanged)

        

################################################ Widgets ###############################################

class subwindow(QWidget):
    def createWindow(self,WindowWidth,WindowHeight):
       parent=None
       super(subwindow,self).__init__(parent)
       self.setWindowFlags(QtCore.Qt.WindowStaysOnTopHint)
       self.resize(WindowWidth,WindowHeight)


class DateWidget(QDateEdit):
    """docstring for DateWidget"""
    def __init__(self, parent=None):
        super(DateWidget, self).__init__(parent)
        self.parent = parent

        self.setDate(QDate.currentDate())
        self.setCalendarPopup(True)
        self.setDisplayFormat('MM/dd/yyyy')
        self.cal = self.calendarWidget()
        self.cal.setFirstDayOfWeek(Qt.Monday)
        self.cal.setHorizontalHeaderFormat(QCalendarWidget.SingleLetterDayNames)
        self.cal.setVerticalHeaderFormat(QCalendarWidget.NoVerticalHeader)
        self.cal.setGridVisible(True)

#############################################################################################################


class MainWindow(QMainWindow):

    #Settings
    CollateralDir="./DB_backups/"
    OutputDir="./TradeAnalyzerOutputs/"
    InputDir="./TraderAnalyzerInputs/"
    StocksList=CollateralDir+"stocks.list"
    ETFList=CollateralDir+"etf.list"
    SIjson=CollateralDir+"classification.json"
    EDIDBName="info_etf.db"
    SDIDBName="info_stock.db"
    PEarningsDBName="earnings.db"
    DEarningsDBName="edb.db"
    Sectors=[]
    Industries=[]
    SIDict={}
    ITDict={}
    TotalStockList=set()
    TotalETFList=set()
    start_date=""
    end_date=""
    ETFColumns="Ticker, Company, PerfWeek, PerfMonth, PerfQuarter, PerfHalfY, PerfYear, PerfYTD, ATR, High52W, Low52W, RSI14, SMA20, SMA50, SMA200, RelVolume, AvgVolume, Volume, Change, Price, PrevClose"
    StockColumns="Ticker, Company, Sector, Industry, Country, PerfWeek, PerfMonth, PerfQuarter, PerfHalfY, PerfYear, PerfYTD, ATR, High52W, Low52W, ShortFloat, RSI14, SMA20, SMA50, SMA200, RelVolume, AvgVolume, Volume, Change, Price, PrevClose"

    def __init__(self):
        QMainWindow.__init__(self)

        self.setMinimumSize(QSize(1000, 300))    
        self.setWindowTitle("TradeAnalyzer") 

        ###################### Collateral Preparation ##################

        if not os.path.exists(self.OutputDir):
            os.mkdir(self.OutputDir)


        self.getSIInfo()
        self.process_file(self.StocksList, self.TotalStockList)
        self.process_file(self.ETFList, self.TotalETFList)
        print (len(self.TotalStockList))
        print(len(self.TotalETFList))

        
        
        ######################   MenuBar #############################

        menuBar = self.menuBar()
        menuBar.setNativeMenuBar(False)

        
        self.createSystemMenu(menuBar)
        self.createClassificationMenu(menuBar)
        self.createEarningsMenu(menuBar)
        self.createDataBaseMenu(menuBar)

        ###############################################################

        """
        # Add button widget
        pybutton = QPushButton('Pyqt', self)
        pybutton.clicked.connect(self.clickMethod)
        pybutton.resize(100,32)
        pybutton.move(130, 30)        
        pybutton.setToolTip('This is a tooltip message.')  

        # Create new action
        newAction = QAction(QIcon('new.png'), '&New', self)        
        newAction.setShortcut('Ctrl+N')
        newAction.setStatusTip('New document')
        newAction.triggered.connect(self.newCall)

        # Create new action
        openAction = QAction(QIcon('open.png'), '&Open', self)        
        openAction.setShortcut('Ctrl+O')
        openAction.setStatusTip('Open document')
        openAction.triggered.connect(self.openCall)

        # Create exit action
        exitAction = QAction(QIcon('exit.png'), '&Exit', self)        
        exitAction.setShortcut('Ctrl+Q')
        exitAction.setStatusTip('Exit application')
        exitAction.triggered.connect(self.exitCall)


        # Create menu bar and add action
        menuBar = self.menuBar()
        menuBar.setNativeMenuBar(False)
        fileMenu = menuBar.addMenu('&File')
        fileMenu.addAction(newAction)
        fileMenu.addAction(openAction)
        fileMenu.addAction(exitAction)
        """

    def process_file(self, fn,name_list):
        with open(fn) as f:
            for elem in f.read().splitlines():
                name_list.add(elem)

    def getSIInfo(self):
        d1={}
        with open(self.SIjson, "r") as fp:
            d1=json.load(fp)

        l1=list(d1.keys())
        self.Sectors+=l1
        l2=[]
        for elem in d1.keys():
            self.SIDict[elem]=list((d1[elem]).keys())
            l2+=d1[elem]
            for elem1 in d1[elem].keys():
                self.ITDict[elem1]=d1[elem][elem1]
        self.Industries+=l2

    def createEarningsMenu(self, menuBar):
        earnAct = QAction('Future Earnings', self)
        menuBar.addAction(earnAct)
        earnAct.triggered.connect(self.earnCall)

    def earnCall(self):
        #print("Earnings")
        self.createASubwindow1(1)

    def createASubwindow2(self):
        self.mySubwindow3=subwindow()
        self.mySubwindow3.createWindow(300,100)
        self.mySubwindow3.setWindowTitle("Detailed Past Earnings") 
        vbox = QVBoxLayout()
        hbox = QHBoxLayout()
        vbox1 = QVBoxLayout()
        self.mySubwindow3.l1=QLabel("Quarter", self.mySubwindow3)
        self.mySubwindow3.l1.setAlignment(Qt.AlignLeft)
        vbox1.addWidget(self.mySubwindow3.l1)
        vbox1.addStretch()
        self.mySubwindow3.combo = QComboBox(self.mySubwindow3)
        self.mySubwindow3.combo.addItem("Q1")
        self.mySubwindow3.combo.addItem("Q2")
        self.mySubwindow3.combo.addItem("Q3")
        self.mySubwindow3.combo.addItem("Q4")
        self.mySubwindow3.combo.activated[str].connect(self.onActivated)
        vbox1.addWidget(self.mySubwindow3.combo)

        vbox2 = QVBoxLayout()
        self.mySubwindow3.l2=QLabel("Year", self.mySubwindow3)
        self.mySubwindow3.l1.setAlignment(Qt.AlignLeft)
        vbox2.addWidget(self.mySubwindow3.l2)
        vbox2.addStretch()
        self.mySubwindow3.sbox = QSpinBox(self.mySubwindow3)
        self.mySubwindow3.sbox.setMinimum(2019)
        self.mySubwindow3.sbox.setMaximum(2029)
        self.mySubwindow3.sbox.singleStep()
        self.mySubwindow3.sbox.valueChanged.connect(self.valuechange)

        vbox2.addWidget(self.mySubwindow3.sbox)
        hbox.addLayout(vbox1)
        hbox.addLayout(vbox2)
        self.mySubwindow3.button1 = QPushButton('Go', self.mySubwindow3)
        self.mySubwindow3.button1.setFixedWidth(50)
        vbox.addLayout(hbox)
        vbox.addStretch()
        vbox.addWidget(self.mySubwindow3.button1)
        self.mySubwindow3.l2=QLabel("Obtaining Detailed Past Earnings Information...", self.mySubwindow3)
        self.mySubwindow3.l2.setAlignment(Qt.AlignLeft)
        vbox.addWidget(self.mySubwindow3.l2)
        vbox.addStretch()
        self.mySubwindow3.progress = QProgressBar(self.mySubwindow3)
        self.mySubwindow3.progress.setGeometry(0, 0, 300, 50)
        self.mySubwindow3.progress.setAlignment(Qt.AlignCenter)
        self.mySubwindow3.button1.clicked.connect(self.onButtonClick4)
        #self.mySubwindow3.progress.setMinimum(0)
        #self.mySubwindow3.progress.setMaximum(len(self.TotalETFList))
        vbox.addWidget(self.mySubwindow3.progress)
        self.mySubwindow3.setLayout(vbox)

        self.mySubwindow3.show()

    def onActivated(self,str):
        print(str)

    def valuechange(self):
        print(str(self.mySubwindow3.sbox.value()))

    def onButtonClick4(self):
        print("Go Button")


    def createASubwindow1(self, flag):
        now = datetime.now()
        self.start_date=month_dict_inv[now.month]+" "+str(now.day)+" "+str(now.year)
        self.end_date=self.start_date
        self.mySubwindow2=subwindow()
        self.mySubwindow2.createWindow(200,100)
        if flag == 1:
            self.mySubwindow2.setWindowTitle("Future Earnings") 
        elif flag == 2 or flag == 3 :
            self.mySubwindow2.setWindowTitle("Past Earnings") 
        vbox = QVBoxLayout()
        hbox = QHBoxLayout()
        vbox1 = QVBoxLayout()
        self.mySubwindow2.l1=QLabel("Start Date", self.mySubwindow2)
        self.mySubwindow2.l1.setAlignment(Qt.AlignLeft)
        self.mySubwindow2.date1=DateWidget(self.mySubwindow2)
        self.mySubwindow2.date1.cal.clicked[QtCore.QDate].connect(self.showSDate)

        vbox1.addWidget(self.mySubwindow2.l1)
        vbox1.addStretch()
        vbox1.addWidget(self.mySubwindow2.date1)
        vbox2 = QVBoxLayout()
        self.mySubwindow2.l2=QLabel("End Date", self.mySubwindow2)
        self.mySubwindow2.l2.setAlignment(Qt.AlignLeft)
        self.mySubwindow2.date2=DateWidget(self.mySubwindow2)
        self.mySubwindow2.date2.cal.clicked[QtCore.QDate].connect(self.showEDate)

        vbox2.addWidget(self.mySubwindow2.l2)
        vbox2.addStretch()
        vbox2.addWidget(self.mySubwindow2.date2)
        self.mySubwindow2.button1 = QPushButton('Go', self.mySubwindow2)
        self.mySubwindow2.button1.setFixedWidth(50)
        if flag == 2 or flag == 3:
            self.mySubwindow2.l1=QLabel("Obtaining Past Earnings Information...", self.mySubwindow2)
            self.mySubwindow2.l1.setAlignment(Qt.AlignLeft)
            vbox.addWidget(self.mySubwindow2.l1)
            vbox.addStretch()
            self.mySubwindow2.progress = QProgressBar(self.mySubwindow2)
            self.mySubwindow2.progress.setGeometry(0, 0, 300, 50)
            self.mySubwindow2.progress.setAlignment(Qt.AlignCenter)
            #self.mySubwindow3.progress.setMinimum(0)
            #self.mySubwindow3.progress.setMaximum(len(self.TotalETFList))
            vbox.addWidget(self.mySubwindow2.progress)
        if flag == 1:
            self.mySubwindow2.button1.clicked.connect(self.onButtonClick2)
        elif flag == 2:
            self.mySubwindow2.button1.clicked.connect(partial(self.onButtonClick3,0))
        elif flag == 3:
            self.mySubwindow2.button1.clicked.connect(partial(self.onButtonClick3,1))
        hbox.addLayout(vbox1)
        hbox.addLayout(vbox2)
        vbox.addLayout(hbox)
        vbox.addStretch()
        vbox.addWidget(self.mySubwindow2.button1)
        self.mySubwindow2.setLayout(vbox)
        self.mySubwindow2.show()
        #print(self.mySubwindow2.date1.date().year())
        #print(self.mySubwindow2.date1.date().month())
        

    def onButtonClick2(self):
        #print("Go Button")
        self.mySubwindow2.close()
        FE=FutureEarnings(self.TotalStockList, self.start_date, self.end_date)
        model=FE.get_future_earnings()
        self.view = QTableView()
        self.view.setModel(model)
        self.view.resize(500, 400)
        self.view.show()

    def onButtonClick3(self, flag):
        #print("Go Button")
        dbname=self.OutputDir+self.PEarningsDBName
        self.calc2 = External1(dbname, self.TotalStockList, self.start_date, self.end_date, flag)
        self.calc2.countChanged.connect(self.onCountChanged2)
        self.calc2.start()

    def onCountChanged2(self, value):
        self.mySubwindow2.progress.setValue(value)    

    def showSDate(self, date):	
        self.start_date=(date.toString())[4:]

    def showEDate(self, date):
        self.end_date=(date.toString())[4:]
    

    def createClassificationMenu(self, menuBar):
        classifyMenu=menuBar.addMenu('Classification')
        sectorMenu=QMenu('Sectors', self)
        industryMenu=QMenu('Industries', self)
        ETFAction=QAction('ETFs', self)
        ETFAction.triggered.connect(self.ETFAct)

        classifyMenu.addMenu(sectorMenu)
        classifyMenu.addMenu(industryMenu)
        classifyMenu.addAction(ETFAction)

        for sector in self.Sectors:
            sMenu=QMenu(sector,self)
            sectorMenu.addMenu(sMenu)
            for ind in self.SIDict[sector]:
                s1Act=QAction(ind,self)
                s1Act.triggered.connect(partial(self.calliAct, self.ITDict[ind]))
                sMenu.addAction(s1Act)

        for industry in self.Industries:
            iAct=QAction(industry,self)
            iAct.triggered.connect(partial(self.calliAct, self.ITDict[industry]))
            industryMenu.addAction(iAct)
            #iMenu=QMenu(industry,self)
            #industryMenu.addMenu(iMenu)
    
    def calliAct(self,lst):
        db_name=self.OutputDir+self.SDIDBName
        ldb=RetrievalDB(db_name)
        #arr=np.array(lst)
        #df = pd.DataFrame(arr, columns=['Ticker'])
        clause=""
        count=1
        length=len(lst)
        for elem in lst:
            if count != length:
                clause += "Ticker = \""+elem+"\" OR "
            else:
                clause += "Ticker = \""+elem+"\""
            count+=1
        query="SELECT "+self.StockColumns+" from stock_info WHERE "+clause
        df=ldb.retrieve(query)
        model = pandasModel(df)
        self.view1 = QTableView()
        self.view1.setModel(model)
        self.view1.resize(2300, 400)
        self.view1.show()
        
    def ETFAct(self):
        db_name=self.OutputDir+self.EDIDBName
        ldb=RetrievalDB(db_name)
        query="SELECT "+self.ETFColumns+" from etf_info"
        df=ldb.retrieve(query)
        model = pandasModel(df)
        self.view2 = QTableView()
        self.view2.setModel(model)
        self.view2.resize(2100, 800)
        self.view2.show()


    def createSystemMenu(self, menuBar):
        systemMenu=menuBar.addMenu("System")
        settingsMenu=QMenu('Settings',self)
        systemMenu.addMenu(settingsMenu)
        exitAct = QAction('Exit', self)
        exitAct.triggered.connect(self.closeexit)
        systemMenu.addAction(exitAct)
    
    def closeexit(self):
        QApplication.instance().closeAllWindows()
    
    def createDataBaseMenu(self, menuBar):
        dbMenu=menuBar.addMenu('DataBases')
        diMenu=QMenu('Daily Info', self)
        dbMenu.addMenu(diMenu)
        diUpdateAct=QAction('Update',self)
        diMenu.addAction(diUpdateAct)
        diUpdateAct.triggered.connect(self.diUpdateCall)
        earnMenu=QMenu('Past Earnings', self)
        dbMenu.addMenu(earnMenu)
        eUpdateAct=QAction('Update',self)
        earnMenu.addAction(eUpdateAct)
        eUpdateAct.triggered.connect(self.eUpdateCall)
        eCreateAct=QAction('Create',self)
        earnMenu.addAction(eCreateAct)
        eCreateAct.triggered.connect(self.eCreateCall)
        dearnMenu=QMenu('Detailed Past Earnings', self)
        dbMenu.addMenu(dearnMenu)
        deUpdateAct=QAction('Update',self)
        dearnMenu.addAction(deUpdateAct)
        deUpdateAct.triggered.connect(self.deUpdateCall)
        deCreateAct=QAction('Create',self)
        dearnMenu.addAction(deCreateAct)
        deCreateAct.triggered.connect(self.deCreateCall)
        stockPriceMenu=QMenu('Stock Prices', self)
        dbMenu.addMenu(stockPriceMenu)
        etfPriceMenu=QMenu('ETF Prices', self)
        dbMenu.addMenu(etfPriceMenu)


    def createASubwindow(self):
        self.mySubwindow1=subwindow()
        self.mySubwindow1.createWindow(400,200)
        self.mySubwindow1.setWindowTitle("Update Daily Info DB") 
        #make pyqt items here for your subwindow
        #for example self.mySubwindow.button=QtGui.QPushButton(self.mySubwindow)
        vbox = QVBoxLayout()
        self.mySubwindow1.l1=QLabel("Updating ETF Information...", self.mySubwindow1)
        self.mySubwindow1.l1.setAlignment(Qt.AlignLeft)
        vbox.addWidget(self.mySubwindow1.l1)
        vbox.addStretch()
        self.mySubwindow1.progress = QProgressBar(self.mySubwindow1)
        self.mySubwindow1.progress.setGeometry(0, 0, 300, 50)
        self.mySubwindow1.progress.setAlignment(Qt.AlignCenter)
        self.mySubwindow1.progress.setMinimum(0)
        self.mySubwindow1.progress.setMaximum(len(self.TotalETFList))
        vbox.addWidget(self.mySubwindow1.progress)
        vbox.addStretch()
        self.mySubwindow1.button = QPushButton('Start', self.mySubwindow1)
        vbox.addWidget(self.mySubwindow1.button)
        self.mySubwindow1.button.setFixedWidth(70)
        self.mySubwindow1.button.clicked.connect(self.onButtonClick)
        self.mySubwindow1.l2=QLabel("Updating Stock Information...", self.mySubwindow1)
        self.mySubwindow1.l2.setAlignment(Qt.AlignLeft)
        vbox.addWidget(self.mySubwindow1.l2)
        vbox.addStretch()
        self.mySubwindow1.progress1 = QProgressBar(self.mySubwindow1)
        self.mySubwindow1.progress1.setGeometry(0, 0, 300, 50)
        self.mySubwindow1.progress1.setAlignment(Qt.AlignCenter)
        self.mySubwindow1.progress1.setMinimum(0)
        self.mySubwindow1.progress1.setMaximum(len(self.TotalStockList))
        vbox.addWidget(self.mySubwindow1.progress1)
        vbox.addStretch()
        self.mySubwindow1.button1 = QPushButton('Start', self.mySubwindow1)
        vbox.addWidget(self.mySubwindow1.button1)
        self.mySubwindow1.button1.setFixedWidth(70)
        self.mySubwindow1.button1.clicked.connect(self.onButtonClick1)

        self.mySubwindow1.setLayout(vbox)
        self.mySubwindow1.show()


    def diUpdateCall(self):
        #print('Update DI DB')
        #dbname=self.OutputDir+self.DIDBName
        #didb=DailyInfoDB(dbname)
        #didb.createDIDB(self.TotalETFList,"etf_info",didb.ETFDIDBdf)
        self.createASubwindow()

    def eUpdateCall(self):
        self.createASubwindow1(3)
    
    def eCreateCall(self):
        self.createASubwindow1(2)

    def deUpdateCall(self):
        print("dUpdate")
    
    def deCreateCall(self):
        self.createASubwindow2()

    def onButtonClick(self):
        print("Button")
        dbname=self.OutputDir+self.EDIDBName
        self.calc = External(dbname, "etf_info", self.TotalETFList)
        self.calc.countChanged.connect(self.onCountChanged)
        self.calc.start()

    def onCountChanged(self, value):
        self.mySubwindow1.progress.setValue(value)
        

    def onButtonClick1(self):
        print("Button1")
        dbname=self.OutputDir+self.SDIDBName
        self.calc1 = External(dbname, "stock_info", self.TotalStockList)
        self.calc1.countChanged.connect(self.onCountChanged1)
        self.calc1.start()

    def onCountChanged1(self, value):
        self.mySubwindow1.progress1.setValue(value)
    

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    mainWin = MainWindow()
    mainWin.show()
    sys.exit( app.exec_() )
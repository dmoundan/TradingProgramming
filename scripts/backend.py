from DataBase import FinVizDataBase
from tabulate import tabulate
from yahoo_earnings_calendar import YahooEarningsCalendar
from datetime import datetime, timezone


def get_earnings_date(ticker, DBName):
    database=FinVizDataBase(DBName,"stock")
    df=database.earnings_date(ticker)
    return df
    

def get_earnings_range(date_from, date_to):
    yec = YahooEarningsCalendar()
    el= yec.earnings_between(date_from, date_to) 
    print_future_earnings(el,name_list)
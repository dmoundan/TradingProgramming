#!/usr/bin/env python3


from datetime import datetime, timezone
from yahoo_earnings_calendar import YahooEarningsCalendar
import pandas
import tzlocal 

yec = YahooEarningsCalendar()
# Returns the next earnings date of BOX in Unix timestamp
result_s=pandas.to_datetime(yec.get_next_earnings_date('aapl'),unit='s')
print(str(result_s))


utc_time = datetime.fromtimestamp(yec.get_next_earnings_date('aapl'), timezone.utc)
local_time = utc_time.astimezone()
print(local_time.strftime("%Y-%m-%d %H:%M:%S.%f%z (%Z)"))

local_timezone = tzlocal.get_localzone() # get pytz timezone
local_time = datetime.fromtimestamp(yec.get_next_earnings_date('aapl'), local_timezone)
print(local_time.strftime("%Y-%m-%d %H:%M:%S.%f%z (%Z)"))
print(local_time.strftime("%B %d %Y"))


print("==============")
"""
date_from = datetime.strptime(
    'Feb 20 2017  10:00AM', '%b %d %Y %I:%M%p')
"""    
date_from = datetime.strptime(
    'Feb 20 2017', '%b %d %Y')   
date_to = datetime.strptime(
    'Feb 28 2017', '%b %d %Y')
yec = YahooEarningsCalendar()
#print(yec.earnings_on(date_from))
print(yec.earnings_between(date_from, date_to))
#print(type(yec.earnings_on(date_from)))

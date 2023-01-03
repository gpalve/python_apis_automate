import json
import base64
import pandas as pd
from queue import Empty
from xmlrpc.client import ResponseError
import requests
import urllib.parse as up
import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql as pm
import time
from datetime import datetime, timedelta, date
from collections import Counter
import requests

# DATA AVAILABLE FROM 12TH APRIL 2011. 
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield end_date - timedelta(n)

from datetime import date
from datetime import timedelta

today = date.today()
yesterday = today - timedelta(days = 1)
day_before_yesterday = yesterday - timedelta(days = 1)

start_date = day_before_yesterday
end_date = yesterday
# Last Date of Accident Available from 12/04/2011. Prior to that there is no data

nodata = 'eyJjb2RlIjoiMyIsIm1lc3NhZ2UiOiJTb3JyeSEgRGF0YSBOb3QgRm91bmQiLCJyZXN1bHQxIjpudWxsfQ=='
    # print(single_date.strftime("%d-%m-%y"))
for single_date in daterange(start_date,end_date):
    # print(single_date.strftime("%d-%m-%y"))
    dateFormatted = single_date.strftime("%d/%m/%y")
    
    url = 'https://safety.indianrail.gov.in/simsws/rest/uccc/uccc_data'
    myobj = {"userId":"123","password":"123","date":dateFormatted}
    x = requests.post(url, json = myobj)

    if nodata == x.text :
        print('no data found for ',dateFormatted) 
    else :
        output = base64.b64decode(x.text+'==')
        result = output.decode()
        # print(result,"\n\n")
        finaldata = json.loads(json.dumps(result[43:-2]))
        # print(finaldata)
        mylist = finaldata.split('},')
        # this if is for multiple accidents in a day
        if len(mylist) > 1:
            for i in range(1,len(mylist)):
                # this if is for first iteration for first element in mylist
                if i < len(mylist)-1:
                    data_to_append = mylist[i] + '}'
                    iyers = json.loads(data_to_append)
                    df= pd.DataFrame([iyers])
                    df['tdate'] = datetime.strptime(single_date.strftime("%Y-%m-%d"), '%Y-%m-%d').date()
                # further accidents in that day
                else:
                    iyers1 = json.loads(mylist[i])
                    df = pd.DataFrame([iyers1])
                    df['tdate'] = datetime.strptime(single_date.strftime("%Y-%m-%d"), '%Y-%m-%d').date()
                conn = create_engine("mysql+pymysql://"+env.username+":"+env.password+"@localhost/"+env.database)
                #Example: conn = create_engine("mysql+pymysql://root:PassWord123!@localhost/my_database")
                df.to_sql(name='accident_db', con=conn, if_exists = 'append', index=False)
                print('success for the date multiple ' ,dateFormatted )
        # this is for single row        
        else:
            data_to_append = json.loads(mylist[0] )
            # print(type(data_to_append))
            df = df = pd.DataFrame([data_to_append])
            df['tdate'] = datetime.strptime(single_date.strftime("%Y-%m-%d"), '%Y-%m-%d').date()
            conn = create_engine("mysql+pymysql://"+env.username+":"+env.password+"@localhost/"+env.database)
            #Example: conn = create_engine("mysql+pymysql://root:PassWord123!@localhost/my_database")
            df.to_sql(name='accident_db', con=conn, if_exists = 'append', index=False)
            print('success for the date ' ,dateFormatted )


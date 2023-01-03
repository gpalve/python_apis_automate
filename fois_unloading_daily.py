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
import os
import env

# def daterange(start_date, end_date):
#     for n in range(int ((end_date - start_date).days)):
#         yield start_date + timedelta(n)


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield end_date - timedelta(n)
# # 23-03-2022 data not available
from datetime import date
from datetime import timedelta

today = date.today()
yesterday = today - timedelta(days = 1)
day_before_yesterday = yesterday - timedelta(days = 1)

start_date = day_before_yesterday
end_date = yesterday
i=0
mydate = []

try:
    for single_date in daterange(start_date,end_date):
        i+=1
        print(single_date.strftime("%d-%m-%Y"))
        x = single_date.strftime("%d-%m-%Y")
        url_interchange_data = (env.master_url+'UCCC-API/subApis/fois_new.php?apiType=uldgdttn&custDate='+x)

        response = requests.get(url_interchange_data, headers={"User-Agent": "XY"})
        
        if(response.text == '[]'):
             
         file1 = open("data_to_send.txt", "a")
         file1.write("\n")
         file1.write("*Data Not Available for " + x +" - "+ os.path.basename(__file__)+ "*")
         file1.write("\n")
         file1.close()         
       
        data = json.loads(json.dumps(response.json()))
        for y in range(len(data)):
            y = x
            mydate += [y]
        df2 = pd.json_normalize(data)
        df = pd.DataFrame(df2)
        # print(df2)

        df['tdate'] = datetime.strptime(single_date.strftime("%Y-%m-%d"), '%Y-%m-%d').date()
        # print(df)
    # print(df)
        conn = create_engine("mysql+pymysql://"+env.username+":"+env.password+"@localhost/"+env.database)
    #Example: conn = create_engine("mysql+pymysql://root:PassWord123!@localhost/my_database")

        df.to_sql(name='fois_unloading_data', con=conn, if_exists = 'append', index=False)
        mydate = []
        print('successfully inserted data for '+ x)
        
        file1 = open("data_to_send.txt", "a")
        file1.write("\n")
        file1.write("Data Inserted for " + x +" - "+ os.path.basename(__file__))
        file1.write("\n")
        file1.close()
        
        if(i%9==0):
            print('sleeping...for 120secs to relase the load on server')
            time.sleep(120)
except ResponseError:
    print('iyers' + x)


    
 


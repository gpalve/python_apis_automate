import json
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql as pm
import time
from datetime import datetime, timedelta, date
import os
import env



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

for single_date in daterange(start_date,end_date):
  print("Outer for loop of date is working....Pls have a patience")
  x = single_date.strftime("%d/%m/%Y")
  url_uti_data = (env.master_url+'UCCC-API/subApis/rsms_uti.php?customDate='+x)
  response = requests.get(url_uti_data, headers={"User-Agent": "XY"})
  # print(response.text , "\n\n\n")
  if(response.text.startswith("<ns:binary")):
    print("there seems to be error")
                
    file1 = open("data_to_send.txt", "a")
    file1.write("\n")
    file1.write("*Data Not Available for " + x +" - "+ os.path.basename(__file__)+ "*")
    file1.write("\n")
    file1.close()         
       
  else:
  # remove null from response and replace backstroke with empty string
    r = json.dumps(response.text).replace('null','''null''')
    data = r.replace("\\","")

    # removing first 2 and last 2 strings '[{'  '}]'
    data = data[2:-2]

    # splitting data by {"sno" 
    intermediate_data = data.split('{"sno"')


    #appending again to make it perfect string dict and then we can convert it into actual dict.
    for i in range(1,len(intermediate_data)):
      print("inner for loop is working for data processing....Pls wait")
      element = '{"sno"' + intermediate_data[i]
      if i == len(intermediate_data) -1 :
        element = json.loads(element)
        first_half = {"sno":element["sno"] ,"sysGenerateNo": element["sysGenerateNo"],"zone": element["zone"],"division": element["division"],"post": element["post"],"regNo": element["regNo"],"categ": element["categ"],"occurDateFrom": element["occurDateFrom"],"occurDateTo": element["occurDateTo"],"regDate": element["regDate"],"occurredAt": element["occurredAt"],"occurLoc": element["occurLoc"],"trainNo": element["trainNo"],"stationFrom": element["stationFrom"],"stationTo": element["stationTo"],"rlySec": element["rlySec"],"kmNo":element["kmNo"],"tdate":single_date.strftime("%Y-%m-%d") }
        # print(first_half)
        conn3 = create_engine("mysql+pymysql://root:uccc1234@localhost/crisapi")
        df_master = pd.DataFrame([first_half])
        df_master.to_sql(name='rsms_uti_master', con=conn3, if_exists = 'append', index=False)
        for k in element["victimDetails"]:
          k["sysGenerateNo"] = element["sysGenerateNo"]
        conn1 = create_engine("mysql+pymysql://root:uccc1234@localhost/crisapi")
        df_victim = pd.DataFrame(element["victimDetails"])
        df_victim.to_sql(name='rsms_uti_victim', con=conn1, if_exists = 'append', index=False)
        for j in element["utiEnquiryStatusDetails"]:
          j["sysGenerateNo"] = element["sysGenerateNo"]
        conn2 = create_engine("mysql+pymysql://root:uccc1234@localhost/crisapi")
        df_enq = pd.DataFrame(element["utiEnquiryStatusDetails"])
        df_enq.to_sql(name='rsms_uti_enquiry_status', con=conn2, if_exists = 'append', index=False)
        # print(element)
      else:
        element = element[:-1]
        element = json.loads(element)
        first_half = {"sno":element["sno"] ,"sysGenerateNo": element["sysGenerateNo"],"zone": element["zone"],"division": element["division"],"post": element["post"],"regNo": element["regNo"],"categ": element["categ"],"occurDateFrom": element["occurDateFrom"],"occurDateTo": element["occurDateTo"],"regDate": element["regDate"],"occurredAt": element["occurredAt"],"occurLoc": element["occurLoc"],"trainNo": element["trainNo"],"stationFrom": element["stationFrom"],"stationTo": element["stationTo"],"rlySec": element["rlySec"],"kmNo":element["kmNo"],"tdate":single_date.strftime("%Y-%m-%d") }
        # print(first_half)
        conn3 = create_engine("mysql+pymysql://root:uccc1234@localhost/crisapi")
        df_master = pd.DataFrame([first_half])
        df_master.to_sql(name='rsms_uti_master', con=conn3, if_exists = 'append', index=False)
        for k in element["victimDetails"]:
          k["sysGenerateNo"] = element["sysGenerateNo"]
        conn = create_engine("mysql+pymysql://"+env.username+":"+env.password+"@localhost/"+env.database)
        # print(element["victimDetails"])
        df_victim = pd.DataFrame(element["victimDetails"])
        df_victim.to_sql(name='rsms_uti_victim', con=conn, if_exists = 'append', index=False)
          
        for j in element["utiEnquiryStatusDetails"]:
          j["sysGenerateNo"] = element["sysGenerateNo"]
        conn2 = create_engine("mysql+pymysql://root:uccc1234@localhost/crisapi")
        df_enq = pd.DataFrame(element["utiEnquiryStatusDetails"])
        df_enq.to_sql(name='rsms_uti_enquiry_status', con=conn2, if_exists = 'append', index=False)
    print("Successfully inserted for the data " + x)
            
    file1 = open("data_to_send.txt", "a")
    file1.write("\n")
    file1.write("Data Inserted for " + x +" - "+ os.path.basename(__file__))
    file1.write("\n")
    file1.close()
        


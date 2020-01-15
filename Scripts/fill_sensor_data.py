
import pandas as pd
import numpy as np
from scipy import signal, misc
from datetime import datetime, timedelta

from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float, Integer
from sqlalchemy.sql import exists
import mysql.connector
import os
import json

import functions

#milk chillers

def get_db_creds(credFile):
    #extracts Promethean DB info from json file
    with open(credFile) as f:
        dbCreds = json.load(f)
    return dbCreds['port'],dbCreds['password'], dbCreds['db_datasource'], dbCreds['host'], dbCreds['user']

PORT,PASSWORD, DB_NAME, HOST, USER = get_db_creds("db_creds.json")

def get_device_list():
        ePASSWORD = "Ekryp#1234"
        eDB_NAME = "Promethean"
        eHOST = "35.199.174.191"
        eUSER = "root1"

        print('Fetching Device List..')
        engine = create_engine("mysql+mysqldb://" + eUSER + ":"+ePASSWORD+"@" + eHOST + "/"+eDB_NAME)
        print('Connection established!')
        device=pd.read_sql("SELECT id,end_customer_id,site_id, promethean_device_id,serial_number as serial_number FROM Promethean.end_customer_devices",con=engine)
        return device
    

def get_start_date():
        result=datetime.now()-timedelta(1)
#        print(result)
        return result
        #result.to_csv('result.csv')

def push_to_sql(df, tablename):
        ePASSWORD = "Ekryp#1234"
        eDB_NAME = "Promethean"
        eHOST = "35.199.174.191"
        eUSER = "root1"

        print('Pushing ',tablename,' to MY SQL')
        engine = create_engine("mysql+mysqldb://" + eUSER + ":"+ePASSWORD+"@" + eHOST + "/"+eDB_NAME)
        df.to_sql(tablename, engine, if_exists= 'append', index=False, chunksize=10000)
        
#create engine and connect to PRomethean DB to get device_ids
engine = create_engine('mysql://' + USER + ":"+PASSWORD+"@" + HOST +":"+PORT+ "/"+DB_NAME)
conn=mysql.connector.connect( host=HOST, database=DB_NAME,user=USER,password=PASSWORD,port=PORT)
print('Promethean DB Connection established')

#Parag sites
all_devices=get_device_list()
device_list=list(all_devices.serial_number[all_devices['end_customer_id'].isin([2,3])])

#assign start date and end date
start_date=functions.split_day(get_start_date())
end_date=functions.split_day(datetime.now()+timedelta(1))

print('Start date: ', start_date)
print('End date: ', end_date)

count=1
#main loop
for device in device_list: 
    print('Fetching Device',device,' data..')
    this_device=all_devices[all_devices['serial_number']==device]
    #fetching data from Promethean DB
    try:
        data = pd.read_sql("SELECT t.TableTimeStamp as time,t.Parameter1 as tss_temp, t.Parameter2 as milk_temp,t.Parameter5 as AC_voltage, t.Parameter6 as compressor,t.Parameter7 as pump_current, t.Parameter11 as tank_switch, t.Parameter15 as discharge_pump, t.Parameter12 as agitator FROM aeron.Device_"+str(device)+" as t where TableTimeStamp> '"+str(start_date)+"' and TableTimeStamp< '"+str(end_date)+"' ",con = engine)

#    data = pd.read_sql("SELECT t.TableTimeStamp as time,t.Parameter1 as tss_temp, t.Parameter2 as milk_temp,t.Parameter5 as AC_voltage, t.Parameter6 as compressor,t.Parameter7 as pump_current, t.Parameter11 as tank_switch, t.Parameter15 as discharge_pump, t.Parameter12 as agitator FROM aeron.Device_"+str(device)+" as t ",con = engine)
        print('Length of raw data:',len(data))
        print(data['time'].sort_values(ascending=False).head(2))
#    try:
        #create seperate columns for date, hours and minutes to help combine 2 min data into hourly
        data=functions.split_time(data)

        #creating new_df 
        compressed_data=functions.compress_data(data)
       
        #creating device_work table data
        work=functions.create_device_work(compressed_data, this_device)
        #print(work.tail(2))
        #pushing into device_work_history table 
        #push_to_sql(work,'device_work_history')
        
        #creating sensor data
        sensor_data=functions.create_sensor_data(compressed_data,this_device)
        print('In sensorr data: \n',sensor_data['timestamp'].sort_values(ascending=False).head(2))       
        print('Length of data :', len(sensor_data))
        #pushing into device_sensor_data table
        push_to_sql(sensor_data,'device_sensor_data')
        #print(sensor_data)
        print('#',count,' Device_',device,' info successfully pushed into DB!\n','#'*40)
        count=count+1
        
    except Exception as e:
            #might me the case if there's no data or all NULL values in sensor data
            print('Data Not Available for Device_',device)
            print('\n\n',e)



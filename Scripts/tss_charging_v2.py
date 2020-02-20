

import pandas as pd
import numpy as np
from scipy import signal, misc
from datetime import datetime, timedelta

from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float, Integer
from sqlalchemy.sql import exists
import mysql.connector
import os
import json

def split_day(t):
            day,hour=str(t).split(" ")
            return day
def give_hour(t):
            day,meh,hour=str(t).split(" ")
            return hour
        
def tss_edgecase(TssTemperature):
    if(TssTemperature>=0):
        return 1
    elif TssTemperature<= -4:
        return 2
    else:
        return 0
    
def push_to_sql(df, tablename):
        print('Pushing ',tablename,' to MY SQL')
        engine = create_engine("mysql+mysqldb://root1:Ekryp#1234@35.199.174.191/Promethean")
        df.to_sql(tablename, engine, if_exists= 'append', index=False, chunksize=10000)
        
def get_device_list():
        print('Fetching Device List..')
        engine = create_engine("mysql+mysqldb://root1:Ekryp#1234@35.199.174.191/Promethean")
        print('Connection established!')
        device=pd.read_sql("SELECT id,end_customer_id,site_id, promethean_device_id,serial_number as serial_number FROM Promethean.end_customer_devices",con=engine)
        return device
    
def get_db_creds(credFile):
    #extracts Promethean DB info from json file
    with open(credFile) as f:
        dbCreds = json.load(f)
    return dbCreds['port'],dbCreds['password'], dbCreds['db_datasource'], dbCreds['host'], dbCreds['user']

PORT,PASSWORD, DB_NAME, HOST, USER = get_db_creds("db_creds.json")    
engine = create_engine('mysql://' + USER + ":"+PASSWORD+"@" + HOST +":"+PORT+ "/"+DB_NAME)
conn=mysql.connector.connect( host=HOST, database=DB_NAME,user=USER,password=PASSWORD,port=PORT)
print('Promethean DB Connection established')

j=0
#Parag sites
all_devices=get_device_list()
device_list=list(all_devices.serial_number[all_devices['end_customer_id']==2])

df=pd.DataFrame(columns=['id','serial_number', 'start_time','start_temp','end_time','end_temp','total_time','event_name'])

for device in device_list:
    print('Fetching Device',device,' data..')
    this_device=all_devices[all_devices['serial_number']==device]
    #fetching data from Promethean DB
    data = pd.read_sql("SELECT t.TableTimeStamp as timestamp,t.Parameter1 as TssTemperature,t.Parameter5 as AC_voltage,t.Parameter2 as milk_temp FROM aeron.Device_"+str(device)+" as t",con = engine)
    data['tss_edge']=data['TssTemperature'].apply(tss_edgecase)
    data['date']=data['timestamp'].apply(split_day)
    
    #print(data.head())
    charging=0
    flag=0
    for row in data.iterrows():
        if row[1][2]!=0 & int(row[1][3]) not in range(8,25):#ac present and milk is present 
            if row[1][4]==1: #if tss_temp is higher than 0, then charging has to start
                charging=1+charging
                if charging==1:
                        flag=1     
                        starttime=row[1][0]
                        starttemp=row[1][1]
                        #print('Starting to charge at ',starttime,' with tss temp: ',starttemp,' and AC at ',row[1][2])
                    
            elif row[1][4]==2: #has reached safe tss_temp
                if flag==1: #if charging is going on, then now it's in safe temp
                        flag=0
                        charging=0
                        if (row[1][0]-starttime)<= timedelta(days=1):#check for milk present
                            #print('Charged at ',row[1][0],' with tss temp: ',row[1][1],' and AC at ',row[1][2])
                            #print('\t'*10,'Time Taken: ', row[1][0]-starttime)
                            df.loc[j]=(j,device,starttime,starttemp,row[1][0],row[1][1],(row[1][0]-starttime),'Tss charging')
                            j=j+1
        else:
            charging=0
            flag=0
    print('Thank you, next...') 

print('Gonna push into DB..')    
df['total_time']=df['total_time'].apply(give_hour)
push_to_sql(df,'insights')
print('All done!')



import pandas as pd
import numpy as np
from scipy import signal, misc
from datetime import datetime, timedelta

from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float, Integer
from sqlalchemy.sql import exists
import mysql.connector
import os
import json

def detect_acm(data):
    #checking for acm mode
    acm=[]
    agitator_on=0
    acm_on=0
    flag=0
    for row in data.iterrows():
        if row[1][1]<=6: #if milk is present and below upper limit
            if row[1][2]==1:
                agitator_on=agitator_on+1
                if agitator_on==1: #could be in acm or just on for long
                    flag=1  
                    starttime=row[1][0]
                    startmilktemp=row[1][1]
                    #print('Starting cycle at ',starttime,' with milk temp: ',startmilktemp)
                    acm.append(1)

                elif acm_on!=0: #end of acm cylce
                    acm.append(1)
                    #print('Ending cycle at ',row[1][0],' with milk temp: ',row[1][1],' with ',acm_on,' cycles')
                    flag=1
                    acm_on=0
                    agitator_on=0
                else:
                    acm.append(0)
            elif row[1][2]==0:
                if flag==1: #cyclic mode has just started
                    acm_on=acm_on+1 
                    acm.append(1)
                    #print('Agitator in cyclic mode for ',acm_on,' cycles')
                else:
                    acm.append(0)
            else:
                acm.append(0)
        else:
                acm.append(0)

    print('Detected ACM cycles')            
    data['acm']=acm
    return data

def give_hour(t):
            day,un,hour=str(t).split(" ")
            return hour
    
def split_month(t):
    year,month,rest=str(t).split("-")
    return month

def split_day(t):
            day,hour=str(t).split(" ")
            return day
def split_year(t):
    year,month,rest=str(t).split("-")
    return year

def combine_mins(t):
            day,hour=str(t).split(" ")
            hour,mins,seconds=str(hour).split(":")
            return hour
        
def split_date(data):
    data['date']=data.timestamp.apply(split_day)
    data['year']=data.timestamp.apply(split_year)
    data['month']=data.timestamp.apply(split_month)
    data['hour']=data.timestamp.apply(combine_mins)
    return data


def get_db_creds(credFile):
    #extracts Promethean DB info from json file
    with open(credFile) as f:
        dbCreds = json.load(f)
    return dbCreds['port'],dbCreds['password'], dbCreds['db_datasource'], dbCreds['host'], dbCreds['user']

PORT,PASSWORD, DB_NAME, HOST, USER = get_db_creds("db_creds.json")

def get_device_list():
        print('Fetching Device List..')
        engine = create_engine("mysql+mysqldb://root1:Ekryp#1234@35.199.174.191/Promethean")
        print('Connection established!')
        device=pd.read_sql("SELECT id,end_customer_id,site_id, promethean_device_id,serial_number as serial_number FROM Promethean.end_customer_devices",con=engine)
        return device
    
def push_to_sql(df, tablename):
        print('Pushing ',tablename,' to MY SQL')
        engine = create_engine("mysql+mysqldb://root1:Ekryp#1234@35.199.174.191/Promethean")
        df.to_sql(tablename, engine, if_exists= 'append', index=False, chunksize=10000)
  

    
engine = create_engine('mysql://' + USER + ":"+PASSWORD+"@" + HOST +":"+PORT+ "/"+DB_NAME)
conn=mysql.connector.connect( host=HOST, database=DB_NAME,user=USER,password=PASSWORD,port=PORT)
print('Promethean DB Connection established')

j=6700
#Parag sites
all_devices=get_device_list()
device_list=list(all_devices.serial_number[all_devices['end_customer_id']==2])
df=pd.DataFrame(columns=['id','serial_number', 'start_time','start_temp','end_time','end_temp','total_time','event_name'])

for device in device_list: 
    print('Fetching Device',device,' data..')
    this_device=all_devices[all_devices['serial_number']==device]
    #fetching data from Promethean DB
    data = pd.read_sql("SELECT t.TableTimeStamp as timestamp,t.Parameter2 as MilkTemperature,t.Parameter12 as AgitatorStatus FROM aeron.Device_"+str(device)+" as t",con = engine)
    data = detect_acm(data)
    data = split_date(data)
    starttime=datetime.now()
    starttemp=0
    count=0
    for row in data.iterrows():
        if row[1][1]<=6: #if milk temperature is below upper limit
            count=count+1
            if count==1:
                starttime=row[1][0]
                starttemp=row[1][1]
                #print(row[1][0],': Agitator ON with milk temp ',row[1][1])

        if row[1][3]==1 & count!=0:

            if count!=1:
                if ((row[1][0]-starttime) > timedelta(minutes=20)) & ((row[1][0]-starttime) < timedelta(days=1)):
                    df.loc[j]=(j,device,starttime,starttemp,row[1][0],row[1][1],(row[1][0]-starttime),'acm')
                    j=j+1
                    #print(row[1][0],': Agitator got into cyclic Mode, with count=',count,' and milk temp=',row[1][1])
                    #print('\t'*12,'Time Taken to get into ACM ',(row[1][0]-starttime))
                    
            count=0
            
            
          
df['total_time']=df['total_time'].apply(give_hour)
print('Pushing to SQL',len(df))
#print(df.head())
push_to_sql(df,'insights')            
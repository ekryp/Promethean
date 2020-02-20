




import pandas as pd
import numpy as np
from scipy import signal, misc
from datetime import datetime, timedelta

from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float, Integer
from sqlalchemy.sql import exists
import mysql.connector
import os
import json

def split_month(t):
    year,month,rest=str(t).split("-")
    return month

def split_day(t):
            day,hour=str(t).split(" ")
            return day
def give_hour(t):
            day,en,hour=str(t).split(" ")
            return hour
        
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

def milk_edgecase(milk_temp):
    if milk_temp<=4: #below lower limit
        return 0
    elif int(milk_temp) in range(4,6): #safe temperature
        return 1
    elif int(milk_temp) in range(6,15): #over upper limit
        return 2
    elif int(milk_temp) in range(15,25): #empty tank
        return 3
    elif milk_temp>25:
        return 4 #fresh product
    else:
        return 5

def set_zero():
    return 0,0,0,0,0,0

engine = create_engine('mysql://' + USER + ":"+PASSWORD+"@" + HOST +":"+PORT+ "/"+DB_NAME)
conn=mysql.connector.connect( host=HOST, database=DB_NAME,user=USER,password=PASSWORD,port=PORT)
print('Promethean DB Connection established')

j=1
#Parag sites
all_devices=get_device_list()
device_list=list(all_devices.serial_number[all_devices['end_customer_id']==2])

df=pd.DataFrame(columns=['id', 'device_id','serial_number', 'start_time','start_temp','end_time','end_temp','total_time','event_name'])

for device in device_list[1:2]:
    print('Fetching Device',device,' data..')
    this_device=all_devices[all_devices['serial_number']==device]
    #fetching data from Promethean DB
    data = pd.read_sql("SELECT t.TableTimeStamp as timestamp,t.Parameter2 as milk_temp, t.Parameter5 as AC_voltage FROM aeron.Device_"+str(device)+" as t",con = engine)
    data = split_date(data)
    data['milk_temp_edgecase']=data['milk_temp'].apply(milk_edgecase)
    data=data[data.date=='2019-09-10']  
   
    count1=0
    count2=0
    count3=0
    flag=0
    flag2=0
    flag3=0
    starttemp1=0
    starttemp2=0
    starttemp3=0
    start1=datetime.now()
    start2=datetime.now()
    start3=datetime.now()
  
    for row in data.iterrows():
        
        
        if row[1][7]==4: #fresh milk
            count1=count1+1
            if count1==1: #first encounter
                if row[1][2]!=0:
                    flag=1
                    start1=row[1][0]
                    starttemp1=row[1][1]
                    #flag3=0
                    #count3=0
                    print('case 1: Starting at ',start1,' with milk temp: ',row[1][1],' and AC at ',row[1][2])
                

        if row[1][7]==2 | row[1][7]==3: #above safe temp
            if flag==1: #cooling fresh milk 
                count1=count1+1
            else: #milk temp just went up for some reason or extra milk added
                
                count2=count2+1
                if count2==1:
                    if row[1][2]!=0:
                        flag2=1
                        start2=row[1][0]
                        starttemp2=row[1][1]
                        flag3=0
                        count3=0
                        print('case 2: Starting at ',start2,' with milk temp: ',row[1][1],' and AC at ',row[1][2])
                    
                                  
                    
        if row[1][7]==1: #in safe temp
            if flag==1:
                if row[1][2]!=0:
                    
                    print('case 1: Reached at ',row[1][0],' with milk temp: ',row[1][1],' and AC at ',row[1][2],', count1=',count1)
                    print('\t'*13,' Time Taken: ', row[1][0]-start1)
                    df.loc[j]=(j,this_device.id,device,start1,starttemp1,row[1][0],row[1][1],(row[1][0]-start1),'Milk Upper')
                    #flag=0
                    j=j+1
                    flag,flag2,flag3,count1,count2,count3=set_zero()
                    #count1=0
                    #if flag2==1:
                     #   flag2=0
                      #  count2=0
                
            elif flag2==1:
                if row[1][2]!=0:
                    if(row[1][0]-start2)>timedelta(minutes=20):
                        print('case 2: Reached at ',row[1][0],' with milk temp: ',row[1][1],' and AC at ',row[1][2],', count2=',count2)
                        print('\t'*13,' Time Taken: ', (row[1][0]-start2))
                        df.loc[j]=(j,this_device.id,device,start2,starttemp2,row[1][0],row[1][1],(row[1][0]-start2),'Fresh Milk')
                        #flag2=0
                        #flag3=0
                        #count3=0
                        #count2=0
                        j=j+1
                    flag,flag2,flag3,count1,count2,count3=set_zero()
            if flag3==0:    
                count3=count3+1
                if count3==1 :
                    if row[1][2]!=0:
                        start3=row[1][0]
                        print('case 3: Starting at ',start3,' with milk temp: ',row[1][1],' and AC at ',row[1][2])
                        starttemp3=row[1][1]
                        flag3=1
                    else: #no ac
                        count3=0
        if row[1][7]==0: #below 4
            if flag3==1:
                 if row[1][2]!=0 & ((row[1][0]-start3)>timedelta(minutes=20)):
                        
                    print('case 3: Reached at ',row[1][0],' with milk temp: ',row[1][1],' and AC at ',row[1][2])
                    print('\t'*13,'Time Taken: ', row[1][0]-start3)
                    df.loc[j]=(j,this_device.id,device,start3,starttemp3,row[1][0],row[1][1],(row[1][0]-start3),'Milk Lower')
                    #flag3=0
                    j=j+1
                    flag,flag2,flag3,count1,count2,count3=set_zero()
                    #count3=0
                         

 
df['total_time']=df['total_time'].apply(give_hour)
#print(df.head())
#push_to_sql(df,'insights')
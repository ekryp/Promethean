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
#            print(t)
            day,un,hour=str(t).split(" ")
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

def give_hour(t):
            day,un,hour=str(t).split(" ")
            return hour
    
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

event_list=['Tss charging','Milk Lower','acm']    
engine = create_engine("mysql+mysqldb://root1:Ekryp#1234@35.199.174.191/Promethean")
print('Promethean DB Connection established')

data_stat=pd.DataFrame(columns=['pattern_name','year','month','occurances','mean','p10','p25','p50','p75','p90'])
j=1
all_devices=get_device_list()
device_list=list(all_devices.serial_number[all_devices['end_customer_id']==2])


for event in event_list:
        total_number_of_occurence=0
        mean_time=0
        median_time=0
        data = pd.read_sql("SELECT start_time as timestamp,serial_number,total_time,event_name FROM Promethean.insights",con = engine)
        df= data[data.event_name==event]
        df = split_date(df)
        #group by date
        year_data=df.groupby('year')
        for year,year_group in year_data:
            month_data=year_group.groupby('month')
            
            for month,month_group in month_data:
                #print(month_group.head())
                total_number_of_occurence=len(month_group)
                #print('Length of data :',total_number_of_occurence)
                mean_time=month_group['total_time'].mean()
                #print('Mean Time: ',mean_time)
                median_time=month_group['total_time'].median()
                #print('Median Time: ',median_time)
                #print('Quanrantiles: \n',month_group['total_time'].quantile([.1, .25, .5, .75]))
                #print('-'*40)
                data_stat.loc[j]=(event,year,month,total_number_of_occurence,mean_time,month_group['total_time'].quantile(.1),month_group['total_time'].quantile( .25),month_group['total_time'].quantile(.5),month_group['total_time'].quantile(.75),month_group['total_time'].quantile(.90) )
                j=j+1
                
                
                
                
print(data_stat.head(100))                
      
print(len(data_stat))
data_stat.to_csv('All_devices_Pattern_stats.csv')
print('All done')
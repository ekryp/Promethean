




import pandas as pd
import numpy as np
from scipy import signal, misc
from datetime import datetime, timedelta

from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float, Integer
from sqlalchemy.sql import exists
import mysql.connector
import os
import json

import incident_history
import functions
import parameters

def milk_edgecase(milk_temp_mean):

    if int(milk_temp_mean) in range(parameters.safe_milk_temp_low,parameters.safe_milk_temp_high): #safe temperature
        return 0
    elif milk_temp_mean >parameters.safe_milk_temp_high: #OVER upperbound - hotter product
        return 1
    else: #BELOW lowerbound - cold product
        return 2
    
def milk_present(milk_temp_mean):
    if int(milk_temp_mean)==0:
        return 0
    if int(milk_temp_mean) in range(parameters.milk_not_present_low,parameters.milk_not_present_high): #no product!!!!! Made changes here
        return 0
    elif milk_temp_mean in range(parameters.fresh_product_low, parameters.fresh_product_high): #fresh product poured in
        return 2
    else:
        return 1
    
def create_life_events(new_df):
    
    new_df['milk_present']=new_df['milk_temp_mean'].apply(milk_present)
    new_df['milk_temp_edgecase']=new_df['milk_temp_mean'].apply(milk_edgecase)
    new_val=[]
    
    i=0
    for row in new_df.milk_present:
        #milk_present=3 indicating milk lift
        if (new_df.milk_present.iloc[i-1]==1)&(row==0):
            new_val.append(3)
        #setting milk_present=1 for when it gets 10-25 range after fresh milk is poured
        elif (new_df.milk_present.iloc[i-1]==2)&(row==0):
            new_val.append(1)
        else:
            new_val.append(row)
        i=i+1
        
    new_df['milk_present']=new_val      
    cooling_process=[]
    #tank switch on and discharge pump on
    for row in new_df.iterrows():

        if (row[1].tank_switch_mean!=0.0) &(row[1].discharge_pump_mean!=0.0):
            cooling_process.append(1) #cooling process in process
        else:
            cooling_process.append(0) 
    new_df['cooling']=cooling_process

    #creatn empty list to fill values into device_life_events table
    datetimelist=[]
    event_id=[]

    
    return new_df
    
      
def get_incident_info():
    engine = create_engine("mysql+mysqldb://root1:Ekryp#1234@35.199.174.191/Promethean_backup")
    data=pd.read_sql("SELECT promethean_device_id,incident_id,threshold_time FROM Promethean_backup.incident_description",con=engine)
    return data

def get_start_date():
        result=datetime.now()-timedelta(1)
        
        return result

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

def push_to_sql(df, tablename):
        ePASSWORD = "Ekryp#1234"
        eDB_NAME = "Promethean"
        eHOST = "35.199.174.191"
        eUSER = "root1"

        eUSER = "root1"

        print('Pushing ',tablename,' to MY SQL')
        engine = create_engine("mysql+mysqldb://" + eUSER + ":"+ePASSWORD+"@" + eHOST + "/"+eDB_NAME)
        df.to_sql(tablename, engine, if_exists= 'append', index=False, chunksize=10000)


#Parag sites
all_devices=get_device_list()
device_list=list(all_devices.serial_number[all_devices['end_customer_id'].isin([2,3])])

        
engine = create_engine("mysql+mysqldb://root1:Ekryp#1234@35.199.174.191/Promethean")
print('Connection established')
#get end_customer_devices table
all_devices=pd.read_sql("SELECT id,end_customer_id,site_id, promethean_device_id,serial_number as serial_number FROM Promethean.end_customer_devices",con=engine)

#incident_descriptions 
incident_info=get_incident_info()
#print(incident_info.head())
#create list of all devices for Licious

#assign start date and end date
start_date=functions.split_day(get_start_date())
end_date=functions.split_day(datetime.now()+timedelta(1))

print('Start date: ', start_date)
print('End date: ', end_date)

j=1
#main loop
for device in device_list: #starting from 4th Device caz issues with devices before that
    print('Fetching Device',device,' data..')
    try:
        this_device=all_devices[all_devices['serial_number']==device]
        this_device_sensor_data=pd.read_sql("SELECT * FROM Promethean.device_sensor_data WHERE serial_number="+str(device)+" and date(timestamp)> '"+start_date+"' and date(timestamp)< '"+end_date+"'",con=engine)
        #this_device_sensor_data=pd.read_sql("SELECT * FROM Promethean.device_sensor_data WHERE serial_number="+str(device),con=engine)

        new_df=create_life_events(this_device_sensor_data)
        #print(new_df.head())
        #life_events.to_sql('device_life_events', engine, if_exists= 'append', index=False, chunksize=10000)
        incidents=incident_history.make_incidents(new_df,this_device)
        incidents.to_sql('incidents', engine, if_exists= 'append', index=False, chunksize=10000)
        print('Shape: ',incidents.shape)
       
        print('#',j,' Device_',device,' info successfully put into DB!')
        j=j+1
        #index2=index2+len(df3)+1
    
    except Exception as e:
            #might me the case if there's no data or all NULL values in sensor data
            print('Data Not Available for Device_',device)
            print('\n\n',e)
    

print('ALL DOne')

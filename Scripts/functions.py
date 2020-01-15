

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

def split_hour(t):
            day,mins,sec=str(t).split(":")
            return mins

def combine_mins(t):
            day,hour=str(t).split(" ")
            hour,mins,seconds=str(hour).split(":")
            return hour

#create seperate columns for date, hours and minutes to help combine 2 min data into hourly
def split_time(data):
    data['date']=data.time.apply(split_day)
    data['hour']=data.time.apply(combine_mins)
    data['mins']=data.time.apply(split_hour)
    data.date=pd.to_datetime(data.date)
    print ('Done splitting data!')
   # print(data.head())
    return data


def create_device_work(data,this_device):
    df=pd.DataFrame({
            'device_id':this_device.id.iloc[0],
            'serial_number':this_device.serial_number.iloc[0],
            'timestamp':data.datetime,
            'work_units':data.n_obs,
            'work_processed':0,
            'created_by':'test',
            'created_on':datetime.now()
            })
    print('Created Device Work')
    #print(df.head())
    return df

def compress_data(compressed_data):
        new_df=pd.DataFrame(columns=['datetime','hours','n_obs',
                                     'AC_voltage_mean','agitator_mean','compressor_mean',
                                     'discharge_pump_mean','milk_temp_mean','pump_current_mean',
                                     'tank_switch_mean','tss_temp_mean'])

        i=0
        #calculating hourly info
        for d in list(compressed_data.date.unique()):
            #fetching sensor data for that a particular date
            tdf=compressed_data[(compressed_data.date==d)]
            #group tdf by each hour on a particular date
            grouped=tdf.groupby('hour')
            for hour, count in grouped:
                    acv=tdf['AC_voltage'][(tdf.hour==hour)].mean()
                    agitator=tdf['agitator'][(tdf.hour==hour)].mean()
                    compressor=tdf['compressor'][(tdf.hour==hour)].mean()
                    discharge_pump=tdf['discharge_pump'][(tdf.hour==hour)].mean()
                    milk_temp=tdf['milk_temp'][(tdf.hour==hour)].mean()
                    pump_current=tdf['pump_current'][(tdf.hour==hour)].mean()
                    tank_switch=tdf['tank_switch'][(tdf.hour==hour)].mean()
                    tss_temp=tdf['tss_temp'][(tdf.hour==hour)].mean()
                    new_df.loc[i]=(tdf.date.iloc[0],hour,len(count),acv,agitator,compressor,
                                   discharge_pump,milk_temp,pump_current,tank_switch,tss_temp)

                    i=1+i

        new_date=[]
        #converts date in proper format
        for d in new_df.iterrows():
            day=datetime.strptime(str(d[1][0]),"%Y-%m-%d %H:%M:%S")
            new_date.append(day+timedelta(hours=int(d[1][1])))
        print('Compressed sensor data!')
        new_df.datetime=new_date
#        print(new_df.head())
        return new_df
        

#function to assign device_sensor_data table values
def create_sensor_data(new_df,device_data):
    df=pd.DataFrame({ 
            'device_id':device_data.id.iloc[0],
            'serial_number':device_data.serial_number.iloc[0],
            'milk_temp_mean':new_df.milk_temp_mean,
            'tank_switch_mean':new_df.tank_switch_mean,
            'timestamp':new_df.datetime,
            'agitator_mean':new_df.agitator_mean,
            'discharge_pump_mean':new_df.discharge_pump_mean,
            'compressor_mean':new_df.compressor_mean,
            'ac_voltage_mean':new_df.AC_voltage_mean,
            'pump_current_mean':new_df.pump_current_mean,
            'tss_temp_mean':new_df.tss_temp_mean,
            'n_obs':new_df.n_obs,
            'created_by':'test',
            'created_on':datetime.now()
            })
    print('Created Sensor Data')
   # print(df.tail())
    return df        



import pandas as pd
import numpy as np
from scipy import signal, misc
from datetime import datetime, timedelta

from sqlalchemy import create_engine, MetaData, Table, Column, Date, BigInteger, SMALLINT, String, Float, Integer
from sqlalchemy.sql import exists
import mysql.connector
import os
import json


import parameters


def incident_6(new_df):
    incident_6=[]
    count_6=0
    i=1
    for t in new_df.iterrows():
        if (t[1].milk_present ==0)  & (t[1].tank_switch_mean==1) :  #incident 6
            incident_6.append(1)
        else:
            incident_6.append(0)
        i=i+1

    new_val=[]
    i=1

    for t in new_df.iterrows():
        if (t[1].milk_present ==0)  & (t[1].tank_switch_mean==1) :  #incident 6    
            count_6=count_6+1
            if (count_6>parameters.tank_switch_on_for_max_hours)&(incident_6[i-1]==1): #checks for tank_switch_on_for_max_hours hours
                new_val.append(1)
            else:
                new_val.append(0)
        else:
                new_val.append(0) 
                count_6=0
        i=i+1  
    new_df['incident_6']=new_val 
    return new_df


def make_incidents(new_df,device_data):
    incident_id =[]
    incident_type=[]
    priority=[]
    incident_time=[]

    new_df['incident_6']=incident_6(new_df)

    i=0
    for t in new_df.iterrows():
    
        # milk_present==1  AND milk_temp_edgecase ==0 AND AC_voltage not in range
        if (t[1].milk_present ==1) & (t[1].milk_temp_edgecase==1) & (int(t[1].ac_voltage_mean) not in range(parameters.ac_voltage_low, parameters.ac_voltage_high)):  #incident 1: AC Voltage if turned OFF Often
                incident_id.append(1)
                priority.append('High')
                incident_time.append(t[1].timestamp)
             
        
        if (t[1].milk_present ==1) & (t[1].milk_temp_mean>parameters.safe_milk_temp_high) & (t[1].discharge_pump_mean==0):  
            #incident 2: Pattern of Discharge pump going OFF
                incident_id.append(2)
                priority.append('High')
                incident_time.append(t[1].timestamp)
                   

        if (t[1].milk_present ==1)  & (t[1].tank_switch_mean==0):  #incident 3: Pattern of Tank Switch being turned OFF
                incident_id.append(3)
                priority.append('Critical')
                incident_time.append(t[1].timestamp)                  
                
        if (t[1].milk_present ==1)  & (t[1].tank_switch_mean==0) & (t[1].discharge_pump_mean==0): 
            #incident 4: Pattern of not enough cooling
                incident_id.append(4)
                priority.append('Critical')
                incident_time.append(t[1].timestamp)
                
        if (t[1].milk_present ==0)  & (t[1].tank_switch_mean==1) : 
            #incident 5: Pattern of Tank switch being ON when not required
                incident_id.append(5)
                priority.append('High')
                incident_time.append(t[1].timestamp)    
                
        if (t[1].incident_6==1): #incident 6: Tank Switch ON more than 3 hours
                incident_id.append(6)
                priority.append('Critical')
                incident_time.append(t[1].timestamp)  
            
        if (t[1].milk_present==3)&(t[1].tank_switch_mean==1):  #incident 10: Tank switch ON during milk lift
                incident_id.append(10)
                priority.append('Critical')
                incident_time.append(t[1].timestamp) 
               
                
                
    df=pd.DataFrame({
#            'id':list(range(index,index+len(incident_id))), 
            'device_id':device_data.id.iloc[0],
            'serial_number':device_data.serial_number.iloc[0],
            'incident_id':incident_id,
            'priority':priority,
            'timestamp':incident_time,
            'created_on':datetime.now()
    })
            
    
    print('Created Incidents','='*70)
    
    return df


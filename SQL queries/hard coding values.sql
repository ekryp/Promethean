#1 - fill life_events_details
insert into Promethean1.life_events_details
(id,event_category,description,created_on)
SELECT id+1 as id, 
event_name as event_category, description, updated_by FROM Promethean.life_events_details

#2 - fill end_customer_devices
insert into Promethean1.end_customer_devices
(id,end_customer_id,site_id,promethean_device_id,serial_number, created_on)
SELECT id+1 as id, end_customer_id, site_id+1 as site_id,
device_id as promethean_device_id, promethean_device_id 
as serial_number, current_timestamp as created_on  FROM Promethean.end_customer_device
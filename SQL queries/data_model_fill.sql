#create new end_customer
insert into Promethean.end_customer 
values (1,'Amul','DP Rd, Shastri Nagar','Kothrud','Pune',
'Maharashtra','India',411058,18002583333,'https://amul.com/',
'https://amul.com/',NULL,NULL,NULL,NULL,NULL,NULL);

select * FROM Promethean.end_customer;

SELECT * FROM Promethean.end_customer_site;

#create BMC device detail
insert into Promethean.promethean_devices
values (1,'BMC',NULL,NULL,NULL,NULL,NULL);


SELECT * FROM Promethean.promethean_devices;

SELECT * FROM Promethean.end_customer_device; 
#all devices are assumed to be at one site

select * FROM Promethean.life_events_details;
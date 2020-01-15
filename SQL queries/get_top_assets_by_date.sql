SELECT daily_count.serial_number, 
			assets.id as device_id,
			customers.end_customer_name as end_customer_name,
            site.site_name as site_name,
            p.device_name as device_name,
			daily_count.high_count, 
            daily_count.critical_count,
            daily_count.date,
            customers.mobile_number as contact
            
FROM daily_incidents1 as daily_count
inner join end_customer_devices as assets on assets.id=daily_count.device_id
inner join end_customers as customers on customers.id= assets.end_customer_id
inner join end_customer_sites as site on site.id=assets.site_id
inner join promethean_devices as p on p.id=assets.promethean_device_id
where date(daily_count.date) = '2019-07-14'
order by daily_count.critical_count desc;
           
            
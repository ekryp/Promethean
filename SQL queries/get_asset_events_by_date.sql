


SELECT inc.serial_number,
description.incident_name as event_category, 
date(inc.date) as incident_date,
ADDDATE(date(inc.date), INTERVAL -7 DAY) as end_date,
count(*) as occurence,
'N' as ind_ideal
FROM daily_incidents as inc
inner  join  incident_description as description
on inc.incident_id = description.id
where inc.serial_number=200011515751535
and date(inc.date) ='2019-05-21'
group by incident_name
having date(incident_date) > end_date
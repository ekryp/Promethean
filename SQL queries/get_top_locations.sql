SELECT site.site_name,
sum(daily_count.critical_count) as total_critical

FROM daily_incidents1 AS daily_count

INNER JOIN end_customer_devices AS assets ON assets.id=daily_count.device_id
INNER JOIN end_customer_sites AS site ON site.id=assets.site_id
GROUP BY daily_count.serial_number
ORDER BY total_critical desc;



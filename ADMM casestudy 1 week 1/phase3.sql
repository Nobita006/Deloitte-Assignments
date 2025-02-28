CREATE DATABASE case1;

USE case1;

SELECT * FROM delivery_status;
SELECT * FROM orders;
SELECT * FROM carriers;

SELECT AVG(delivery_time) AS avg_delivery_time
FROM delivery_analytics;

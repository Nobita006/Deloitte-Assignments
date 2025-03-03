CREATE DATABASE case1;

DROP DATABASE case11;
CREATE DATABASE case11;
USE case11;

USE case1;

SELECT * FROM delivery_status;
SELECT * FROM orders;
SELECT * FROM carriers;

SELECT AVG(delivery_time) AS avg_delivery_time
FROM delivery_analytics;

DROP DATABASE case6;
CREATE DATABASE case6;
USE case6;

-- 1. Show traffic congestion by location and time
SELECT
    dl.location,
    dt.date_time,
    ft.congestion_level,
    ft.vehicle_count,
    ft.average_speed
FROM fact_traffic AS ft
JOIN dim_time AS dt 
    ON ft.fk_time_id = dt.time_id
JOIN dim_location AS dl
    ON ft.fk_location_id = dl.location_id
ORDER BY dt.date_time;

-- 2. Show top 10 accident-prone areas
SELECT
    dl.location,
    COUNT(*) AS total_accidents
FROM fact_accident AS fa
JOIN dim_location AS dl
    ON fa.fk_location_id = dl.location_id
GROUP BY dl.location
ORDER BY total_accidents DESC
LIMIT 10;

-- Example: top locations with the highest count of 'Fatal' accidents
SELECT 
    dl.location,
    COUNT(*) AS fatal_accidents
FROM fact_accident AS fa
JOIN dim_location AS dl
    ON fa.fk_location_id = dl.location_id
WHERE fa.accident_severity = 'Fatal'
GROUP BY dl.location
ORDER BY fatal_accidents DESC
LIMIT 10;


-- 3. Peak hour analysis for accidents
SELECT
    dt.hour,
    COUNT(*) AS total_accidents
FROM fact_accident AS fa
JOIN dim_time AS dt
    ON fa.fk_time_id = dt.time_id
GROUP BY dt.hour
ORDER BY total_accidents DESC;

-- Peak Hour for Traffic Volume
SELECT
    dt.hour,
    SUM(ft.vehicle_count) AS total_vehicles
FROM fact_traffic AS ft
JOIN dim_time AS dt
    ON ft.fk_time_id = dt.time_id
GROUP BY dt.hour
ORDER BY total_vehicles DESC;

-- Average speed by hour
SELECT
    dt.hour,
    AVG(ft.average_speed) AS avg_speed
FROM fact_traffic AS ft
JOIN dim_time AS dt
    ON ft.fk_time_id = dt.time_id
GROUP BY dt.hour
ORDER BY avg_speed;


-- 4. Peak hour analysis for traffic volume
SELECT
    dt.hour,
    SUM(ft.vehicle_count) AS total_vehicles
FROM fact_traffic AS ft
JOIN dim_time AS dt
    ON ft.fk_time_id = dt.time_id
GROUP BY dt.hour
ORDER BY total_vehicles DESC;

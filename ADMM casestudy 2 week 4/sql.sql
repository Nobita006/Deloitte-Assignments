DROP DATABASE case8;
CREATE DATABASE case8;
USE case8;

SELECT 
    DATE(admission_date) AS admission_day,
    COUNT(*) AS total_admissions
FROM fact_admissions
GROUP BY DATE(admission_date)
ORDER BY admission_day;

SELECT 
    diagnosis,
    AVG(risk_score) AS avg_risk
FROM fact_admissions
WHERE risk_score IS NOT NULL
GROUP BY diagnosis
ORDER BY avg_risk DESC;

SELECT
    diagnosis,
    COUNT(*) AS diagnosis_count
FROM fact_admissions
GROUP BY diagnosis
ORDER BY diagnosis_count DESC
LIMIT 5;

SELECT
    diagnosis,
    AVG(DATEDIFF(discharge_date, admission_date)) AS avg_length_of_stay
FROM fact_admissions
WHERE discharge_date IS NOT NULL
GROUP BY diagnosis
ORDER BY avg_length_of_stay DESC;

SELECT
    medication,
    COUNT(*) AS usage_count
FROM fact_treatments
GROUP BY medication
ORDER BY usage_count DESC
LIMIT 5;

SELECT
    fa.diagnosis,
    AVG(fv.heart_rate) AS avg_heart_rate
FROM fact_admissions AS fa
JOIN fact_vitals AS fv
    ON fa.admission_id = fv.admission_id
GROUP BY fa.diagnosis
ORDER BY avg_heart_rate DESC;

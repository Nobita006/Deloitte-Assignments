DROP DATABASE case3;
CREATE DATABASE case3;
USE case3;

select * from fact_employee_performance;

SET SQL_SAFE_UPDATES = 0;

-- Add a temporary auto-increment primary key column (if one doesn't exist)
ALTER TABLE fact_employee_performance 
ADD COLUMN temp_id INT AUTO_INCREMENT PRIMARY KEY;

-- Delete duplicate rows, keeping the row with the smallest temp_id for each Employee_ID
DELETE f1 
FROM fact_employee_performance f1
INNER JOIN fact_employee_performance f2 
    ON f1.Employee_ID = f2.Employee_ID 
    AND f1.temp_id > f2.temp_id;

-- Drop the temporary primary key column
ALTER TABLE fact_employee_performance 
DROP COLUMN temp_id;


-- 1. Employee Satisfaction Score (using Job_Satisfaction as a proxy)
SELECT AVG(Job_Satisfaction) AS avg_employee_satisfaction
FROM fact_employee_performance;

-- 2. Average Tenure (from the employee dimension)
SELECT AVG(Job_Tenure) AS avg_tenure
FROM dim_employee;

-- 3. Attrition Rate: Percentage of employees who left
SELECT 
    (SUM(CASE WHEN attrition = TRUE THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS attrition_rate_percentage
FROM fact_employee_performance;

-- 4. Performance Rating Distribution: Count employees by performance rating
SELECT 
    Performance_Rating,
    COUNT(*) AS employee_count
FROM fact_employee_performance
GROUP BY Performance_Rating
ORDER BY Performance_Rating;

-- 5. Department-wise Attrition Trends: Attrition rate per department
SELECT 
    d.Department,
    COUNT(*) AS total_employees,
    SUM(CASE WHEN f.attrition = TRUE THEN 1 ELSE 0 END) AS total_attritions,
    (SUM(CASE WHEN f.attrition = TRUE THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS attrition_rate_percentage
FROM fact_employee_performance f
JOIN dim_department d ON f.Department_ID = d.Department_ID
GROUP BY d.Department;

SELECT 
    d.Department,
    AVG(f.Exit_Interview_Score) AS avg_exit_interview_score
FROM 
    fact_employee_performance f
JOIN 
    dim_department d 
    ON f.Department_ID = d.Department_ID
GROUP BY 
    d.Department;

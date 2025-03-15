DROP DATABASE case5;
CREATE DATABASE case5;
USE case5;

-- Top 10 products by total revenue
SELECT 
    p.ProductKey,
    p.ProductName,
    SUM(f.Revenue) AS TotalRevenue
FROM fact_sales AS f
JOIN dim_product AS p 
    ON f.ProductKey = p.ProductKey
GROUP BY p.ProductKey, p.ProductName
ORDER BY TotalRevenue DESC
LIMIT 10;


-- Bottom 5 product subcategories by total revenue
SELECT 
    p.ProductSubcategoryKey,
    SUM(f.Revenue) AS SubcategoryRevenue
FROM fact_sales AS f
JOIN dim_product AS p 
    ON f.ProductKey = p.ProductKey
GROUP BY p.ProductSubcategoryKey
ORDER BY SubcategoryRevenue ASC
LIMIT 5;


WITH customer_summary AS (
    SELECT 
        c.CustomerKey,
        COUNT(DISTINCT f.CompositeKey) AS total_orders,
        SUM(f.Revenue) AS total_spent
    FROM fact_sales AS f
    JOIN dim_customer AS c 
        ON f.CustomerKey = c.CustomerKey
    GROUP BY c.CustomerKey
)
SELECT 
    CustomerKey,
    total_orders,
    total_spent,
    CASE
        WHEN total_spent >= 1000 THEN 'High-Value'
        WHEN total_orders >= 10 THEN 'Frequent Buyer'
        WHEN total_orders BETWEEN 2 AND 9 THEN 'Occasional Buyer'
        ELSE 'Rare Buyer'
    END AS Segment
FROM customer_summary
ORDER BY total_spent DESC;

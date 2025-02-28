ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY '12345';
FLUSH PRIVILEGES;

CREATE DATABASE case2;

USE case2;

SELECT 
  p.ProductID, 
  p.ProductName, 
  SUM(f.SalesAmount) AS TotalSales, 
  SUM(f.Quantity) AS TotalQuantity
FROM fact_sales f
JOIN dim_products p ON f.ProductID = p.ProductID
GROUP BY p.ProductID, p.ProductName
ORDER BY TotalSales DESC;

SELECT 
  c.CustomerID, 
  c.FirstName, 
  c.LastName, 
  COUNT(f.SaleID) AS NumPurchases, 
  SUM(f.SalesAmount) AS TotalSpent
FROM fact_sales f
JOIN dim_customers c ON f.CustomerID = c.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName
ORDER BY TotalSpent DESC;

SELECT 
  c.Region, 
  DATE_FORMAT(f.Timestamp, '%Y-%m') AS Month, 
  SUM(f.SalesAmount) AS MonthlySales
FROM fact_sales f
JOIN dim_customers c ON f.CustomerID = c.CustomerID
GROUP BY c.Region, DATE_FORMAT(f.Timestamp, '%Y-%m')
ORDER BY c.Region, Month;

SELECT SUM(SalesAmount) AS TotalSalesRevenue
FROM fact_sales;

SELECT 
  Month, 
  TotalSales,
  LAG(TotalSales) OVER (ORDER BY Month) AS PreviousMonthSales,
  ROUND(((TotalSales - LAG(TotalSales) OVER (ORDER BY Month)) / LAG(TotalSales) OVER (ORDER BY Month)) * 100, 2) AS GrowthRatePercentage
FROM (
  SELECT DATE_FORMAT(Timestamp, '%Y-%m') AS Month, SUM(SalesAmount) AS TotalSales
  FROM fact_sales
  GROUP BY DATE_FORMAT(Timestamp, '%Y-%m')
) t;

SELECT 
  c.CustomerID, 
  c.FirstName, 
  c.LastName, 
  SUM(f.SalesAmount) AS LifetimeRevenue
FROM fact_sales f
JOIN dim_customers c ON f.CustomerID = c.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName
ORDER BY LifetimeRevenue DESC;

SELECT * FROM fact_sales;

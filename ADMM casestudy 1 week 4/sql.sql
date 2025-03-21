DROP DATABASE case7;
CREATE DATABASE case7;
USE case7;

SELECT p.Category AS ProductCategory,
       SUM(f.SalesAmount) AS TotalSales
  FROM fact_internetsales f
  JOIN dimproduct p ON f.ProductKey = p.ProductKey
 GROUP BY p.Category
 ORDER BY TotalSales DESC;

SELECT r.ResellerName,
       SUM(f.SalesAmount) AS TotalSales
  FROM fact_resellersales f
  JOIN dimreseller r ON f.ResellerKey = r.ResellerKey
 GROUP BY r.ResellerName
 ORDER BY TotalSales DESC
 LIMIT 5;

SELECT t.SalesTerritoryRegion AS TerritoryRegion,
       t.SalesTerritoryCountry AS TerritoryCountry,
       SUM(f.SalesAmount) AS TotalSales
  FROM fact_internetsales AS f
  JOIN dimsalesterritory AS t ON f.SalesTerritoryKey = t.SalesTerritoryKey
 GROUP BY t.SalesTerritoryRegion, t.SalesTerritoryCountry
 ORDER BY TotalSales DESC;

SELECT e.EmployeeName,
       SUM(f.SalesAmount) AS TotalSales
  FROM fact_resellersales AS f
  JOIN dimemployee AS e ON f.EmployeeKey = e.EmployeeKey
 GROUP BY e.EmployeeName
 ORDER BY TotalSales DESC;

SELECT r.ResellerName,
       SUM(f.SalesAmount) AS TotalSales
  FROM fact_resellersales AS f
  JOIN dimreseller AS r ON f.ResellerKey = r.ResellerKey
 GROUP BY r.ResellerName
 ORDER BY TotalSales DESC
 LIMIT 5;

SELECT YEAR(OrderDate) AS OrderYear,
       SUM(SalesAmount) AS TotalSales
  FROM fact_internetsales
 GROUP BY YEAR(OrderDate)
 ORDER BY OrderYear;

SELECT r.ResellerName,
       SUM(f.SalesAmount) AS TotalSales
  FROM fact_resellersales AS f
  JOIN dimreseller AS r ON f.ResellerKey = r.ResellerKey
 GROUP BY r.ResellerName
 ORDER BY TotalSales DESC
 LIMIT 5;

SELECT c.Occupation,
       SUM(f.SalesAmount) AS TotalSales
  FROM fact_internetsales AS f
  JOIN dimcustomer AS c ON f.CustomerKey = c.CustomerKey
 GROUP BY c.Occupation
 ORDER BY TotalSales DESC;

SELECT SUM(DiscountAmount) AS TotalDiscount,
       SUM(SalesAmount) AS TotalSales
  FROM fact_resellersales;

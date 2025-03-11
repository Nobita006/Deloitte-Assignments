DROP DATABASE case4;
CREATE DATABASE case4;
USE case4;

select * from sales;

-- Fast-Moving Products
SELECT 
  dp.Product_ID,
  SUM(fs.Quantity_Sold) AS Total_Sales
FROM fact_sales fs
JOIN dim_products dp 
  ON fs.product_key = dp.product_key
JOIN dim_dates dd 
  ON fs.date_key = dd.date_key
WHERE dd.date >= CURDATE() - INTERVAL 3 MONTH
GROUP BY dp.Product_ID
ORDER BY Total_Sales DESC
LIMIT 10;

-- Slow-Moving Products:
SELECT 
  dp.Product_ID,
  SUM(fs.Quantity_Sold) AS Total_Sales
FROM fact_sales fs
JOIN dim_products dp 
  ON fs.product_key = dp.product_key
JOIN dim_dates dd 
  ON fs.date_key = dd.date_key
WHERE dd.date >= CURDATE() - INTERVAL 3 MONTH
GROUP BY dp.Product_ID
ORDER BY Total_Sales ASC
LIMIT 10;

-- Find Products Below Reorder Level
SELECT 
  dp.Product_ID,
  ds.Store_ID,
  dw.Warehouse_ID,
  fi.Stock_Level,
  fi.Reorder_Level,
  dd.date AS Last_Updated
FROM fact_inventory fi
JOIN dim_products dp 
  ON fi.product_key = dp.product_key
JOIN dim_stores ds 
  ON fi.store_key = ds.store_key
JOIN dim_warehouses dw 
  ON fi.warehouse_key = dw.warehouse_key
JOIN dim_dates dd 
  ON fi.date_key = dd.date_key
WHERE fi.Stock_Level < fi.Reorder_Level;


-- Identify Suppliers with High Lead Times
SELECT 
  supplier_key,
  Supplier_ID,
  Supplier_Name,
  `Lead_Time (days)` AS Lead_Time,
  `Order_Frequency`
FROM dim_suppliers
WHERE `Lead_Time (days)` > (SELECT AVG(`Lead_Time (days)`) FROM dim_suppliers)
ORDER BY `Lead_Time (days)` DESC;

-- Suggest Alternative Suppliers for a Product
SELECT 
  dp.Product_ID,
  ds1.Supplier_ID AS High_LeadTime_Supplier,
  ds1.Supplier_Name AS High_LeadTime_Supplier_Name,
  ds1.`Lead_Time (days)` AS High_Lead_Time,
  ds2.Supplier_ID AS Alternative_Supplier,
  ds2.Supplier_Name AS Alternative_Supplier_Name,
  ds2.`Lead_Time (days)` AS Alternative_Lead_Time
FROM fact_purchase_orders fpo1
JOIN dim_products dp 
  ON fpo1.product_key = dp.product_key
JOIN dim_suppliers ds1 
  ON fpo1.supplier_key = ds1.supplier_key
JOIN fact_purchase_orders fpo2 
  ON fpo1.product_key = fpo2.product_key
JOIN dim_suppliers ds2 
  ON fpo2.supplier_key = ds2.supplier_key
WHERE ds1.`Lead_Time (days)` > ds2.`Lead_Time (days)`
ORDER BY dp.Product_ID, ds1.`Lead_Time (days)` DESC, ds2.`Lead_Time (days)` ASC;

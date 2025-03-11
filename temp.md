Below is a detailed report outlining the entire process I followed for this project. The report describes how I loaded the CSV data into Pandas DataFrames, performed data cleaning and transformations (including removing duplicates and handling composite keys), built a star schema by creating fact and dimension tables with surrogate keys, loaded the transformed data into MySQL, and finally executed SQL queries for supply chain analysis. I have also left a section where I can later insert the KPI Tracking & Monitoring results.

---

# Detailed Report on Supply Chain ETL & Analytics Project

## Introduction

In this project, I developed a complete ETL pipeline for a supply chain and inventory management scenario. I extracted data from CSV files, performed rigorous data quality checks and transformations, built a star schema to support fast and efficient reporting, loaded the transformed data into a MySQL database, and wrote SQL queries to perform key analyses. This report outlines each step of the process in detail.

---

## 1. Data Extraction

I started by loading the CSV files that contained data for sales, inventory, suppliers, and purchase orders. For this purpose, I used the Pandas library to read the CSVs and convert date columns to proper datetime objects:

```python
import pandas as pd

# Load CSV files into DataFrames with appropriate date parsing
sales_df = pd.read_csv("sales_data-2.csv", parse_dates=["Sale_Date"])
inventory_df = pd.read_csv("inventory_data.csv", parse_dates=["Last_Updated"])
suppliers_df = pd.read_csv("suppliers_data.csv")
purchase_orders_df = pd.read_csv("purchase_orders_data.csv", parse_dates=["Order_Date", "Arrival_Date"])
```

---

## 2. Data Cleaning & Transformation

### 2.1 Data Quality Checks

I performed multiple data quality checks to ensure the integrity of the data:

- **Missing Data:**  
  I examined each DataFrame for missing values in key columns (such as primary keys) and dropped rows that were missing essential identifiers.

- **Duplicate Handling:**  
  For tables with single-column primary keys (Sales, Suppliers, and Purchase Orders), I dropped duplicate records by keeping the first occurrence.  
  The Inventory data required special attention because it has a composite key (`Product_ID`, `Store_ID`, `Warehouse_ID`). I sorted this data by `Last_Updated` (so the most recent record came first) and then removed duplicates based on the composite key.

```python
# Drop rows with missing primary key values
sales_df.dropna(subset=["Sale_ID"], inplace=True)
suppliers_df.dropna(subset=["Supplier_ID"], inplace=True)
purchase_orders_df.dropna(subset=["Order_ID"], inplace=True)
inventory_df.dropna(subset=["Product_ID", "Store_ID", "Warehouse_ID"], inplace=True)

# Remove duplicates in single-column primary keys
sales_df.drop_duplicates(subset=["Sale_ID"], keep="first", inplace=True)
suppliers_df.drop_duplicates(subset=["Supplier_ID"], keep="first", inplace=True)
purchase_orders_df.drop_duplicates(subset=["Order_ID"], keep="first", inplace=True)

# Resolve duplicates in Inventory by keeping the latest record based on Last_Updated
inventory_df.sort_values(by="Last_Updated", ascending=False, inplace=True)
inventory_df.drop_duplicates(subset=["Product_ID", "Store_ID", "Warehouse_ID"], keep="first", inplace=True)
```

---

### 2.2 Building the Star Schema

To enable efficient analytical queries, I transformed the data into a star schema by creating separate dimension and fact tables. The main dimensions I built include:

- **dim_products:** Contains unique product IDs with surrogate keys.
- **dim_suppliers:** Consolidates supplier information with a surrogate key.
- **dim_stores:** Contains unique store IDs.
- **dim_warehouses:** Contains unique warehouse IDs.
- **dim_dates:** A date dimension built from all unique dates in the datasets, including year, month, and day.

The fact tables include:

- **fact_sales:** Records sales transactions with references (via surrogate keys) to products, stores, and dates.
- **fact_inventory:** Records current inventory levels with references to products, stores, warehouses, and last update dates.
- **fact_purchase_orders:** Records purchase orders with references to products, suppliers, and both order and arrival dates.

Below is a snippet showing how I built one of the dimension tables and mapped surrogate keys:

```python
# --- Create Dimension: Products ---
all_product_ids = set(sales_df["Product_ID"].dropna().unique()) \
    .union(inventory_df["Product_ID"].dropna().unique()) \
    .union(purchase_orders_df["Product_ID"].dropna().unique()) \
    .union(suppliers_df["Product_ID"].dropna().unique())

dim_products = pd.DataFrame({"Product_ID": sorted(all_product_ids)})
dim_products["product_key"] = range(1, len(dim_products) + 1)
dim_products = dim_products[["product_key", "Product_ID"]]

# Example helper function to map product keys to fact tables:
def map_product_key(df, product_id_col):
    return pd.merge(
        df, 
        dim_products, 
        how="left", 
        left_on=product_id_col, 
        right_on="Product_ID"
    )

# --- Build Fact Table: Sales ---
fact_sales = sales_df.copy()
fact_sales = map_product_key(fact_sales, "Product_ID")
# Similar mapping functions were created for stores and dates.
```

I followed similar procedures for creating the remaining dimension and fact tables, ensuring that each fact table referenced its corresponding dimension tables via surrogate keys.

---

## 3. Loading Data into MySQL

After transforming the data, I loaded the dimension and fact tables into a MySQL database using SQLAlchemy. This allowed for efficient querying and integration with BI tools like Power BI.

```python
from sqlalchemy import create_engine
import pymysql

# MySQL connection parameters
username = 'root'
password = '12345'
host = 'localhost'
port = '3306'
database = 'case1'

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

# Loading dimension tables
dim_products.to_sql("case4_dim_products", con=engine, if_exists="replace", index=False)
# (Other dimensions: dim_suppliers, dim_stores, dim_warehouses, dim_dates)

# Loading fact tables
fact_sales.to_sql("case4_fact_sales", con=engine, if_exists="replace", index=False)
# (Other facts: fact_inventory, fact_purchase_orders)

print("Data successfully loaded into MySQL.")
```

---

## 4. SQL Analysis for Supply Chain Insights

I then created several SQL queries to perform key analyses on the data:

### 4.1 Identifying Fast-Moving and Slow-Moving Products

These queries aggregate sales data over the past three months to determine the highest and lowest sales performers.

**Fast-Moving Products:**

```sql
SELECT 
  dp.Product_ID,
  SUM(fs.Quantity_Sold) AS Total_Sales
FROM case4_fact_sales fs
JOIN case4_dim_products dp 
  ON fs.product_key = dp.product_key
JOIN case4_dim_dates dd 
  ON fs.date_key = dd.date_key
WHERE dd.date >= CURDATE() - INTERVAL 3 MONTH
GROUP BY dp.Product_ID
ORDER BY Total_Sales DESC
LIMIT 10;
```

**Slow-Moving Products:**

```sql
SELECT 
  dp.Product_ID,
  SUM(fs.Quantity_Sold) AS Total_Sales
FROM case4_fact_sales fs
JOIN case4_dim_products dp 
  ON fs.product_key = dp.product_key
JOIN case4_dim_dates dd 
  ON fs.date_key = dd.date_key
WHERE dd.date >= CURDATE() - INTERVAL 3 MONTH
GROUP BY dp.Product_ID
ORDER BY Total_Sales ASC
LIMIT 10;
```

### 4.2 Reporting Products Below Reorder Level

This query identifies products where the current stock level is lower than the reorder level:

```sql
SELECT 
  dp.Product_ID,
  ds.Store_ID,
  dw.Warehouse_ID,
  fi.Stock_Level,
  fi.Reorder_Level,
  dd.date AS Last_Updated
FROM case4_fact_inventory fi
JOIN case4_dim_products dp 
  ON fi.product_key = dp.product_key
JOIN case4_dim_stores ds 
  ON fi.store_key = ds.store_key
JOIN case4_dim_warehouses dw 
  ON fi.warehouse_key = dw.warehouse_key
JOIN case4_dim_dates dd 
  ON fi.date_key = dd.date_key
WHERE fi.Stock_Level < fi.Reorder_Level;
```

### 4.3 Supplier Lead Time Analysis

I performed an analysis to identify suppliers with above-average lead times, and then suggested alternative suppliers with lower lead times for the same products.

**Identify Suppliers with High Lead Times:**

```sql
SELECT 
  supplier_key,
  Supplier_ID,
  Supplier_Name,
  `Lead_Time (days)` AS Lead_Time,
  `Order_Frequency`
FROM case4_dim_suppliers
WHERE `Lead_Time (days)` > (SELECT AVG(`Lead_Time (days)`) FROM case4_dim_suppliers)
ORDER BY `Lead_Time (days)` DESC;
```

**Suggest Alternative Suppliers:**

```sql
SELECT 
  dp.Product_ID,
  ds1.Supplier_ID AS High_LeadTime_Supplier,
  ds1.Supplier_Name AS High_LeadTime_Supplier_Name,
  ds1.`Lead_Time (days)` AS High_Lead_Time,
  ds2.Supplier_ID AS Alternative_Supplier,
  ds2.Supplier_Name AS Alternative_Supplier_Name,
  ds2.`Lead_Time (days)` AS Alternative_Lead_Time
FROM case4_fact_purchase_orders fpo1
JOIN case4_dim_products dp 
  ON fpo1.product_key = dp.product_key
JOIN case4_dim_suppliers ds1 
  ON fpo1.supplier_key = ds1.supplier_key
JOIN case4_fact_purchase_orders fpo2 
  ON fpo1.product_key = fpo2.product_key
JOIN case4_dim_suppliers ds2 
  ON fpo2.supplier_key = ds2.supplier_key
WHERE ds1.`Lead_Time (days)` > ds2.`Lead_Time (days)`
ORDER BY dp.Product_ID, ds1.`Lead_Time (days)` DESC, ds2.`Lead_Time (days)` ASC;
```

---

## 5. KPI Tracking & Monitoring Results

*Insert KPI Tracking & Monitoring results here.*  
*(This section will be updated once I perform further analysis and obtain the necessary metrics. Examples of KPIs include total sales, average lead time, stockout rates, etc.)*

---

## Conclusion

In this project, I successfully built a complete ETL pipeline to support supply chain and inventory management analytics. I:
- Extracted data from CSV files into Pandas DataFrames.
- Cleaned the data by handling missing values and duplicates.
- Transformed the data into a star schema with dedicated dimension and fact tables.
- Loaded the transformed data into a MySQL database.
- Developed SQL queries to identify fast- and slow-moving products, report on products below reorder levels, and analyze supplier lead times.

This project has not only reinforced my skills in data processing, ETL, and data warehousing but also provided me with actionable insights that can be leveraged in a BI tool like Power BI for further KPI tracking and supply chain optimization.


# Employee Performance and Attrition Report

## 1. Data Extraction: Loading CSV Files into Pandas

I began by extracting the data from the provided CSV files. The project involved three datasets:
- **Employee Data:** Contains personal and job-related details.
- **Attrition Data:** Contains attrition flags and exit interview scores.
- **Employee Performance Data:** Contains performance ratings, training hours, and other performance metrics.

I loaded each CSV into a Pandas DataFrame and ensured that the lower-case `gender` column was removed so that only the `Gender` column was used. Here is the snippet I used:

```python
import pandas as pd

# Read CSV files
employee_df = pd.read_csv('employee_data 1.csv')
attrition_df = pd.read_csv('Attrition 1.csv')
performance_df = pd.read_csv('employee_performance_data 1.csv')

# Remove lower-case 'gender' column, if present
if 'gender' in employee_df.columns:
    employee_df = employee_df.drop(columns=['gender'])

print("Employee Data Columns:", employee_df.columns.tolist())
```

## 2. Data Transformation & Cleaning

After loading the data, I validated the uniqueness of the primary key (`Employee_ID`) in each dataset to ensure data integrity. I then merged the datasets:
- I performed an inner join between `employee_df` and `performance_df` on `Employee_ID` to capture only the employees with available performance data.
- I then left-joined the resulting DataFrame with `attrition_df` on `Employee_ID`.

Since the project required only complete records, I removed any rows that had missing values in either the `attrition` or `Exit_Interview_Score` columns:

```python
# Merge employee and performance data on Employee_ID
emp_perf_df = pd.merge(employee_df, performance_df, on='Employee_ID', how='inner')

# Merge with attrition data (left join)
full_df = pd.merge(emp_perf_df, attrition_df, on='Employee_ID', how='left')

# Drop rows with missing 'attrition' or 'Exit_Interview_Score'
full_df = full_df.dropna(subset=['attrition', 'Exit_Interview_Score'])
print("Data shape after dropping incomplete records:", full_df.shape)
```

I also merged the `first_name` and `last_name` columns into a single `name` column in the employee dimension later in the process.

## 3. Creating Fact and Dimension Tables (Star Schema)

I then transformed the cleaned DataFrame into a star schema by creating one fact table and several dimension tables.

### Fact Table: `fact_employee_performance`

This table captures performance metrics along with attrition and exit interview scores. It also includes surrogate key references for departments and job roles.

```python
fact_table = full_df[['Employee_ID', 'Performance_Rating', 'Last_Promotion_Year', 
                        'Training_Hours', 'Work_Life_Balance', 'Job_Satisfaction', 
                        'attrition', 'Exit_Interview_Score']].copy()
```

### Dimension Tables

#### Employee Dimension (`dim_employee`)
I excluded `Department` and `Job_Role` from this dimension, and I merged `first_name` and `last_name` into a new `name` column.

```python
dim_employee = full_df[['Employee_ID', 'Age', 'first_name', 'last_name', 'Gender', 
                          'Education_Level', 'Marital_Status', 'Job_Tenure', 'Distance_From_Home']].drop_duplicates()

# Merge first name and last name into a single column 'name'
dim_employee['name'] = dim_employee['first_name'] + ' ' + dim_employee['last_name']
dim_employee = dim_employee.drop(columns=['first_name', 'last_name'])
```

#### Department Dimension (`dim_department`)
I created a table containing unique departments and added a surrogate key:

```python
dim_department = full_df[['Department']].drop_duplicates().reset_index(drop=True)
dim_department['Department_ID'] = dim_department.index + 1
```

#### Role Dimension (`dim_role`)
Similarly, I created a role dimension table:

```python
dim_role = full_df[['Job_Role']].drop_duplicates().reset_index(drop=True)
dim_role['Role_ID'] = dim_role.index + 1
```

Next, I merged the department and role information into the fact table to reference their surrogate keys:

```python
# Merge department and role info into the fact table for key references
fact_table = pd.merge(fact_table, full_df[['Employee_ID', 'Department', 'Job_Role']], on='Employee_ID', how='left')
fact_table = pd.merge(fact_table, dim_department, on='Department', how='left')
fact_table = pd.merge(fact_table, dim_role, on='Job_Role', how='left')
fact_table.drop(columns=['Department', 'Job_Role'], inplace=True)
```

## 4. Removing Duplicate Employee_ID Records

Although I ensured data integrity during transformation, I also implemented a method to remove duplicate `Employee_ID` records directly from the `fact_employee_performance` table.

### In Python:
I checked for duplicate `Employee_ID` values and removed them prior to loading the data into MySQL. For example:

```python
fact_table = fact_table.drop_duplicates(subset=['Employee_ID'])
```

### In SQL:
After loading the transformed data into MySQL, I used the following SQL code to remove any duplicate records from the `fact_employee_performance` table. To work around MySQL safe update mode, I disabled safe updates for the session:

```sql
-- Disable safe update mode for this session
SET SQL_SAFE_UPDATES = 0;

-- Add a temporary auto-increment primary key column
ALTER TABLE fact_employee_performance 
ADD COLUMN temp_id INT AUTO_INCREMENT PRIMARY KEY;

-- Delete duplicate rows, keeping the row with the smallest temp_id for each Employee_ID
DELETE f1 
FROM fact_employee_performance f1
INNER JOIN fact_employee_performance f2 
    ON f1.Employee_ID = f2.Employee_ID 
    AND f1.temp_id > f2.temp_id;

-- Remove the temporary column
ALTER TABLE fact_employee_performance 
DROP COLUMN temp_id;
```

This SQL code ensures that only one record per `Employee_ID` remains in the fact table.

## 5. Loading Transformed Data into MySQL

I used SQLAlchemy to connect to the MySQL database and loaded the fact and dimension tables into their respective tables. Here is the code snippet:

```python
from sqlalchemy import create_engine

# MySQL connection details
username = 'root'
password = '12345'
host = 'localhost'
port = '3306'
database = 'case3'
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

# Load tables into MySQL
fact_table.to_sql('fact_employee_performance', con=engine, if_exists='replace', index=False)
dim_employee.to_sql('dim_employee', con=engine, if_exists='replace', index=False)
dim_department.to_sql('dim_department', con=engine, if_exists='replace', index=False)
dim_role.to_sql('dim_role', con=engine, if_exists='replace', index=False)

print("Data loaded to MySQL successfully.")
```

## 6. KPI Tracking & Monitoring

At this stage, I have prepared the data warehouse and the star schema. I will now use Power BI to create dynamic dashboards that display various key performance indicators (KPIs) such as Attrition Rate, Retention Rate, Average Tenure, Department-wise Employee Score, Average Performance Rating, Average Exit Interview Satisfaction Score, and Department-wise Attrition Rate.

<!--  
**KPI Tracking & Monitoring Results:**

*Here, I will insert the KPI results, charts, and visualizations once the Power BI dashboard is complete.*
-->

## 7. Conclusion

In this project, I:
- Loaded data from CSV files into Pandas DataFrames.
- Cleaned and transformed the data by merging datasets, removing rows with missing critical values, and consolidating names.
- Created a star schema with a fact table (`fact_employee_performance`) and dimension tables (`dim_employee`, `dim_department`, `dim_role`).
- Ensured data integrity by removing duplicate `Employee_ID` entries both in Python and using SQL.
- Loaded the transformed data into MySQL.
- Prepared the environment for Power BI to perform KPI tracking and monitoring, leaving placeholders for the final visualization outputs.

This process allowed me to build a robust data warehouse that supports detailed analysis and interactive reporting on employee performance and attrition.

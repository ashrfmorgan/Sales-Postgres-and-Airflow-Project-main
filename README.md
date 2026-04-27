# Sales Data Warehouse ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline that transforms sales data from multiple CSV files into a structured Data Warehouse (Star Schema) using PostgreSQL, Jupyter for development, and Apache Airflow for orchestration.

**Side Note:** This project was built using sample sales data from three CSV files representing sales transactions over multiple months.

---

## 📌 Table of Contents

- [🔎 Project Overview](#-project-overview)
- [🚧 Problem Statement](#-problem-statement)
- [🚀 Project Objectives](#-project-objectives)
- [❗ Source and Destination](#-source-and-destination)
- [💡 Dataset Description](#-dataset-description)
- [📈 Proposed Solution](#-proposed-solution)
- [🔧 System Architecture](#-system-architecture)
- [🧪 Tools and Technologies](#-tools-and-technologies)
- [🧬 Database Design (Star Schema with SCD Type 2)](#-database-design-star-schema-with-scd-type-2)
- [📦 Airflow Workflow Design](#-airflow-workflow-design)
- [🐛 Issues Encountered and Solutions](#-issues-encountered-and-solutions)
- [🧠 Expected Outcomes](#-expected-outcomes)
- [📝 Project Deliverables](#-project-deliverables)
- [💡 Benefits of the Project](#-benefits-of-the-project)
- [📬 Future Enhancements](#-future-enhancements)
- [👨‍💻 Author](#-author)
- [📊 Project Statistics](#-project-statistics)

---

## 🔎 Project Overview

The purpose of this project is to design and implement a complete ETL pipeline for sales data stored across multiple CSV files. The pipeline extracts data from source flat files, performs data cleaning and transformation operations, and loads the processed data into a PostgreSQL database designed as a **Data Warehouse (Star Schema)** with **Type 2 Slowly Changing Dimensions (SCD)** for historical accuracy.

To ensure automation, scheduling, and monitoring, the entire ETL workflow is orchestrated using Apache Airflow. Development and exploration were performed using Jupyter notebooks, with production code promoted to Python scripts in the Airflow scripts folder. This project demonstrates core Data Engineering concepts including data ingestion, SCD management, data warehouse modeling, and workflow orchestration.

## 🚧 Problem Statement

Sales data is often stored across multiple CSV files (e.g., monthly sales exports) that may contain **inconsistent data** for the same business entities. For example, the same customer might appear with different cities in different files, or the same product might have different prices over time. This creates several challenges:

1. **Data Inconsistency**: The same customer ID might have different attributes across files
2. **Historical Accuracy**: Product prices change over time, but simple joins lose this history
3. **Referential Integrity**: Merging inconsistent dimensions creates cartesian products in fact tables
4. **Duplicate Records**: Naive joins produce duplicate fact rows that need manual cleanup

Without a proper ETL process that handles **Slowly Changing Dimensions**, business reports would be inaccurate and historical analysis impossible. This project solves these problems by implementing a Type 2 SCD approach that preserves historical changes while maintaining data integrity.

## 🚀 Project Objectives

The main objectives of this project are:
- **Extract** data from multiple CSV files representing monthly sales data
- **Clean and preprocess** raw data with proper data type conversion
- **Implement Type 2 Slowly Changing Dimensions** to track historical changes in customers and products
- **Transform** source data into a Fact and Dimension (Star Schema) model
- **Resolve data inconsistencies** across multiple source files
- **Load** transformed data into PostgreSQL with referential integrity
- **Automate** the entire ETL workflow using Apache Airflow
- **Enable** historical accurate reporting and analysis

## ❗ Source and Destination

**Source:**
- Multiple CSV files (sales_2025_01.csv, sales_2025_02.csv, sales_2025_03.csv) containing sales transactions, located in the `./data/raw/` directory.

**Destination:**
- PostgreSQL database (`sales_dw`), modeled as a **Data Warehouse** with Type 2 SCD dimensions.

**Development Environment:**
- Jupyter notebooks in the `./dev_code/` folder for interactive development and testing.

**Production Code:**
- Python scripts in the `./airflow/scripts/` folder for automated execution.

**Automation / Orchestration Tool:**
- Apache Airflow with LocalExecutor, with DAGs stored in `./airflow/dags/`.

## 💡 Dataset Description

The dataset represents sales transactions from a retail business and includes the following fields:

| Column | Description | Example |
|--------|-------------|---------|
| `order_id` | Unique order identifier | 1001 |
| `order_date` | Date of the order | 2024-01-15 |
| `customer_id` | Customer identifier | C001 |
| `customer_name` | Customer full name | Ahmed Hassan |
| `city` | Customer city | Cairo |
| `country` | Customer country | Egypt |
| `product_id` | Product identifier | P100 |
| `product_name` | Product name | Laptop |
| `category` | Product category | Electronics |
| `quantity` | Number of units sold | 1 |
| `unit_price` | Price per unit | 1200.00 |
| `unit_cost` | Cost per unit | 900.00 |

**Source Files Distribution:**
- `sales_2025_01.csv`: 20 rows (40%)
- `sales_2025_02.csv`: 20 rows (40%)
- `sales_2025_03.csv`: 10 rows (20%)

## 📈 Proposed Solution

The proposed solution is to build an end-to-end ETL pipeline with the following stages:

### Extract
- Read all CSV files from the `/data/raw/` directory (mounted from `./data/raw/` on the host).
- Add `source_file` column to track data provenance.
- Load and combine all files into a single DataFrame.

### Transform
- **Clean data** by converting data types and handling inconsistencies.
- **Implement Type 2 SCD** for customers and products:
  - Track changes in customer attributes (e.g., city changes)
  - Track changes in product prices over time
  - Add `valid_from`, `valid_to`, and `is_current` columns
- **Create Date Dimension** with various date attributes.
- **Build Fact Table** by joining with the correct dimension versions based on order date.
- **Calculate Measures**: `total_sales`, `total_cost`, `profit`, `profit_margin`.

### Load
- Create a fresh `sales_dw` database with proper schema.
- Load transformed data into PostgreSQL with proper schema and type mapping.
- Implement foreign key constraints for referential integrity.
- Add primary keys for query performance.

### Export
- Export all dimension and fact tables to CSV files in `/data/transformed/` (mounted from `./data/transformed/` on the host) for backup and external use.

### Automation
- Use Apache Airflow DAGs to:
  - Schedule the ETL job daily at 2 AM
  - Execute the production Python script
  - Monitor execution and log pipeline activity
  - Handle retries and error reporting with email notifications

## 🔧 System Architecture

```mermaid
graph TD
    subgraph INPUT["Input Layer"]
        CSV1[sales_2025_01.csv]
        CSV2[sales_2025_02.csv]
        CSV3[sales_2025_03.csv]
    end

    subgraph DEV["Development Environment"]
        JUPYTER[Jupyter Lab<br>Port 8888]
        DEV_CODE["./dev_code/<br>Notebooks"]
    end

    subgraph PROD["Production Environment"]
        SCRIPT["./airflow/scripts/<br>data_transformation.py"]
    end

    subgraph ORCH["Orchestration"]
        DAG[Airflow DAG<br>sales_data_warehouse]
        SCHEDULE[Schedule: 2 AM Daily]
    end

    subgraph TRANSFORM["Transformation Logic"]
        SCD[Type 2 SCD Implementation]
        CUSTOMER[dim_customer<br>with history]
        PRODUCT[dim_product<br>with history]
        DATE[dim_date]
        FACT[fact_sales]
    end

    subgraph DB["Data Warehouse"]
        PG[(PostgreSQL<br>sales_dw)]
        SCHEMA[Star Schema<br>with SCD Type 2]
    end

    subgraph EXPORT["Export"]
        CSV_OUT[CSV Backups<br>./data/transformed/]
    end

    subgraph QUALITY["Quality Checks"]
        INTEGRITY[Referential Integrity]
        COUNTS[Row Count Validation]
        NEGATIVE[No Negative Values]
    end

    subgraph NOTIFY["Notifications"]
        EMAIL[Email Reports]
    end

    %% Connections
    CSV1 & CSV2 & CSV3 --> JUPYTER
    CSV1 & CSV2 & CSV3 --> SCRIPT
    
    JUPYTER -->|Develop| DEV_CODE
    DEV_CODE -->|Convert to| SCRIPT
    
    SCRIPT -->|Executed by| DAG
    DAG -->|Runs at| SCHEDULE
    
    SCRIPT -->|Implements| SCD
    SCD --> CUSTOMER
    SCD --> PRODUCT
    SCD --> DATE
    CUSTOMER & PRODUCT & DATE --> FACT
    
    FACT -->|Load to| PG
    PG -->|Verify| QUALITY
    QUALITY --> INTEGRITY
    QUALITY --> COUNTS
    QUALITY --> NEGATIVE
    
    PG -->|Export to| CSV_OUT
    
    DAG -->|On Complete| EMAIL
```

### Data Flow Description

| Step | Layer | Technology | Description |
|------|-------|------------|-------------|
| 1 | Source | CSV Files | Raw monthly sales data files in `./data/raw/` |
| 2 | Extract | Python/Pandas | Read CSVs, add source_file, combine |
| 3 | Transform | Python/Pandas | Type 2 SCD implementation, dimension creation |
| 4 | Model | Python/Pandas | Star schema transformations with historical joins |
| 5 | Load | PostgreSQL | Load into fact and dimension tables in `sales_dw` |
| 6 | Export | CSV Files | Export all tables to `./data/transformed/` for backup |
| 7 | Orchestrate | Apache Airflow | Schedule, monitor, and manage ETL tasks via DAG |

## 🧪 Tools and Technologies

The following tools and technologies were used in this project:
- **Python 3.11** for ETL scripting and data transformation
- **Pandas** for data manipulation and analysis
- **SQLAlchemy** for database interaction and type mapping
- **PostgreSQL 15** as the target data warehouse
- **Apache Airflow 2.7.3** for workflow orchestration (LocalExecutor)
- **Jupyter Notebooks** for interactive development and testing (in `./dev_code/`)
- **Docker & Docker Compose** for containerized environment setup
- **pgAdmin** for database management and visualization
- **psycopg2** for PostgreSQL connection
- **nbconvert** for Jupyter notebook to Python script conversion
- **Git** for version control

## 🧬 Database Design (Star Schema with SCD Type 2)

The PostgreSQL data warehouse is modeled as a Star Schema with Type 2 Slowly Changing Dimensions:

### Dimension Tables:

#### `dim_customer` (Type 2 SCD)
| Column | Type | Description |
|--------|------|-------------|
| `customer_sk` | INTEGER (PK) | Surrogate key |
| `customer_id` | VARCHAR(50) | Business key |
| `customer_name` | VARCHAR(100) | Customer name |
| `city` | VARCHAR(100) | Customer city |
| `country` | VARCHAR(100) | Customer country |
| `valid_from` | DATE | Start date of this version |
| `valid_to` | DATE | End date of this version (NULL if current) |
| `is_current` | BOOLEAN | Flag indicating current version |
| `source_files` | TEXT | Source files containing this version |

#### `dim_product` (Type 2 SCD)
| Column | Type | Description |
|--------|------|-------------|
| `product_sk` | INTEGER (PK) | Surrogate key |
| `product_id` | VARCHAR(50) | Business key |
| `product_name` | VARCHAR(200) | Product name |
| `category` | VARCHAR(100) | Product category |
| `unit_price` | DECIMAL(10,2) | Price at this version |
| `unit_cost` | DECIMAL(10,2) | Cost at this version |
| `profit_margin` | DECIMAL(10,2) | Calculated margin |
| `valid_from` | DATE | Start date of this version |
| `valid_to` | DATE | End date of this version (NULL if current) |
| `is_current` | BOOLEAN | Flag indicating current version |
| `source_files` | TEXT | Source files containing this version |

#### `dim_date`
| Column | Type | Description |
|--------|------|-------------|
| `date_sk` | INTEGER (PK) | Surrogate key |
| `date` | DATE | Calendar date |
| `year` | INTEGER | Year |
| `month` | INTEGER | Month (1-12) |
| `day` | INTEGER | Day of month |
| `quarter` | INTEGER | Quarter (1-4) |
| `day_of_week` | INTEGER | Day of week (0-6) |
| `day_name` | VARCHAR(20) | Day name (Monday, etc.) |
| `month_name` | VARCHAR(20) | Month name (January, etc.) |
| `is_weekend` | BOOLEAN | Weekend flag |

### Fact Table:

#### `fact_sales`
| Column | Type | Description |
|--------|------|-------------|
| `order_id` | VARCHAR(50) | Order identifier |
| `date_sk` | INTEGER (FK) | Reference to `dim_date` |
| `customer_sk` | INTEGER (FK) | Reference to `dim_customer` |
| `product_sk` | INTEGER (FK) | Reference to `dim_product` |
| `quantity` | INTEGER | Number of units sold |
| `unit_price` | DECIMAL(10,2) | Price at time of order |
| `unit_cost` | DECIMAL(10,2) | Cost at time of order |
| `total_sales` | DECIMAL(12,2) | Quantity × unit_price |
| `total_cost` | DECIMAL(12,2) | Quantity × unit_cost |
| `profit` | DECIMAL(12,2) | total_sales - total_cost |
| `profit_margin` | DECIMAL(5,2) | (profit / total_sales) × 100 |
| `source_file` | VARCHAR(100) | Original source file |
| PRIMARY KEY | (`order_id`, `product_sk`) | Composite key |

## 📦 Airflow Workflow Design

The Airflow DAG (`sales_data_warehouse`) includes the following tasks:

1. **run_transformation** 📁: Executes the production Python script (`data_transformation.py`) located in `/opt/airflow/scripts/`
2. **verify_results** ✅: Performs quick verification by checking row counts in the fact table

```python
# DAG Structure
run_task >> verify_task
```

**Key Features:**
- **Schedule**: Daily at 2:00 AM (`0 2 * * *`)
- **Retries**: 1 retry with 5-minute delay
- **Logging**: Comprehensive output logging with truncation for large outputs
- **Timeout**: 30-minute execution timeout
- **Email**: Configured for failure notifications (SMTP with Gmail)

**Production Code Location:**
- Transformation logic is stored in `./airflow/scripts/data_transformation.py`
- This Python script was converted from the original Jupyter notebook using `nbconvert`

## 🐛 Issues Encountered and Solutions

Throughout this project, we encountered several interesting data engineering challenges. Here's how we identified and solved them:

### Issue 1: Duplicate Rows in Fact Table

**Problem:** After merging raw data with dimension tables, we discovered duplicate rows in the fact table with the same `(order_id, product_sk)` combination.

**Discovery:** Using data quality checks, we found:
```
Orders with multiple customers: 2
Found 17 duplicate rows based on (order_id, product_sk)
```

**Root Cause Analysis:**
- Running dimension analysis revealed that the same `customer_id` appeared multiple times with different cities:
  ```
  Customer ID C001 appears 2 times:
    - (Cairo, Egypt) - customer_sk=1
    - (Giza, Egypt) - customer_sk=33
  ```
- Similarly, `product_id` P100 appeared 3 times with different prices:
  ```
  Product ID P100 appears 3 times:
    - price $1200 (product_sk=1)
    - price $1180 (product_sk=21)
    - price $1300 (product_sk=30)
  ```

**Solution:** Implemented Type 2 Slowly Changing Dimensions:
- Added `valid_from`, `valid_to`, and `is_current` columns to track changes
- Modified fact table joins to use the correct dimension version based on order date
- Preserved historical accuracy while eliminating cartesian products

### Issue 2: PostgreSQL Transaction Error with CREATE DATABASE

**Problem:** When trying to create a new database, we encountered:
```
InternalError: CREATE DATABASE cannot run inside a transaction block
```

**Root Cause:** SQLAlchemy runs all statements within implicit transactions by default, but PostgreSQL doesn't allow `CREATE DATABASE` inside transactions.

**Solution:** Used psycopg2 directly with autocommit enabled:
```python
conn = psycopg2.connect(...)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE {NEW_DB_NAME}")
```

### Issue 3: Airflow Admin User Creation Failed

**Problem:** Airflow webserver started but login with admin/admin failed because no user existed.

**Root Cause:** The user creation command had syntax errors due to backslashes in the bash command.

**Solution:** 
- Removed backslashes and used single-line command
- Added user verification step
- Implemented error handling with clear messages

### Issue 4: Data Type Mismatches

**Problem:** When loading data to PostgreSQL, we encountered type conversion errors.

**Solution:** Implemented explicit type mapping using SQLAlchemy types:
```python
type_mapping = {
    'unit_price': types.Numeric(10,2),
    'quantity': types.Integer(),
    'order_date': types.Date(),
    # ... etc.
}
```

### Issue 5: Slow Jupyter Container Startup

**Problem:** The `python-dev` service showed as unhealthy for several minutes due to large package downloads.

**Root Cause:** pyspark package is 316MB and takes time to download and install.

**Solution:** 
- Added `pip-cache` volume to cache downloads
- Increased health check `start_period` to 300 seconds
- Packages are now cached and don't need redownloading on subsequent starts

### Issue 6: Missing Date Dimension in Schema

**Problem:** The fact table referenced `dim_date` but the table wasn't created in the initial schema.

**Solution:** Reordered table creation to respect dependencies:
1. Create dimension tables first (`dim_date`, `dim_customer`, `dim_product`)
2. Create fact table with foreign key references

### Issue 7: Profit Margin Calculation Error

**Problem:** The profit margin calculation failed with `AttributeError: 'float' object has no attribute 'round'`.

**Root Cause:** Using `.round(2)` on a float value inside a lambda function.

**Solution:** Switched to vectorized operations:
```python
fact_sales['profit_margin'] = (
    (fact_sales['profit'] / fact_sales['total_sales'] * 100)
    .round(2)
    .where(fact_sales['total_sales'] != 0, 0)
)
```

### Issue 8: Inconsistent Source Data

**Problem:** The same business entities appeared with different attributes across source files.

**Discovery:** Through dimension analysis:
- Customer C001: Cairo in Jan, Giza in Feb
- Product P100: $1200 in Jan, $1180 in Feb, $1300 in Mar

**Solution:** Implemented Type 2 SCD to track all versions:
```python
# Track changes for each product
for product_id, group in raw_data_sorted.groupby('product_id'):
    # Detect attribute changes
    if attrs != current_attrs:
        # End previous version
        # Start new version
```

### Issue 9: Airflow DAG Couldn't Find Jupyter Command

**Problem:** When trying to run the Jupyter notebook directly from Airflow, we encountered:
```
PermissionError: [Errno 13] Permission denied: 'jupyter'
```

**Root Cause:** The Airflow container didn't have Jupyter installed, and installing it would duplicate the large Jupyter image already present in the `python-dev` container.

**Solution:** Converted the Jupyter notebook to a Python script using `nbconvert` and placed it in the `./airflow/scripts/` folder. The Airflow DAG now executes the Python script directly, avoiding the need for Jupyter in the Airflow container.

### Issue 10: Path Configuration Between Environments

**Problem:** The transformation script worked in Jupyter but failed in Airflow because it was looking for CSV files in `/home/jovyan/data/raw/` (Jupyter path) instead of `/data/raw/` (Airflow path).

**Solution:** Standardized paths to use the shared `/data` volume:
```python
# For reading CSV files (works in both containers)
data_path = '/data/raw/'

# For exporting results (works in both containers)
output_path = '/data/transformed/'
```

### Issue 11: Permission Denied for Export Directory

**Problem:** The script failed with `PermissionError: [Errno 13] Permission denied: '/home/jovyan'` when trying to create the export directory in Airflow.

**Root Cause:** The script was still trying to write to Jupyter's home directory (`/home/jovyan/data/transformed/`) when running in Airflow.

**Solution:** Updated the export path to use the shared `/data/transformed/` directory, which is properly mounted in both containers and writable by the Airflow user.

## 🧠 Expected Outcomes

At the end of this project, the following outcomes were achieved:
- A fully functional, automated ETL pipeline handling data inconsistencies
- A PostgreSQL data warehouse with Type 2 SCD for historical accuracy
- Clean, deduplicated fact table with referential integrity
- Automated, scheduled execution through Airflow at 2 AM daily
- Complete data lineage with source file attribution
- Production-ready data engineering workflow with separate development and production code paths
- CSV backups of all dimension and fact tables in `./data/transformed/`

## 📝 Project Deliverables

The final deliverables of this project include:
- Python ETL script (`data_transformation.py`) with SCD Type 2 implementation in `./airflow/scripts/`
- SQL scripts for PostgreSQL schema creation (embedded in the Python script)
- Jupyter notebook (`data_transformation.ipynb`) for development in `./dev_code/`
- Apache Airflow DAG definition (`sales_data_warehouse.py`) in `./airflow/dags/`
- Sample CSV source data (3 files) in `./data/raw/`
- Docker Compose configuration for easy setup with all services
- Comprehensive documentation including this README
- Data quality validation scripts within the ETL pipeline
- Exported CSV backups of all tables in `./data/transformed/`

## 💡 Benefits of the Project

This project provides practical value in several areas:
- **Real-world ETL pipeline**: Handling messy, inconsistent source data from multiple files
- **SCD Type 2 implementation**: Preserving historical changes accurately for customers and products
- **Data quality focus**: Comprehensive validation at each step with referential integrity checks
- **Containerized environment**: Easy setup for team collaboration with Docker Compose
- **Workflow automation**: Production-ready Airflow orchestration with email notifications
- **Separation of concerns**: Clear distinction between development (Jupyter) and production (Python scripts)
- **Educational value**: Demonstrates common data engineering challenges and their solutions

## 📬 Future Enhancements

Possible future improvements include:
- Implementing **incremental loading** instead of full refreshes
- Adding **data quality dashboards** in Airflow
- Building interactive dashboards with **Power BI or Tableau** connected to the data warehouse
- Adding **unit tests** for transformation logic
- Implementing **data lineage tracking** with OpenLineage
- Adding **error notification** via Slack in addition to email
- Migrating to **cloud environment** (AWS/GCP/Azure)
- Implementing **data profiling** for source files
- Adding **parallel processing** for larger datasets
- Setting up **CI/CD pipeline** for automated testing and deployment
- Adding **restart policies** to Docker services for production resilience

## 👨‍💻 Author

- **Ibrahim Hegazi** - Data Engineer

---

## 📊 Project Statistics

| Metric | Value |
|--------|-------|
| Source CSV Files | 3 |
| Total Raw Rows | 50 |
| Fact Table Rows | 86 |
| Customer Versions | 38 (2 customers with history) |
| Product Versions | 38 (11 products with history) |
| Date Range | 25 unique dates |
| Data Quality Checks Passed | 100% |
| Duplicate Rows After SCD | 0 |
| Airflow DAG Tasks | 2 |
| Docker Services | 6 |

---
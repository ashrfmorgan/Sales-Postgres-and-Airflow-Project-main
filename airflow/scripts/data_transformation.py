# ==============================================================================
# End-to-End Data Engineering Pipeline:
# Raw CSV → Data Cleaning → SCD Dimensions → Fact Table → Data Warehouse
# ==============================================================================

# ==============================================================================
# 1. SETUP AND IMPORTS
# ==============================================================================
import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text, types

# ==============================================================================
# 2. DATABASE CONNECTION TO SOURCE DB
# ==============================================================================
# Database connection parameters (from your .env)
DB_USER = 'source_user'
DB_PASSWORD = 'source_pass'
DB_HOST = 'postgres-source'  # Service name from docker-compose
DB_PORT = '5432'
DB_NAME = 'tpch' # Old DB

# Create connection engine
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Test connection
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print("✅ Connected to source database successfully!")


# ==============================================================================
# STAGING LAYER
# ==============================================================================

# =====================================
# 3. LOAD AND COMBINE CSV FILES
# =====================================
# Path to your data folder (with /raw subfolder)
# data_path = '/home/jovyan/data/raw/'
# data_path = '/opt/airflow/data/raw/'
data_path = '/data/raw/' 

# Find all CSV files
csv_files = glob.glob(f'{data_path}*.csv')
print(f"\nFound {len(csv_files)} CSV files:")
for file in csv_files:
    print(f"  - {os.path.basename(file)}")

# Load and combine all CSV files, adding source_file column for data traceability
df_list = []
for file in csv_files:
    df = pd.read_csv(file)
    df['source_file'] = os.path.basename(file)
    df_list.append(df)
    print(f"Loaded {os.path.basename(file)}: {len(df)} rows")

raw_data = pd.concat(df_list, ignore_index=True)
print(f"\n✅ Combined data: {len(raw_data)} total rows")
print(f"Columns: {list(raw_data.columns)}")
print(raw_data.head())

# =====================================
# 4. DATA CLEANING AND VALIDATION
# =====================================
print("\nData Info:")
raw_data.info()

print("\nMissing Values:")
print(raw_data.isnull().sum())

# Ensure order_date is in datetime format for time-based joins
raw_data['order_date'] = pd.to_datetime(raw_data['order_date'])

# Remove any duplicates if needed
raw_data = raw_data.drop_duplicates()
print(f"\nAfter removing duplicates: {len(raw_data)} rows")


# ==============================================================================
# DIMENSION LAYER
# ==============================================================================

# =====================================
# 5. CREATE DIMENSION TABLES
# =====================================

# 5.1 Customer Dimension with Type 2 SCD
print("\n=== 5.1 CREATING CUSTOMER DIMENSION (TYPE 2 SCD) ===")

customer_history = []
customer_attributes = ['customer_id', 'customer_name', 'city', 'country']

# Sort by date to track changes over time
raw_data_sorted = raw_data.sort_values(['customer_id', 'order_date'])

# Track changes for each customer
for customer_id, group in raw_data_sorted.groupby('customer_id'):
    current_attrs = None
    start_date = None
    
    for idx, row in group.iterrows():
        attrs = tuple(row[['customer_name', 'city', 'country']].values)
        
        if current_attrs is None:
            current_attrs = attrs
            start_date = row['order_date']
        elif attrs != current_attrs:
            # End the previous version
            customer_history.append({
                'customer_id': customer_id,
                'customer_name': current_attrs[0],
                'city': current_attrs[1],
                'country': current_attrs[2],
                'valid_from': start_date,
                'valid_to': row['order_date'] - pd.Timedelta(days=1),  # Day before change
                'is_current': False
            })
            # Start new version
            current_attrs = attrs
            start_date = row['order_date']
    
    # Add the final version (still current)
    if current_attrs is not None:
        customer_history.append({
            'customer_id': customer_id,
            'customer_name': current_attrs[0],
            'city': current_attrs[1],
            'country': current_attrs[2],
            'valid_from': start_date,
            'valid_to': pd.NaT,
            'is_current': True
        })

dim_customer = pd.DataFrame(customer_history)
dim_customer['customer_sk'] = range(1, len(dim_customer) + 1)

# Add source tracking (show which files contributed to this version)
source_tracking = raw_data.groupby(['customer_id', 'customer_name', 'city', 'country'])['source_file'].agg(lambda x: ', '.join(x.unique())).reset_index()
dim_customer = dim_customer.merge(
    source_tracking, 
    on=['customer_id', 'customer_name', 'city', 'country'], 
    how='left'
)
dim_customer.rename(columns={'source_file': 'source_files'}, inplace=True)

# Ensure correct data types
dim_customer['customer_id'] = dim_customer['customer_id'].astype(str)
dim_customer['customer_name'] = dim_customer['customer_name'].astype(str)
dim_customer['city'] = dim_customer['city'].astype(str)
dim_customer['country'] = dim_customer['country'].astype(str)
dim_customer['valid_from'] = pd.to_datetime(dim_customer['valid_from'])
dim_customer['valid_to'] = pd.to_datetime(dim_customer['valid_to'])
dim_customer['is_current'] = dim_customer['is_current'].astype(bool)

print(f"Customer Dimension: {len(dim_customer)} rows (including historical changes)")

# 5.2 Product Dimension with Type 2 SCD and Source Attribution
print("\n=== 5.2 CREATING PRODUCT DIMENSION (TYPE 2 SCD) ===")

product_history = []
raw_data_sorted = raw_data.sort_values(['product_id', 'order_date'])

for product_id, group in raw_data_sorted.groupby('product_id'):
    current_attrs = None
    start_date = None
    
    for idx, row in group.iterrows():
        attrs = tuple(row[['product_name', 'category', 'unit_price', 'unit_cost']].values)
        
        if current_attrs is None:
            current_attrs = attrs
            start_date = row['order_date']
        elif attrs != current_attrs:
            product_history.append({
                'product_id': product_id,
                'product_name': current_attrs[0],
                'category': current_attrs[1],
                'unit_price': current_attrs[2],
                'unit_cost': current_attrs[3],
                'valid_from': start_date,
                'valid_to': row['order_date'] - pd.Timedelta(days=1),
                'is_current': False
            })
            current_attrs = attrs
            start_date = row['order_date']
    
    if current_attrs is not None:
        product_history.append({
            'product_id': product_id,
            'product_name': current_attrs[0],
            'category': current_attrs[1],
            'unit_price': current_attrs[2],
            'unit_cost': current_attrs[3],
            'valid_from': start_date,
            'valid_to': pd.NaT,
            'is_current': True
        })

dim_product = pd.DataFrame(product_history)
dim_product['product_sk'] = range(1, len(dim_product) + 1)
dim_product['profit_margin'] = dim_product['unit_price'] - dim_product['unit_cost']

source_tracking_product = raw_data.groupby(['product_id', 'product_name', 'category', 'unit_price', 'unit_cost'])['source_file'].agg(lambda x: ', '.join(x.unique())).reset_index()
dim_product = dim_product.merge(
    source_tracking_product,
    on=['product_id', 'product_name', 'category', 'unit_price', 'unit_cost'],
    how='left'
)
dim_product.rename(columns={'source_file': 'source_files'}, inplace=True)

dim_product['product_id'] = dim_product['product_id'].astype(str)
dim_product['product_name'] = dim_product['product_name'].astype(str)
dim_product['category'] = dim_product['category'].astype(str)
dim_product['unit_price'] = dim_product['unit_price'].astype(float)
dim_product['unit_cost'] = dim_product['unit_cost'].astype(float)
dim_product['profit_margin'] = dim_product['profit_margin'].astype(float)
dim_product['valid_from'] = pd.to_datetime(dim_product['valid_from'])
dim_product['valid_to'] = pd.to_datetime(dim_product['valid_to'])
dim_product['is_current'] = dim_product['is_current'].astype(bool)

print(f"Product Dimension: {len(dim_product)} rows (including historical changes)")

# 5.3 Date Dimension
print("\n=== 5.3 CREATING DATE DIMENSION ===")
dates = raw_data['order_date'].drop_duplicates().reset_index(drop=True)

dim_date = pd.DataFrame({
    'date_sk': range(1, len(dates) + 1),
    'date': dates,
    'year': dates.dt.year.astype(int),
    'month': dates.dt.month.astype(int),
    'day': dates.dt.day.astype(int),
    'quarter': dates.dt.quarter.astype(int),
    'day_of_week': dates.dt.dayofweek.astype(int),
    'day_name': dates.dt.day_name().astype(str),
    'month_name': dates.dt.month_name().astype(str),
    'is_weekend': (dates.dt.dayofweek >= 5).astype(bool)
})
print(f"Date Dimension: {len(dim_date)} unique dates")

# 5.4 Validate Dimensions for Duplicates/Inconsistencies
print("\n=== 5.4 DIMENSION ANALYSIS & CONSISTENCY CHECK ===")
customer_dupes = dim_customer.groupby('customer_id').size().reset_index(name='occurrences')
problem_customers = customer_dupes[customer_dupes['occurrences'] > 1]
if len(problem_customers) > 0:
    print(f"⚠️ Found {len(problem_customers)} customer_ids with multiple rows (Expected for SCD).")
else:
    print("✅ All customer_ids are unique")

product_dupes = dim_product.groupby('product_id').size().reset_index(name='occurrences')
problem_products = product_dupes[product_dupes['occurrences'] > 1]
if len(problem_products) > 0:
    print(f"⚠️ Found {len(problem_products)} product_ids with multiple rows (Expected for SCD).")
else:
    print("✅ All product_ids are unique")


# ==============================================================================
# FACT LAYER
# ==============================================================================

# =====================================
# 6. CREATE FACT TABLE
# =====================================

# 6.1 Prepare Raw Data
print("\n" + "=" * 60)
print("STEP 6.1: PREPARING RAW DATA")
print("=" * 60)
print(f"✅ Raw data prepared: {len(raw_data)} rows")

# 6.2 Join with Customer Dimension (SCD)
print("\n" + "=" * 60)
print("STEP 6.2: JOINING WITH CUSTOMER DIMENSION (TYPE 2 SCD)")
print("=" * 60)

fact_sales = raw_data.merge(
    dim_customer[['customer_id', 'customer_sk', 'valid_from', 'valid_to']],
    on='customer_id',
    how='left'
)
before_filter = len(fact_sales)
fact_sales = fact_sales[
    (fact_sales['order_date'] >= fact_sales['valid_from']) & 
    ((fact_sales['valid_to'].isna()) | (fact_sales['order_date'] <= fact_sales['valid_to']))
]
after_filter = len(fact_sales)
print(f"✅ After filtering to correct version: {after_filter} rows")

# 6.3 Join with Product Dimension (SCD)
print("\n" + "=" * 60)
print("STEP 6.3: JOINING WITH PRODUCT DIMENSION (TYPE 2 SCD)")
print("=" * 60)

fact_sales = fact_sales.merge(
    dim_product[['product_id', 'product_sk', 'valid_from', 'valid_to']],
    on='product_id',
    how='left',
    suffixes=('_cust', '_prod')
)
before_filter = len(fact_sales)
fact_sales = fact_sales[
    (fact_sales['order_date'] >= fact_sales['valid_from_prod']) & 
    ((fact_sales['valid_to_prod'].isna()) | (fact_sales['order_date'] <= fact_sales['valid_to_prod']))
]
after_filter = len(fact_sales)
print(f"✅ After filtering to correct version: {after_filter} rows")

# 6.4 Join with Date Dimension
print("\n" + "=" * 60)
print("STEP 6.4: JOINING WITH DATE DIMENSION")
print("=" * 60)

fact_sales = fact_sales.merge(
    dim_date[['date', 'date_sk']],
    left_on='order_date',
    right_on='date',
    how='left'
)
print(f"✅ Date join complete: {len(fact_sales)} rows")

# 6.5 Select only required fact table columns and validate completeness
print("\n" + "=" * 60)
print("STEP 6.5: SELECT AND VALIDATE CORE COLUMNS")
print("=" * 60)

core_columns = ['order_id', 'date_sk', 'customer_sk', 'product_sk',
                'quantity', 'unit_price', 'unit_cost', 'source_file']

missing_columns = [col for col in core_columns if col not in fact_sales.columns]
if missing_columns:
    print(f"❌ ERROR: Missing columns: {missing_columns}")
else:
    fact_sales = fact_sales[core_columns].copy()
    print(f"✅ Selected {len(core_columns)} core columns")

# 6.6 Convert Data Types
print("\n" + "=" * 60)
print("STEP 6.6: CONVERTING DATA TYPES")
print("=" * 60)

fact_sales['order_id'] = fact_sales['order_id'].astype(str)
fact_sales['date_sk'] = fact_sales['date_sk'].astype(int)
fact_sales['customer_sk'] = fact_sales['customer_sk'].astype(int)
fact_sales['product_sk'] = fact_sales['product_sk'].astype(int)
fact_sales['quantity'] = fact_sales['quantity'].astype(int)
fact_sales['unit_price'] = fact_sales['unit_price'].astype(float)
fact_sales['unit_cost'] = fact_sales['unit_cost'].astype(float)
fact_sales['source_file'] = fact_sales['source_file'].astype(str)
print("✅ Data types converted.")

# 6.7 Calculate Measures
print("\n" + "=" * 60)
print("STEP 6.7: CALCULATING MEASURES")
print("=" * 60)

fact_sales['total_sales'] = (fact_sales['quantity'] * fact_sales['unit_price']).round(2)
fact_sales['total_cost'] = (fact_sales['quantity'] * fact_sales['unit_cost']).round(2)
fact_sales['profit'] = (fact_sales['total_sales'] - fact_sales['total_cost']).round(2)

fact_sales['profit_margin'] = fact_sales.apply(
    lambda row: round((row['profit'] / row['total_sales'] * 100), 2) 
    if row['total_sales'] != 0 else 0, 
    axis=1
)
print(f"✅ Calculated measures (total_sales, profit, profit_margin).")

# 6.8 Final Fact Table Summary
print("\n" + "=" * 60)
print("STEP 6.8: FINAL FACT TABLE SUMMARY")
print("=" * 60)
print(f"📊 Total rows: {len(fact_sales):,}")
print(f"📊 Total sales: ${fact_sales['total_sales'].sum():,.2f}")

# 6.9 Quality Checks
# Step 6.9: Quality Checks

print("\n" + "=" * 60)
print("STEP 9: QUALITY CHECKS")
print("=" * 60)

# Check 1: Referential integrity - do all foreign keys exist?
print("🔍 Check 1: Referential Integrity")

# Customer check
missing_customers = fact_sales[~fact_sales['customer_sk'].isin(dim_customer['customer_sk'])]
print(f"   - Missing customer references: {len(missing_customers)} rows")

# Product check
missing_products = fact_sales[~fact_sales['product_sk'].isin(dim_product['product_sk'])]
print(f"   - Missing product references: {len(missing_products)} rows")

# Date check
missing_dates = fact_sales[~fact_sales['date_sk'].isin(dim_date['date_sk'])]
print(f"   - Missing date references: {len(missing_dates)} rows")

# Check 2: Data quality
print("\n🔍 Check 2: Data Quality")
print(f"   - Negative quantities: {(fact_sales['quantity'] < 0).sum()} rows")
print(f"   - Negative prices: {(fact_sales['unit_price'] < 0).sum()} rows")
print(f"   - Negative profit margins: {(fact_sales['profit_margin'] < 0).sum()} rows")
print(f"   - Zero total sales: {(fact_sales['total_sales'] == 0).sum()} rows")

# Check 3: Source file distribution
print("\n🔍 Check 3: Source File Distribution")
source_dist = fact_sales['source_file'].value_counts()
for source, count in source_dist.items():
    pct = count / len(fact_sales) * 100
    print(f"   - {source}: {count:,} rows ({pct:.1f}%)")


# ==============================================================================
# DATA WAREHOUSE LAYER
# ==============================================================================

# =====================================
# 7. DEFINE DATA WAREHOUSE SCHEMA (DDL)
# =====================================

# 7.1 Schema Definition SQL
print("\n=== STEP 7.1: SQL FOR TABLE CREATION (WITH SCD) ===")
scd_sql = """
CREATE TABLE dim_date (
    date_sk INTEGER PRIMARY KEY,
    date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    is_weekend BOOLEAN
);

CREATE TABLE dim_customer (
    customer_sk INTEGER PRIMARY KEY,
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    source_files TEXT
);

CREATE TABLE dim_product (
    product_sk INTEGER PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    unit_price DECIMAL(10,2),
    unit_cost DECIMAL(10,2),
    profit_margin DECIMAL(10,2),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    source_files TEXT
);

CREATE TABLE fact_sales (
    order_id VARCHAR(50),
    date_sk INTEGER REFERENCES dim_date(date_sk),
    customer_sk INTEGER REFERENCES dim_customer(customer_sk),
    product_sk INTEGER REFERENCES dim_product(product_sk),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    unit_cost DECIMAL(10,2),
    total_sales DECIMAL(12,2),
    total_cost DECIMAL(12,2),
    profit DECIMAL(12,2),
    profit_margin DECIMAL(5,2),
    source_file VARCHAR(100),
    PRIMARY KEY (order_id, product_sk)
);
"""

# 7.2 Create Database
print("\n" + "=" * 60)
print("STEP 7.2: CREATING DATABASE")
print("=" * 60)

NEW_DB_NAME = 'sales_dw'

conn = psycopg2.connect(
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database='postgres'
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{NEW_DB_NAME}'")
exists = cursor.fetchone()

if exists:
    cursor.execute(f"""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = '{NEW_DB_NAME}'
    """)
    cursor.execute(f"DROP DATABASE {NEW_DB_NAME}")
    print(f"✅ Dropped existing database '{NEW_DB_NAME}'")

cursor.execute(f"CREATE DATABASE {NEW_DB_NAME}")
print(f"✅ Created fresh database: {NEW_DB_NAME}")

cursor.close()
conn.close()

# Reconnect to new DB
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{NEW_DB_NAME}')

# 7.3 Create Tables
print("\n" + "=" * 60)
print("STEP 7.3: CREATING TABLES IN DATABASE")
print("=" * 60)

with engine.begin() as conn:
    # Creating tables one by one for clarity
    conn.execute(text("""
        CREATE TABLE dim_date (
            date_sk INTEGER PRIMARY KEY, date DATE, year INTEGER, month INTEGER,
            day INTEGER, quarter INTEGER, day_of_week INTEGER, day_name VARCHAR(20),
            month_name VARCHAR(20), is_weekend BOOLEAN
        )
    """))
    print("✅ dim_date created")
    
    conn.execute(text("""
        CREATE TABLE dim_customer (
            customer_sk INTEGER PRIMARY KEY, customer_id VARCHAR(50), customer_name VARCHAR(100),
            city VARCHAR(100), country VARCHAR(100), valid_from DATE, valid_to DATE,
            is_current BOOLEAN, source_files TEXT
        )
    """))
    print("✅ dim_customer created")
    
    conn.execute(text("""
        CREATE TABLE dim_product (
            product_sk INTEGER PRIMARY KEY, product_id VARCHAR(50), product_name VARCHAR(200),
            category VARCHAR(100), unit_price DECIMAL(10,2), unit_cost DECIMAL(10,2),
            profit_margin DECIMAL(10,2), valid_from DATE, valid_to DATE,
            is_current BOOLEAN, source_files TEXT
        )
    """))
    print("✅ dim_product created")
    
    conn.execute(text("""
        CREATE TABLE fact_sales (
            order_id VARCHAR(50), date_sk INTEGER REFERENCES dim_date(date_sk),
            customer_sk INTEGER REFERENCES dim_customer(customer_sk),
            product_sk INTEGER REFERENCES dim_product(product_sk),
            quantity INTEGER, unit_price DECIMAL(10,2), unit_cost DECIMAL(10,2),
            total_sales DECIMAL(12,2), total_cost DECIMAL(12,2), profit DECIMAL(12,2),
            profit_margin DECIMAL(5,2), source_file VARCHAR(100),
            PRIMARY KEY (order_id, product_sk)
        )
    """))
    print("✅ fact_sales created")


# =====================================
# 8. LOAD TO DATABASE WITH PROPER SCHEMA
# =====================================
print("\n" + "=" * 60)
print("STEP 8: LOADING DATA TO DATABASE")
print("=" * 60)

type_mapping = {
    'customer_sk': types.Integer(), 'customer_id': types.String(50), 'customer_name': types.String(100),
    'city': types.String(100), 'country': types.String(100), 'valid_from': types.Date(),
    'valid_to': types.Date(), 'is_current': types.Boolean(), 'source_files': types.Text(),
    
    'product_sk': types.Integer(), 'product_id': types.String(50), 'product_name': types.String(200),
    'category': types.String(100), 'unit_price': types.Numeric(10,2), 'unit_cost': types.Numeric(10,2),
    'profit_margin': types.Numeric(10,2), 'valid_from': types.Date(), 'valid_to': types.Date(),
    'is_current': types.Boolean(), 'source_files': types.Text(),
    
    'date_sk': types.Integer(), 'date': types.Date(), 'year': types.Integer(), 'month': types.Integer(),
    'day': types.Integer(), 'quarter': types.Integer(), 'day_of_week': types.Integer(),
    'day_name': types.String(20), 'month_name': types.String(20), 'is_weekend': types.Boolean(),
    
    'order_id': types.String(50), 'quantity': types.Integer(), 'unit_price': types.Numeric(10,2),
    'unit_cost': types.Numeric(10,2), 'total_sales': types.Numeric(12,2), 'total_cost': types.Numeric(12,2),
    'profit': types.Numeric(12,2), 'profit_margin': types.Numeric(5,2), 'source_file': types.String(100)
}

dim_customer.to_sql('dim_customer', engine, if_exists='append', index=False, dtype=type_mapping)
print(f"✅ dim_customer loaded: {len(dim_customer)} rows")

dim_product.to_sql('dim_product', engine, if_exists='append', index=False, dtype=type_mapping)
print(f"✅ dim_product loaded: {len(dim_product)} rows")

dim_date.to_sql('dim_date', engine, if_exists='append', index=False, dtype=type_mapping)
print(f"✅ dim_date loaded: {len(dim_date)} rows")

fact_sales.to_sql('fact_sales', engine, if_exists='append', index=False, dtype=type_mapping)
print(f"✅ fact_sales loaded: {len(fact_sales)} rows")


# =====================================
# 9. VERIFY THE DATA WAREHOUSE
# =====================================
print("\n" + "=" * 60)
print("STEP 9: DATA WAREHOUSE VERIFICATION")
print("=" * 60)

with engine.connect() as conn:
    tables = ['dim_customer', 'dim_product', 'dim_date', 'fact_sales']
    print("\n📊 Row counts in DB:")
    for table in tables:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
        print(f"  {table}: {result.scalar():,} rows")

# =====================================
# 10. EXPORT ALL TABLES
# =====================================
print("\n" + "=" * 60)
print("STEP 10: EXPORTING ALL TABLES")
print("=" * 60)

output_path = './data/transformed/' 
os.makedirs(output_path, exist_ok=True)

tables_to_export = {
    'dim_customer': dim_customer,
    'dim_product': dim_product,
    'dim_date': dim_date,
    'fact_sales': fact_sales
}

for name, df in tables_to_export.items():
    filename = f'{output_path}{name}.csv'
    df.to_csv(filename, index=False)
    print(f"✅ Exported {name}.csv")

print(f"\n🎉 Pipeline complete. Exported to {output_path}")
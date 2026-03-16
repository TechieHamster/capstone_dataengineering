# Retail Data Warehouse Capstone Project

**Snowflake + dbt + Azure Data Lake (Medallion Architecture)**

## Project Overview

This project implements a **modern cloud data warehouse pipeline** using **Azure Data Lake Storage, Snowflake, and dbt** following the **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline ingests raw JSON data from Azure Data Lake into Snowflake, transforms and cleans the data using dbt, and builds a **Star Schema data warehouse** optimized for analytics.

The project also implements **Slowly Changing Dimensions (SCD Type 2)** using dbt snapshots and **incremental pipelines** for efficient processing.

---

# Architecture

Azure Data Lake Storage (ADLS)
        ↓  
Snowflake External Tables (Bronze Layer)
        ↓
dbt Staging Models (Silver Layer)
        ↓
Star Schema Warehouse (Gold Layer)
        ↓
Reporting View
        ↓
Analytics & Reporting

---

# Tech Stack

* **Azure Data Lake Storage (ADLS)** – Raw data storage
* **Snowflake** – Cloud Data Warehouse
* **dbt (Data Build Tool)** – Data transformation framework
* **SQL** – Transformation logic
* **GitHub** – Version control

---

# Data Source

The data consists of **JSON files stored in Azure Data Lake** organized by dataset and ingestion date.

Example structure:

Capstone_Project_Data/

campaign_data/
customers_data/
employee_data/
orders_data/
product_data/
store_data/
supplier_data/

Each dataset contains **daily JSON files** such as:

campaigns_2024-04-01.json
orders_2024-04-02.json

These files contain nested JSON arrays which are flattened and processed in the pipeline.

---

# Data Warehouse Architecture

The project follows the **Medallion Architecture**.

## Bronze Layer (Raw Data)

Raw JSON data is ingested into Snowflake using **External Tables**.

Tables:

customers_raw
campaign_data_raw
employee_data_raw
orders_raw_data
product_raw_data
store_raw_data
supplier_raw_data

This layer stores **raw, unprocessed data**.

---

## Silver Layer (Staging & Cleaning)

Data is cleaned and standardized using dbt staging models.

Transformations include:

* JSON flattening
* Data type validation
* Date standardization
* Text normalization
* Phone and email validation
* Currency conversion
* Deduplication

Staging Models:

stg_customers
stg_products
stg_orders
stg_stores
stg_employees
stg_suppliers
stg_campaigns

---

## Gold Layer (Analytics Warehouse)

A **Star Schema** is implemented for analytics.

### Dimension Tables

dim_customer
dim_product
dim_store
dim_employee
dim_supplier
dim_campaign
dim_date

### Fact Tables

fact_sales
fact_inventory
fact_marketing_performance

These tables enable analytics such as:

* Sales performance analysis
* Inventory monitoring
* Customer insights
* Marketing campaign ROI

---

# Fact Table Grain

### fact_sales

Grain: **1 row = 1 product sold per order**

Metrics:

quantity
gross_sales
total_cost
profit_amount

---

# Slowly Changing Dimensions (SCD Type 2)

dbt snapshots are used to track historical changes.

Snapshots:

customer_snapshot
product_snapshot
employee_snapshot

Snapshots maintain historical records using:

dbt_valid_from
dbt_valid_to
dbt_scd_id

This enables tracking changes such as:

* Customer loyalty tier changes
* Product price updates
* Employee role changes

---

# Incremental Pipelines

The **fact_sales table is incremental** to efficiently process new data.

Only new records are loaded using:

order_date > max(order_date)

This improves performance for large datasets.

---

# Data Quality Testing

dbt tests ensure data reliability.

Tests implemented:

not_null
unique
relationship tests

Example:

fact_sales.customer_id → dim_customer
fact_sales.product_id → dim_product
fact_sales.store_id → dim_store

---

# Key Transformations Implemented

### Customer Transformations

* Full name creation
* Customer age calculation
* Customer segmentation
* Address standardization

### Product Transformations

* Product hierarchy creation
* Profit margin calculation
* Low stock detection

### Order Transformations

* Order profitability
* Time-of-day classification
* Delivery status tracking

### Store Transformations

* Store size categorization
* Store performance metrics

### Marketing Transformations

* Campaign ROI calculation
* Audience segmentation

---

# How to Run the Project

### 1 Install dbt

pip install dbt-snowflake

### 2 Run transformations

dbt run

### 3 Run tests

dbt test

### 4 Run snapshots

dbt snapshot

---

# Example Analytics Queries

### Top Products by Revenue

SELECT
product_id,
SUM(gross_sales) AS revenue
FROM fact_sales
GROUP BY product_id
ORDER BY revenue DESC

---

### Campaign Performance

SELECT
campaign_name,
campaign_roi
FROM fact_marketing_performance
ORDER BY campaign_roi DESC

---

# Key Learnings

This project demonstrates:

* Building a modern data warehouse architecture
* Implementing the Medallion Architecture
* Transforming nested JSON data
* Building Star Schema models
* Implementing Slowly Changing Dimensions
* Creating incremental pipelines
* Data quality testing with dbt

---

# Future Improvements

Potential enhancements include:

* Stream processing for real-time ingestion
* Automated orchestration using Airflow
* Dashboarding using Power BI or Tableau
* Data observability tools
* CI/CD for dbt pipelines

---

# Author

Divyansh Upadhyay

Data Engineering Capstone Project

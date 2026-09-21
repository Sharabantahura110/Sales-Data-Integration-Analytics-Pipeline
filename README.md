# Sales Data Integration & Analytics Pipeline

An end-to-end **data engineering pipeline** built with **Databricks, PySpark, SQL, Delta Lake, and AWS S3** to integrate sales data from two companies into a unified analytics platform.

The project demonstrates how raw and inconsistent source data can be ingested, cleaned, standardized, transformed, incrementally processed, and consolidated using the **Medallion Architecture (Bronze, Silver, Gold)**.

---

## Project Overview

The project simulates a business scenario in which a parent company, **Atlon**, acquires another company, **Sports Bar**.

Atlon already has an established analytics infrastructure, while Sports Bar stores its operational data in inconsistent formats and does not have a proper data engineering pipeline.

The goal is to build a reliable pipeline for Sports Bar and integrate its sales data with Atlon's existing analytical data so that management can analyze both companies through a single reporting layer.

The final pipeline follows:

```text
Sports Bar Source Data
        |
        v
      AWS S3
        |
        v
     Bronze Layer
        |
        v
     Silver Layer
        |
        v
      Gold Layer
        |
        v
Merge with Atlon Gold Data
        |
        v
Consolidated Analytics Layer
        |
        +----> Databricks Dashboard
        |
        +----> Databricks Genie
```

---

## Business Problem

Following the acquisition, the two companies had different:

* Data formats
* Column names
* Product and customer identifiers
* Reporting structures
* Data quality standards
* Transaction granularities

The objective was to create a scalable data platform capable of:

* Integrating data from both companies
* Cleaning and standardizing inconsistent source data
* Supporting historical data backfills
* Processing new data incrementally
* Producing consolidated fact and dimension tables
* Providing BI-ready datasets for analytics and reporting

---

## Tech Stack

| Technology               | Purpose                                                              |
| ------------------------ | -------------------------------------------------------------------- |
| **Databricks**           | Data engineering and analytics platform                              |
| **PySpark**              | Distributed data processing and transformation                       |
| **SQL**                  | Data querying, transformation, and analytics                         |
| **AWS S3**               | Cloud data lake / landing storage                                    |
| **Delta Lake**           | Reliable table storage, MERGE operations, and incremental processing |
| **Databricks Workflows** | Pipeline orchestration                                               |
| **Databricks Dashboard** | Business intelligence and visualization                              |
| **Databricks Genie**     | Natural-language analytics                                           |
| **Python**               | Pipeline logic and reusable configuration                            |

---

## Data Architecture

The project uses the **Medallion Architecture**.

### Bronze Layer

The Bronze layer stores raw source data ingested from AWS S3.

Typical operations include:

* Reading CSV files from S3
* Preserving raw source values
* Inferring source schemas
* Adding ingestion metadata

Additional metadata such as the following is stored for lineage and debugging:

```text
read_timestamp
source_file_name
file_size
```

---

### Silver Layer

The Silver layer contains cleaned and standardized data.

Data quality operations include:

* Duplicate removal
* Trimming leading and trailing spaces
* Standardizing text capitalization
* Correcting inconsistent city names
* Handling missing values
* Handling invalid or unknown values
* Correcting negative price values
* Standardizing date formats
* Converting data types
* Harmonizing schemas between the two companies

---

### Gold Layer

The Gold layer contains business-ready datasets used for reporting and analytics.

The main tables include:

```text
dim_customers
dim_products
dim_gross_price
dim_date
fact_orders
```

Sports Bar's transformed Gold tables are then integrated with Atlon's existing Gold tables.

---

## Data Model

The analytical model follows a **star schema**.

```text
                 dim_customers
                      |
                      |
dim_products ---- fact_orders ---- dim_date
                      |
                      |
              dim_gross_price
```

### Dimension Tables

* Customer
* Product
* Gross Price
* Date

### Fact Table

* Orders / Sales Transactions

The Date dimension is generated programmatically using PySpark and contains attributes such as:

```text
date_key
year
month
short_month
quarter
year_quarter
```

---

## Pipeline Workflow

### 1. Source Data Analysis

The source datasets are first analyzed to understand:

* Table relationships
* Data types
* Data quality issues
* Schema differences
* Business keys
* Fact and dimension structures

---

### 2. AWS S3 Data Lake

Sports Bar source files are uploaded to an AWS S3 bucket.

The bucket acts as the landing zone for the data pipeline.

Example structure:

```text
S3 Bucket
|
+-- customers/
|
+-- products/
|
+-- gross_price/
|
+-- orders/
     |
     +-- landing/
     |
     +-- archive/
```

---

### 3. Databricks-S3 Integration

Databricks is connected directly to the S3 bucket using an external AWS connection.

This allows PySpark pipelines to read source files directly from cloud storage.

---

### 4. Parameterized Pipeline Configuration

Reusable pipeline configuration is implemented using:

* Utility notebooks
* Databricks widgets
* Configurable schema names
* Configurable data source paths

Example configurable parameters:

```text
catalog_name
bronze_schema
silver_schema
gold_schema
data_source
```

This avoids hardcoding values throughout multiple notebooks.

---

## Dimension Data Processing

Three major dimension datasets are processed:

```text
Customers
Products
Gross Price
```

The general flow is:

```text
AWS S3
   |
   v
Bronze
   |
   v
Silver
   |
   v
Sports Bar Gold
   |
   v
Consolidated Gold
```

---

## Customer Data Processing

Customer data undergoes several quality checks and transformations.

Operations include:

* Duplicate detection
* Duplicate removal
* Customer-name trimming
* Capitalization standardization
* City-name standardization
* Null detection
* Missing-city enrichment
* Schema validation

The cleaned dataset is written into the Silver layer before being promoted to Gold.

---

## Product Data Processing

The two companies use different identifiers and schemas for products.

For example:

```text
Company A → product_code
Company B → product_id
```

The pipeline standardizes these differences and creates compatible product records.

A reliable product business key is then used to merge Sports Bar products into the consolidated product dimension.

---

## Pricing Data Processing

Price data contains several quality problems including:

* Negative prices
* Unknown price values
* Inconsistent date formats

The pipeline cleans these records and applies window-based logic to identify the appropriate/latest price where required.

The transformed records are then merged into the consolidated pricing dimension.

---

## Delta Lake MERGE / Upsert

Delta Lake `MERGE` operations are used to integrate child-company data into the consolidated Gold tables.

Conceptually:

```sql
WHEN MATCHED
    UPDATE

WHEN NOT MATCHED
    INSERT
```

This supports **upsert-based data integration** instead of blindly appending duplicate records.

---

## Historical Data Load

The project first performs a **historical backfill**.

Several months of historical Sports Bar data are processed through:

```text
Historical CSV Files
        |
        v
      Bronze
        |
        v
      Silver
        |
        v
       Gold
```

This initializes the analytical platform with historical data before incremental processing begins.

---

## Fact Data Processing

Sports Bar provides order transactions at the **daily level**, while the parent company's analytical model stores sales at the **monthly level**.

Therefore, the child-company data must first be aggregated.

Example:

```text
July 02 → 10 units
July 04 → 5 units
July 15 → 8 units
```

becomes:

```text
July → 23 units
```

The aggregation is performed using:

```text
month
product_code
customer_code
```

The result can then be merged into the parent's consolidated fact table.

---

## Incremental Data Processing

After the historical load, the pipeline switches to incremental processing.

New order files are periodically placed in the S3 landing folder.

Example:

```text
orders_2025_12_01.csv
orders_2025_12_02.csv
orders_2025_12_03.csv
```

Only newly arrived data is processed instead of reprocessing the full historical dataset.

The incremental flow is:

```text
New S3 File
    |
    v
  Bronze
    |
    v
 Staging
    |
    v
  Silver
    |
    v
   Gold
    |
    v
Consolidated Gold
```

---

## Staging Tables

Staging tables are used to isolate newly arrived records.

This allows transformations to operate only on the latest batch rather than scanning and transforming the complete historical dataset.

Benefits include:

* Reduced processing overhead
* Faster incremental pipelines
* Easier debugging
* Better separation between historical and new data

---

## File Archiving

After successful processing, source files are moved from the landing area to an archive location.

```text
Before Processing

orders/
└── landing/
    └── orders_2025_12_02.csv
```

```text
After Processing

orders/
├── landing/
└── archive/
    └── orders_2025_12_02.csv
```

This prevents already processed files from being picked up again.

---

## Pipeline Orchestration

The individual notebooks are organized into a complete data workflow.

Conceptually:

```text
Data Ingestion
      |
      v
Bronze Processing
      |
      v
Silver Transformation
      |
      v
Gold Processing
      |
      v
Parent/Child Consolidation
      |
      v
File Archive
```

Databricks workflows can be used to control notebook dependencies and execution order.

---

## Consolidated Analytics Layer

After the parent and child datasets are integrated, a denormalized analytical view is created.

The view joins:

```text
fact_orders
+
dim_customers
+
dim_products
+
dim_gross_price
+
dim_date
```

to create a single BI-friendly dataset.

This simplifies dashboard queries and improves the usability of the analytics layer.

---

## Analytics

The final dataset supports business questions such as:

* What is the total revenue across both companies?
* Which products generate the most sales?
* How are monthly sales changing?
* Which customers generate the most revenue?
* Which product categories perform best?
* How does acquired-company performance compare across different periods?

The analytical layer can be explored using:

* Databricks SQL
* Databricks Dashboards
* Databricks Genie

---

## Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

* End-to-end ETL/ELT pipeline development
* AWS S3 data ingestion
* Databricks Lakehouse architecture
* Medallion Architecture
* PySpark transformations
* SQL analytics
* Delta Lake
* Delta MERGE / upsert
* Historical data backfills
* Incremental data loading
* Staging tables
* Data quality validation
* Schema harmonization
* Dimensional modeling
* Star schema
* Fact and dimension tables
* Data aggregation
* Parameterized pipelines
* Data lineage metadata
* File lifecycle management
* Pipeline orchestration
* BI-ready data modeling

---

## Project Architecture

```text
                         +------------------+
                         | Sports Bar OLTP  |
                         +--------+---------+
                                  |
                                  | CSV Extract
                                  v
                         +------------------+
                         |     AWS S3       |
                         |   Data Lake      |
                         +--------+---------+
                                  |
                                  v
                    +---------------------------+
                    |     Databricks Bronze     |
                    |        Raw Data           |
                    +-------------+-------------+
                                  |
                                  v
                    +---------------------------+
                    |     Databricks Silver     |
                    | Cleaned / Standardized    |
                    +-------------+-------------+
                                  |
                                  v
                    +---------------------------+
                    |      Sports Bar Gold      |
                    |     Business-Ready        |
                    +-------------+-------------+
                                  |
                                  | Delta MERGE
                                  v
                +--------------------------------------+
                |          Consolidated Gold           |
                |          Atlon + Sports Bar          |
                +------------------+-------------------+
                                   |
                                   v
                      +-------------------------+
                      | Denormalized Analytics  |
                      |          View           |
                      +------------+------------+
                                   |
                         +---------+---------+
                         |                   |
                         v                   v
                +----------------+    +---------------+
                |   Dashboard    |    |    Genie      |
                +----------------+    +---------------+
```

---

## Future Improvements

Possible improvements to make the pipeline more production-ready include:

* Add automated data-quality tests
* Add schema-evolution handling
* Add pipeline monitoring and alerting
* Add error and quarantine tables
* Implement CI/CD for Databricks notebooks
* Add environment-specific configuration for Dev/Test/Prod
* Implement automated source-file arrival triggers
* Add unit and integration tests
* Add data observability metrics
* Add infrastructure-as-code deployment

---

The final result is a scalable **AWS S3 + Databricks Lakehouse pipeline** that combines sales information from two different business systems into a unified analytics platform.

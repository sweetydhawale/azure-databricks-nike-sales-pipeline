
# Azure Databricks Nike Sales Data Engineering Pipeline

# 1. Project Overview

This project demonstrates an end-to-end Azure Data Engineering pipeline using Azure Databricks, PySpark, Delta Lake, and Azure Data Factory (ADF). The pipeline processes raw Nike sales CSV data through Bronze, Silver, and Gold layers following Medallion Architecture principles.

The project includes data ingestion, transformation, incremental loading, data quality validation, Delta Lake implementation, and ADF orchestration.

---

# 2. Architecture

The pipeline follows a modern Medallion Architecture approach:

Raw CSV File
→ Azure Databricks Volume (Landing Zone)
→ Bronze Layer
→ Silver Layer
→ Incremental Merge/Upsert
→ Gold KPI Layer
→ Data Quality Checks
→ ADF Orchestration

ADF orchestrates the Databricks notebooks sequentially to automate the ETL workflow.

---

# 3. Tech Stack

- Azure Databricks
- PySpark
- Delta Lake
- Azure Data Factory (ADF)
- Unity Catalog Volumes
- GitHub
- Medallion Architecture

---

# 4. Pipeline Flow

The pipeline is divided into multiple enterprise-style Databricks notebooks:

## 01_bronze_ingestion
- Reads raw Nike sales CSV data
- Validates schema
- Performs initial row count validation

## 02_silver_transformation
- Removes duplicate records using Order_ID
- Handles invalid/null business records
- Standardizes multiple date formats
- Performs data type casting
- Calculates validated revenue column
- Saves cleaned data into Silver Delta table

## 04_incremental_load_merge
- Creates incremental incoming sales records
- Cleans incremental dataset
- Performs Delta Lake MERGE/UPSERT into Silver layer

## 03_gold_kpi_generation
- Generates business KPI datasets
- Product sales KPIs
- Region sales KPIs
- Sales channel KPIs
- Profit aggregations

## 05_data_quality_checks
- Row count validation
- Duplicate Order_ID validation
- Null checks
- Saves DQ summary table

---

# 5. Medallion Architecture

The project follows Medallion Architecture design principles.

## Bronze Layer
Stores raw ingested Nike sales CSV data without transformation.

## Silver Layer
Stores cleaned and standardized data after:
- Deduplication
- Schema standardization
- Date parsing
- Business validation
- Revenue validation

## Gold Layer
Stores business-ready curated KPI datasets for analytics and reporting.

---

# 6. Incremental Load

The project implements incremental loading using Delta Lake MERGE functionality.

The pipeline:
- Identifies new incoming sales records
- Updates existing records based on Order_ID
- Inserts new records into the Silver Delta table

This simulates enterprise-grade upsert processing used in production ETL pipelines.

---

# 7. Data Quality Checks

The project implements multiple data quality validations:

- Row count validation
- Duplicate Order_ID detection
- Null validation
- Business rule filtering
- Revenue validation

Invalid business records such as:
- negative quantities
- invalid discounts
- malformed dates
- null pricing values

are filtered before loading into the Silver layer.

---

# 8. ADF Orchestration

Azure Data Factory orchestrates the Databricks notebooks sequentially.

Pipeline Flow:

01_bronze_ingestion
→ 02_silver_transformation
→ 04_incremental_load_merge
→ 03_gold_kpi_generation
→ 05_data_quality_checks

ADF uses Azure Databricks Notebook Activities with linked services and notebook path integration to automate the ETL workflow.

---

# 9. Screenshots

The repository includes screenshots for:

- ADF pipeline orchestration
- Databricks linked service configuration
- Notebook activity configuration
- Silver Delta output
- Gold KPI output
- Incremental merge results
- Data quality summary

---

# Interview Explanation

This project demonstrates an enterprise-style Azure Data Engineering pipeline using Databricks, PySpark, Delta Lake, and Azure Data Factory. The pipeline processes raw Nike sales data through Bronze, Silver, and Gold layers following Medallion Architecture principles. Delta Lake was used for reliable storage and incremental MERGE/UPSERT processing. Data quality validations were implemented to ensure standardized and curated business-ready datasets.

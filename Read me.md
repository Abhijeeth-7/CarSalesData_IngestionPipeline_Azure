# Azure Data Engineering Project - GitHub API to Azure Data Lake

## Overview

This project builds an **end-to-end data pipeline** that extracts sales data from a **GitHub API**, processes it through multiple layers in **Azure Data Lake Storage (ADLS)**, and loads it into **Power BI / Azure Synapse** for analytics. The pipeline follows a **monolithic architecture** with Bronze, Silver, and Gold layers.

## Architecture

1. **Source Data**:

   - Sales data is pushed to a **GitHub repository** as files.
   - Azure Data Factory (ADF) extracts data from GitHub and loads it into **Azure SQL Server**.

2. **Ingestion Pipeline**:

   - Incremental data is extracted from **Azure SQL Server** and loaded into the **Bronze Layer** of Azure Data Lake Storage.
   - Data moves through **Bronze → Silver → Gold** layers via **Azure Data Factory (ADF) and Azure Databricks**.

## Data Processing

### **Bronze Layer (Raw Data Storage)**

- Stores **raw, unprocessed** data as received from the **source system (SQL Server)**.
- Data is **incrementally ingested** using ADF, ensuring only new data is added.
- No transformations are applied at this stage.

### **Silver Layer (Cleansed & Transformed Data)**

- Data from **Bronze is cleaned, standardized, and transformed** using **Azure Databricks**.
- Implements **Slowly Changing Dimension (SCD) Type 1**, ensuring the latest record is always stored.
- Removes duplicates, fixes data inconsistencies.
- Optimized for **data quality and consistency** before moving to the Gold layer.

### **Gold Layer (Analytics & Reporting-Ready Data)**

- **Dimension Tables (SCD Type 1):** Silver data is broken into **structured dimension tables** based on business entities.
- **Fact Table:** Joins **dimension tables** to create a **centralized fact table** for analytics.
- Data is structured for efficient querying in **Power BI.**

## Data Transformation

- **Azure Data Factory (ADF)** moves data from \*\*Github API -> SQL Server->\*\*\*\*Bronze, \*\*handling ingestion and incremental loads.
- **Azure Databricks** performs **data transformation** from Bronze -> **Silver -> Gold**, ensuring data is in **analytics-ready format**.
- Fact tables are built using **aggregations and joins** on dimension tables, enabling **business intelligence and reporting**.

## Technologies Used

- **Azure Data Factory (ADF)** - Data ingestion and orchestration.
- **Azure SQL Server** - Intermediate storage before moving to ADLS.
- **Azure Data Lake Storage (ADLS)** - Multi-layered data storage.
- **Azure Databricks** - Data transformation and processing.
- **Power BI / Azure Synapse** - Data visualization and analytics.
- **GitHub API** - Source system for raw data.

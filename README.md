# ETL Pipeline with Apache Airflow

## Overview

This project implements an **ETL (Extract, Transform, Load) data pipeline** for a music streaming service using **Apache Airflow**. The pipeline integrates data from multiple sources, processes it, and generates **key performance indicators (KPIs)** for business intelligence.

## Objective

- Ingest user and song metadata from Amazon RDS (simulated with CSV files) and streaming data from Amazon S3.
- Perform data validation, transformation, and compute meaningful KPIs.
- Load the processed data into **Amazon Redshift** for analytical processing.

## Key Features

- **Automated data ingestion** from RDS (CSV simulation) and S3.
- **Data validation** to ensure integrity before processing.
- **Transformation and KPI Computation**:
  - **Genre-Level KPIs:** Listen Count, Average Track Duration, Popularity Index, Most Popular Track per Genre.
  - **Hourly KPIs:** Unique Listeners, Top Artists per Hour, Track Diversity Index.
- **Efficient data loading** into Amazon Redshift using an **Upsert strategy**.
- **Logging and error handling** for monitoring and troubleshooting.

## Architecture

The data pipeline follows the **ETL (Extract, Transform, Load) workflow**:

1. **Extraction:**
   - User and song metadata from RDS (CSV-based simulation).
   - Streaming data from Amazon S3.
2. **Transformation & Validation:**
   - Ensure all required columns exist.
   - Compute genre-based and hourly streaming KPIs.
3. **Loading:**
   - Insert transformed data into Amazon Redshift.
   - Use **staging tables** and **Upsert strategy** to handle new and duplicate records.

## Technology Stack

- **Apache Airflow** (DAG orchestration)
- **Amazon S3** (Streaming data storage)
- \*\*Amazon RDS (CS\*\*\*\*V \*\***Simulation)** (Metadata source)
- **Amazon Redshift** (Data warehouse for analytics)
- **Python** (Data processing scripts)
- **SQL** (Queries for Redshift data validation)V \*\*

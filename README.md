# Data-Ingestion-and-Migration-Pipeline-

## Overview
This project demonstrates a data ingestion and migration workflow built using Snowflake and AWS S3. It involves ingesting raw CSV data from cloud storage into Snowflake via external stages, performing load-time validation to identify data issues, and implementing robust error handling to ensure reliable ingestion. Post-loading, the data is cleaned and transformed using SQL techniques such as deduplication and NULL handling to produce high-quality, analysis-ready datasets.

## Tech Stack
- Snowflake – Data warehousing and processing  
- AWS S3 – Cloud storage for raw data  
- SQL – Data ingestion, validation, and transformation  

## Key Features
- Integration between AWS S3 and Snowflake using storage integration  
- Structured data ingestion using external stages and file formats  
- Load-time validation using `VALIDATION_MODE = RETURN_ERRORS`  
- Fault-tolerant data loading with `ON_ERROR = CONTINUE`  
- Deduplication using window functions (`ROW_NUMBER`)  
- NULL handling and standardization using `COALESCE`  
- Separation of staging and final layers for better data management  

## Workflow

### 1. Storage Integration
- Secure connection established between Snowflake and AWS S3 using storage integration.

### 2. Data Ingestion
- Raw CSV files are ingested from S3 into a staging table using `COPY INTO`.

### 3. Data Validation
- Validation mode is used to detect and inspect data load errors before ingestion.

### 4. Error Handling
- Fault-tolerant loading ensures ingestion continues even when encountering bad records.

### 5. Data Cleaning & Transformation
- Duplicate records removed using window functions  
- Missing values handled using `COALESCE`  
- Data standardized for consistency  

### 6. Final Data Storage
- Cleaned data is stored in a final table for downstream use.

## Data Quality Handling
- Identified ingestion issues using validation mode  
- Handled erroneous records during load using error-handling strategies  
- Removed duplicates based on `Registration_ID`  
- Standardized NULL values for consistency  

## Outcome
- Successfully migrated raw data from AWS S3 to Snowflake  
- Built a reliable and reusable ingestion workflow  
- Produced clean, structured, and analysis-ready datasets  
- Demonstrated core data engineering practices: ingestion, validation, error handling, and transformation  

DBT Repository - Data Transformation Layer (3rd Repo in a 4-Repo End-to-End Project)
Project Overview
This repository is part of a 4-repository end-to-end data pipeline project. It specifically serves as the third stage in the pipeline, focusing on data transformation and modeling using DBT (Data Build Tool).

Role of this Repository in the Data Pipeline
Receives JSON-formatted data from S3, which was processed in the previous stage (AWS Lambda)
Transforms raw data into structured tables, including:
Fact Tables – Transactional data, events, or key business actions
Dimension Tables – Master data (e.g., customers, products, dates)
Aggregations – Precomputed metrics for performance optimization
Implements Data Quality Tests to ensure:
Data integrity (e.g., uniqueness, non-null constraints)
Referential integrity (valid relationships between dimensions & facts)
Business rule validations
Uses DBT models, macros, and tests to create a scalable and maintainable transformation layer.
Technology Stack
DBT (Data Build Tool): For SQL-based transformations and data modeling
AWS S3: Source for raw JSON data
AWS Lambda: Previous stage processing data before it reaches DBT
Redshift / Snowflake / BigQuery (or any DWH): Target data warehouse for structured tables

Next Step in the Pipeline
The 4th repository holds the Streamlit Web Application can is found here 


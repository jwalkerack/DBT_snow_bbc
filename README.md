DBT Repository - Data Transformation Layer

(3rd Repo in a 4-Repo End-to-End Project)

This is the third repo of four as part of an end-to-end data project that sources data from the BBC Sport website and transforms it into an analytical database. This database is then used by a Streamlit application to build interactive workflows for users to interact with the data.

The DBT section focuses on transformation instructions that take the raw JSON data, which has been sourced into Snowflake from S3, and transforms it into several models, including:

Staging

Dimension

Fact

Aggregations

During this step, data quality elements are added to provide insights into areas of concern. The code runs via DBT Cloud, with a production job set up linking to the production environment.

The next step in the project is the Streamlit application. Its repository can be found here:


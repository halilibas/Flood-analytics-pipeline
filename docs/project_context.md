# Project Context

This project is an end-to-end data engineering pipeline for flood claims analytics.

The goal is to build a realistic portfolio project that shows how raw insurance data can be ingested, cleaned, modeled, tested, and served for business analysis. The project uses FEMA NFIP flood insurance claims data as the main public dataset, along with synthetic policy, customer, and agent data to make the data model closer to what an insurance company might work with internally.

I chose this domain because insurance claims data has a good mix of real-world complexity: dates, locations, claim amounts, policy attributes, customer history, catastrophe events, and changing business entities over time. These are useful problems for practicing data engineering concepts like medallion architecture, dimensional modeling, data quality checks, incremental processing, and orchestration.

The pipeline follows a medallion architecture:

- **Bronze:** raw or lightly processed source data, stored with ingestion metadata
- **Silver:** cleaned, standardized, typed, and deduplicated data
- **Gold:** business-ready tables modeled for analytics and reporting

The final gold layer will support insurance KPIs such as claim frequency, claim severity, paid loss trends, claim cycle time, regional loss patterns, and estimated loss ratio where premium data is available or synthetically generated.

This project is intentionally built as a learning-focused but realistic system. It is not meant to copy any specific company's internal architecture. Instead, it uses insurance as the business context while practicing data engineering patterns that apply across many industries, including finance, healthcare, retail, logistics, and SaaS.

The main skills demonstrated in this project include:

- PySpark DataFrame transformations
- Databricks notebook development
- Delta Lake table design
- Bronze, silver, and gold data modeling
- SQL and dimensional modeling
- Slowly changing dimensions for policy/customer history
- dbt models and tests
- Data quality validation
- Pipeline orchestration
- Streamlit dashboarding
- Technical documentation and design reasoning

A major goal of the project is to be able to explain not only what was built, but why each design choice was made. For that reason, this repository includes supporting documentation around architecture, data modeling, KPI definitions, and technical decisions.

By the end, the project should show a complete path from raw claims data to business-ready analytics: source data ingestion, cleaned and conformed datasets, a dimensional gold layer, validation checks, orchestration, and a dashboard that answers practical insurance business questions.


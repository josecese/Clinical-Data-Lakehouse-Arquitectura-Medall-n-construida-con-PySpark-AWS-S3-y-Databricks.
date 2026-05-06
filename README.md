# 🏥 Clinical Data Lakehouse Project

![Status](https://img.shields.io/badge/Status-Under_Construction-warning?style=for-the-badge)
![Apache Spark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=Apache-Spark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-844FBA?style=for-the-badge&logo=Terraform&logoColor=white)

Welcome to the **Clinical Data Lakehouse Project** repository! 🚀  
This project demonstrates a comprehensive, scalable data engineering solution designed for the healthcare sector. It highlights industry best practices in data lakehouse architecture, data quality assurance, and automated infrastructure.

---
## 🏗️ Data Architecture

The data architecture for this project follows the **Medallion Architecture**, leveraging the ACID compliance and time-travel capabilities of **Delta Lake**:

1. **🥉 Bronze Layer**: Stores raw clinical data as-is from source systems. Data is ingested from Parquet/CSV files into the data lake.
2. **🥈 Silver Layer**: This layer executes data cleansing, standardization, null handling, and strict PII (Personally Identifiable Information) masking. Pipelines use PySpark and MERGE (Upsert) operations for incremental loading.
3. **🥇 Gold Layer**: Houses business-ready data modeled into a star schema, optimized for business intelligence, reporting, and advanced analytics.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a modern Data Lakehouse utilizing the Medallion Architecture.
2. **ETL Pipelines**: Developing scalable data pipelines using **PySpark** to extract, transform, and load clinical datasets.
3. **Data Quality & Security**: Implementing robust Exploratory Data Analysis (EDA) scripts, regex pattern validation (e.g., SSN formats), and logic validation.
4. **Infrastructure as Code (IaC)**: Provisioning cloud resources (AWS S3, EC2, Databricks workspaces) using **Terraform**.
5. **Orchestration**: Managing complex pipeline dependencies using **Apache Airflow**.

🎯 This repository is an excellent resource for showcasing expertise in:
- Big Data Engineering
- PySpark & Delta Lake Development
- Data Architecture & Modeling
- Infrastructure as Code (IaC)
- Data Quality & Governance

---

## 🚀 Project Requirements & Scope

### Data Engineering Pipeline

#### Objective
Develop a robust data pipeline to consolidate clinical entities (Patients, Encounters, Medications), enabling secure analytical reporting while maintaining patient privacy.

#### Specifications
- **Data Sources**: Clinical datasets ingested in Parquet format.
- **Data Quality & EDA**: Extensive data profiling, including exact structural validation and detection of biological/logical anomalies (e.g., death dates preceding birth dates).
- **Security Compliance**: Strict adherence to healthcare privacy standards by removing or masking sensitive identifiers (SSN, Passports, Drivers Licenses).
- **Integration**: Utilizing Delta Lake for efficient Upserts, ensuring no duplicate records exist during incremental loads.
- **Documentation**: Comprehensive logging configured within Python scripts to maintain an auditable trail of all transformations.

---

## 📂 Repository Structure
```text
clinical-data-lakehouse/
│
├── Airflow/                            # Orchestration DAGs for ETL pipelines
│   └── dags/
│
├── terraform/                          # Infrastructure as Code (AWS/Databricks setup)
│
├── notebooks/                          # Experimental and exploratory Databricks notebooks
│
├── src/                                # Production-grade PySpark scripts
│   ├── ingestion/                      # Scripts for extracting and loading raw data
│   │   └── 01_bronze_ingestion.py
│   ├── bronze/                         # Exploratory Data Analysis (EDA) and profiling
│   │   └── 02_eda_patients.py
│   ├── silver/                         # Cleansing, PII masking, and Upsert pipelines
│   │   └── 01_silver_patients_pipeline.py
│   └── gold/                           # Analytical models and Star Schema (Pending)
│
├── docs/                               # Architecture diagrams and documentation
│
├── .env                                # Environment variables (ignored in Git)
├── .gitignore                          # Files and directories to be ignored by Git
└── README.md                           # Project overview and instructions
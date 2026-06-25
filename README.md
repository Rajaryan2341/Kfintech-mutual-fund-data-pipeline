# 🚀 Mutual Fund Data Engineering Pipeline (PySpark ETL)

## 🧠 Project Overview

This project is an end-to-end **ETL (Extract, Transform, Load) pipeline** built using **Apache Spark (PySpark)** to process large-scale Mutual Fund NAV data.

The pipeline transforms raw financial data into structured analytical models using **Star Schema design (Fact & Dimension tables)** and generates business insights for reporting.

It demonstrates real-world **data engineering skills** like distributed processing, data modeling, and performance-optimized transformations.

 

## 🏗️ Architecture

Raw CSV Data  
→ PySpark Extraction  
→ Data Cleaning & Transformation  
→ Star Schema Modeling  
→ Fact & Dimension Tables  
→ Business Aggregations  
→ Parquet Output Storage  

 

## ⚙️ Tech Stack

- Python 🐍  
- Apache Spark (PySpark) ⚡  
- Spark SQL  
- Parquet (Columnar Storage)  
- Git & GitHub  

 

## 📂 Project Structure
MutualFund_ETL/
│
├── pipeline.py # Main ETL pipeline
├── .gitignore # Excludes dataset and system files
├── Input/ # Raw dataset (NOT pushed to GitHub)
└── output/ # Processed Parquet files
├── dim_scheme/
├── dim_date/
├── fact_nav/
├── fund_house_summary/
├── category_summary/
└── monthly_trend/

 

## 🔄 ETL Pipeline Workflow

### 1️⃣ Extract
- Load raw CSV dataset using Spark DataFrame API
- Handle large-scale dataset efficiently

### 2️⃣ Transform
- Remove null values using `dropna()`
- Remove duplicates using `dropDuplicates()`
- Convert date column to proper DateType
- Create derived columns:
  - Year
  - Month
  - Quarter

 

### 3️⃣ Data Modeling (Star Schema)

#### 📌 Dimension Tables

**dim_scheme**
- Scheme_Code  
- Scheme_Name  
- Scheme_Type  
- Scheme_Category  
- Fund_House  

**dim_date**
- Date  
- Year  
- Month  
- Quarter  

 

#### 📌 Fact Table

**fact_nav**
- Scheme_Code  
- Date  
- NAV  

 

## 📊 Business Reports Generated

### 📈 Average NAV by Fund House
Used to analyze performance of fund houses.

### 📊 Average NAV by Scheme Category
Compares different mutual fund categories.

### 🔝 Top Performing Schemes
Identifies schemes with highest NAV values.

### 📅 Monthly NAV Trend Analysis
Tracks NAV movement over time.

 

## 🧠 Key Skills Demonstrated

- PySpark distributed data processing  
- ETL pipeline design  
- Data cleaning & transformation  
- Star schema data modeling  
- Spark SQL aggregations  
- Handling large datasets efficiently  
- Parquet file optimization  

 

## ⚠️ Data Note

The dataset is excluded from this repository due to GitHub size limits (>100MB).  
Only the ETL pipeline code is included for reproducibility.

 

## 🚀 Future Improvements

- Apache Airflow integration for scheduling ETL jobs  
- Kafka integration for real-time data streaming  
- Cloud deployment on AWS / GCP  
- Data quality validation layer  
- Dashboard using Power BI / Tableau  

 

## 👨‍💻 Author

**Raj Aryan**  
Aspiring Data Engineer  

Skills: Python | SQL | PySpark | ETL | Data Engineering

 

## 🎯 Interview Summary

This project demonstrates a **production-style ETL pipeline** capable of processing large-scale financial datasets using Spark, applying data modeling techniques, and generating business insights in a scalable way.

---

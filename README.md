# End-to-End Data Warehouse and ETL Pipeline for Banking Analytics

## Project Overview

This project is a comprehensive, end-to-end simulation of a real-world data engineering task developed during my project-based internship with **ID/X Partners** and **Rakamin Academy**. The primary objective was to address a common business challenge for a banking client: inefficient and delayed reporting due to operational data being scattered across multiple, disparate systems.

This repository contains the complete solution, which transforms raw data from various sources into a centralized, analytics-ready Data Warehouse, complete with automated ETL pipelines and pre-built analytical queries.

---

## 🏛️ Solution Architecture

The solution follows a classic ETL (Extract, Transform, Load) architecture, designed to create a robust and scalable single source of truth.

**The data flows through four key stages:**
1.  **Data Sources:** Raw data is ingested from three different types of systems:
    - Relational Database (SQL Server)
    - Excel Files (`.xlsx`)
    - CSV Files (`.csv`)
2.  **ETL Processing (Talend):** Talend Open Studio is used as the core ETL engine to perform:
    - **Extraction:** Pulling data from all 8 distinct sources.
    - **Transformation:** Cleansing data, joining multiple tables, unifying different data streams, and deduplicating records.
    - **Loading:** Loading the clean, transformed data into the target Data Warehouse.
3.  **Data Warehouse (SQL Server):** A centralized DWH built on Microsoft SQL Server using a Star Schema data model. It consists of:
    - **3 Dimension Tables:** `DimCustomer`, `DimAccount`, `DimBranch`
    - **1 Fact Table:** `FactTransaction`
4.  **Data Access & Analytics:** Pre-built Stored Procedures provide quick, aggregated insights for business users and analysts, enabling faster decision-making.

---

## 🛠️ Tech Stack

*   **Database:** Microsoft SQL Server
*   **ETL Tool:** Talend Open Studio for Data Integration
*   **Data Modeling:** Star Schema
*   **Language:** T-SQL (for Stored Procedures)
*   **Version Control:** Git & GitHub

---

## ✨ Key Features & Implementation Details

### 1. Data Warehouse Design (Star Schema)
The DWH was designed from scratch with a focus on analytical performance and clarity.

- **`DimCustomer`**: A consolidated view of customer information, enriched with city and state data from separate tables.
- **`DimAccount` & `DimBranch`**: Dimension tables providing descriptive context for accounts and bank branches.
- **`FactTransaction`**: The core table containing all unique transaction records from the three source systems. Primary and Foreign keys were implemented to ensure data integrity.

### 2. Modular ETL Pipelines in Talend
A total of four distinct Talend jobs were created for modularity and maintainability:

- **`Load_DimBranch` & `Load_DimAccount`**: Simple pipelines to load master data.
- **`Load_DimCustomer`**: A more complex pipeline featuring multi-table **JOINs** within `tMap` to combine `customer`, `city`, and `state` data. It also includes data cleansing steps like converting text fields to uppercase.
- **`Load_FactTransaction`**: The main integration pipeline that unifies data from all three transaction sources (`tUnite`), removes duplicates based on `transaction_id` (`tUniqRow`), and formats the final output for loading.

### 3. Automated Business Reports (Stored Procedures)
To provide immediate value to the "client," two parameterized Stored Procedures were developed:

- **`sp_DailyTransaction`**: Generates a daily summary of transaction volume and total amount for a given date range.
- **`sp_BalancePerCustomer`**: A sophisticated procedure that calculates the current balance of each active account for a specific customer, applying business logic (`CASE WHEN`) to handle deposits and withdrawals.

---

## 🚀 How to Run This Project

To replicate this solution, follow these steps:

1.  **Prerequisites:**
    - Microsoft SQL Server and SQL Server Management Studio (SSMS) installed.
    - Talend Open Studio for Data Integration installed.

2.  **Setup the Source Database:**
    - In SSMS, restore the source database using the provided `sample.bak` file. This will create the `sample` database with all necessary source tables.

3.  **Build the Data Warehouse:**
    - In the `sql_scripts` folder of this repository, you will find `create_tables.sql`.
    - Open this script in SSMS and execute it to create the `DWH` database and all dimension and fact tables.

4.  **Configure and Run the Talend Jobs:**
    - Open Talend Studio and import the project/jobs from this repository.
    - Set up the database connections in the Metadata section for both the `sample` and `DWH` databases.
    - Run the Talend jobs in the following order to ensure data dependencies are met:
        1. `Load_DimBranch`
        2. `Load_DimAccount`
        3. `Load_DimCustomer`
        4. `Load_FactTransaction`

5.  **Deploy and Test the Stored Procedures:**
    - In the `sql_scripts` folder, open `create_procedures.sql`.
    - Execute this script in SSMS against the `DWH` database.
    - You can now test the procedures with sample commands, e.g., `EXEC sp_DailyTransaction @start_date = '2024-01-18', @end_date = '2024-01-20';`.

---

## 🌟 Project Outcomes

This project successfully demonstrates a complete data engineering lifecycle. The final solution transforms a chaotic, multi-source data environment into a clean, reliable, and high-performance Data Warehouse, ready to power business intelligence and analytics.

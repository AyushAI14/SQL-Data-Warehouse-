# SQL-Data-Warehouse

## What is Data Warehouse 
A data warehouse is a **central storage system that collects data from multiple sources, cleans it, organizes it, and stores it for reporting, analytics, and business decisions.**
It stores historical data and is optimized for analysis, not daily transactions.

### Key idea

Instead of data being scattered in different systems (CRM, sales DB, marketing tool), a warehouse brings it all into **one place** so you can analyze it easily.

### Example (real-world)

Flipkart collects data from: 

* Orders database
* Payments system
* Delivery tracking
* Customer support system

All this data goes into a data warehouse like **Snowflake / Redshift / BigQuery**.

<img width="200" height="200" alt="Image" src="https://github.com/user-attachments/assets/dad2abb8-3ee9-4570-a3cd-a93b91c1bbf4" />

## What is ETL 

ETL stands for **Extract, Transform, Load** — a data pipeline process used to move data from source systems into a data warehouse or data store.

### What it means

1. **Extract** → Pull data from multiple sources
2. **Transform** → Clean, format, validate, and enrich data
3. **Load** → Store it in the target system (e.g., data warehouse)

### Example (real-world)

Amazon wants to analyze sales:

* **Extract** data from

  * Order DB
  * Delivery system
  * Payment logs
  * Customer database

* **Transform**

  * Remove duplicates
  * Convert date formats
  * Correct data errors
  * Join customer & order data
  * Apply business rules (e.g., completed orders only)

* **Load**

  * Store final cleaned data into **Redshift/Snowflake/BigQuery**

### Different Methods of ETL
<img width="1000" height="1000" alt="Image" src="https://github.com/user-attachments/assets/de329fc8-b985-41aa-ae9c-a30ea007797e" />


### Different type of Data Achitecture
<img width="300" height="303" alt="Image" src="https://github.com/user-attachments/assets/19fb3199-1c4f-48d9-b67c-098de83f561a" />

---

### **Data Warehouse**

Stores **clean, structured, historical data** for reporting and BI.

**Example:** Snowflake, BigQuery, Redshift
**Use:** Dashboards, business KPIs, analytics

---

### **Data Lake**

Stores **raw data** in any format (CSV, JSON, logs, images).

**Example:** AWS S3, Azure Data Lake, GCS
**Use:** ML training data, large-scale storage

---

### **Data Lakehouse**

**Blend** of Data Lake + Data Warehouse — one system for raw + structured data.

**Example:** Databricks, Delta Lake
**Use:** BI + ML workloads together

---

### **Data Mesh**

**Decentralized approach** — each domain/team owns and serves its own data.

**Example:** Architecture style (not a tool)
**Use:** Large companies like Netflix, Uber

---


## Four Approach for DataWareHouse

<img width="443" height="405" alt="Image" src="https://github.com/user-attachments/assets/0bace3ac-1470-477d-ae25-b99a606a1cd1" />

---

**EDW** = central warehouse for entire company
**Data Mart** = subset for one department

### **1) Inmon (Top-Down)**

* Central **enterprise data warehouse (EDW)** first (normalized data)
* Then build **data marts** for departments

**Best for:** Large enterprises with complex data governance
**Key idea:** “Build warehouse first, marts later”

---

### **2) Kimball (Bottom-Up)**

* Build **data marts first** (dimensional modeling)
* Combine them into a warehouse

**Best for:** Faster business reporting, BI-driven companies
**Key idea:** “Quick wins — marts first”

---

### **3) Data Vault**

* Load raw data first (Raw Vault)
* Add business rules later (Business Vault)
* Then create marts

**Best for:** Complex, evolving data environments; audit-heavy orgs
**Key idea:** **Separate raw data** and **business logic**

---

### **4) Medallion Architecture (Lakehouse)**

* **Bronze:** Raw data
* **Silver:** Cleaned / structured
* **Gold:** Business-ready datasets / marts

**Best for:** Modern data lake / lakehouse systems (Databricks, Spark)
**Key idea:** Multi-layer refinement pipeline

---

## **Blueprint of All 3 Layers (Medallion Architecture)**

<img width="562" height="654" alt="Image" src="https://github.com/user-attachments/assets/0a658fdd-4efd-4cd2-9fa5-cd0f07b12dd4" />

### **Separation Of Concern (SOC)** (Secret of top Data Engineer)

* Never Mix thing , every layer has to be isolated and doing only thier repective work

<img width="576" height="678" alt="Image" src="https://github.com/user-attachments/assets/59a60ddb-1cd3-4af6-9e92-ccc291f5fc74" />

# **-------- Project Starts --------**

[**Notion Project Plan**](https://www.notion.so/DataWareHouse-Projects-2a66ed840d9b80f1b28fe82f23975ff2?source=copy_link)
### **Our Project Achitecture**
<img width="622" height="505" alt="Image" src="https://github.com/user-attachments/assets/bc03b357-131f-4635-938a-795810825aef" />

#### **Data Flow**
[**Data Flow Diagram**](Drawings/Data_Flow.png)
### **Building the Data Warehouse (Data Engineering)**

### **Objective**
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

### **Specifications**
- **Data Sources:** Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality:** Cleanse and resolve data quality issues prior to analysis.
- **Integration:** Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope:** Focus on the latest dataset only; historization of data is not required.
- **Documentation:** Provide clear documentation of the data model to support both business stakeholders and analytics teams.

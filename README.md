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
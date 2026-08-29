# Walmart Data Engineering Pipeline

### End-to-End CDC Pipeline using Databricks, Apache Airflow, dbt & PostgreSQL

An end-to-end data engineering project that demonstrates how raw Walmart retail data can be ingested, processed using **Change Data Capture (CDC)**, orchestrated with **Apache Airflow**, transformed using **dbt**, and organized into **Silver and Gold data layers** for analytics.

The project uses **Databricks for CDC ingestion and processing**, **Apache Airflow for orchestration**, **dbt for SQL-based transformations and data modeling**, and **PostgreSQL as the source/raw data store**.

---

## 🚀 Project Overview

The goal of this project is to build a reliable and automated data pipeline for Walmart's retail data.

The pipeline starts with raw datasets containing:

* Customers
* Stores
* Products
* Employees
* Orders
* Order Items

These datasets are loaded into raw tables and then processed through a multi-layer data architecture.

The pipeline performs:

1. Raw data ingestion
2. CDC-based data processing
3. Data freshness validation
4. Technical transformations
5. Business transformations
6. Dimensional modeling
7. Historical data tracking using dbt snapshots
8. Fact table creation
9. Automated orchestration using Apache Airflow

---

## 🏗️ Architecture

```text
                    ┌─────────────────────┐
                    │   Walmart Dataset   │
                    │      CSV Files      │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │    PostgreSQL       │
                    │     RAW Layer       │
                    │                     │
                    │ customers           │
                    │ stores              │
                    │ products            │
                    │ employees           │
                    │ orders              │
                    │ order_items         │
                    └──────────┬──────────┘
                               │
                               │ CDC
                               ▼
                    ┌─────────────────────┐
                    │      Databricks     │
                    │   CDC Processing    │
                    └──────────┬──────────┘
                               │
                               │
                     Apache Airflow
                    ┌──────────▼──────────┐
                    │      Orchestration  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │        dbt          │
                    │   Transformations   │
                    └──────────┬──────────┘
                               │
              ┌────────────────┴────────────────┐
              │                                 │
              ▼                                 ▼
     ┌──────────────────┐             ┌──────────────────┐
     │   Silver Layer   │             │    Gold Layer    │
     │                  │             │                  │
     │ Silver Technical │             │ Dimensions       │
     │ Silver Business  │             │ Facts            │
     │ OBT              │             │ Snapshots        │
     └──────────────────┘             └────────┬─────────┘
                                               │
                                               ▼
                                      Analytics / BI
```

---

## 🛠️ Technology Stack

| Technology         | Purpose                               |
| ------------------ | ------------------------------------- |
| **Python**         | Data loading and orchestration logic  |
| **PostgreSQL**     | Raw/source data storage               |
| **Databricks**     | CDC ingestion and processing          |
| **Apache Airflow** | Pipeline orchestration                |
| **dbt**            | SQL transformations and data modeling |
| **Docker**         | Containerized Airflow/dbt environment |
| **SQL**            | Data transformation and modeling      |
| **Jinja**          | Dynamic SQL generation in dbt         |

---

# 📂 Project Structure

```text
Walmart_Airflow_DBT_Project/
│
├── airflow_dbt_project/
│   │
│   ├── config/
│   │
│   ├── dags/
│   │   └── orchestrate.py
│   │
│   ├── logs/
│   │
│   ├── walmart_project/
│   │   │
│   │   ├── analyses/
│   │   │
│   │   ├── macros/
│   │   │
│   │   ├── models/
│   │   │   ├── source/
│   │   │   ├── silver_t/
│   │   │   ├── silver_b/
│   │   │   └── gold/
│   │   │       ├── ephemeral/
│   │   │       └── fact/
│   │   │
│   │   ├── seeds/
│   │   ├── snapshots/
│   │   ├── tests/
│   │   ├── dbt_project.yml
│   │   └── profiles.yml
│   │
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── requirements.txt
│
├── walmart_dataset/
│   ├── data/
│   ├── ddl/
│   │   └── walmart_schema.sql
│   └── load_data.py
│
├── Notes.png
└── README.md
```

The repository separates the dataset/loading portion from the Airflow + dbt project. The Airflow/dbt directory contains the DAG, Docker setup and dbt project, while the dataset directory contains CSV data, DDL and the Python loader.

---

# 📊 Source Data

The raw dataset consists of six main entities:

### 1. Customers

Contains customer information such as:

* `customer_id`
* `first_name`
* `last_name`
* `email`
* `phone`
* `city`
* `province`
* `country`
* `created_timestamp`
* `updated_timestamp`
* `is_active`

### 2. Stores

Contains:

* `store_id`
* `store_name`
* `city`
* `province`
* `country`
* `created_timestamp`
* `updated_timestamp`
* `is_active`

### 3. Products

Contains:

* `product_id`
* `product_name`
* `category`
* `brand`
* `price`
* `created_timestamp`
* `updated_timestamp`
* `is_active`

### 4. Employees

Contains:

* `employee_id`
* `store_id`
* `first_name`
* `last_name`
* `email`
* `job_title`
* `salary`
* `created_timestamp`
* `updated_timestamp`
* `is_active`

### 5. Orders

Contains:

* `order_id`
* `customer_id`
* `store_id`
* `order_timestamp`
* `payment_method`
* `order_status`
* `total_amount`
* `created_timestamp`
* `updated_timestamp`
* `is_active`

### 6. Order Items

Contains:

* `order_item_id`
* `order_id`
* `product_id`
* `quantity`
* `unit_price`
* `line_amount`
* `created_timestamp`
* `updated_timestamp`
* `is_active`

These schemas are defined in the project's Walmart DDL.

---

# 🔄 Data Loading

The `load_data.py` script uses `psycopg2` to connect to PostgreSQL and load CSV files into their corresponding raw tables.

The mapping is:

```text
customers.csv      → raw.customers
stores.csv         → raw.stores
products.csv       → raw.products
employees.csv      → raw.employees
orders.csv         → raw.orders
order_items.csv    → raw.order_items
```

The loader uses PostgreSQL's `COPY` operation for bulk CSV ingestion rather than inserting rows individually.

This makes the initial data load considerably more appropriate for a data engineering pipeline than performing one `INSERT` per row.

---

# ⚡ Change Data Capture (CDC)

A major component of the project is the CDC ingestion process.

Apache Airflow triggers a Databricks job responsible for CDC ingestion.

The Airflow DAG uses the Databricks SDK to:

1. Connect to the Databricks workspace.
2. Trigger a Databricks job.
3. Retrieve the job run status.
4. Poll the job until completion.
5. Continue the pipeline only when the Databricks job succeeds.
6. Fail the Airflow task when the Databricks job fails.

Conceptually:

```text
Airflow
   │
   ▼
Trigger Databricks Job
   │
   ▼
Databricks CDC Processing
   │
   ▼
Check Job Status
   │
   ├── SUCCESS ──► Continue Pipeline
   │
   └── FAILURE ──► Fail DAG
```

The DAG polls the Databricks run approximately every five seconds while waiting for completion.

---

# 🪄 dbt Transformation Layer

After CDC ingestion, dbt performs the transformation and modeling work.

The project organizes transformations into:

```text
Source
   │
   ▼
Silver Technical
   │
   ▼
Silver Business
   │
   ▼
Gold Ephemeral
   │
   ▼
Gold Dimensions / Snapshots
   │
   ▼
Gold Fact
```

The dbt project explicitly configures:

* `silver_t` models as tables
* `silver_b` models as tables
* `gold` models as tables
* `gold.ephemeral` models as ephemeral models

---

# 🥈 Silver Layer

The Silver layer is divided into two stages.

## Silver Technical

The `silver_t` directory contains technical transformation models:

```text
customers_t.sql
employees_t.sql
order_items_t.sql
orders_t.sql
products_t.sql
stores_t.sql
```

These models create cleaned/standardized technical representations of the raw source entities.

The technical layer also carries operational metadata such as:

* creation timestamp
* update timestamp
* active flag
* processing timestamp

This metadata is useful for tracking changes throughout the pipeline.

---

# 🥈 Silver Business

The `silver_b` layer currently contains an **OBT (One Big Table)** model.

```text
obt_b.sql
```

The OBT combines:

```text
Orders
   │
   ├── Customers
   │
   ├── Order Items
   │
   ├── Products
   │
   ├── Employees
   │
   └── Stores
```

The joins are based on business relationships such as:

```text
orders.customer_id = customers.customer_id

orders.order_id = order_items.order_id

order_items.product_id = products.product_id

orders.store_id = employees.store_id

orders.store_id = stores.store_id
```

The project uses Jinja configuration to dynamically construct the SELECT and JOIN portions of the OBT model.

---

# 🥇 Gold Layer

The Gold layer contains analytics-oriented models.

It is divided into:

```text
gold/
├── ephemeral/
└── fact/
```

The repository currently contains ephemeral models for:

```text
customers
employees
orders
products
stores
```

and a fact model:

```text
fact_orders.sql
```

The purpose of the Gold layer is to provide data in a form that is easier for downstream analytics and reporting workloads to consume.

---

# 📸 dbt Snapshots

The project uses **dbt snapshots** to preserve historical versions of dimensional data.

Snapshot definitions exist for:

```text
dim_customers
dim_employees
dim_orders
dim_products
dim_stores
```

For example, the customer snapshot uses:

```yaml
unique_key: customer_id
strategy: timestamp
updated_at: customer_updated_timestamp
```

This allows dbt to track changes to customer records over time instead of simply overwriting the previous version.

Conceptually:

```text
Customer Version 1
        │
        │ customer updated
        ▼
Customer Version 2
        │
        │ customer updated again
        ▼
Customer Version 3
```

This is useful when historical analysis requires knowing **what the customer record looked like at a particular point in time**.

---

# 🕒 Source Freshness

Before running the transformation models, the Airflow DAG executes:

```bash
dbt source freshness
```

This provides a freshness check for source data before the downstream transformation process continues.

The DAG therefore follows the principle:

```text
Ingestion
   ↓
Clean dbt artifacts
   ↓
Check source freshness
   ↓
Transform data
```

---

# 🔄 Airflow Orchestration

The entire pipeline is orchestrated using an Apache Airflow DAG named:

```text
orchestrate
```

The DAG defines the following execution sequence:

```text
ingest_cdc
     ↓
clean_target
     ↓
source_freshness
     ↓
silver_technical
     ↓
silver_technical_tests
     ↓
silver_business
     ↓
silver_business_tests
     ↓
gold_ephemeral
     ↓
gold_dimensions
     ↓
gold_facts
```

### Airflow Responsibilities

Airflow is responsible for:

* Triggering the Databricks CDC job
* Monitoring Databricks execution
* Cleaning dbt artifacts
* Running source freshness checks
* Running dbt models
* Running dbt tests
* Running dbt snapshots
* Managing task dependencies
* Stopping downstream processing when an upstream task fails

This separation is important because **Airflow handles orchestration while dbt handles SQL transformations**. This is a common division of responsibilities in modern data platforms.

---

# 🐳 Docker

The project includes Docker configuration for the Airflow/dbt environment:

```text
Dockerfile
docker-compose.yaml
requirements.txt
```

This allows the project environment and dependencies to be packaged consistently rather than requiring every dependency to be installed manually on the host machine.

---

# 🔍 Data Quality

The pipeline includes dbt tests after the Silver transformations.

The DAG explicitly executes:

```bash
dbt test --select silver_t
```

followed by:

```bash
dbt test --select silver_b
```

This means transformation testing is incorporated directly into the orchestrated workflow rather than being treated as a completely separate manual step.

---

# 📈 End-to-End Workflow

The complete workflow can be summarized as:

```text
                Walmart CSV Data
                       │
                       ▼
                PostgreSQL RAW
                       │
                       ▼
              Databricks CDC Job
                       │
                       ▼
              Apache Airflow
                       │
                       ▼
             dbt Source Freshness
                       │
                       ▼
             Silver Technical
                       │
                       ▼
              Silver Business
                       │
                       ▼
               Gold Ephemeral
                       │
                       ▼
              dbt Snapshots
                       │
                       ▼
                 Gold Facts
                       │
                       ▼
             Analytics / BI
```

---

# 🎯 Key Data Engineering Concepts Demonstrated

This project demonstrates several concepts relevant to modern data engineering roles:

* ETL / ELT pipelines
* Change Data Capture (CDC)
* Batch data ingestion
* Data orchestration
* Apache Airflow DAGs
* Databricks job orchestration
* dbt transformations
* Jinja templating
* Source freshness checks
* Data quality testing
* Silver/Gold data layers
* Dimensional modeling
* Fact tables
* Slowly changing historical data
* dbt snapshots
* Dockerized data environments
* PostgreSQL
* SQL transformations
* Pipeline dependency management

---

# 🧠 What I Learned

Through this project, I explored how different tools can work together rather than treating each technology as an isolated component.

### Airflow

Used for:

```text
"When should this pipeline run?"
"What happens after this task?"
"What happens if a task fails?"
```

### Databricks

Used for:

```text
"How do we process and ingest changes?"
```

### dbt

Used for:

```text
"How should the data be transformed and modeled?"
```

### PostgreSQL

Used as the relational data storage layer for the raw Walmart data.

### Docker

Used to provide a reproducible environment for the pipeline.

---

# ⚙️ Setup

## Prerequisites

Install:

* Docker Desktop
* Git
* Python
* PostgreSQL
* Databricks account/workspace
* Databricks job configured for CDC ingestion

---

## 1. Clone the Repository

```bash
git clone https://github.com/anshlambagit/Walmart_Airflow_DBT_Project.git

cd Walmart_Airflow_DBT_Project
```

---

## 2. Configure PostgreSQL

Create the required database/schema and tables using:

```text
walmart_dataset/ddl/walmart_schema.sql
```

The project defines tables for customers, stores, products, employees, orders and order items.

---

## 3. Load the Raw Data

Update the PostgreSQL connection string in:

```text
walmart_dataset/load_data.py
```

Then execute:

```bash
python load_data.py
```

The script loads the CSV files into the corresponding raw tables using PostgreSQL's `COPY` command.

---

## 4. Configure Databricks

Update the Databricks configuration used by the Airflow DAG:

```text
Databricks Host
Databricks Token
Databricks Job ID
Databricks HTTP Path
```

The dbt profile also requires Databricks connection information.

**Do not commit real credentials or access tokens to GitHub.**

Use environment variables or a secrets manager for production deployments.

---

## 5. Start the Airflow Environment

Navigate to:

```bash
cd airflow_dbt_project
```

Then start the Docker environment:

```bash
docker compose up
```

After the containers start, open the Airflow UI and trigger the DAG.

---

# ▶️ Running dbt Manually

From the dbt project directory:

```bash
cd walmart_project
```

Check the connection:

```bash
dbt debug
```

Check source freshness:

```bash
dbt source freshness
```

Run Silver Technical models:

```bash
dbt run --select silver_t
```

Test Silver Technical models:

```bash
dbt test --select silver_t
```

Run Silver Business models:

```bash
dbt run --select silver_b
```

Test Silver Business models:

```bash
dbt test --select silver_b
```

Run Gold ephemeral models:

```bash
dbt run --select gold/ephemeral
```

Run snapshots:

```bash
dbt snapshot
```

Run Gold fact models:

```bash
dbt run --select gold/fact
```

---

# 📌 Important Note

This repository is primarily a learning/portfolio implementation demonstrating how **CDC ingestion, Airflow orchestration, Databricks processing and dbt transformations** can be combined into a single data engineering workflow.

For a production implementation, additional improvements would be appropriate, including:

* Secret management
* Incremental dbt models
* More comprehensive data-quality tests
* CI/CD
* Monitoring and alerting
* Retry policies
* Idempotent ingestion
* Better environment separation
* Production-grade logging
* Data lineage
* Infrastructure as Code

---

# 🔮 Future Improvements

Possible improvements include:

### 1. Incremental Processing

Instead of processing complete datasets, process only newly changed records.

### 2. Better CDC Handling

Implement a robust CDC mechanism using:

```text
Source
  ↓
CDC
  ↓
Bronze
  ↓
Silver
  ↓
Gold
```

### 3. CI/CD

Add GitHub Actions to automatically:

```text
Pull Request
     ↓
Lint
     ↓
dbt compile
     ↓
dbt test
     ↓
Deploy
```

### 4. Monitoring

Add monitoring for:

* Pipeline failures
* Data freshness
* Row-count anomalies
* CDC failures
* dbt test failures
* Databricks job failures

### 5. BI Layer

Connect the Gold layer to a BI tool such as:

* Power BI
* Tableau
* Looker

to create dashboards for:

* Sales performance
* Store performance
* Product performance
* Customer analytics
* Order trends

---

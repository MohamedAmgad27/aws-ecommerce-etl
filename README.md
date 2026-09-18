# 🛒 AWS PySpark E-Commerce ETL Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/AWS%20Glue-Interactive%20Sessions-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" alt="AWS Glue"/>
  <img src="https://img.shields.io/badge/Amazon%20S3-Data%20Lake-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="S3"/>
  <img src="https://img.shields.io/badge/Parquet-Snappy-3776AB?style=for-the-badge" alt="Parquet"/>
  <img src="https://img.shields.io/badge/status-complete-brightgreen?style=for-the-badge" alt="Status"/>
</p>

<p align="center">
  An enterprise-style, end-to-end <b>batch ETL pipeline</b> built with <b>PySpark</b> on <b>AWS Glue</b>,
  transforming 12 raw relational e-commerce tables into a clean, analytics-ready star-schema
  data model persisted as partitioned Parquet on Amazon S3.
</p>

---

## 📖 Table of Contents

- [Overview](#-overview)
- [System Architecture](#-system-architecture)
- [Tech Stack](#-tech-stack)
- [Dataset](#-dataset)
- [Pipeline Stages](#-pipeline-stages)
  - [1. Ingestion & Profiling](#1️⃣-ingestion--profiling)
  - [2. Data Cleaning & Type Casting](#2️⃣-data-cleaning--type-casting)
  - [3. Business Transformations & Segmentation](#3️⃣-business-transformations--segmentation)
  - [4. Window Functions & Advanced Analytics](#4️⃣-window-functions--advanced-analytics)
  - [5. Joins & Aggregations](#5️⃣-joins--aggregations)
  - [6. Data Modeling & S3 Output](#6️⃣-data-modeling--s3-output)
- [Final Data Model](#-final-data-model)
- [Repository Structure](#-repository-structure)
- [Getting Started](#-getting-started)
- [Execution Guide (AWS Glue)](#-execution-guide-aws-glue)
- [Sample Outputs](#-sample-outputs)
- [Data Quality Checks](#-data-quality-checks)
- [Possible Extensions](#-possible-extensions)
- [License](#-license)

---

## 🔍 Overview

This repository implements a **12-table relational e-commerce ETL pipeline** designed to mirror a
real-world data engineering workload: raw CSV extracts land in an S3 **raw zone**, get profiled and
cleaned, pass through business logic and analytical modeling, and are finally written back to an S3
**processed zone** as **partitioned, Snappy-compressed Parquet** — ready for consumption by a BI
tool, a Glue Catalog / Athena layer, or a downstream warehouse.

The pipeline is implemented entirely in **PySpark** and executed inside an **AWS Glue Interactive
Session (Notebook)**, which provides a fully managed, serverless Spark runtime with native S3
connectivity and IAM-based access control.

**Highlights:**
- 🔄 Fully automated, idempotent pipeline (`overwrite` write mode)
- 🧹 Systematic data cleaning, standardization, and anomaly detection
- 🧮 Business-driven segmentation logic (customer tiers, order value tiers)
- 🪟 Advanced window-function analytics (rankings, latest/first events, top-N per group)
- 🔗 Multi-table joins producing a unified, query-friendly sales dataset
- 📊 Multi-grain aggregations (customer, product, category, geography, time)
- 🏗️ A dimensional `fact_sales` table + curated aggregate marts
- 📦 Partitioned Parquet output optimized for downstream analytics engines

---

## 🏛 System Architecture

```
┌───────────────────────┐        ┌────────────────────────────────────────┐        ┌───────────────────────────┐
│   Synthetic Data Gen   │        │        AWS Glue Interactive Session      │        │        Amazon S3           │
│  (Python + Faker)      │        │              (PySpark / Spark)           │        │      Processed Zone        │
│                        │        │                                          │        │                             │
│  generate_ecommerce_   │  CSV   │  1. Ingest & Profile                     │Parquet │  processed/ecommerce/      │
│  data.py               │──────► │  2. Clean & Standardize                  │+Snappy │   ├── customers/            │
│                        │        │  3. Cast Types                           │──────► │   ├── products/             │
│  12 CSVs → S3 Raw Zone │        │  4. Business Rules & Segmentation        │        │   ├── categories/           │
│  raw/ecommerce/        │        │  5. Window Functions                     │        │   ├── orders/ (year/month)  │
└───────────────────────┘        │  6. Joins                                │        │   ├── order_details/        │
                                  │  7. Aggregations                         │        │   ├── payments/             │
                                  │  8. Advanced / Anomaly Analytics         │        │   ├── shipments/            │
                                  │  9. Date Analysis                        │        │   ├── fact_sales/           │
                                  │ 10. Build fact_sales + marts             │        │   ├── customer_sales/       │
                                  │ 11. Write to S3 (Parquet, Snappy)        │        │   ├── product_sales/        │
                                  └────────────────────────────────────────┘        │   ├── category_sales/      │
                                                                                       │   └── monthly_sales/        │
                                                                                       └───────────────────────────┘
```

**Data flow in one line:**

> `CSV in S3 (raw)` → `AWS Glue + PySpark (read → clean → transform → analyze)` → `Parquet + Snappy in S3 (processed)`

---

## 🧰 Tech Stack

| Layer                  | Technology                                             |
|-------------------------|--------------------------------------------------------|
| Language                | Python 3.x                                              |
| Distributed Processing  | Apache Spark / PySpark                                 |
| Managed Spark Runtime   | AWS Glue Interactive Sessions (Glue PySpark Notebook)   |
| Storage                 | Amazon S3 (raw & processed zones)                       |
| File Format             | Apache Parquet (Snappy compression)                     |
| Synthetic Data          | Python `Faker` library                                  |
| AWS SDK                 | `boto3` (bucket/object management, role setup)          |
| IAM                      | `AWSGlueServiceRole-Ecommerce` (custom Glue service role) |

---

## 🗄 Dataset

The pipeline processes **12 relational CSV tables** representing a typical e-commerce OLTP schema:

| Table                | Approx. Rows | Description                          |
|-----------------------|:------------:|----------------------------------------|
| `departments`         | 10           | Internal company departments           |
| `employees`           | 200          | Employee records                       |
| `suppliers`           | 100          | Product suppliers                      |
| `product_suppliers`   | 2,027        | Product ↔ supplier bridge table        |
| `categories`          | —            | Product categories                     |
| `products`            | —            | Product catalog                        |
| `customers`           | —            | Customer master data                   |
| `orders`              | 50,000       | Order headers                          |
| `order_details`       | 100,000      | Order line items                       |
| `payments`            | 45,000       | Payment transactions                   |
| `shippers`            | 10           | Shipping carriers                      |
| `shipments`           | 40,000       | Shipment tracking records              |

**Raw location:** `s3://<bucket>/raw/ecommerce/`
**Processed location:** `s3://<bucket>/processed/ecommerce/`

> Data was synthetically generated via `generate_ecommerce_data.py`, using `Faker`, to simulate
> realistic — and intentionally imperfect — relational e-commerce data (nulls, duplicates,
> inconsistent casing, invalid formats) for the pipeline to detect and clean.

---

## ⚙️ Pipeline Stages

### 1️⃣ Ingestion & Profiling

Reads all 12 CSVs from the raw S3 zone into a `dict[str, DataFrame]`, then profiles each table.

- Reads all tables with `header=True, inferSchema=True`
- Prints schema (`printSchema()`) and a 10-row sample (`show(10)`) per table
- Computes row counts per table
- Computes **NULL counts per column** for `customers`
- Detects **duplicate `customerid`** and **duplicate `orderid`** values

```python
tables = ["customers", "categories", "products", "departments", "employees",
          "suppliers", "orders", "order_details", "payments",
          "product_suppliers", "shippers", "shipments"]

for table in tables:
    df = spark.read.csv(f"{raw_path}{table}.csv", header=True, inferSchema=True)
    dataframes[table] = df
```

### 2️⃣ Data Cleaning & Type Casting

- Standardizes **all column names to lowercase** across every table
- **Trims** leading/trailing whitespace from every `string`-typed column
- Lowercases customer **emails**; uppercases order **statuses**
- Drops duplicate `customerid` / `orderid` rows
- Removes rows with a **NULL primary key** (`customerid`, `orderid`, `productid`)
- Flags **data quality anomalies**:
  - Invalid email formats (regex validation)
  - Orders with a status outside `{PENDING, SHIPPED, DELIVERED, CANCELLED}`
  - Products with `NULL` prices
  - Products with price `<= 0`
- **Type casting**:
  - All `*id` columns → `LongType`
  - Prices / payment amounts → `DecimalType(12,2)`
  - Quantities → `IntegerType`
  - `orderdate` → Spark `DateType`, with `year`, `month`, `quarter`, `day`, `day_of_week` extracted

### 3️⃣ Business Transformations & Segmentation

- `order_details.total_amount = quantity × unitprice`
- `customers.full_name = concat(firstname, lastname)`
- **`order_value_category`** derived per order:

  | Order Total   | Category |
  |----------------|----------|
  | `>= 1000`      | `HIGH`   |
  | `500 – 999`    | `MEDIUM` |
  | `< 500`        | `LOW`    |

- **`customer_segment`** derived from lifetime `total_customer_sales`:

  | Total Sales        | Segment    |
  |----------------------|------------|
  | `>= 10,000`          | `VIP`      |
  | `5,000 – 9,999`      | `PREMIUM`  |
  | `1,000 – 4,999`      | `REGULAR`  |
  | `< 1,000`            | `LOW_VALUE`|

- Aggregated `order_total` and `total_order_quantity` per order (`order_summary`)
- Average product price computed globally

### 4️⃣ Window Functions & Advanced Analytics

Implemented using `pyspark.sql.window.Window`, `row_number()`, and `rank()`:

| # | Analysis                                       | Technique                                             |
|---|--------------------------------------------------|--------------------------------------------------------|
| 32| Latest order per customer                        | `partitionBy("customerid").orderBy(desc("orderdate"))` |
| 33| First order per customer                         | `partitionBy("customerid").orderBy(asc("orderdate"))`  |
| 34| Rank customers by total lifetime sales           | `orderBy(desc("total_customer_sales"))` + `rank()`     |
| 35| Top 3 products per category (by sales)           | `partitionBy("categoryid").orderBy(desc("product_sales"))` |
| 36| Most expensive product per category              | `partitionBy("categoryid").orderBy(desc("price"))`     |
| 37| Latest shipment per order                        | `partitionBy("orderid").orderBy(desc("shipdate"))`     |

**Anomaly / referential-integrity checks** via `left_anti` joins:
- Customers who never placed an order
- Products that were never ordered
- Customers with more than 10 orders
- Orders whose paid amount doesn't reconcile with the calculated order total
- Orders missing a shipment record
- Shipments with no matching order

### 5️⃣ Joins & Aggregations

**Joins:**
| Join                                      | Purpose                              |
|---------------------------------------------|----------------------------------------|
| `orders + customers`                        | Enriched order/customer view          |
| `orders + order_details + products`         | Unified **sales dataset**             |
| `products + categories`                     | Category-enriched product catalog     |
| `products + product_suppliers + suppliers`  | Supplier lineage per product          |
| `orders + payments`                          | Payment reconciliation                |
| `orders + shipments + shippers`              | Fulfillment tracking                  |

**Aggregations** (all via `groupBy().agg()`):
- Total sales, total orders, total quantity sold, average order value
- Sales by customer / product / category / country / city
- Monthly & yearly sales; sales by order status
- Orders per customer

### 6️⃣ Data Modeling & S3 Output

- Builds the central **`fact_sales`** table from the joined sales dataset
- Assembles a dictionary of **11 output datasets** (dimensions, fact table, and 4 aggregate marts)
- Writes every dataset to S3 as **Parquet with Snappy compression**, `mode="overwrite"`
- **`orders`** is additionally **partitioned by `year` and `month`** for efficient time-based querying

```python
for folder_name, df in output_datasets.items():
    output_location = f"{processed_path}{folder_name}/"
    if folder_name == "orders":
        df.write.mode("overwrite") \
            .partitionBy("year", "month") \
            .option("compression", "snappy") \
            .parquet(output_location)
    else:
        df.write.mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(output_location)
```

---

## 🌟 Final Data Model

A lightweight **star schema** centered on `fact_sales`:

```
                         orders
                            │
                            ▼
     products ────────► fact_sales ◄──────── (customerid)
        │                   │
        ▼                   │
    categories               ▼
                        order_details
```

**`fact_sales` grain:** one row per order line item, containing:

`orderid` · `customerid` · `productid` · `categoryid` · `orderdate` · `quantity` · `unitprice` · `total_amount` · `status`

**Curated aggregate marts:**
- `customer_sales` — lifetime sales per customer
- `product_sales` — total sales per product
- `category_sales` — total sales per category
- `monthly_sales` — total sales per calendar month

---

## 📂 Repository Structure

```
aws-pyspark-ecommerce-etl/
│
├── data_generation/
│   └── generate_ecommerce_data.py      # Faker-based synthetic data generator (12 CSVs)
│
├── notebooks/
│   └── Ecommerce_Notebook.ipynb        # Main AWS Glue PySpark interactive notebook
│
├── docs/
│   ├── Task28_Assignment.pdf           # Original assignment specification
│   └── Task28_Submission.pdf           # Annotated execution walkthrough & screenshots
│
├── architecture/
│   └── pipeline_diagram.png            # System architecture diagram
│
├── README.md                           # You are here
└── LICENSE
```

---

## 🚀 Getting Started

### Prerequisites

- An AWS account with permissions for **Glue**, **S3**, and **IAM**
- An S3 bucket for `raw/` and `processed/` zones
- Python 3.9+ locally (for the data generator script only)
- `boto3` and `faker` installed locally if regenerating source data:

```bash
pip install boto3 faker
```

### 1. Generate & Upload Synthetic Data

```bash
python data_generation/generate_ecommerce_data.py
```

This creates 12 CSV files locally under `ecommerce_data/` and uploads them to:

```
s3://<your-bucket>/raw/ecommerce/
```

### 2. Create the Glue IAM Service Role

Create a role named `AWSGlueServiceRole-Ecommerce` with the **AWS Service: glue** trust policy and
attach:
- `AWSGlueServiceRole` (managed policy)
- An inline/custom policy granting `s3:GetObject`, `s3:PutObject`, and `s3:ListBucket` on your
  raw and processed bucket paths

---

## 🖥 Execution Guide (AWS Glue)

1. **Open AWS Glue Studio → Notebooks** and create a new **Glue PySpark** notebook session
   attached to the `AWSGlueServiceRole-Ecommerce` role.
2. Initialize the Spark session:
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("Ecommerce_Transformation").getOrCreate()
   ```
3. Run the notebook **cell-by-cell, in order**, following the stage structure below — each stage
   builds on the `dataframes` dictionary populated by the previous one:

   | Order | Cell / Stage                        |
   |:-----:|---------------------------------------|
   | 1     | Read all 12 CSVs from S3 (`raw_path`) |
   | 2     | Data Cleaning                         |
   | 3     | Data Type Transformation              |
   | 4     | Business Transformations              |
   | 5     | Window Functions                      |
   | 6     | Joins                                 |
   | 7     | Aggregations                          |
   | 8     | Advanced Analytics                    |
   | 9     | Date Analysis                         |
   | 10–11 | Build `fact_sales` + Write to S3       |

4. Confirm successful execution — the notebook prints `ETL Pipeline Complete. All files written to S3.`
5. Verify output in the **S3 console** under `processed/ecommerce/` — you should see 11 top-level
   "folders" (each a Parquet dataset), with `orders/` further partitioned into `year=YYYY/month=M/`.
6. *(Optional)* Register the processed Parquet datasets in the **AWS Glue Data Catalog** and query
   them via **Amazon Athena** for ad-hoc analytics.

---

## 📊 Sample Outputs

| Metric                          | Example Value        |
|-----------------------------------|:---------------------:|
| Total Sales                      | `$840,507,631.75`     |
| Total Orders                     | `50,000`               |
| Total Quantity Sold               | `549,473`              |
| Average Order Value               | `$19,485.06`           |
| Average Product Price             | `$1,530.10`            |
| Top Customer (lifetime sales)      | `$368,984.90`          |
| Month with Highest Sales          | `2025-03 ($27.9M)`     |
| Day with Most Orders              | `2024-09-10 (75 orders)` |
| Avg. Orders per Month             | `1,515.15`              |

---

## ✅ Data Quality Checks

The pipeline actively surfaces (rather than silently drops) the following classes of issues, so
they can be triaged before downstream consumption:

- ❌ Invalid / malformed customer email addresses
- ❌ Orders with a non-standard status value
- ❌ Products with missing or non-positive prices
- ❌ Duplicate primary keys (`customerid`, `orderid`)
- ❌ Orders with no corresponding shipment
- ❌ Shipments with no corresponding order
- ❌ Payment totals that don't reconcile with computed order totals
- ❌ Customers with zero order history (never converted)
- ❌ Products with zero lifetime sales (never ordered)

---

## 🔮 Possible Extensions

- Convert the notebook into a scheduled **AWS Glue Job** with a **Glue Trigger / Workflow**
- Register outputs in the **Glue Data Catalog** for querying via **Athena** / **Redshift Spectrum**
- Add **Great Expectations** or **Glue Data Quality** rules for automated validation gating
- Incorporate **incremental / CDC-based ingestion** instead of full overwrite
- Visualize the marts in **QuickSight**, **Tableau**, or **Power BI**
- Add unit tests for transformation logic using `pytest` + `chispa`

---

## 📄 License

This project is released under the [MIT License](LICENSE).


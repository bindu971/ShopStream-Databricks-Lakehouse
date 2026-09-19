# ShopStream — End-to-End Databricks Lakehouse Project

An end-to-end **Batch + Streaming Data Engineering project** built using **Databricks Free Edition**, following the Medallion Architecture.

The project demonstrates batch ingestion, streaming ingestion with Auto Loader, Delta Lake transformations, Lakeflow pipelines, scheduled data processing, and sales analytics.

## 🚀 Project Overview

ShopStream simulates an e-commerce data platform where customer, product, and order data are processed through a **Bronze → Silver → Gold** architecture.

### Architecture

```text
                 ┌─────────────────────┐
                 │   Source CSV Data   │
                 │ Customers / Products│
                 │       / Orders      │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Bronze Layer      │
                 │ Raw Batch Data      │
                 │ COPY INTO           │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Silver Layer      │
                 │ Cleaning &          │
                 │ Transformation      │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │    Gold Layer       │
                 │ Business Metrics    │
                 │ Revenue & Category  │
                 └──────────┬──────────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │   Sales Dashboard   │
                 └─────────────────────┘


Live Events
     │
     ▼
JSON Event Files
     │
     ▼
Auto Loader
     │
     ▼
Streaming Bronze
     │
     ▼
Lakeflow Pipeline
     │
     ▼
Streaming Analytics
```

## 🛠️ Technologies Used

* Databricks
* Apache Spark / PySpark
* Spark SQL
* Delta Lake
* Unity Catalog
* Auto Loader
* Lakeflow Declarative Pipelines
* Databricks Workflows
* Python
* SQL

## 📂 Project Notebooks

| Notebook                          | Description                                                |
| --------------------------------- | ---------------------------------------------------------- |
| `01_bronze_batch_ingestion.ipynb` | Ingests order data into the Bronze layer using `COPY INTO` |
| `02_silver_layer.ipynb`           | Cleans and transforms Bronze data into the Silver layer    |
| `03_gold_layer.ipynb`             | Creates business-level Gold tables for analytics           |
| `04_stream_events.ipynb`          | Generates simulated live order events as JSON files        |
| `05_streaming_bronze.ipynb`       | Uses Databricks Auto Loader to ingest streaming events     |

## 🗄️ Data Architecture

The project uses Unity Catalog with the following structure:

```text
shopstream
└── core
    ├── bronze_orders
    ├── bronze_orders_stream
    ├── gold_category_performance
    └── gold_daily_revenue
```

Volumes are used for raw and streaming event data:

```text
/Volumes/shopstream/core/raw
/Volumes/shopstream/core/events
```

## 📦 Seed Data

The project uses three initial datasets:

* **Customers:** 1,000 records
* **Products:** 197 records
* **Orders:** 13,717 order lines

The seed data contains intentionally introduced data-quality issues such as duplicates, invalid quantities, and inconsistent status casing to demonstrate data-cleaning and transformation workflows.

## 🔄 Batch Pipeline

The batch pipeline follows:

```text
CSV Files
   ↓
COPY INTO
   ↓
Bronze
   ↓
Data Cleaning & Transformation
   ↓
Silver
   ↓
Business Aggregations
   ↓
Gold
   ↓
Dashboard
```

The Gold layer includes:

### Gold Category Performance

Provides category-level sales performance and revenue metrics.

### Gold Daily Revenue

Provides daily sales and revenue metrics for dashboard reporting.

## ⚡ Streaming Pipeline

The streaming component simulates real-time order events.

`04_stream_events.ipynb` generates:

* 60 JSON event files
* 25 events per file
* Approximately 1,500 events
* Events generated every 5 seconds

The events are written to:

```text
/Volumes/shopstream/core/events/orders_stream
```

The `05_streaming_bronze.ipynb` notebook uses **Databricks Auto Loader** to continuously discover and ingest new JSON files into the streaming Bronze table.

## 🔥 Auto Loader

The streaming ingestion uses:

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json")
```

Auto Loader provides incremental file discovery and streaming ingestion without manually tracking newly arrived files.

A checkpoint is used to maintain streaming progress and support reliable processing.

## 🔁 Lakeflow Pipeline

The project also demonstrates a Lakeflow Declarative Pipeline for processing streaming data.

The pipeline includes transformations for:

* Streaming Bronze data
* Daily revenue calculations
* Event-level processing

The pipeline uses the `shopstream` catalog and `core` schema.

## ⏰ Scheduled Databricks Job

A Databricks Workflow is configured to execute the batch pipeline in dependency order:

```text
bronze_ingestion
       ↓
silver_layer
       ↓
gold_layer
```

This demonstrates how dependent data-engineering tasks can be orchestrated using Databricks Jobs.

## 📊 Analytics Dashboard

The Gold-layer tables are used to build the **ShopStream Sales Dashboard**.

The dashboard provides visibility into:

* Daily revenue
* Sales performance
* Product categories
* Completed transactions
* Order/event metrics

## 📁 Repository Structure

```text
ShopStream-Databricks-Lakehouse/
│
├── 01_bronze_batch_ingestion.ipynb
├── 02_silver_layer.ipynb
├── 03_gold_layer.ipynb
├── 04_stream_events.ipynb
├── 05_streaming_bronze.ipynb
└── README.md
```

## 🎯 Key Data Engineering Concepts Demonstrated

* Medallion Architecture
* Batch ETL
* Streaming ETL
* Data ingestion
* Data quality handling
* Delta Lake
* Unity Catalog
* Auto Loader
* Checkpointing
* Incremental processing
* Lakeflow Declarative Pipelines
* Databricks Workflows
* Data aggregation
* Analytics dashboards

## 📚 Project Resources

This project was completed as part of the **DataVidhya Data Engineering course**.

* [ShopStream project resources](https://datavidhya.com/learn/projects/shopstream-lakehouse-databricks/)
* [DataVidhya Data Engineering Course Resources](https://github.com/darshilparmar/DataVidhya-Data-Engineering-Course-Resources)

The repository contains my implementation and notebooks created while working through the project.

## 👩‍💻 Author

**Himabindu Velpula**

GitHub: [bindu971](https://github.com/bindu971)

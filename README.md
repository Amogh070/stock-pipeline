# Real-Time Stock Market Data Pipeline

## Overview

This project is an end-to-end real-time data engineering pipeline that ingests stock market data, processes streaming events using Apache Spark Structured Streaming, loads curated data into Snowflake, transforms data using dbt, and orchestrates workflows using Apache Airflow.

The project demonstrates modern data engineering practices including event-driven architecture, stream processing, cloud data warehousing, data modeling, and workflow orchestration.

---

## Architecture

```text
Stock API
    │
    ▼
Kafka Producer
    │
    ▼
Apache Kafka
    │
    ▼
Spark Structured Streaming
    │
    ▼
Snowflake (Raw Layer)
    │
    ▼
dbt Models
    │
    ▼
Analytics Layer
    │
    ▼
Airflow Orchestration
```

---

## Tech Stack

| Category          | Technology                        |
| ----------------- | --------------------------------- |
| Programming       | Python                            |
| Messaging         | Apache Kafka                      |
| Stream Processing | Apache Spark Structured Streaming |
| Data Warehouse    | Snowflake                         |
| Transformation    | dbt                               |
| Orchestration     | Apache Airflow                    |
| Containerization  | Docker                            |
| Version Control   | Git & GitHub                      |

---

## Project Objectives

* Build a real-time stock market data pipeline.
* Process streaming events with low latency.
* Store historical stock data in Snowflake.
* Create analytics-ready models using dbt.
* Automate pipeline execution using Airflow.
* Demonstrate production-style data engineering workflows.

---

## Data Flow

### 1. Data Ingestion

A Kafka Producer continuously fetches stock market data and publishes events to Kafka topics.

### 2. Stream Processing

Spark Structured Streaming consumes Kafka messages and performs:

* Schema validation
* Type casting
* Data cleansing
* Timestamp standardization

### 3. Data Storage

Processed records are loaded into Snowflake tables.

Example fields:

* Symbol
* Price
* Volume
* Event Timestamp
* Processing Timestamp

### 4. Data Transformation

dbt transforms raw data into analytics-ready models.

Layers:

#### Staging

* Data cleaning
* Column standardization

#### Intermediate

* Business logic
* Calculated metrics

#### Mart Layer

* Daily stock summaries
* Aggregated reporting tables

### 5. Orchestration

Apache Airflow schedules and monitors:

* Streaming jobs
* dbt transformations
* Data quality checks

---

## Repository Structure

```text
stock-pipeline/
│
├── airflow/
│   └── dags/
│
├── producers/
│   └── stock_producer.py
│
├── jobs/
│   └── stream_stocks.py
│
├── dbt_project/
│   └── stock_pipeline/
│
├── docker/
│
├── jars/
│
├── docker-compose.yml
│
└── README.md
```

---

## Key Features

### Real-Time Processing

Processes stock events as they arrive through Kafka.

### Scalable Architecture

Uses distributed processing with Apache Spark.

### Modern ELT Design

Raw ingestion followed by transformation using dbt.

### Workflow Automation

Airflow manages scheduling and execution.

### Analytics Ready

Produces curated datasets for dashboards and reporting.

---

## Example Analytics Questions

The pipeline can answer questions such as:

* What are the daily average stock prices?
* Which stocks had the highest trading volume?
* What are the daily high and low prices?
* How do stock prices trend over time?
* Which stocks show the highest volatility?

---

## Local Setup

### Clone Repository

```bash
git clone https://github.com/Amogh070/stock-pipeline.git
cd stock-pipeline
```

### Start Services

```bash
docker compose up -d
```

### Run Kafka Producer

```bash
python producers/stock_producer.py
```

### Run Spark Streaming Job

```bash
spark-submit jobs/stream_stocks.py
```

### Execute dbt Models

```bash
dbt run
```

### Start Airflow

```bash
airflow standalone
```

---

## Skills Demonstrated

* Data Modeling
* Stream Processing
* ETL / ELT Design
* Workflow Orchestration
* Distributed Computing
* Cloud Data Warehousing
* Data Quality Practices
* Containerized Development

---

## Future Enhancements

* CI/CD using GitHub Actions
* Data Quality Testing with Great Expectations
* Monitoring with Prometheus and Grafana
* Real-Time Dashboards using Streamlit
* Data Lake Integration using S3/GCS
* Kubernetes Deployment
* Change Data Capture (CDC)

---

## Author

**Amogha Mahadev**

Data Engineer

Tech Stack: Python, SQL, Spark, Airflow, Snowflake, dbt, Kafka

GitHub: https://github.com/Amogh070

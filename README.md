# Food-Intel — Data Engineering & AI Analytics Platform on AWS

A production-style data engineering project that takes a food delivery dataset from raw CSV files all the way to an AI-powered analytics platform. It covers every layer a real data team works with: ingestion, transformation, orchestration, and an AI layer on top.

---

## What This Project Does

Raw CSV files (orders, reviews, restaurants, users) are uploaded to AWS S3. From there, Snowflake loads them into a raw schema. dbt transforms the data through staging and mart layers. Apache Airflow orchestrates the full pipeline daily. An AI layer uses a Groq LLM to classify customer reviews and power a RAG chat interface.

The pipeline handles 10M+ orders, 23M+ order line items, and 300K+ customer reviews.

---

## Architecture

![Architecture Diagram](Doc/Architecture.png)

```
CSV Files  ->  AWS S3  ->  Snowflake RAW  ->  dbt (Staging -> Marts)  ->  AI Layer
                                    |
                             Airflow orchestrates everything daily
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud Storage | AWS S3 |
| Data Warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake 1.8.x) |
| Orchestration | Apache Airflow 3.0 on Docker |
| AI / LLM | Groq API |
| Applications | Streamlit |
| Language | Python 3.12 |

---

## Dataset

| Table | Description | Size |
|---|---|---|
| orders | Order transactions — status, amounts, delivery time | 10M rows |
| order_items | Line items per order | 23M rows |
| reviews | Customer comments + ratings | 300K rows |
| restaurants | Name, city, cuisine, rating | ~149K rows |
| users | Customer demographics | Dimension |
| food | Menu items, veg/non-veg flag | Dimension |
| menu | Restaurant x food x price | Dimension |

The raw CSV files are not included in this repo (2.3 GB total). Download them separately and place under `Data/`.

---

## Project Structure

```
├── Airflow/
│   ├── dags/
│   │   └── zomato_batch.py       # The main Airflow DAG
│   ├── Dockerfile                 # Airflow image with dbt + Groq installed
│   ├── docker-compose.yml         # Spins up 5 containers
│   └── .env                       # Airflow environment variables
│
├── zomato/                        # dbt project
│   ├── models/
│   │   ├── staging/               # 7 staging views (Silver layer)
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_reviews.sql
│   │   │   └── ...
│   │   └── marts/                 # Gold layer
│   │       ├── dim_customer.sql
│   │       ├── dim_restaurants.sql
│   │       ├── fct_orders.sql
│   │       ├── fact_order_items.sql
│   │       ├── mart_daily_city_revenue.sql
│   │       ├── mart_delivery_sla.sql
│   │       ├── mart_restaurant_performance.sql
│   │       └── mart_review_insights.sql
│   └── dbt_project.yml
│
├── ai/
│   ├── enrich_reviews.py          # Classifies reviews using Groq LLM
│   ├── Rag_Chat.py                # RAG chatbot (Streamlit)
│   ├── text_sql.py                # Text-to-SQL app (Streamlit)
│   └── .env
│
├── Snowflake/
│   ├── 01_setup.sql               # Warehouse, database, schemas, roles
│   ├── 02_storage_integration.sql # S3 -> Snowflake connection
│   ├── 03_stage_and_formats.sql   # External stage + CSV format
│   ├── 04_raw_table.sql           # RAW table definitions
│   └── 05_copy_into.sql           # Load data from S3
│
├── scripts/
│   └── upload_to_s3.py            # Auto-uploads local CSVs to S3
│
├── AWS/IAM/                       # IAM policies for Snowflake-S3 access
├── Data/                          # Raw CSVs (not committed)
└── Doc/                           # Architecture and data model diagrams
```

---

## Key Features

### Medallion Architecture

Data flows through three layers in Snowflake:

- **RAW** — All columns loaded as text, tolerant of source messiness
- **STAGING** — Typed, cleaned, and deduplicated views built with dbt
- **MARTS** — Business-ready dimension tables, fact tables, and aggregates

### Airflow DAG

The `zomato_batch` DAG runs daily and has four tasks in sequence:

```
reload_raw  ->  dbt_build_core  ->  enrich_reviews  ->  dbt_build_ai
```

| Task | What it does |
|---|---|
| reload_raw | Runs COPY INTO for all 7 raw tables from S3 |
| dbt_build_core | Builds staging models and core marts |
| enrich_reviews | Calls Groq LLM to classify unprocessed reviews |
| dbt_build_ai | Builds the review insights mart from enriched data |

### AI Layer

`enrich_reviews.py` sends each review to a Groq LLM and stores the result back in Snowflake under `ZOMATO.AI.REVIEW_ENRICHED`:

- **sentiment_label** — positive, negative, or neutral
- **sentiment_score** — float between -1.0 and 1.0
- **topic** — food quality, delivery, pricing, service, packaging, or other
- **key_issue** — a short phrase summarising the main complaint if there is one

### Streamlit Apps

- **RAG Chat** (`Rag_Chat.py`) — Ask plain English questions about customer reviews. Uses local embeddings to find relevant reviews and Groq to generate answers.
- **Text-to-SQL** (`text_sql.py`) — Type a question in English, get a live SQL result from Snowflake.

---

## Setup Guide

### Prerequisites

- AWS account with S3 access
- Snowflake account (free trial works fine)
- Docker Desktop running
- Python 3.12+
- Groq API key — free at [console.groq.com](https://console.groq.com)

---

### Step 1 — Clone the repo

```bash
git clone https://github.com/Santhoshkumar-123/Food-Intel-Data-Engineering-AI-Analytics-Platform-on-AWS
```

---

### Step 2 — Configure environment variables

**Root `.env`:**
```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=ap-south-1
S3_BUCKET=food-intel-datalake
```

**`Airflow/.env`:**
```
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
GROQ_API_KEY=<Paste your API key here>
```

**`ai/.env`:**
```
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=ZOMATO_WH
SNOWFLAKE_DATABASE=ZOMATO
SNOWFLAKE_SCHEMA=AI
GROQ_API_KEY=<Paste your API key here>
```

---

### Step 3 — Upload data to S3

Manually create a bucket on AWS S3 rest of the files and folders are automatically created through python script
```bash
cd scripts
pip install -r requirements.txt
python upload_to_s3.py
```
### Step 3.1 — Create a policy refer AWS/IAM in the folder
    Create a policy as s3-read-policy and set adminstration access to it.

    Create a user snowflake-role-trust-policy and attach s3-read-policy to it
---

### Step 4 — Set up Snowflake

Run each SQL file in `Snowflake/` in order inside Snowsight (the Snowflake UI).

After running `02_storage_integration.sql`, copy the `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` values into your AWS IAM role trust policy. This is what gives Snowflake permission to read from S3 without using access keys.

---

### Step 5 — Set up dbt

```bash
cd zomato
pip install dbt-snowflake
dbt debug       # verify the connection
dbt build       # run all models
```

---

### Step 6 — Start Airflow

```bash
cd Airflow
docker-compose build
docker-compose up -d
```

Open `http://localhost:8081` in your browser and log in with `admin / admin`. Find the `zomato_batch` DAG, toggle it on, and trigger a run.

---

### Step 7 — Run the AI enrichment (manual)

```bash
cd ai
pip install groq snowflake-connector-python python-dotenv
python enrich_reviews.py
```

---

### Step 8 — Launch the Streamlit apps

```bash
cd ai
pip install streamlit fastembed

streamlit run Rag_Chat.py    # RAG chat
streamlit run text_sql.py    # Text-to-SQL
```

---

## dbt Commands

```bash
dbt build                                     # build and test everything
dbt build --select staging                    # staging only
dbt build --select tag:ai                     # AI models only
dbt build --full-refresh                      # rebuild incremental models from scratch
dbt test                                      # run tests only
dbt docs generate && dbt docs serve           # local documentation site
```

---

## Security Notes

- Snowflake connects to S3 via a storage integration and IAM role — no AWS keys stored in Snowflake.
- dbt and Airflow run as `DBT_ROLE`, not as `ACCOUNTADMIN`.
- The text-to-SQL app only allows SELECT statements.
- All credentials are in `.env` files which are excluded from Git via `.gitignore`.

---

## Skills Learned while doing this project

AWS S3, Snowflake, dbt, Apache Airflow, Docker, Python, SQL, Medallion Architecture, Incremental Models, SCD2 Snapshots, LLM Integration, RAG, Text-to-SQL, Data Quality Testing, ELT Pipelines

---

## Screenshots

See the `Snapshots/` folder for screenshots of dbt docs, the Airflow UI, Snowflake data previews, and the Streamlit apps.

---

## Connect me on LinkedIn

'https://www.linkedin.com/in/santhosh01kumar/'


---



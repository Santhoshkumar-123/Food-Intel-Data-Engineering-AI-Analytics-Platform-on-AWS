# Food-Intel - Data Engineering & AI Analytics Platform on AWS

> **From raw CSVs to an AI-powered data warehouse** — A complete end-to-end data engineering pipeline built on AWS, Snowflake, dbt, Airflow, and OpenAI.

---

## 📊 Project Overview

Food-Intel is a production-grade data engineering platform that demonstrates the complete modern data stack. It ingests food delivery data (10M+ orders, 23M+ line items, 300K+ reviews) from CSV files into Snowflake via AWS S3, transforms them through medallion architecture layers using dbt, orchestrates everything with Apache Airflow 3 on Docker, and adds an AI layer powered by LLMs.

**What makes it unique:**
- Real-world **medallion architecture** (Bronze → Silver → Gold)
- **AI enrichment layer** — LLM-powered sentiment analysis, topic classification, and key issue extraction
- **RAG-powered chat** — Ask questions about customer reviews in natural language
- **Text-to-SQL interface** — Convert English questions into live Snowflake queries
- **Production patterns** — Incremental models, SCD2 snapshots, orchestration, testing

---

## 🏗️ Architecture

![Architecture Diagram](Doc/Architecture.png)

```
CSV Files → AWS S3 → Snowflake (RAW) → dbt (STAGING → MARTS) → AI Layer → Analytics
                                ↑
                           Airflow orchestration
```

### Data Flow

1. **Ingestion** — Python script uploads raw CSVs to S3
2. **Loading** — Snowflake COPY INTO from S3 external stage
3. **Transformation** — dbt builds staging views and mart tables
4. **AI Enrichment** — Python script calls Groq LLM to classify reviews
5. **Orchestration** — Airflow DAG runs the full pipeline daily
6. **Analytics** — Streamlit apps query the data warehouse

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| **Cloud Storage** | AWS S3 (ap-south-1) |
| **Data Warehouse** | Snowflake |
| **Transformation** | dbt (dbt-snowflake 1.8.x) |
| **Orchestration** | Apache Airflow 3.0.3 (Docker) |
| **AI/LLM** | Groq API (llama models) |
| **Applications** | Streamlit |
| **Language** | Python 3.12 |
| **Containerization** | Docker + Docker Compose |

---

## 📁 Dataset

| Table | Records | Description |
|-------|---------|-------------|
| **orders** | 10M | Order transactions with status, amounts, timestamps |
| **order_items** | 23M | Line items per order |
| **reviews** | 300K | Customer reviews with free-text comments |
| **restaurants** | ~149K | Restaurant details, cuisine, ratings |
| **users** | Dimension | Customer demographics |
| **food** | Dimension | Menu items (veg/non-veg) |
| **menu** | Dimension | Restaurant × food × price mapping |

---

## 📂 Project Structure

```
Food-Intel - Data Engineering and AI-Analytics Platform on AWS/
├── Airflow/
│   ├── dags/
│   │   └── zomato_batch.py          # Main DAG (4 tasks)
│   ├── Dockerfile                    # Airflow 3 + dbt + Groq
│   ├── docker-compose.yml            # Airflow stack (5 services)
│   └── .env                          # Airflow environment config
│
├── zomato/                           # dbt project
│   ├── models/
│   │   ├── staging/                  # Silver layer (7 staging views)
│   │   ├── marts/                    # Gold layer (dims + facts + aggregates)
│   │   └── snapshots/                # SCD2 snapshot (restaurant ratings)
│   ├── dbt_project.yml
│   └── profiles.yml                  # dbt Snowflake connection
│
├── ai/
│   ├── enrich_reviews.py             # LLM enrichment script
│   ├── Rag_Chat.py                   # RAG chatbot (Streamlit)
│   ├── text_sql.py                   # Text-to-SQL interface
│   └── .env                          # AI layer credentials
│
├── Snowflake/                        # SQL setup scripts
│   ├── 01_setup.sql                  # Warehouse, database, roles
│   ├── 02_storage_integration.sql    # S3 → Snowflake link
│   ├── 03_stage_and_formats.sql      # External stage + CSV format
│   ├── 04_raw_table.sql              # RAW schema definitions
│   └── 05_copy_into.sql              # Load data from S3
│
├── AWS/IAM/                          # IAM policies for Snowflake
│
├── scripts/
│   ├── upload_to_s3.py               # Automated S3 upload
│   └── requirements.txt
│
├── Data/                             # Raw CSV files (not in repo)
│   ├── orders.csv
│   ├── order_items.csv
│   ├── reviews.csv
│   └── ...
│
├── Doc/
│   ├── Architecture.png              # Architecture diagram
│   └── Data Model.png
│
├── Snapshots/                        # Project screenshots
├── .env                              # Root environment config
└── README.md
```

---

## 🎯 Features

### 1. **Medallion Architecture**

- **RAW (Bronze)** — All columns as TEXT, tolerant ingestion from S3
- **STAGING (Silver)** — Typed, cleaned, deduplicated views
- **MARTS (Gold)** — Business-ready dimensions, facts, and aggregates

### 2. **dbt Transformation Layer**

**Staging models:**
- `stg_restaurants`, `stg_users`, `stg_food`, `stg_menu`
- `stg_orders`, `stg_order_items`, `stg_reviews`

**Mart models:**
- **Dimensions:** `dim_restaurants`, `dim_users`, `dim_food`, `dim_date`
- **Facts:** `fct_orders` (incremental), `fct_order_items`
- **Aggregates:** 
  - `mart_revenue_by_city`
  - `mart_top_restaurants`
  - `mart_user_cohorts`
  - `mart_delivery_performance`
  - `mart_review_insights` (AI-powered)

**Snapshots:**
- `snap_restaurant_ratings` (SCD Type 2)

### 3. **AI-Powered Enrichment**

`enrich_reviews.py` classifies every review using Groq LLM:
- **sentiment_label**: positive / negative / neutral
- **sentiment_score**: -1.0 to 1.0
- **topic**: food quality, delivery, pricing, service, packaging, other
- **key_issue**: 6-word summary of main complaint (if any)

Results stored in `ZOMATO.AI.REVIEW_ENRICHED`.

### 4. **Airflow Orchestration**

**DAG:** `zomato_batch` (runs daily)

```
reload_raw → dbt_build_core → enrich_reviews → dbt_build_ai
```

| Task | Description |
|------|-------------|
| `reload_raw` | COPY INTO all 7 RAW tables from S3 |
| `dbt_build_core` | Build staging + marts (excluding AI tag) |
| `enrich_reviews` | Call Groq API to classify unprocessed reviews |
| `dbt_build_ai` | Build AI marts using enriched data |

### 5. **Streamlit Applications**

- **RAG Chat** (`Rag_Chat.py`) — Ask questions, get answers from real reviews
- **Text-to-SQL** (`text_sql.py`) — Natural language → Snowflake queries

---

## 🚀 Setup Guide

### Prerequisites

- **AWS Account** with S3 access
- **Snowflake Account** (free trial works)
- **Docker Desktop** installed and running
- **Python 3.12+**
- **Groq API Key** (free tier at [console.groq.com](https://console.groq.com))

---

### Step 1 — Clone the Repository

```bash
git clone <your-repo-url>
cd "Food-Intel - Data Engineering and AI-Analytics Platform on AWS"
```

---

### Step 2 — Set Up Environment Variables

Create `.env` files in the required locations:

**Root `.env`:**
```bash
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=ap-south-1
S3_BUCKET=food-intel-datalake

SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
```

**Airflow/.env:**
```bash
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
GROQ_API_KEY=gsk_...
```

**ai/.env:**
```bash
SNOWFLAKE_ACCOUNT=your_account.region
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=ZOMATO_WH
SNOWFLAKE_DATABASE=ZOMATO
SNOWFLAKE_SCHEMA=AI
GROQ_API_KEY=gsk_...
```

---

### Step 3 — Set Up AWS S3

1. Create an S3 bucket (e.g., `food-intel-datalake`)
2. Upload CSV files to `s3://food-intel-datalake/raw/<table>/`

**Automated upload:**
```bash
cd scripts
pip install -r requirements.txt
python upload_to_s3.py
```

---

### Step 4 — Set Up Snowflake

Run these SQL scripts **in order** in Snowsight:

```sql
-- Run each script in the Snowflake/ folder
01_setup.sql                  -- Create warehouse, database, schemas, roles
02_storage_integration.sql    -- Link Snowflake to S3 (copy ARN values to AWS IAM)
03_stage_and_formats.sql      -- Create external stage
04_raw_table.sql              -- Create RAW tables
05_copy_into.sql              -- Load data from S3
```

**Important:** After running `02_storage_integration.sql`, copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` into your AWS IAM role trust policy.

---

### Step 5 — Set Up dbt

```bash
cd zomato
pip install dbt-snowflake
dbt debug          # Test connection
dbt build          # Run all models
```

---

### Step 6 — Start Airflow

```bash
cd Airflow
docker-compose build
docker-compose up -d
```

**Access Airflow UI:** `http://localhost:8081`  
**Login:** `admin` / `admin`

**Trigger the DAG:** Find `zomato_batch` and click the ▶️ trigger button.

---

### Step 7 — Run AI Enrichment (Optional Manual Run)

```bash
cd ai
pip install groq snowflake-connector-python python-dotenv
python enrich_reviews.py
```

---

### Step 8 — Launch Streamlit Apps

```bash
cd ai
pip install streamlit sentence-transformers fastembed

# RAG Chat
streamlit run Rag_Chat.py

# Text-to-SQL
streamlit run text_sql.py
```

---

## 🧪 dbt Commands

```bash
# Build everything
dbt build

# Build only staging
dbt build --select staging

# Build AI models
dbt build --select tag:ai

# Full refresh
dbt build --full-refresh

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

---

## 📊 Key Metrics

- **Orders processed:** 10M+
- **Line items:** 23M+
- **Customer reviews:** 300K+
- **Restaurants:** ~149K
- **Pipeline runtime:** ~12 minutes (full refresh)
- **AI enrichment rate:** ~100 reviews/minute

---

## 🔐 Security Best Practices

✅ **No hardcoded credentials** — All secrets in `.env` files  
✅ **IAM role-based access** — Snowflake → S3 via storage integration (no keys)  
✅ **Least privilege roles** — dbt runs as `DBT_ROLE`, not `ACCOUNTADMIN`  
✅ **SELECT-only SQL** — Text-to-SQL interface enforces read-only access  
✅ **`.env` in `.gitignore`** — Secrets never committed to Git  

---

## 🎓 Skills Demonstrated

`Data Engineering` · `ETL/ELT` · `Medallion Architecture` · `AWS S3` · `Snowflake` · `dbt` · `Apache Airflow` · `Docker` · `Python` · `SQL` · `LLM Integration` · `RAG` · `Text-to-SQL` · `Incremental Models` · `SCD2 Snapshots` · `CI/CD for Data` · `Data Quality Testing`

---

## 📸 Screenshots

Check the `Snapshots/` folder for:
- dbt documentation
- Airflow DAG execution
- Snowflake data preview
- Streamlit apps

---

## 🤝 Contributing

This is a portfolio project. Feel free to fork and adapt for your own learning.

---

## 📄 License

MIT License

---

## 📧 Contact

**Santhosh Kumar**  
[LinkedIn](#) | [GitHub](#) | [Portfolio](#)

---

**Built with** ❤️ **as a comprehensive data engineering portfolio project**

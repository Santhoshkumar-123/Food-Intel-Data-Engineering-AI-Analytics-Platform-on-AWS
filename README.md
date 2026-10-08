# Food-Intel - Data Engineering & AI Analytics Platform

> **From raw CSVs to an AI-powered warehouse** — a complete modern data stack built on a food-delivery dataset.

## Overview

Food-Intel is a production-style data engineering project that takes a food-delivery dataset from raw CSVs all the way to an AI-powered analytics platform - covering every layer a real data team works with.

The pipeline ingests 10M orders, 23M line items, and 300K customer reviews into Snowflake through AWS S3, transforms them across Bronze, Silver, and Gold layers using dbt, and orchestrates the whole flow daily with Apache Airflow running on Docker.

What makes it different - an AI layer built on top of the warehouse. GPT-4o-mini classifies every customer review into sentiment, topic, and urgency. A RAG pipeline lets you ask natural language questions answered from real reviews. A text-to-SQL interface converts plain English into live Snowflake queries.

The output is three Streamlit apps: a BI dashboard covering revenue, cancellations, delivery SLA, and cohort retention — a RAG chat app grounded in actual review data — and a text-to-SQL app with SELECT-only guardrails so it is safe to demo.

# Architecture Diagram

![Architecture](Doc/Architecture.png)
 
---

## Tech Stack

## Step-by-step Setup Guide

# 1.Ingestion of raw data to AWS S3 Bucket 

## 1.1 Create a s3 bucket name it as food-intel-datalake
There are 2 ways to ingest the raw data to s3 
1.Create a floders in AWS s3 console and manually upload the CSV file to the s3 buckets

2.Write python script which automatically uploads the raw CSV files from local to S3 by accessing the AWS Access_key credentials from .env file.

```bash
"""
upload_to_s3.py

Uploads the raw CSV files to S3 under the raw/<table>/ prefix layout
that the Snowflake external stage expects. Creates the bucket if it
does not already exist.

Usage:
    python scripts/upload_to_s3.py

Dependencies:
    pip install boto3 python-dotenv

Required env vars (set these in .env):
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_BUCKET
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION    = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
S3_BUCKET             = os.getenv("S3_BUCKET", "food-intel-datalake")

# Root of the local data folder (one level above scripts/)
DATA_DIR = Path(__file__).parent.parent / "Data"

# Maps local filename (no extension) to the S3 prefix under raw/
# Keeping restaurant -> restaurants to match the Snowflake stage path
FILE_MAP = {
    "food"        : "food",
    "menu"        : "menu",
    "order_items" : "order_items",
    "orders"      : "order",
    "restaurant"  : "restaurant",
    "reviews"     : "reviews",
    "users"       : "users",
}


def check_env():
    missing = [v for v in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET")
               if not os.getenv(v)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        print("        Copy .env.example to .env and fill in the values.")
        sys.exit(1)

    if not DATA_DIR.exists():
        print(f"[ERROR] Data folder not found: {DATA_DIR}")
        sys.exit(1)


def bucket_exists(client, bucket):
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            return False
        if code == "403":
            print(f"[ERROR] Bucket '{bucket}' exists but access is denied. Check IAM.")
            sys.exit(1)
        raise


def create_bucket(client, bucket, region):
    try:
        if region == "us-east-1":
            client.create_bucket(Bucket=bucket)
        else:
            client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"  Bucket created: s3://{bucket} ({region})")
    except ClientError as e:
        print(f"[ERROR] Could not create bucket: {e}")
        print("        Make sure your IAM user has the s3:CreateBucket permission.")
        sys.exit(1)


def file_size_mb(path):
    return path.stat().st_size / (1024 * 1024)


def upload_with_progress(client, local_path, s3_key, bucket):
    """Uploads a file and prints a simple inline progress bar."""
    total = local_path.stat().st_size
    done  = [0]

    def callback(n):
        done[0] += n
        pct    = done[0] / total * 100
        blocks = int(pct / 5)
        bar    = "#" * blocks + "-" * (20 - blocks)
        print(f"\r    [{bar}] {pct:5.1f}%  {done[0]/1024/1024:.1f} MB", end="", flush=True)

    try:
        client.upload_file(str(local_path), bucket, s3_key, Callback=callback)
        print()
        return True
    except ClientError as e:
        print(f"\n    [ERROR] {e}")
        return False


def already_exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def main():
    check_env()

    print("-" * 55)
    print("  Food-Intel  |  Upload raw CSVs to S3")
    print("-" * 55)
    print(f"  bucket : s3://{S3_BUCKET}")
    print(f"  region : {AWS_DEFAULT_REGION}")
    print(f"  source : {DATA_DIR}")
    print("-" * 55)

    client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )

    if bucket_exists(client, S3_BUCKET):
        print(f"\n  Bucket found: s3://{S3_BUCKET}")
    else:
        print(f"\n  Bucket not found. Creating s3://{S3_BUCKET} ...")
        create_bucket(client, S3_BUCKET, AWS_DEFAULT_REGION)

    uploaded = []
    skipped  = []
    failed   = []
    missing  = []

    print()
    for name, prefix in FILE_MAP.items():
        local = DATA_DIR / f"{name}.csv"
        key   = f"raw/{prefix}/{name}.csv"

        print(f"  {name}.csv  ->  s3://{S3_BUCKET}/{key}")

        if not local.exists():
            print(f"    not found locally, skipping\n")
            missing.append(name)
            continue

        print(f"    size: {file_size_mb(local):.1f} MB")

        if already_exists(client, S3_BUCKET, key):
            print(f"    already in S3, skipping\n")
            skipped.append(name)
            continue

        if upload_with_progress(client, local, key, S3_BUCKET):
            print(f"    done\n")
            uploaded.append(name)
        else:
            failed.append(name)
            print()

    print("-" * 55)
    print("  Results")
    print("-" * 55)
    print(f"  uploaded : {len(uploaded)}  {uploaded}")
    print(f"  skipped  : {len(skipped)}  {skipped}")
    print(f"  missing  : {len(missing)}  {missing}")
    print(f"  failed   : {len(failed)}  {failed}")
    print("-" * 55)

    print(f"\n  s3://{S3_BUCKET}/raw/")
    for name, prefix in FILE_MAP.items():
        status = "ok" if (name in uploaded or name in skipped) else "not uploaded"
        print(f"    {prefix}/{name}.csv  [{status}]")

    if failed:
        print("\n  Some files failed to upload. See errors above.")
        sys.exit(1)

    print("\n  All files in S3. Ready to run COPY INTO in Snowflake.\n")


if __name__ == "__main__":
    main()
```


## .env  file template
```bash

# AWS Credentials
AWS_ACCESS_KEY_ID=<Paste_your_access_key>
AWS_SECRET_ACCESS_KEY=<paste_your_secret_key>
AWS_DEFAULT_REGION=ap-south-1

# S3
S3_BUCKET=<bucket_name> # food-intel-datalake

# Snowflake
SNOWFLAKE_ACCOUNT=your_account.ap-south-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=ZOMATO
SNOWFLAKE_WAREHOUSE=FOOD_INTEL_WH
SNOWFLAKE_ROLE=DBT_ROLE

# OpenAI
OPENAI_API_KEY=sk-your-key-here

# AI Enrichment
SAMPLE_N=500

```

# Step 2 Create Snowflake account 

1.Store the username and pssowrd in .env for future requirement
2. Create data warehouse on the snowflake and also in locally
3. Create data base and req schema
4. Grant all access to the dbt to snowflake
5. Create policy for snowflake to access s3, policy to read the s3 bucket
6. Create a role
5. refer the code below

``` bash
-- =====================================================================
-- Phase 2 · Step 1 — Warehouse, database, schemas, role
-- Run in Snowsight as ACCOUNTADMIN (a worksheet).
-- =====================================================================
USE ROLE ACCOUNTADMIN;

-- Compute: extra-small, auto-suspend fast so the trial credits last.
CREATE WAREHOUSE IF NOT EXISTS ZOMATO_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Database + medallion schemas.
CREATE DATABASE IF NOT EXISTS ZOMATO;
CREATE SCHEMA  IF NOT EXISTS ZOMATO.BRONZE;    -- Iceberg tables Spark wrote (dbt sources)
CREATE SCHEMA  IF NOT EXISTS ZOMATO.RAW;       -- only used by the COPY fallback path
CREATE SCHEMA  IF NOT EXISTS ZOMATO.STAGING;   -- cleaned / conformed (dbt)
CREATE SCHEMA  IF NOT EXISTS ZOMATO.MARTS;     -- Gold, Iceberg (dbt)
CREATE SCHEMA  IF NOT EXISTS ZOMATO.SNAPSHOTS; -- SCD2 history (dbt)
CREATE SCHEMA  IF NOT EXISTS ZOMATO.AI;        -- LLM-enriched tables (OpenAI jobs)

-- A role dbt/Airflow will use.
CREATE ROLE IF NOT EXISTS DBT_ROLE;
GRANT USAGE   ON WAREHOUSE ZOMATO_WH TO ROLE DBT_ROLE;
GRANT OPERATE ON WAREHOUSE ZOMATO_WH TO ROLE DBT_ROLE;
GRANT ALL     ON DATABASE  ZOMATO    TO ROLE DBT_ROLE;
GRANT ALL     ON ALL SCHEMAS IN DATABASE ZOMATO TO ROLE DBT_ROLE;
GRANT ALL     ON FUTURE SCHEMAS IN DATABASE ZOMATO TO ROLE DBT_ROLE;
GRANT ALL     ON FUTURE TABLES IN DATABASE ZOMATO TO ROLE DBT_ROLE;
GRANT ALL     ON FUTURE VIEWS  IN DATABASE ZOMATO TO ROLE DBT_ROLE;

-- Let your login use the role (replace with your Snowflake username).
SET my_user = CURRENT_USER();
GRANT ROLE DBT_ROLE TO USER IDENTIFIER($my_user);

SELECT 'setup complete' AS status;
```

## Snowflake Step 2 storage integration with s3


``` bash

-- =====================================================================
-- Phase 2 · Step 2 — Secure S3 <-> Snowflake link (Storage Integration)
-- This is the real-world way to connect (no keys stored in Snowflake).
--
-- ORDER OF OPERATIONS (see RUNBOOK.md for the AWS clicks):
--   A. In AWS IAM, create role `snowflake-zomato-role` with a PLACEHOLDER
--      trust policy (trust your own account for now).
--   B. Run CREATE STORAGE INTEGRATION below with that role's ARN.
--   C. DESC INTEGRATION -> copy STORAGE_AWS_IAM_USER_ARN + EXTERNAL_ID.
--   D. Edit the IAM role's trust policy with those two values.
-- =====================================================================


USE ROLE ACCOUNTADMIN;

-- >>> EDIT THESE TWO <<<
--   <ROLE_ARN> = arn:aws:iam::<your-account-id>:role/snowflake-zomato-role
--   <BUCKET>   = your bucket, e.g. zomato-dl-yourname
CREATE OR REPLACE STORAGE INTEGRATION ZOMATO_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::314944603211:role/snowflake-read-s3'
  STORAGE_ALLOWED_LOCATIONS = ('s3://food-intel-datalake-s3');

GRANT USAGE ON INTEGRATION ZOMATO_S3_INT TO ROLE DBT_ROLE;

-- Run this, then copy the two values into the IAM role trust policy (Step D).
DESC INTEGRATION ZOMATO_S3_INT;
--   STORAGE_AWS_IAM_USER_ARN  ->  the "AWS": principal in the trust policy
--   STORAGE_AWS_EXTERNAL_ID   ->  the sts:ExternalId condition

 

```

## Step 3 - File formating in snowfalke

``` bash
-- =====================================================================
-- Phase 2 · Step 3 — External stage on S3 + CSV file format
-- =====================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO;
USE SCHEMA RAW;

-- Plain CSV (manual upload, no gzip). Files KEEP their header row -> SKIP_HEADER = 1.
-- Comment fields (reviews) contain commas but are quoted, so keep the quote char.
CREATE OR REPLACE FILE FORMAT ZOMATO.RAW.CSV_FMT
  TYPE = 'CSV'
  COMPRESSION = 'AUTO'                       -- plain CSV (also fine if you ever switch to .gz)
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1                            -- skip the header row your CSVs still have
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('', '\\N', 'NULL')
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;   -- messy source rows (e.g. food.csv missing a
                                            -- trailing field) NULL-fill instead of aborting

-- >>> EDIT <BUCKET> <<<
-- Upload one CSV per folder:  raw/restaurants/  raw/users/  raw/food/  raw/menu/
--                             raw/orders/  raw/order_items/  raw/reviews/
CREATE OR REPLACE STAGE ZOMATO.RAW.ZOMATO_RAW_STAGE
  STORAGE_INTEGRATION = ZOMATO_S3_INT
  URL = 's3://food-intel-datalake/raw/'
  FILE_FORMAT = ZOMATO.RAW.CSV_FMT;

-- Confirm Snowflake can see your files (should list the seven table folders).
LIST @ZOMATO.RAW.ZOMATO_RAW_STAGE;


```

## Step 4 - raw data to tables - Buiding Schema(Column names) into the RAW

```
bash

-- =====================================================================
-- Phase 2 · Step 4 — RAW tables (Path 1 batch, manual plain-CSV upload)
-- Column ORDER matches the CSV files you upload, with headers skipped.
-- NOTE: the four DIMENSION files (restaurant/users/food/menu) carry a leading
-- unnamed index column, so their tables start with a throwaway `_idx` column.
-- The fact files (orders/order_items/reviews) have no index column.
-- =====================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO;
USE SCHEMA RAW;

-- restaurant.csv  (index col dropped):
-- id,name,city,rating,rating_count,cost,cuisine,lic_no,link,address,menu
CREATE OR REPLACE TABLE RAW.restaurants (
  _idx          STRING,                    -- leading index column in the CSV (ignored downstream)
  id            STRING, name        STRING, city    STRING, rating   STRING,
  rating_count  STRING, cost        STRING, cuisine STRING, lic_no   STRING,
  link          STRING, address     STRING, menu    STRING
);

-- users.csv: user_id,name,email,password,Age,Gender,Marital Status,
--            Occupation,Monthly Income,Educational Qualifications,Family size
CREATE OR REPLACE TABLE RAW.users (
  _idx STRING,                             -- leading index column in the CSV
  user_id STRING, name STRING, email STRING, password STRING, age STRING,
  gender STRING, marital_status STRING, occupation STRING, monthly_income STRING,
  education STRING, family_size STRING
);

-- food.csv: f_id,item,veg_or_non_veg
CREATE OR REPLACE TABLE RAW.food (
  _idx STRING,                             -- leading index column in the CSV
  f_id STRING, item STRING, veg_or_non_veg STRING
);

-- menu.csv: ,menu_id,r_id,f_id,cuisine,price
CREATE OR REPLACE TABLE RAW.menu (
  _idx STRING,                             -- leading index column in the CSV
  menu_id STRING, r_id STRING, f_id STRING, cuisine STRING, price STRING
);

-- generated/orders.csv (clean, typed):
CREATE OR REPLACE TABLE RAW.orders (
  order_id          NUMBER,
  order_timestamp   TIMESTAMP_NTZ,
  order_date        DATE,
  user_id           NUMBER,
  r_id              NUMBER,
  restaurant_city   STRING,
  cuisine           STRING,
  items_count       NUMBER,
  sales_qty         NUMBER,
  subtotal          NUMBER,
  discount          NUMBER,
  delivery_fee      NUMBER,
  gst               NUMBER,
  sales_amount      NUMBER,
  currency          STRING,
  payment_method    STRING,
  order_status      STRING,
  customer_rating   NUMBER,
  delivery_time_min NUMBER
);

-- generated/order_items.csv (clean, typed):
CREATE OR REPLACE TABLE RAW.order_items (
  order_item_id NUMBER,
  order_id      NUMBER,
  r_id          NUMBER,
  f_id          STRING,
  price         NUMBER,
  quantity      NUMBER,
  line_amount   NUMBER
);

-- generated/reviews.csv (clean, typed) — free text for the AI layer:
CREATE OR REPLACE TABLE RAW.reviews (
  review_id     NUMBER,
  order_id      NUMBER,
  user_id       NUMBER,
  restaurant_id NUMBER,
  rating        NUMBER,
  comment       STRING,
  review_date   DATE
);

```


## Step 5 copy data from staging to RAW

```
bash

-- =====================================================================
-- Phase 2 · Step 5 — Load RAW from S3 (Path 1 batch)
-- Loads the plain CSVs you uploaded to each raw/<table>/ folder. The header
-- row is skipped by the file format (SKIP_HEADER=1), and rows load by position.
-- =====================================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE ZOMATO;
USE SCHEMA RAW;
USE WAREHOUSE ZOMATO_WH;

-- Dimensions = messy real source data -> tolerate & skip bad rows (CONTINUE).
COPY INTO RAW.restaurants FROM @ZOMATO_RAW_STAGE/restaurants/  ON_ERROR = 'CONTINUE';
COPY INTO RAW.users       FROM @ZOMATO_RAW_STAGE/users/        ON_ERROR = 'CONTINUE';
COPY INTO RAW.food        FROM @ZOMATO_RAW_STAGE/food/         ON_ERROR = 'CONTINUE';
COPY INTO RAW.menu        FROM @ZOMATO_RAW_STAGE/menu/         ON_ERROR = 'CONTINUE';

-- Facts = clean generated data -> stay strict so counts are exact.
COPY INTO RAW.orders      FROM @ZOMATO_RAW_STAGE/orders/       ON_ERROR = 'ABORT_STATEMENT';
COPY INTO RAW.order_items FROM @ZOMATO_RAW_STAGE/order_items/  ON_ERROR = 'ABORT_STATEMENT';
COPY INTO RAW.reviews     FROM @ZOMATO_RAW_STAGE/reviews/      ON_ERROR = 'ABORT_STATEMENT';

-- Sanity check.
SELECT 'restaurants' t, COUNT(*) n FROM RAW.restaurants
UNION ALL SELECT 'users',       COUNT(*) FROM RAW.users
UNION ALL SELECT 'food',        COUNT(*) FROM RAW.food
UNION ALL SELECT 'menu',        COUNT(*) FROM RAW.menu
UNION ALL SELECT 'orders',      COUNT(*) FROM RAW.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM RAW.order_items
UNION ALL SELECT 'reviews',     COUNT(*) FROM RAW.reviews
ORDER BY t;


-- Expect: orders = 10,000,000 · order_items ≈ 23,000,000 · restaurants ≈ 148,541 ...

-- ---------------------------------------------------------------------
-- OPTIONAL — auto-ingest new files with Snowpipe (teach this on camera):
-- CREATE PIPE RAW.orders_pipe AUTO_INGEST = TRUE AS
--   COPY INTO RAW.orders FROM @ZOMATO_RAW_STAGE/orders/;
-- then wire the pipe's SQS ARN to an S3 event notification.
-- ---------------------------------------------------------------------


```


### Step 6 — Set up dbt

```bash
cd dbt_project - curent project folder
pip install dbt-snowflake
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake credentials

# Test connection
dbt debug

# Run all models
dbt build
```

### Step 7 Run dbt commonds

``` 
bash

cd <dbt project>
dbt run --select staging 
dbt run --select 
dbt run --full-refresh --select staging marts 
dbt test
dbt run

```
## Step 8 orchestrate using Airflow
Create docker image
```
docker-compose bulid 

```

```
docker-compose up

```

It spins up a complete Airflow 3.x environment locally so you can orchestrate your data pipeline — dbt transformations on Snowflake + AI-powered review enrichment — without installing Airflow directly on your machine.

AirFlow Server is running on http://localhost:8081

## Step 9: Implemented AI layer on top of Data pipeline

```
enrich_reviews.py
```












```
Food-Delivery CSVs → Amazon S3 → Snowflake (Bronze→Silver→Gold) → dbt → Airflow → AI Layer → Streamlit
```

**Three AI capabilities on top of the warehouse:**
- 🤖 **LLM Enrichment** — GPT-4o-mini classifies every review into sentiment + topic + urgency flag
- 💬 **RAG Chat** — Ask questions, get answers grounded in real customer reviews
- 🔍 **Text-to-SQL** — Type plain English, get live Snowflake query results

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Storage | Amazon S3 (ap-south-1) |
| Warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Orchestration | Apache Airflow 3 on Docker |
| AI / LLM | OpenAI GPT-4o-mini + text-embedding-3-small |
| Serving | Streamlit |
| Language | Python 3.11+ |

---

## Dataset

| Table | Description | Volume |
|-------|-------------|--------|
| orders | Core fact — status, amounts, times | 10M rows |
| order_items | Line items per order | 23M rows |
| reviews | Free-text comments + ratings | 300K rows |
| restaurants | Who sells — cuisine, city, rating | Dimension |
| users | Who orders — city, signup date | Dimension |
| food | Menu items — veg/non-veg | Dimension |
| menu | Restaurant × food × price | Dimension |

> 📦 Dataset is not committed to this repo (2.3 GB). Download from the [Google Drive folder](#) and place files under `Data/`.

---

## Architecture

### Medallion Layers

```
RAW (Bronze)     — All columns as TEXT, append-only, tolerant COPY INTO
STAGING (Silver) — Typed + cleaned dbt views (TRY_TO_DECIMAL, NULLIF, INITCAP)
MARTS (Gold)     — Dims, incremental facts, aggregate marts
AI               — LLM-enriched review table (REVIEW_ENRICHED)
```

### Airflow DAG — 4 Tasks

```
reload_raw → dbt_build_core → enrich_reviews → dbt_build_ai
```

| Task | What it does |
|------|-------------|
| `reload_raw` | COPY INTO all 7 RAW tables from S3 |
| `dbt_build_core` | Build + test all models except AI tag |
| `enrich_reviews` | Call GPT-4o-mini on un-enriched reviews |
| `dbt_build_ai` | Build AI marts on enriched data |

---

## Repository Structure

```
food-intel-data-platform/
├── dags/
│   └── zomato_batch.py          # Airflow DAG (4 tasks, daily)
├── dbt_project/
│   ├── models/
│   │   ├── staging/             # 7 staging views (Silver)
│   │   ├── marts/
│   │   │   ├── dimensions/      # dim_restaurants, dim_users, dim_food, dim_date
│   │   │   ├── facts/           # fct_orders (incremental), fct_order_items
│   │   │   └── aggregates/      # 6 business marts
│   │   └── ai/                  # mart_review_insights (tag:ai)
│   └── snapshots/               # SCD2 on restaurant ratings
├── ai/
│   ├── enrich_reviews.py        # LLM enrichment → REVIEW_ENRICHED
│   ├── rag_chat.py              # RAG chat with reviews
│   └── text_to_sql.py           # Natural language → Snowflake SQL
├── streamlit/
│   ├── app_dashboard.py         # BI dashboard
│   ├── app_rag_chat.py          # RAG chat app
│   └── app_text_to_sql.py       # Text-to-SQL app
├── infra/
│   ├── snowflake_setup.sql      # One-time Snowflake setup
│   └── iam_policy.json          # AWS IAM policy
├── Doc/
│   └── Architecture.png
├── docker-compose.yml
├── Dockerfile.airflow
├── .env.example
└── README.md
```

---

## Setup Guide

### Prerequisites

- AWS account with S3 access
- Snowflake account (free trial works)
- Docker Desktop installed
- Python 3.11+
- OpenAI API key

---

### Step 1 — Clone the repo

```bash
git clone https://github.com/<your-username>/food-intel-data-platform.git
cd food-intel-data-platform
```

---

### Step 2 — Set up environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in your values:

```env
# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=ZOMATO
SNOWFLAKE_WAREHOUSE=FOOD_INTEL_WH
SNOWFLAKE_ROLE=DBT_ROLE

# AWS
AWS_DEFAULT_REGION=ap-south-1
S3_BUCKET=food-intel-datalake

# OpenAI
OPENAI_API_KEY=sk-...
```

---

### Step 3 — Set up Snowflake

Run these SQL files **in order** in Snowsight (Snowflake UI):

```
infra/snowflake_setup.sql     # Creates warehouse, database, schemas, roles
```

---

### Step 4 — Upload data to S3

```bash
# Upload all CSVs to S3
aws s3 sync Data/ s3://food-intel-datalake/raw/
```

---



---

### Step 6 — Start Airflow with Docker

```bash
# From project root
docker compose up --build -d

# Open Airflow UI
# http://localhost:8080
# Username: airflow | Password: airflow
```

Add the Snowflake connection in Airflow UI:
- **Conn ID:** `snowflake_default`
- **Conn Type:** Snowflake
- Fill in your account, user, password, database, warehouse, role

Trigger the DAG: `zomato_batch`

---

### Step 7 — Run AI enrichment manually (optional)

```bash
cd ai
pip install -r requirements.txt
python enrich_reviews.py --sample-n 500
```

---

### Step 8 — Launch Streamlit apps

```bash
cd streamlit
pip install -r requirements.txt

# BI Dashboard
streamlit run app_dashboard.py

# RAG Chat (open new terminal)
streamlit run app_rag_chat.py

# Text-to-SQL (open new terminal)
streamlit run app_text_to_sql.py
```

---

## dbt Models

### Run commands

```bash
# Build everything
dbt build

# Build only staging
dbt build --select staging

# Build only AI models
dbt build --select tag:ai

# Full refresh incremental models
dbt build --full-refresh --select fct_orders

# Run tests only
dbt test

# Generate + serve docs
dbt docs generate && dbt docs serve
```

---

## What Makes This Unique

| Feature | This Project | Reference Project |
|---------|-------------|-------------------|
| LLM enrichment fields | sentiment + topic + **urgency_flag** | sentiment + topic only |
| Extra Gold mart | **mart_top_items_by_city** | Not present |
| SCD2 snapshot | **Restaurant ratings history** | Not present |
| AWS region | **ap-south-1 (Mumbai)** | us-east-1 |
| Airflow version | **Airflow 3** | Airflow 2.9 |

---

## Security

- ✅ No AWS keys stored — keyless S3 access via IAM role trust
- ✅ dbt runs as `DBT_ROLE`, not ACCOUNTADMIN
- ✅ Text-to-SQL uses SELECT-only guard + DBT_ROLE (read-only)
- ✅ All secrets in environment variables, never in code
- ✅ `.env` is in `.gitignore`

---

## Skills Demonstrated

`AWS S3` · `Snowflake` · `dbt` · `Apache Airflow` · `Docker` · `OpenAI API` · `RAG` · `Text-to-SQL` · `Medallion Architecture` · `Incremental Models` · `SCD2 Snapshots` · `ELT Pipeline` · `Streamlit` · `Python` · `SQL`

---

## License

MIT License — free to use, modify, and share.

---

*Built as a portfolio data engineering project. Inspired by the Zomato AI Data Engineering curriculum.*
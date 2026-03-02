# Cloud Data Engineering Technologies - Labs (TPIUO)

Hands-on repository for building a modern, cloud-native data engineering workflow end-to-end:
**ingestion → messaging → validation → storage → warehouse → transformations → data quality → BI**.

The labs progressively evolve a pipeline from local tooling fundamentals (Git/Docker/Kafka) to a production-style,
event-driven architecture on **Google Cloud Platform (GCP)**, ending with analytics modeling (**dbt**),
data quality checks (**Great Expectations**), and dashboarding (**Looker Studio**).

---

## What’s inside (high-level)
- **Lab 0**: Git/GitHub workflow + Docker basics + Apache Kafka fundamentals (local stack)
- **Lab 1**: Serverless ingestion with **Pub/Sub + Cloud Run** (producer job → consumer service)
- **Lab 2**: Production-grade pipeline: **Schema Registry (AVRO) + Dead Letter Topic + GCS (JSON/Parquet) + BigQuery + CI/CD**
- **Lab 3**: Analytics & BI layer: **dbt medallion architecture + SQL windowing/aggregations + Great Expectations + Looker Studio**

---

## End-to-end architecture (what the full pipeline becomes)

```text
┌───────────────┐      ┌──────────────┐      ┌────────────────────┐
│  External API │ ---> │   Pub/Sub    │ ---> │  Cloud Run Service │
│ (Reddit/SO)   │      │  (AVRO+DLQ)  │      │  consumer (push)   │
└───────────────┘      └──────┬───────┘      └─────────┬──────────┘
                              │                        │
                              │ invalid                │ valid
                              v                        v
                         ┌─────────────┐        ┌───────────────────┐
                         │   DLQ topic │        │   GCS raw JSON    │
                         └─────────────┘        └───────────────────┘
                                                     │
                                                     v
                                            ┌───────────────────┐
                                            │ GCS processed      │
                                            │ Parquet partitioned│
                                            │ (year/month/day/hr)│
                                            └─────────┬─────────┘
                                                      v
                                            ┌───────────────────┐
                                            │ BigQuery (DWH)    │
                                            │ load jobs + tables│
                                            └─────────┬─────────┘
                                                      v
                                            ┌───────────────────┐
                                            │ dbt bronze/silver/│
                                            │ gold models       │
                                            └─────────┬─────────┘
                                                      v
                                            ┌───────────────────┐
                                            │ Great Expectations │
                                            │ checkpoints/reports│
                                            └─────────┬─────────┘
                                                      v
                                            ┌───────────────────┐
                                            │ Looker Studio BI  │
                                            │ dashboard         │
                                            └───────────────────┘
```

---

## Tech stack used
- **GCP**: Pub/Sub, Cloud Run (Jobs + Services), Cloud Storage (GCS), BigQuery
- **Messaging patterns**: push subscriptions, schema validation, dead-letter handling
- **Formats**: AVRO (message encoding/validation), JSON (raw), Parquet (columnar analytics)
- **Analytics**: dbt (SQL models, lineage/DAG, tests), BigQuery window functions & aggregations
- **Data quality**: Great Expectations (expectation suites + checkpoints + HTML reports)
- **DevOps**: Docker, GitHub Actions CI/CD, branch protection rules
- **Local tooling**: Python, `uv`, `.editorconfig`, linting (pylint), editorconfig-checker

---

## Security / repo hygiene (important)
Do **NOT** commit secrets or credentials:
- `.env`, service account keys like `gcp-sa-key.json`, API tokens, etc.

Recommended:
- use **GCP Secret Manager** or GitHub Actions secrets for CI/CD deployments
- ensure sensitive files are in `.gitignore`

```text
# examples to keep local-only:
.env
gcp-sa-key.json
.venv/
.idea/
__pycache__/
.ipynb_checkpoints/
```

---

## Prerequisites
- Docker + Docker Compose
- Python (and `uv` if you use the lockfile workflow)
- Google Cloud CLI (`gcloud`) and access to a GCP project
- BigQuery enabled in the project

---

## Quickstart (local environment)
If you use `uv`:

```bash
uv sync
```

```bash
uv run python --version
```

---

# Lab 0 - Git/GitHub + Docker + Kafka (local)

### What I did
- Practiced core **Git workflows**: branching, merging, pull requests, and conflict resolution.
- Learned Docker basics: images vs containers, Dockerfiles, docker-compose.
- Ran a local **Kafka** stack (single broker + Zookeeper) and worked with producer/consumer concepts.

### How to run (typical)
From the lab folder (example stack name may differ):

```bash
docker compose up -d
```

Then run the demo app (example):

```bash
node app.js
```

Stop:

```bash
docker compose down
```

---

# Lab 1 - Serverless ingestion: StackOverflow → Pub/Sub → Cloud Run

### Goal
Build a minimal event-driven pipeline on GCP:
- a **producer** fetches latest items from an API and publishes messages to **Pub/Sub**
- a **consumer** receives push messages and processes/logs them on **Cloud Run**

### What I implemented
**Producer (Cloud Run Job)**
- Python job that fetches the **latest StackOverflow questions** (tag: `data-engineering`)
- publishes each item as JSON to a Pub/Sub topic
- deployed as a **Cloud Run Job** (runs on demand, then exits)

**Consumer (Cloud Run Service)**
- Flask service with endpoints:
  - `/listening` - health check
  - `/` - Pub/Sub push handler
- decodes Pub/Sub payloads and logs/prints structured question data
- deployed as a **Cloud Run Service** (scales to zero)

**Pub/Sub**
- topic is the transport layer between producer and consumer
- **push subscription** forwards each message to the Cloud Run service endpoint
- consumer acknowledges processing via HTTP response (push delivery semantics)

### Commands I used most
Authenticate and select project:

```bash
gcloud auth login
gcloud config set project "$PROJECT_ID"
```

Load env vars (option):

```bash
export $(grep -v '^#' .env | xargs)
```

Run the producer job (example name):

```bash
gcloud run jobs execute stackoverflow-producer --region="$GCP_REGION"
```

Consumer is deployed once and stays available (push subscription delivers messages automatically).

---

# Lab 2 - Production-style pipeline: Pub/Sub (AVRO+DLQ) → GCS (JSON/Parquet) → BigQuery + CI/CD

### Goal
Upgrade Lab 1 into a robust pipeline with:
- **Schema Registry** on Pub/Sub (AVRO validation)
- **Dead Letter Topic (DLQ)** for invalid messages
- durable storage in **GCS**:
  - RAW JSON
  - PROCESSED Parquet (partitioned by time)
- loading into **BigQuery** (warehouse layer)
- CI/CD automation with **GitHub Actions** + branch protection

### What I implemented (pipeline stages)
1) **Producer → Pub/Sub**
- producer encodes messages into **AVRO**
- Pub/Sub validates schema; invalid messages route to **DLQ**

2) **Consumer (Cloud Run Service)**
- receives Pub/Sub push messages
- decodes AVRO into Python dict
- writes:
  - RAW JSON to **raw bucket**
  - PROCESSED Parquet to **processed bucket**
- organizes Parquet by: `year=YYYY/month=MM/day=DD/hour=HH/`

3) **Loader (Cloud Run Job / local)**
- reads processed Parquet hour-folders
- loads into **BigQuery** using load jobs
- uses an **idempotent checkpoint** to avoid duplicate loads

4) **CI/CD**
- CI runs on Pull Requests (linting/checks)
- CD runs only when changes merge into `main`:
  - build & push Docker images
  - deploy consumer as Cloud Run Service
  - deploy producer/loader as Cloud Run Jobs
  - (optionally) execute jobs after deployment
- branch protection enforces PR + required checks

### Environment variables (example)
```bash
export PROJECT_ID="your-project"
export GCP_REGION="europe-west1"

export PUBSUB_TOPIC="stackoverflow-topic-<id>"
export DLQ_TOPIC="stackoverflow-dead-letter-topic"

export RAW_BUCKET="gcs-stackoverflow-raw"
export PROCESSED_BUCKET="gcs-stackoverflow-processed"
export PREFIX="stackoverflow"

export BQ_DATASET="stackoverflow_pipeline"
export BQ_TABLE="stackoverflow_questions"
```

### Reset / cleanup (optional)
```bash
bq rm -f -t "$PROJECT_ID:$BQ_DATASET.$BQ_TABLE"
```

```bash
gcloud storage rm "gs://$RAW_BUCKET/$PREFIX/_checkpoints/bq_loader_state.json"
```

```bash
gcloud storage rm -r "gs://$RAW_BUCKET/$PREFIX/**"
gcloud storage rm -r "gs://$PROCESSED_BUCKET/$PREFIX/**"
```

### Run pipeline
Producer:

```bash
gcloud run jobs execute stackoverflow-producer --region="$GCP_REGION"
```

Consumer: no manual run (push subscription triggers it).

Loader (local test):

```bash
uv run python3 TPIUO_Labos_2/Loader/load_to_bq.py
```

Loader (Cloud Run Job):

```bash
gcloud run jobs execute stackoverflow-bq-loader --region="$GCP_REGION"
```

### Validate results (BigQuery)
Row count:

```bash
bq query --use_legacy_sql=false \
"SELECT COUNT(*) AS total_rows
 FROM \`$PROJECT_ID.$BQ_DATASET.$BQ_TABLE\`"
```

Latest records:

```bash
bq query --use_legacy_sql=false \
"SELECT question_id, title, creation_date
 FROM \`$PROJECT_ID.$BQ_DATASET.$BQ_TABLE\`
 ORDER BY creation_date DESC
 LIMIT 10"
```

---

# Lab 3 - Analytics layer: dbt (medallion) + Great Expectations + Looker Studio

### Goal
Turn the raw warehouse table into an analytics-ready model:
- **dbt medallion architecture**: bronze → silver → gold
- SQL **window functions** and **aggregations** for trends (e.g., by time / sentiment)
- **Great Expectations** to validate quality before BI
- **Looker Studio** dashboard on top of gold tables

### What I built
**dbt project**
- bronze: lightweight views over raw BigQuery tables
- silver: cleaned/standardized tables (dedup, null handling, normalized columns)
- gold: aggregated tables optimized for BI (daily/hourly trends, sentiment counts, rankings)

**Great Expectations**
- expectation suites for key models (e.g., not-null, uniqueness, accepted values, length checks)
- checkpoints producing HTML reports (pass/fail visibility)

**Looker Studio**
- dashboard connected to BigQuery (gold layer)
- filters/time controls work efficiently because metrics are pre-aggregated

### How to run (typical dbt workflow)
From the dbt project directory:

```bash
dbt debug
dbt run
dbt test
```

Docs / lineage graph:

```bash
dbt docs generate
dbt docs serve
```

### Great Expectations (typical)
Initialize and create suites/checkpoints (once), then run:

```bash
great_expectations checkpoint run <checkpoint_name>
```

---

## What I learned (summary)
- How to design an **event-driven ingestion pipeline** using Pub/Sub and serverless compute (Cloud Run).
- Why **schemas (AVRO) + DLQ** matter in streaming systems for reliability and debuggability.
- How to structure storage as **raw vs processed**, and why **Parquet + partitioning** are essential for analytics.
- How to load data into **BigQuery** in an idempotent, scalable way (load jobs + checkpoints).
- How to apply **software engineering best practices** in data engineering:
  - containerization, CI/CD, branch protection, linting, reproducible environments
- How to build an analytics layer with **dbt**, validate it with **Great Expectations**, and serve results in **Looker Studio**.

---

## Notes
- Keep all GCP resources in the **same region** to reduce latency/cost.
- If some GCP services are blocked on certain networks, switch to an alternative connection.
- This repository is primarily for academic lab submission and reproducibility.

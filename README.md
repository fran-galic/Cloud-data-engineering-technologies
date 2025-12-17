# Cloud-data-engineering-technologies

This repository contains hands-on materials for learning essential processes used in modern cloud-based data engineering projects.
Throughout the course, students gain practical skills across the full data lifecycle, including:

- data ingestion and storage
- data transfer and transformation
- data cleaning and validation
- data serving and delivery

In addition, the course introduces key engineering practices such as:

- source code management
- serverless execution environments
- containerization with Docker
- orchestration using Kubernetes
- NoSQL databases (MongoDB)
- continuous integration and delivery (CI/CD)
- infrastructure as code (Terraform)

These skills prepare students for real-world data engineering workflows in cloud environments.

---

## Lab 1 — StackOverflow → Pub/Sub → Cloud Run Pipeline

### Overview

Lab 1 focuses on implementing a simple serverless data ingestion pipeline using Google Cloud Platform.
The goal is to understand message-driven architecture, containerized workloads, and event-based processing through Pub/Sub and Cloud Run.

### Implementation Summary

#### **Producer (Cloud Run Job)**
- Written in Python
- Fetches the **10 newest** StackOverflow questions tagged `data-engineering`
- Publishes each question as a JSON message to a Pub/Sub topic
- Packaged as a Docker container stored in Artifact Registry
- Deployed as a Cloud Run **Job** which runs on demand

#### **Consumer (Cloud Run Service)**
- Flask-based Python service
- Exposes:
  - `/listening` — health check endpoint
  - `/` — Pub/Sub push message handler
- Receives Pub/Sub push messages
- Decodes base64 payloads and logs the question data
- Runs as a Cloud Run **Service**, scaling to zero when idle

#### **Pub/Sub Integration**
- A topic acts as the communication channel between producer and consumer
- A **push subscription** automatically forwards every message to the Cloud Run consumer endpoint
- The system is fully event-driven and loosely coupled

This lab demonstrates the fundamentals of serverless data ingestion using containerized Python applications, Pub/Sub messaging, and Cloud Run execution models.

---

Redom pokrenuti:
-  gcloud auth login    -  za ulogirat se na gcloud
-  export $(grep -v '^#' .env | xargs)  -  za ucitati enviroment varijable u shell
- gc run jobs execute so-producer-job --region=${GCP_REGION}    -   za pokrnetui GC run, producer job, koji uzima sa StackOverflow-a (s reditta ne radi i komplicirano je )
- conusmer servis je vec deployan i netreba nista raditi  (nas GC subscrtion je napravljen u Push nacinu rada da kada dobije pusha ih na nas consumer servis dok mu ovaj ne potvrdi)



za job:
gcloud run jobs execute stackoverflow-producer --region="$GCP_REGION"



Labos 2. : End-to-End Data Engineering Pipeline (Pub/Sub → GCS → BigQuery)

Ovaj projekt implementira kompletan cloud-native data pipeline koristeći Google Cloud.
Pipeline koristi Pub/Sub s AVRO shemom i Dead-Letter topic, Cloud Run (producer, consumer, loader),
Google Cloud Storage za trajnu pohranu i BigQuery za analitiku.

------------------------------------------------------------
Environment varijable
------------------------------------------------------------

Prije pokretanja potrebno je učitati environment varijable.

Opcija A – ručno u shellu:

export PROJECT_ID=tpiuo-labosi
export GCP_REGION=europe-west1

export PUBSUB_TOPIC=reddit-topic-0036546889
export DLQ_TOPIC=stack-overflow-dead-letter-topic

export RAW_BUCKET=gcs-stack-overflow-bucket-raw
export PROCESSED_BUCKET=gcs-stack-overflow-bucket-processed
export PREFIX=stackoverflow

export BQ_DATASET=stackoverflow_pipeline
export BQ_TABLE=stackoverflow_questions

Opcija B – .env file:

set -a
source .env
set +a

------------------------------------------------------------
Reset pipeline stanja (čišćenje)
------------------------------------------------------------

Obriši BigQuery tablicu (reset analitičkog sloja):

bq rm -f -t "$PROJECT_ID:$BQ_DATASET.$BQ_TABLE"

Obriši loader checkpoint (omogućuje ponovno učitavanje svih podataka):

gcloud storage rm "gs://$RAW_BUCKET/$PREFIX/_checkpoints/bq_loader_state.json"

Opcionalno – očisti GCS bucket-e:

gcloud storage rm -r "gs://$RAW_BUCKET/$PREFIX/**"
gcloud storage rm -r "gs://$PROCESSED_BUCKET/$PREFIX/**"

------------------------------------------------------------
Pokretanje producera (API → Pub/Sub)
------------------------------------------------------------

Producer dohvaća podatke s API-ja, enkodira ih u AVRO i šalje u Pub/Sub.
Nevaljane poruke se spremaju u Dead-Letter topic.

gcloud run jobs execute stackoverflow-producer \
  --region="$GCP_REGION"

------------------------------------------------------------
Obrada poruka (Pub/Sub → GCS)
------------------------------------------------------------

Consumer je Cloud Run Service s Pub/Sub push subscriptionom i automatski:
- dekodira AVRO poruke
- sprema RAW JSON u GCS
- sprema PROCESSED Parquet u GCS
- organizira podatke po year/month/day/hour

Nema ručnog pokretanja.

Provjera sadržaja u GCS-u:

gcloud storage ls "gs://$PROCESSED_BUCKET/$PREFIX/"

------------------------------------------------------------
Učitavanje u BigQuery (GCS → BigQuery)
------------------------------------------------------------

Loader učitava Parquet datoteke u BigQuery koristeći idempotentan checkpoint
(baziran na hour-folderima, bez duplikata).

Lokalno pokretanje (test):

uv run python3 TPIUO_Labos_2/Loader/load_to_bq_v2.py

Cloud Run Job pokretanje:

gcloud run jobs execute stackoverflow-bq-loader \
  --region="$GCP_REGION"

------------------------------------------------------------
Provjera podataka u BigQuery
------------------------------------------------------------

Broj redaka:

bq query --use_legacy_sql=false \
"SELECT COUNT(*) AS total_rows
FROM \`tpiuo-labosi.stackoverflow_pipeline.stackoverflow_questions\`"

Pregled zapisa:

bq query --use_legacy_sql=false \
"SELECT question_id, title, content_license, creation_date
FROM tpiuo-labosi.stackoverflow_pipeline.stackoverflow_questions
ORDER BY creation_date DESC
LIMIT 10"


------------------------------------------------------------
Vizualna provjera (Google Cloud Console)
------------------------------------------------------------

Pub/Sub:
- glavni topic
- Dead-Letter topic

Cloud Run:
- Services (consumer)
- Jobs (producer, loader)

Cloud Storage:
- RAW bucket
- PROCESSED bucket

BigQuery:
- Dataset: stackoverflow_pipeline
- Table: stackoverflow_questions


------------------------------------------------------------
Zaključak
------------------------------------------------------------

Pipeline implementira:
- AVRO schema validaciju
- Dead-letter handling
- Event-driven obradu podataka
- Data Lake u Google Cloud Storageu
- Columnar storage (Parquet)
- Incremental BigQuery load
- Cloud-native execution (Cloud Run Jobs i Services)


      - name: EditorConfig check (non-blocking)
        continue-on-error: true
        run: |
          python -m pip install --upgrade pip
          python -m pip install editorconfig-checker
          ec -version
          ec .

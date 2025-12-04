# Cloud-data-engineering-technologies
Hands-on materials for learning core data engineering processes: ingestion, storage, transformation, validation, and serving. Includes cloud practices with Go, Docker, Kubernetes, MongoDB, Git, Jenkins, and Terraform for modern CI/CD and serverless environments.

- Ime: Fran
- Prezime: Galić
- Email: fran.galic@fer.hr

### Cloud Data Engineering Lab 1 – StackOverflow → Pub/Sub → Cloud Run

This project implements a simple data pipeline on Google Cloud Platform using:

- **StackOverflow API** as the data source
- **Pub/Sub** as the messaging backbone
- **Cloud Run Job** as a _producer_
- **Cloud Run Service** as a _consumer_
- **Artifact Registry** as the container image repository
- **Python + Flask + Docker** for the application code

Originally the lab was based on Reddit, but this implementation uses **StackOverflow** questions tagged `data-engineering`.

---

## Architecture

High-level flow:

1. **Producer (Cloud Run Job)**
   - Fetches the **10 newest** StackOverflow questions with tag `data-engineering`
   - Sends each question as a separate JSON message to a **Pub/Sub topic**

2. **Pub/Sub Topic**
   - Acts as the message broker between producer and consumer
   - Has a **push subscription** configured

3. **Push Subscription**
   - For every incoming message, Pub/Sub sends an HTTP `POST` request to the consumer’s Cloud Run URL

4. **Consumer (Cloud Run Service)**
   - Exposes an HTTP endpoint (`/`) built with Flask
   - Receives the Pub/Sub push payload, decodes the base64 data, and logs the raw JSON to stdout (visible in Cloud Logging)

The whole system is fully decoupled: producer and consumer never talk to each other directly, they communicate only through Pub/Sub.

---

## Components

### 1. Producer

File: `TPIUO_Labos_1/producer/producer.py`

Responsibilities:

- Load configuration from environment variables:
  - `PROJECT_ID` – GCP project ID
  - `PUBSUB_TOPIC` – Pub/Sub topic name
- Call StackExchange API:
  - Endpoint: `https://api.stackexchange.com/2.3/questions`
  - Parameters: `order=desc`, `sort=creation`, `site=stackoverflow`, `tagged=data-engineering`, `pagesize=10`
- Publish each question to Pub/Sub as a JSON-encoded message using `google-cloud-pubsub`.

The producer is packaged into a Docker image via:

- `TPIUO_Labos_1/producer/Dockerfile`

and deployed as a **Cloud Run Job**.

---

### 2. Consumer

File: `TPIUO_Labos_1/consumer/consumer.py`

Responsibilities:

- Run a Flask app with two routes:
  - `GET /listening` – simple health-check endpoint (`"Consumer service is listening"`)
  - `POST /` – Pub/Sub push endpoint
- For each incoming Pub/Sub message:
  - Parse the JSON envelope
  - Base64-decode `message.data`
  - Log `messageId`, attributes, and the decoded payload to stdout

The consumer is packaged into a Docker image via:

- `TPIUO_Labos_1/consumer/Dockerfile`

and deployed as a **Cloud Run Service**.

Pub/Sub is configured with a **push subscription** that sends messages to the service root URL (`/`).


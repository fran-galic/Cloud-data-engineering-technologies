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
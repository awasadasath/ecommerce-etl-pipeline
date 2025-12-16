# 🛒 End-to-End Ecommerce Data Pipeline & Dashboard

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-orange?style=for-the-badge&logo=apache-airflow&logoColor=white)
![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![Looker](https://img.shields.io/badge/Looker_Studio-4285F4?style=for-the-badge&logo=looker&logoColor=white)

## 📖 Project Overview

This project demonstrates a robust, end-to-end **Data Engineering solution** designed to simulate a real-world e-commerce scenario. The primary goal is to centralize fragmented transactional data, enrich it with external financial contexts, and automate the flow from raw data to actionable business insights.

In this pipeline, raw transaction records are extracted from an **OLTP Database (MySQL)** and enriched with dynamic exchange rates from an external **Currency API**. The data undergoes rigorous cleaning and transformation processes before being loaded into a **Data Warehouse (Google BigQuery)**. Finally, the processed data is visualized using **Looker Studio** to track key performance metrics (KPIs) such as daily revenue in local currency (THB) and product performance.

### 🎯 Key Objectives
* **Automation:** Orchestrate a daily ETL workflow using **Apache Airflow** to ensure data freshness without manual intervention.
* **Data Enrichment:** Integrate internal sales data with external exchange rate APIs to calculate accurate revenue figures in THB.
* **Infrastructure as Code (IaC):** Provision and manage Google Cloud Platform resources (GCS Buckets, BigQuery Datasets) using **Terraform** for reproducibility and standard compliance.
* **Data Quality & Reliability:** Implement data quality checks (e.g., handling negative values, deduplication) and real-time monitoring via **Discord Webhooks** to ensure pipeline stability.

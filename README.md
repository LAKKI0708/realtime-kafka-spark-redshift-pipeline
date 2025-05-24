# 🧠 Real-Time Data Pipeline with Kafka, Spark & Redshift

This project demonstrates a full-fledged real-time data processing pipeline using Apache Kafka, Apache Spark, and AWS Redshift. Data is simulated using a Python script and sent to Kafka. Spark reads from Kafka, transforms the data, and finally writes the processed results to an S3 bucket, which is later crawled and ingested into Redshift.

---

## 📌 Project Architecture

```mermaid
graph TD
    A[Python Data Generator] --> B[Kafka Broker]
    B --> C[Spark Structured Streaming]
    C --> D[S3 Bucket]
    D --> E[Glue Crawler]
    E --> F[Redshift Table]
---

## 🛠️ Technologies Used

- **Apache Kafka**
- **Apache Spark Structured Streaming**
- **Docker Compose**
- **AWS S3**
- **AWS Redshift**
- **AWS Glue**
- **Python**
- **PySpark**
- **Confluent Kafka Broker**

---
## 📌 Use Case

This project simulates a smart city scenario where various data streams (vehicle telemetry, GPS coordinates, weather data, and traffic conditions) are produced in real-time, processed, and stored for analytical queries.

---

## 📦 Features

- Python-based data generator to simulate smart city telemetry.
- Kafka broker (with Zookeeper) to handle event ingestion.
- Spark Structured Streaming to process data from Kafka and write to S3 in Parquet format.
- AWS Glue crawlers to crawl S3 data and load into Redshift.
- Modular and containerized setup using Docker Compose.

---

## 🧾 Kafka Topics

- `vehicle_data`
- `gps_data`
- `traffic_data`
- `weather_data`

---
## ⚙️ Streaming Pipeline

```text
+-------------+      +--------+      +--------+      +-----+      +----------+
| Data Source | -->  | Kafka  | -->  | Spark  | -->  | S3  | -->  | Redshift |
+-------------+      +--------+      +--------+      +-----+      +----------+

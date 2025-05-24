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

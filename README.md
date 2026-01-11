# 🚦 Smart City Real-Time Traffic Analytics Pipeline

<img width="600" height="195" alt="depositphotos_37708513-stock-photo-traffic-jam-on-german-highway" src="https://github.com/user-attachments/assets/162a6a09-78e7-4e2b-9e81-05a43ddca5e2" />


[![Docker](https://img.shields.io/badge/Docker-Enabled-blue.svg)](https://www.docker.com/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.0.0-orange.svg)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.1-red.svg)](https://airflow.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.0-black.svg)](https://kafka.apache.org/)
[![Grafana](https://img.shields.io/badge/Grafana-Latest-orange.svg)](https://grafana.com/)

## 📖 Project Overview

The **Smart City Traffic Analytics Pipeline** is an end-to-end Big Data engineering project designed to simulate, process, and perform real-time analysis on urban traffic flows. 

In modern smart cities, IoT sensors generate massive amounts of data. This project demonstrates how to handle that scale using varied technologies: consuming raw streams, storing them in a Data Lake, processing them with distributed computing, and orchestrating the entire workflow for actionable insights.

**Key Goals:**
*   **Ingest**: Handle high-throughput simulated traffic sensor data.
*   **Store**: Build a scalable Data Lake using Hadoop HDFS.
*   **Process**: Aggregate data (avg speed, congestion intensity) using Apache Spark.
*   **Visualize**: Monitor city traffic health in real-time using Grafana.
*   **Automate**: Orchestrate the entire ETL workflow using Apache Airflow.

---

## 🏗️ Architecture

The pipeline follows a classic **Lambda Architecture** approach (simplified for this demo), handling batch/micro-batch processing on streaming data.

<img width="871" height="431" alt="Diagramme sans nom drawio (2)" src="https://github.com/user-attachments/assets/1c865709-3af8-409f-a51c-23693a800ed0" />


### Data Flow
1.  **Data Source**: A customized Python **Traffic Generator** simulates IoT sensor events (Vehicle count, Speed, GPS location) and pushes them to **Apache Kafka**.
2.  **Ingestion Layer**: A consumer service reads from Kafka and writes raw JSON data into **Hadoop HDFS** (The Data Lake), partitioned by timestamp.
3.  **Processing Layer**: **Apache Spark** jobs run periodically to:
    *   Read raw data from HDFS.
    *   Perform aggregations (Window functions for time-based analysis).
    *   Detect anomalies (Congestion alerts).
    *   Write processed data back to HDFS (Parquet format) and **PostgreSQL** (for the dashboard).
4.  **Orchestration**: **Apache Airflow** schedules and monitors the spark jobs, ensuring dependencies are met and handling retries.
5.  **Presentation**: **Grafana** connects to PostgreSQL to visualize KPIs like "Average Speed per Road" and "Congestion Heatmap".

---

## 🛠️ Technology Stack

*   **Containerization**: Docker & Docker Compose (Full stack in containers).
*   **Message Broker**: Apache Kafka & Zookeeper.
*   **Storage (Data Lake)**: Hadoop HDFS (Namenode/Datanode).
*   **Processing Engine**: Apache Spark (PySpark).
*   **Orchestration**: Apache Airflow.
*   **Database**: PostgreSQL (Serving layer).
*   **Visualization**: Grafana.

---

## 🚀 Getting Started

### Prerequisites
*   **Docker Desktop** (Engine 20.10+) with at least 8GB RAM allocated.
*   **Git**.

### Installation
1.  **Clone the Repository**
    ```bash
    git clone https://github.com/ynstf/end-to-end-bigdata-traffic-pipeline.git
    cd end-to-end-bigdata-traffic-pipeline
    ```

2.  **Start the Infrastructure**
    This command spins up Zookeeper, Kafka, Hadoop, Spark, Postgres, Airflow, and Grafana.
    ```bash
    docker-compose up -d
    ```

3.  **Check Service Health**
    Ensure all containers are running (`docker ps`).
    *   **Airflow UI**: [http://localhost:8082](http://localhost:8082) (User/Pass: `admin`/`admin`)
    *   **Grafana UI**: [http://localhost:3000](http://localhost:3000) (User/Pass: `admin`/`admin`)
    *   **Spark Master**: [http://localhost:8080](http://localhost:8080)

---

## 🕹️ Operations Guide

### 1. Generating Traffic Data
The `traffic-generator` container starts automatically. It simulates "Rush Hours" (7-9 AM/PM) where traffic spike, and normal flow otherwise.
*   *Note*: If the generator fails to connect to Kafka initially (race condition), run:
    ```bash
    docker restart traffic-generator
    ```

### 2. Orchestration with Airflow
We use Airflow to manage the Spark jobs. The DAG `smart_city_pipeline` includes three main tasks:
1.  **`check_ingestion`**: Validates that data exists in HDFS.
2.  **`process_traffic_spark`**: Submits the Spark job to the cluster.
3.  **`validate_data`**: Checks Postgres to ensure new metrics were written.

<img width="1338" height="578" alt="image" src="https://github.com/user-attachments/assets/de3d4e23-576e-4534-9312-303b403b00e8" />



**To Run**:
1.  Go to **Airflow UI**.
2.  Toggle the `smart_city_pipeline` to **ON**.
3.  Click the **Play Button** (Trigger DAG) to run it immediately.

### 3. Monitoring with Grafana
Once the pipeline runs successfully, data flows into Postgres. Grafana visualizes this data.

**Key Dashboards:**
*   **Traffic Volume**: Real-time vehicle counts per city zone.
*   **Congestion Rate**: Bars indicating zones with >20% occupancy.
*   **Road Efficiency**: Average speed tracking to identify bottlenecks.

<img width="1349" height="681" alt="grafna" src="https://github.com/user-attachments/assets/b6d8931c-bb1e-4a50-9647-b7a44de29d01" />


---

## 📈 Evolution & Future Improvements
*   [ ] **Stream Processing**: Switch from Batch (Airflow) to Stream (Spark Structured Streaming) for sub-second latency.
*   [ ] **Cloud Deployment**: Terraform scripts to deploy on AWS EMR and MSK.
*   [ ] **Machine Learning**: Add a predictive model to forecast traffic 1 hour ahead.

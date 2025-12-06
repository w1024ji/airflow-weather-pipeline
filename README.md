# E2E Weather Data Pipeline with Airflow, Spark, and Snowflake

This repository contains the infrastructure and code for an End-to-End (E2E) Data Pipeline designed to process raw weather data from AWS S3 and load the cleaned, transformed results into a Snowflake Data Warehouse.

The entire project environment is containerized using Docker and orchestrated by Apache Airflow.

## Project Goal

The primary objective of this project is to build a robust, scalable, and fully automated pipeline that can handle large volumes of data while demonstrating proficiency in modern data engineering tools.

### Key Objectives:

1.  **Orchestration:** Use **Apache Airflow** to manage complex task dependencies and schedule the pipeline.
2.  **Transformation:** Utilize **PySpark** (running within the Dockerized Airflow environment) for fast, distributed processing and data transformation.
3.  **Cloud Integration:** Seamlessly integrate with **AWS S3** for data staging and **Snowflake** for final storage and analysis.

## Current Status: Full Pipeline Success!

As of now, the entire four-stage pipeline has been successfully built, configured, and verified using minimal data records. This verification process confirms that **all service connections and transformation logic are sound.**

| Task ID | Status | Role |
| :--- | :--- | :--- |
| `create_snowflake_table_if_not_exists` | **Success** | Destination setup |
| `extract_and_load_to_s3` | **Success** | Data extraction |
| `spark_transform_data` | **Success** | Core data transformation |
| `copy_s3_to_snowflake` | **Success** | Final data loading |

***
### Next Steps: Scaling and Performance Testing

Up to this point, we've successfully executed our tasks using a **single, minimal data record** purely to **verify the pipeline's logic and connectivity.**

Now that we've confirmed the entire flow—from Airflow orchestration through Spark processing, all the way to Snowflake loading—is robust and running smoothly, we are ready to scale up!

Our next step is to challenge this architecture by running the pipeline with **massive, real-world weather data** to perform stress testing and benchmark performance optimization.
***

## 🛠️ Infrastructure and Technologies

The entire project environment is set up for reproducibility using Docker and Docker Compose.

### Core Stack:

| Category | Technology | Purpose |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow | DAG scheduling and dependency management. |
| **Transformation** | PySpark (3.x) | Data cleaning, validation, and conversion to Parquet format. |
| **Cloud Storage** | AWS S3 | Used for staging raw and processed data. |
| **Data Warehouse** | Snowflake | Final destination for loading clean, transformed data. |
| **Environment** | Docker / Docker Compose | Containerization of Airflow services (Scheduler, Webserver, Worker). |

### Infrastructure Note: EC2 Instance Upgrade

To ensure reliable Spark performance, the development environment was migrated from a basic resource-constrained instance (e.g., `t3.small`) to a **dedicated `m7i-flex.large`** EC2 instance. This was a critical step to eliminate pipeline failures and freezing caused by insufficient memory (RAM) resources for the Spark JVM. 

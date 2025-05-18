# User Guide

## Real-Time Data Ingestion Using Spark Structured Streaming & PostgreSQL

---

## Overview

This guide explains how to set up, run, and verify the real-time data pipeline for simulated e-commerce event tracking. You’ll use Docker Compose to orchestrate a data generator, Spark Structured Streaming job, PostgreSQL database, and pgAdmin (for monitoring). Following these steps will let you observe data flow from CSV generation, through Spark processing, into persistent relational storage.

---

## Prerequisites

- **Software:**  
  - [Docker](https://docs.docker.com/get-docker/) (latest stable version)
  - [Docker Compose](https://docs.docker.com/compose/) (typically included with Docker Desktop)
- **System:**  
  - Unix/Linux/macOS/Windows with permissions to use Docker
- **Repository:**  
  - Download or clone the project repository to your local machine

---

## Directory Structure

```
real_time_data_pipeline/
├── compose/
│   └── docker-compose.yml
├── docker/
│   ├── Dockerfile.generator
│   └── Dockerfile.spark
├── db/
│   ├── postgres_setup.sql
│   └── pgdata/                 # host-mounted directory for PostgreSQL data
├── data/
│   └── events/                 # host-mounted directory for generated CSVs
├── src/
│   ├── data_generator.py
│   └── spark_streaming_to_postgres.py
├── jars/
│   └── postgresql-xx.x.x.jar   # download PostgreSQL JDBC driver & create folder
├── docs/
│   ├── project_overview.md
│   └── (other .md and .png deliverables)
└── postgres_connection_details.txt
```

---

## Step 1: Clone the Repository

```bash
git clone https://github.com/b-kenneth/DataEng_Phase1_labs.git 
git checkout lab4_spark_structured_streaming
cd ../compose
```

---

## Step 2: Add Environment Variables

Ensure your directory contains a `.env` file, e.g.:

```
POSTGRES_USER=******
POSTGRES_PASSWORD=******
DB_NAME=******
```

---

## Step 3: Build and Launch the Full Pipeline

From the `compose/` directory:

```bash
docker-compose up --build
```

- This command will build images (for the generator and Spark, if custom), start all containers, and set up volumes for data sharing and persistence.
- **Wait** until all containers show "healthy" or "ready" (especially PostgreSQL and Spark).

---

## Step 4: Monitor Services and UIs

### **pgAdmin Web UI (PostgreSQL Monitoring)**
- Visit [http://localhost:8081](http://localhost:8081)
- Login:  
  - Email: `admin@admin.com`  
  - Password: `admin`
- Add a new server:
  - **Host:** `postgres`  
  - **Port:** `5432`  
  - **Username:** As in `.env`  
  - **Password:** As in `.env`  
  - **Database:** As in `.env`

### **Spark UI**
- Visit [http://localhost:8080](http://localhost:8080) for cluster status.
- Visit [http://localhost:4040](http://localhost:4040) for job/stream metrics (only available while a Spark job is running).

---

## Step 5: Verify Data Generator

- The generator service should periodically create CSV files in the `data/events/` host directory.
- Example file: `events_20250501_204015.csv`
- Example row:
    ```
    user_id,session_id,actions,product_id,price,event_time,user_agent
    10001,4e7a257a-...,view,126,,2025-05-01T20:40:11,Mozilla/5.0 ...
    ```
- Confirm new CSVs are being created every 2–5 seconds.

---

## Step 6: Verify Data in PostgreSQL

- In pgAdmin, browse the `events` table in the `sparkdb` database.
- Confirm new rows appear as data is generated and processed.
- Inspect columns for correct types, values, and transformations.

---

## Step 8: Stopping Services

From the `compose/` directory:

```bash
docker-compose down
```

- This will stop all running containers but will **not erase persistent data** (Postgres and CSV files are stored in mapped volumes/directories).

---

## Troubleshooting

- **Postgres or pgAdmin won't connect:** Double-check container names and ports, and ensure the `.env` variables match.
- **No CSV files generated:** Check generator logs, ensure `/data/events` exists on the host.
- **Spark job errors (e.g., JDBC driver):** Confirm correct JAR is mounted and referenced in your spark-submit command.
- **Database schema mismatch:** Reinitialize the database by stopping containers, deleting the contents of `db/pgdata/`, and bringing services up again.

---

## Common Commands

| What you want to do                                  | Command                                                   |
|------------------------------------------------------|-----------------------------------------------------------|
| Build and start all services                         | `docker-compose up --build`                               |
| Stop all services                                    | `docker-compose down`                                     |
| View Spark logs                                      | `docker logs spark`                                       |
| Shell into Spark master                              | `docker exec -it compose-spark-1 bash`                              |
| Shell into generator                                 | `docker exec -it generator bash`                          |
| Manually trigger generator (if needed)               | `python data_generator.py` (inside generator container)   |
| Manually submit Spark job                            | see Step 6 above                                          |

---

## Data Flow Summary

```
[Data Generator] --> [CSV Folder] --> [Spark Stream] --> [Transformed Data] --> [PostgreSQL] --> [pgAdmin]
```

---

## Validation Checklist

- [ ] CSV files appear in `data/events/`
- [ ] Spark job sees new files and processes them
- [ ] Events show up in `events` table in PostgreSQL via pgAdmin
- [ ] Data types and transformations are correct per your test_cases.md
- [ ] Spark UI and pgAdmin show healthy pipeline operation

---

## Where to Go Next

- For test scenarios and expected/actual output, see `test_cases.md`
- For performance benchmarking and tuning, see `performance_metrics.md`
- For architecture visuals, see `docs/system_architecture.png`

# performance_metrics.md

## Real-Time Data Pipeline Performance Metrics

This document summarizes the observed latency and throughput of the real-time data pipeline.

---

### 1. **Latency Measurement**
 
*Latency* is the time elapsed between the generation of a new CSV file by the data generator and the appearance of corresponding records in the PostgreSQL database.

**Measurement Method:**
- Noted the `event_time` field in a specific CSV file and the file’s creation time.
- Observed the time the corresponding record appeared in pgAdmin or the Spark log for batch completion.

**Results Table:**

| CSV File Name               | File Creation Time | First Event `event_time` | DB Insert Time (pgAdmin) | Latency (seconds) |
|-----------------------------|-------------------|-------------------------|--------------------------|-------------------|
| events_20250501_155324.csv  | 15:53:24          | 2025-05-01T15:53:24     | 15:53:28                 | 4                 |
| events_20250501_155416.csv  | 15:54:16          | 2025-05-01T15:54:16     | 15:54:19                 | 3                 |
| ...                         | ...               | ...                     | ...                      | ...               |

**Summary:**  
Average latency across 5 measurements: **~4 seconds** (from CSV file appearance to record in DB).

---

### 2. **Throughput Measurement**

*Throughput* is the number of events processed and inserted into the database per unit of time.

**Measurement Method:**
- Used generator logs and directory listing to count events produced per 30 seconds.
- Queried the events table in PostgreSQL (`SELECT COUNT(*) WHERE event_time >= NOW() - interval '30 seconds';`).
- Cross-verified with Spark UI “Input Rate” and “Processed Rows / sec”.

**Results Table:**

| Interval Measured (seconds) | Total Events Generated | Events Inserted into DB | Ingestion Rate (events/sec) |
|-----------------------------|-----------------------|-------------------------|-----------------------------|
| 30                          | 65                    | 64                      | 2.13                        |
| 60                          | 128                   | 127                     | 2.12                        |
| ...                         | ...                   | ...                     | ...                         |

**Summary:**  
Average throughput: **approx. 2.1 events/second**.

---

### 3. **Performance Bottlenecks or Observations**

- Average file-to-DB latency is well within the 10-second “real-time” target.
- Throughput is limited by data generator speed (5 sec/file, 10–50 records/file) and Spark micro-batch settings.
- The system is able to keep up with the generator rate without backlog or failed batches under test conditions.

---

## Conclusion

The pipeline meets and exceeds specified performance criteria for real-time ingestion and processing in a containerized environment.

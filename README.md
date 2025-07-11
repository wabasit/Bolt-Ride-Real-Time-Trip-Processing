# Real-Time Taxi Trip KPI Aggregation Pipeline

---

## Overview

This project is a **real-time data processing pipeline** designed to capture taxi trip events (start and end), aggregate KPIs daily (total fare, trip count, average fare, durations, etc.), monitor data quality, and visualize aggregated KPIs in **Amazon QuickSight** via **Athena**. Built entirely using serverless and cost-effective AWS services, the system ensures fault-tolerance, modular design, and observability.

---

## Project Architecture
```
┌───────────────────────────────┐
│       Trip Event Stream       │
│       (CSV File Input)        │
└──────────────┬────────────────┘
               │
               ▼
┌───────────────────────────────┐
│        Amazon Kinesis         │
│ (Real-Time Stream Processor)  │
└──────────────┬────────────────┘
               │
               ▼
┌────────────────────────────────────────────┐
│   Lambda Function: KinesisIngestion        │
│ ─ Ingests incoming trip events             │
│ ─ Validates structure                      │
│ ─ Persists events to DynamoDB              │
│   → Writes to `trip_state` table           │
└──────────────┬─────────────────────────────┘
               │
               ▼
        ┌──────────────┐
        │ DynamoDB:    │
        │ trip_state   │
        │ ─ Stores     │
        │   trip_start │
        │  and trip_end│
        └──────┬───────┘
               │
     ┌─────────▼────────────┐
     │ Lambda: monitor_     │
     │   data_quality       │
     │ ─ Scheduled by       │
     │   EventBridge        │
     │ ─ Checks for:        │
     │   - Duplicate IDs    │
     │   - Missing trip_end │
     │   - Orphaned ends    │
     │   - Suspicious fares │
     │ ─ Logs issues        │
     │ ─ Writes to:         │
     │   quarantined_events │
     │ ─ Sends SNS Alerts   │
     └─────────┬────────────┘
               │
               ▼
   ┌──────────────────────────────┐
   │ DynamoDB:                    │
   │ quarantined_events           │
   │ ─ Stores malformed or        │
   │   suspicious trip events     │
   └──────────────────────────────┘

               ▲
               │
        ┌──────┴──────┐
        │             ▼
┌────────────────────────────────────────────┐
│ Lambda Function: aggregate_daily_kpis      │
│ ─ Scheduled by EventBridge                 │
│ ─ Scans `trip_state` for completed trips   │
│ ─ Groups trips by dropoff date             │
│ ─ Computes KPIs:                           │
│   - total_trips, avg_fare, etc.            │
│ ─ Outputs JSON to S3 partitioned by date   │
└──────────────┬─────────────────────────────┘
               │
               ▼
┌───────────────────────────────┐
│        Amazon S3 Bucket       │
│       (bolt-project7)         │
│─ Partitioned by year/month/day│
│─ Stores `kpis.json`           │
└──────────────┬────────────────┘
               ▼
┌───────────────────────────────┐
│         Amazon Athena         │
│ ─ SQL queries on KPIs from S3 │
│ ─ Table partitioned by date   │
│ ─ Used as source in QuickSight│
└──────────────┬────────────────┘
               ▼
┌───────────────────────────────┐
│         Amazon QuickSight     │
│ ─ Visualizes Athena results   │
│ ─ Dashboards: KPIs, trends    │
└───────────────────────────────┘
```

---
![alt text](imgs/architecture.png)

## AWS Services Used and Justifications

| Service         | Purpose                                                           |
| --------------- | ----------------------------------------------------------------- |
| **Kinesis**     | Real-time streaming of taxi trip events.                          |
| **Lambda**      | Serverless compute for ingestion, aggregation, and monitoring.    |
| **DynamoDB**    | Stores intermediate state (`trip_state`) and quarantined records. |
| **EventBridge** | Triggers daily KPI aggregation and monitoring jobs.               |
| **S3**          | Stores partitioned KPI JSON files.                                |
| **Athena**      | Query S3 JSONs to feed into QuickSight.                           |
| **QuickSight**  | Business Intelligence visualization of daily KPIs.                |
| **SNS**         | Sends alerts when data quality issues arise.                      |
| **IAM Roles**   | Grants least-privilege access to all resources.                   |

---

## Setup Instructions

1. **Create AWS Resources** (IAM roles, DynamoDB tables, S3 bucket, Lambda functions, Kinesis stream, etc.)
2. **Deploy Code** using GitHub Actions (CI/CD)
3. **Subscribe Email** to SNS for data quality alerts.
4. **Configure Athena + QuickSight**:

   * Grant QuickSight access to Athena and S3.
   * Create Athena table pointing to `s3://<bucket>/kpis/year=...`
   * Visualize in QuickSight.

---

## Project Directory Structure

```
project7/
├── data/
│   ├── trip_start.csv
│   └── trip_end.csv
├── data-processing/
│   ├── lambda_functions/
│   │   ├── lambda_function.py         # Stream processor
│   │   ├── aggregate_daily_kpis.py    # Daily KPI aggregator
│   │   └── monitor_data_quality.py    # Daily data monitor
│   ├── requirements.txt
├── .github/
│   └── workflows/
│       └── deploy.yml
├── kpis/ (S3 output)
└── README.md
```

---

## Entity Relationship Diagram (ERD)

```
trip_state
----------
trip_id (PK)
status: "started" | "completed"
trip_start: {
    pickup_datetime, city
}
trip_end: {
    dropoff_datetime, fare_amount, duration
}
```
![alt text](imgs/trip_state.png)

```
quarantined_events
------------------
trip_id (PK)
reason
raw_payload
ingest_time
```
![alt text](imgs/quarantine_events.png)
---

## Sample Stream Data (trip\_start)

```json
{
  "event_type": "trip_start",
  "data": {
    "trip_id": "TX-194",
    "pickup_datetime": "2024-05-25 10:45:12",
    "city": "Accra"
  },
  "schema_version": "1.0"
}
```

---

## Sample KPI Data (Stored in S3)

```json
{
  "date": "2024-05-25",
  "total_fare": 1154.23,
  "count_trips": 18,
  "average_fare": 66.47,
  "max_fare": 97.93,
  "min_fare": 15.57,
  "average_duration_minutes": 32.94,
  "city_with_most_trips": "Unknown",
  "year": 2024,
  "month": 05,
  "day": 25
}
```
![alt text](imgs/s3_kpi.png)
---

## Project Flow

1. **CSV Events Streaming**: Custom Python script reads events randomly and pushes to Kinesis.
2. **Lambda (Stream Processor)**: Captures events, merges them into trip state.
3. **Data Quality Monitor**:

   * Scheduled daily.
   * Detects issues (missing end, duplicate ID, invalid fare).
   * Quarantines suspicious data and sends alerts to SNS.
4. **KPI Aggregator**:

   * Groups completed trips by dropoff date.
   * Aggregates metrics and writes partitioned JSON to S3.
5. **Visualization**:

   * Athena reads from S3.
   * QuickSight dashboards track daily performance.
   ![alt text](imgs/kpi_analytics.png)
   ![alt text](imgs/quicksight.png)

---

## Core Lambda Snippets

### Stream Processor (lambda\_function.py)

```python
# Parses event, stores to trip_state
status = "started" if event_type == "trip_start" else "completed"
table.update_item(
    Key={"trip_id": trip_id},
    UpdateExpression=f"SET {status} = :val, #s = :status",
    ExpressionAttributeValues={":val": event["data"], ":status": status},
    ExpressionAttributeNames={"#s": "status"}
)
```

### Aggregator (aggregate\_daily\_kpis.py)

```python
# Group trips by date and calculate KPIs
for date, trips in grouped.items():
    kpi = calculate_kpis(trips)
    write_to_s3_partitioned(date, kpi)
```

### Monitor (monitor\_data\_quality.py)

```python
# Check data issues
if status == "started" and trip has no end after 6 hours:
    log_issue(trip_id, "Trip has not ended after 6+ hours", item)
```

---

## EventBridge Configuration

```bash
aws events put-rule \
  --name "DailyAggregateKPI" \
  --schedule-expression "cron(0 0 * * ? *)"

aws events put-rule \
  --name "DailyDataQualityCheck" \
  --schedule-expression "cron(30 0 * * ? *)"
```

---

## SNS Configuration

```bash
aws sns create-topic --name data-quality-alerts
aws sns subscribe \
  --topic-arn arn:aws:sns:<region>:<account>:data-quality-alerts \
  --protocol email \
  --notification-endpoint your_email@example.com
```

---

## GitHub Actions CI/CD Pipeline

* Deploys and updates Lambda code
* Manages S3, SNS, EventBridge
* Injects environment variables

```yaml
- name: Set Lambda Env Vars
  run: |
    aws lambda update-function-configuration \
    --function-name monitor_data_quality \
    --environment "Variables={SNS_TOPIC_ARN=..., AWS_REGION=..., AWS_ACCOUNT_ID=...}"
```

---

## Security Measures Implemented

* IAM least-privilege roles for Lambda, QuickSight, Athena
* Explicit permissions for S3 (`s3:GetObject`, `s3:ListBucket`)
* SNS topic restricted to Lambda only
* GitHub secrets used for credentials
* No hardcoded sensitive data

---

## Cost Optimization Strategies

* Serverless stack: No EC2 or managed clusters
* DynamoDB on-demand billing
* Athena charged only on query (pay-per-scan)
* EventBridge & SNS only cost per invocation
* S3 Lifecycle rules (optional)

---

## Challenges and Resolutions

| Challenge                  | Resolution                                                      |
| -------------------------- | --------------------------------------------------------------- |
| Lambda update conflicts    | Introduced delays between updates in GitHub Actions             |
| QuickSight Access Denied   | Enabled access via QuickSight UI under permissions              |
| IAM\_ROLE\_UNAVAILABLE     | Authorized QuickSight to access Athena and S3                   |
| SNS Permission Errors      | Added Lambda-to-SNS publish policy and topic permissions        |
| EventBridge Rule Conflicts | Used unique `statement-id` to avoid `ResourceConflictException` |

---

## Why This Design is Efficient

* Real-time ingestion & near real-time analytics
* Event-driven: no polling, no batch delays
* Auto-scalable with Lambda & Kinesis
* Partitioned S3 layout for Athena optimization
* Visual dashboards with near-zero latency

---

## Conclusion

This real-time KPI pipeline is a modular, serverless, and production-ready system tailored for scalable analytics. It demonstrates effective use of AWS services, thoughtful design of data flows, observability mechanisms, cost efficiency, and proper DevOps with GitHub Actions.

All errors encountered were mitigated with proper debugging and service-level permissions. With rich dashboards from QuickSight and real-time anomaly detection via SNS, this project is a complete pipeline from ingestion to insight.

---

> Author: \A.W. Basit
> Project Date: 11 July, 2025
> Contact: [awbasit99@gmail.com](mailto:awbasit99@gmail.com)
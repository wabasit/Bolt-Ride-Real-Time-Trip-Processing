import boto3
from datetime import datetime, timedelta
from decimal import Decimal
import json
import os

# AWS Config
REGION = os.environ["AWS_REGION"]
ACCOUNT_ID = os.environ["AWS_ACCOUNT_ID"]
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:data-quality-alerts"

# AWS Resources
dynamodb = boto3.resource('dynamodb', region_name=REGION)
sns = boto3.client("sns", region_name=REGION)
table = dynamodb.Table("trip_state")
quarantine_table = dynamodb.Table("quarantined_events")

THRESHOLD_HOURS = 6
issues_logged = []

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

def log_issue(trip_id, issue, data):
    record = {
        "trip_id": trip_id,
        "reason": issue,
        "raw_payload": data,
        "ingest_time": datetime.utcnow().isoformat()
    }

    quarantine_table.put_item(Item=record)
    issues_logged.append(record)
    print(f"Quarantined {trip_id}: {issue}")

def send_alert():
    if not issues_logged:
        print("No data issues found.")
        return

    subject = f"Data Quality Issues Detected: {len(issues_logged)} records"
    message = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_issues": len(issues_logged),
        "issues": issues_logged[:10],  # show only first 10 to limit size
    }

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=json.dumps(message, default=str)
    )
    print("Alert sent to SNS topic.")

def lambda_handler(event=None, context=None):
    print("Running data quality checks...")

    response = table.scan()
    items = response.get("Items", [])

    seen_trip_ids = set()

    for item in items:
        trip_id = item.get("trip_id")
        status = item.get("status")
        start = item.get("trip_start", {})
        end = item.get("trip_end", {})

        # Duplicate trip_id
        if trip_id in seen_trip_ids:
            log_issue(trip_id, "Duplicate trip_id", item)
        else:
            seen_trip_ids.add(trip_id)

        # Missing trip_end after 6 hours
        if status == "started":
            try:
                start_time = datetime.strptime(start["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
                if datetime.utcnow() - start_time > timedelta(hours=THRESHOLD_HOURS):
                    log_issue(trip_id, "Trip has not ended after 6+ hours", item)
            except Exception as e:
                log_issue(trip_id, f"Invalid start datetime format: {e}", item)

        # Orphan trip_end
        if status == "completed" and not start:
            log_issue(trip_id, "Trip has trip_end but no trip_start", item)

        # Suspicious fare
        if end and "fare_amount" in end:
            try:
                fare = float(end["fare_amount"])
                if fare < 1 or fare > 500:
                    log_issue(trip_id, "Suspicious fare", item)
            except Exception as e:
                log_issue(trip_id, f"Invalid fare value: {e}", item)

    send_alert()
    print("Data quality check completed.")

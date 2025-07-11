import base64
import json
import boto3
from datetime import datetime

# Initializing DynamoDB clients for future use
dynamodb = boto3.resource('dynamodb')
trip_state_table = dynamodb.Table('trip_state')
quarantine_table = dynamodb.Table('quarantined_events')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Decode base64 payload
            payload_raw = base64.b64decode(record['kinesis']['data']).decode('utf-8')

            # Parse JSON
            event_data = json.loads(payload_raw)

            # Extract common fields
            event_type = event_data.get("event_type")
            schema_version = event_data.get("schema_version")
            event_body = event_data.get("data")

            print(f"Received {event_type} event: {event_body.get('trip_id', 'unknown')}")

            # Route by event type (we’ll flesh this out later)
            if event_type == "trip_start":
                handle_trip_start(event_body)
            elif event_type == "trip_end":
                handle_trip_end(event_body)
            else:
                raise ValueError(f"Unsupported event_type: {event_type}")

        except (json.JSONDecodeError, ValueError, KeyError) as e:
            print("Failed to process record")
            print("Error:", e)
            print("Raw Payload:", record['kinesis']['data'])

            # Optionally quarantine the raw record (decoded)
            quarantine_table.put_item(Item={
                "event_id": record['eventID'],
                "raw_data": base64.b64decode(record['kinesis']['data']).decode('utf-8'),
                "error": str(e)
            })


def handle_trip_start(event_body):
    trip_id = event_body.get("trip_id")
    if not trip_id:
        raise ValueError("Missing trip_id in trip_start")

    trip_state_table.put_item(Item={
        "trip_id": trip_id,
        "status": "started",
        "trip_start": event_body
    })


def handle_trip_end(event_body):
    trip_id = event_body.get("trip_id")
    if not trip_id:
        raise ValueError("Missing trip_id in trip_end")

    response = trip_state_table.get_item(Key={"trip_id": trip_id})
    item = response.get("Item")

    if not item or "trip_start" not in item:
        raise ValueError(f"No matching trip_start for trip_id {trip_id}")

    # Merge trip_end with existing record
    trip_state_table.put_item(Item={
        "trip_id": trip_id,
        "status": "completed",
        "trip_start": item["trip_start"],
        "trip_end": event_body
    })


def quarantine(trip_id, raw_event, reason):
    quarantine_table.put_item(Item={
        "trip_id": trip_id or f"unknown-{datetime.utcnow().timestamp()}",
        "event_type": raw_event.get("event_type", "UNKNOWN"),
        "reason": reason,
        "raw_payload": raw_event,
        "ingest_time": datetime.utcnow().isoformat()
    })
import json
import boto3
import os
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
trip_state_table = dynamodb.Table(os.environ["TRIP_STATE_TABLE"])
quarantine_table = dynamodb.Table(os.environ["QUARANTINE_TABLE"])

def lambda_handler(event, context):
    for record in event["Records"]:
        payload = json.loads(record["kinesis"]["data"])
        event_type = payload.get("event_type")
        event_data = payload.get("data")
        trip_id = event_data.get("trip_id")

        if not event_type or not event_data or not trip_id:
            quarantine("UNKNOWN", payload, "INVALID_SCHEMA")
            continue

        try:
            if event_type == "trip_start":
                handle_trip_start(trip_id, event_data)
            elif event_type == "trip_end":
                handle_trip_end(trip_id, event_data)
            else:
                quarantine(trip_id, payload, "UNKNOWN_EVENT_TYPE")
        except Exception as e:
            quarantine(trip_id, payload, f"PROCESSING_ERROR: {str(e)}")

    return {"statusCode": 200}

def handle_trip_start(trip_id, event_data):
    trip_state_table.update_item(
        Key={"trip_id": trip_id},
        UpdateExpression="""
            SET pickup_datetime = :pickup,
                estimated_fare_amount = :fare,
                trip_start_data = :raw,
                state = if_not_exists(state, :partial)
        """,
        ExpressionAttributeValues={
            ":pickup": event_data.get("pickup_datetime"),
            ":fare": float(event_data.get("estimated_fare_amount", 0)),
            ":raw": event_data,
            ":partial": "PARTIAL"
        }
    )

def handle_trip_end(trip_id, event_data):
    response = trip_state_table.get_item(Key={"trip_id": trip_id})
    if "Item" not in response:
        quarantine(trip_id, event_data, "ORPHAN_END")
        return

    trip_state_table.update_item(
        Key={"trip_id": trip_id},
        UpdateExpression="""
            SET dropoff_datetime = :drop,
                fare_amount = :fare,
                trip_end_data = :raw,
                state = :complete
        """,
        ExpressionAttributeValues={
            ":drop": event_data.get("dropoff_datetime"),
            ":fare": float(event_data.get("fare_amount", 0)),
            ":raw": event_data,
            ":complete": "COMPLETE"
        }
    )

def quarantine(trip_id, raw_event, reason):
    quarantine_table.put_item(Item={
        "trip_id": trip_id or f"unknown-{datetime.utcnow().timestamp()}",
        "event_type": raw_event.get("event_type", "UNKNOWN"),
        "reason": reason,
        "raw_payload": raw_event,
        "ingest_time": datetime.utcnow().isoformat()
    })
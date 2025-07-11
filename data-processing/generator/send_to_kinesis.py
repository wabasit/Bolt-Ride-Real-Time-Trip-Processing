import csv
import json
import time
import random
import boto3

KINESIS_STREAM_NAME = "trip-events"
REGION = "eu-north-1"

# Track trip_ids that have started
started_trips = set()

# Clients
kinesis_client = boto3.client("kinesis", region_name=REGION)
dynamodb_client = boto3.client("dynamodb", region_name=REGION)


def send_event_to_kinesis(event: dict, event_type: str):
    record = {
        "event_type": event_type,
        "schema_version": "1.0",
        "data": event
    }

    partition_key = event.get("trip_id", str(random.randint(1, 100000)))

    response = kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=json.dumps(record),
        PartitionKey=partition_key
    )

    print(f"Sent {event_type}: {event['trip_id']} | Seq: {response['SequenceNumber']}")


def quarantine_event(event: dict, reason: str):
    event_id = event.get("trip_id", f"unknown-{random.randint(1000, 9999)}")
    print(f"Quarantining {event_id}: {reason}")

    dynamodb_client.put_item(
        TableName="quarantined_events",
        Item={
            "trip_id": {"S": event_id},
            "event_data": {"S": json.dumps(event)},
            "reason": {"S": reason},
            "timestamp": {"S": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        }
    )


def stream_csv_events(csv_path, event_type, delay=1.0):
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trip_id = row.get("trip_id")

            if event_type == "trip_start":
                started_trips.add(trip_id)
                send_event_to_kinesis(row, event_type)

            elif event_type == "trip_end":
                if trip_id not in started_trips:
                    quarantine_event(row, "Trip end received before trip start")
                else:
                    send_event_to_kinesis(row, event_type)

            time.sleep(delay)


def main():
    while True:
        if random.choice(["start", "end"]) == "start":
            print("Streaming trip_start event...")
            stream_csv_events(r"data\trip_start.csv", "trip_start", delay=0.5)
        else:
            print("Streaming trip_end event...")
            stream_csv_events(r"data\trip_end.csv", "trip_end", delay=0.5)


if __name__ == "__main__":
    main()
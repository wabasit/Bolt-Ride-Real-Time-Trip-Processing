import csv
import json
import time
import random
import boto3

KINESIS_STREAM_NAME = "trip-events"
REGION = "eu-north-1"

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


def load_csv_events(path: str):
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        return list(reader)


def stream_fixed_batch(trip_start_path, trip_end_path, batch_size=10, delay=1.0):
    trip_starts = load_csv_events(trip_start_path)[:batch_size]
    trip_ends = load_csv_events(trip_end_path)[:batch_size]

    trip_id_set = set()

    print(f"\nSending {batch_size} trip_start events...")
    for row in trip_starts:
        trip_id_set.add(row["trip_id"])
        send_event_to_kinesis(row, "trip_start")
        time.sleep(delay)

    print(f"\nSending {batch_size} trip_end events...")
    for row in trip_ends:
        if row["trip_id"] in trip_id_set:
            send_event_to_kinesis(row, "trip_end")
        else:
            quarantine_event(row, "Trip end received before trip start")
        time.sleep(delay)


def main():
    stream_fixed_batch(
        trip_start_path=r"data\trip_start.csv",
        trip_end_path=r"data\trip_end.csv",
        batch_size=10,
        delay=0.5
    )


if __name__ == "__main__":
    main()
import csv
import json
import time
import random
import boto3

KINESIS_STREAM_NAME = "trip-events"
REGION = "eu-north-1"  # Change if needed

kinesis_client = boto3.client("kinesis", region_name=REGION)

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

    print(f"Sent {event_type} event: {event['trip_id']} | Response: {response['SequenceNumber']}")


def stream_csv_events(csv_path, event_type, delay=1.0):
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            send_event_to_kinesis(row, event_type)
            time.sleep(delay)


def main():
    # You can stream both files in parallel or one after the other
    print("Streaming trip_start events...")
    stream_csv_events(r"data\trip_start.csv", "trip_start", delay=0.5)

    print("Streaming trip_end events...")
    stream_csv_events(r"data\trip_end.csv", "trip_end", delay=0.5)


if __name__ == "__main__":
    main()

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

    print(f"✅ Sent {event_type}: {event['trip_id']} | Seq: {response['SequenceNumber']}")


def quarantine_event(event: dict, reason: str):
    event_id = event.get("trip_id", f"unknown-{random.randint(1000, 9999)}")
    print(f"🚨 Quarantining {event_id}: {reason}")

    dynamodb_client.put_item(
        TableName="quarantined_events",
        Item={
            "trip_id": {"S": event_id},
            "event_data": {"S": json.dumps(event)},
            "reason": {"S": reason},
            "timestamp": {"S": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        }
    )


def load_csv_events(path: str, label: str):
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        return [{"event": row, "type": label} for row in reader]


def stream_randomized(trip_start_path, trip_end_path, delay=0.5):
    trip_starts = load_csv_events(trip_start_path, "trip_start")
    trip_ends = load_csv_events(trip_end_path, "trip_end")

    combined = trip_starts + trip_ends
    random.shuffle(combined)

    seen_trip_ids = set()

    print(f"\n🎲 Streaming {len(combined)} randomized events...\n")
    for item in combined:
        event = item["event"]
        event_type = item["type"]
        trip_id = event.get("trip_id")

        # Realistic quarantine logic for ends without starts
        if event_type == "trip_end" and trip_id not in seen_trip_ids:
            quarantine_event(event, "Trip end received before trip start")
        else:
            send_event_to_kinesis(event, event_type)
            if event_type == "trip_start":
                seen_trip_ids.add(trip_id)

        time.sleep(delay)


def main():
    stream_randomized(
        trip_start_path=r"data/trip_start.csv",
        trip_end_path=r"data/trip_end.csv",
        delay=0.3  # Increase speed for more activity
    )


if __name__ == "__main__":
    main()
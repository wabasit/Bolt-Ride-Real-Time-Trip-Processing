import boto3
from decimal import Decimal
from collections import defaultdict, Counter
from datetime import datetime
import json

# Constants
TABLE_NAME = "trip_state"
BUCKET_NAME = "bolt-project7"
OUTPUT_PREFIX = "kpis/"
REGION = "eu-north-1"

dynamodb = boto3.resource("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

def fetch_completed_trips():
    response = table.scan(
        FilterExpression="attribute_exists(trip_end) AND #status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":status": "completed"}
    )
    return response.get("Items", [])

def group_by_date(trips):
    grouped = defaultdict(list)
    for trip in trips:
        dropoff_datetime = trip["trip_end"].get("dropoff_datetime")
        if dropoff_datetime:
            dropoff_date = dropoff_datetime.split(" ")[0]
            grouped[dropoff_date].append(trip)
    return grouped

def calculate_kpis(trips):
    fares = []
    durations = []
    cities = []

    for trip in trips:
        try:
            fare = float(trip["trip_end"]["fare_amount"])
            fares.append(fare)

            start_time = datetime.strptime(trip["trip_start"]["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
            end_time = datetime.strptime(trip["trip_end"]["dropoff_datetime"], "%Y-%m-%d %H:%M:%S")
            duration_minutes = (end_time - start_time).total_seconds() / 60
            durations.append(duration_minutes)

            city = trip["trip_start"].get("pickup_location", {}).get("city", "Unknown")
            if city:
                cities.append(city)
        except Exception as e:
            print(f"Skipping trip due to error: {e}")

    city_counter = Counter(cities)
    top_city = city_counter.most_common(1)[0][0] if city_counter else "Unknown"

    return {
        "total_trips": len(fares),
        "average_fare": round(sum(fares) / len(fares), 2) if fares else 0,
        "highest_fare": max(fares) if fares else 0,
        "lowest_fare": min(fares) if fares else 0,
        "average_duration_minutes": round(sum(durations) / len(durations), 2) if durations else 0,
        "city_with_most_trips": top_city
    }

def write_to_s3_partitioned(date_str, kpi):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%m")
    day = date_obj.strftime("%d")

    s3_key = f"{OUTPUT_PREFIX}year={year}/month={month}/day={day}/kpis.json"
    
    body = json.dumps({
        "date": date_str,
        **kpi,
        "year": year,
        "month": month,
        "day": day
    }, default=convert_decimal)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=body,
        ContentType="application/json"
    )

    print(f"Uploaded KPIs for {date_str} to s3://{BUCKET_NAME}/{s3_key}")

def lambda_handler(event, context):
    main()

def main():
    print("Fetching completed trips...")
    trips = fetch_completed_trips()
    grouped = group_by_date(trips)

    for date, date_trips in grouped.items():
        kpis = calculate_kpis(date_trips)
        write_to_s3_partitioned(date, kpis)

if __name__ == "__main__":
    main()
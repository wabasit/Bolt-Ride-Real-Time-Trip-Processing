from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKinesisConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
import json
import boto3
from datetime import datetime

# Constants
STREAM_NAME = "trip-events"
REGION = "eu-north-1"
DYNAMODB_TABLE = "trip_state"

# DynamoDB client
ddb = boto3.client("dynamodb", region_name=REGION)

# Trip processing function
class TripEventProcessor(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        # You can initialize any local state here if needed
        pass

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        try:
            event_record = json.loads(value)
            event_type = event_record.get("event_type")
            trip_id = event_record["data"].get("trip_id")
            data = event_record["data"]

            if event_type == "trip_start":
                # Put or update the trip with partial data
                ddb.update_item(
                    TableName=DYNAMODB_TABLE,
                    Key={"trip_id": {"S": trip_id}},
                    UpdateExpression="SET pickup_location_id=:pl, dropoff_location_id=:dl, vendor_id=:v, pickup_datetime=:pd, estimated_dropoff_datetime=:edd, estimated_fare_amount=:efa, updated_at=:now",
                    ExpressionAttributeValues={
                        ":pl": {"S": data["pickup_location_id"]},
                        ":dl": {"S": data["dropoff_location_id"]},
                        ":v": {"S": data["vendor_id"]},
                        ":pd": {"S": data["pickup_datetime"]},
                        ":edd": {"S": data["estimated_dropoff_datetime"]},
                        ":efa": {"N": str(data["estimated_fare_amount"])},
                        ":now": {"S": datetime.utcnow().isoformat()}
                    },
                    ReturnValues="NONE"
                )

            elif event_type == "trip_end":
                # Update trip with end details and mark complete
                ddb.update_item(
                    TableName=DYNAMODB_TABLE,
                    Key={"trip_id": {"S": trip_id}},
                    UpdateExpression="SET dropoff_datetime=:dd, rate_code=:rc, passenger_count=:pc, trip_distance=:td, fare_amount=:fa, tip_amount=:ta, payment_type=:pt, trip_type=:tt, trip_complete=:done, updated_at=:now",
                    ExpressionAttributeValues={
                        ":dd": {"S": data["dropoff_datetime"]},
                        ":rc": {"N": str(data["rate_code"])},
                        ":pc": {"N": str(data["passenger_count"])},
                        ":td": {"N": str(data["trip_distance"])},
                        ":fa": {"N": str(data["fare_amount"])},
                        ":ta": {"N": str(data["tip_amount"])},
                        ":pt": {"N": str(data["payment_type"])},
                        ":tt": {"N": str(data["trip_type"])},
                        ":done": {"BOOL": True},
                        ":now": {"S": datetime.utcnow().isoformat()}
                    },
                    ReturnValues="NONE"
                )

        except Exception as e:
            print(f"Error processing event: {e}")

# Setup execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Add source: Kinesis consumer
properties = {
    'aws.region': REGION,
    'stream.initial.position': 'LATEST'
}

kinesis_consumer = FlinkKinesisConsumer(
    STREAM_NAME,
    SimpleStringSchema(),
    properties
)

data_stream = env.add_source(kinesis_consumer).name("KinesisSource")

# Process and send to DynamoDB
data_stream \
    .key_by(lambda record: json.loads(record)["data"].get("trip_id"), key_type=Types.STRING()) \
    .process(TripEventProcessor())

# Execute the job
env.execute("Trip Event to DynamoDB Processor")


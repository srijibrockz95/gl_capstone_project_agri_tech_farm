import boto3
from decimal import Decimal
import json
from datetime import datetime


# function to create aggregate table

def create_aggregate_data_table(dynamodb):

    table = dynamodb.create_table(
        TableName='aggregate_data',
        KeySchema=[
            {
                'AttributeName': 'sprinkler_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'sensor_timestamp',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'sprinkler_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'sensor_timestamp',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    print("Table status:", table.table_status)


# function to create anomaly table

def create_anomaly_data_table(dynamodb):

    table = dynamodb.create_table(
        TableName='anomaly_data',
        KeySchema=[
            {
                'AttributeName': 'sensor_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'
            }

        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'sensor_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    print("Table status:", table.table_status)


# function to create sprinkler master table

def create_device_data_table(dynamodb):
    table = dynamodb.create_table(
        TableName='device_data',
        KeySchema=[
            {
                'AttributeName': 'device_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'device_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    print("Table status:", table.table_status)


# function to insert data into sprinkler table

def insert_device_data(dynamodb):
    table = dynamodb.Table('device_data')
    sprinkler_latitude = 28.5355
    sprinkler_longitude = 77.3910
    sensor_latitude = 28.5355
    sensor_longitude = 77.3910
    timestamp = str(datetime.now())
    # data for sprinklers
    for i in range(1, 6):
        raw_data = {
            # The PK and the sort keys are mandatory
            'device_id': f'Sprinkler{i}',
            'device_status': 'OFF',
            # Due to the schemaless nature the following keys are not required in the table definition
            'device_lat': sprinkler_latitude,
            'device_long': sprinkler_longitude,
            'device_timestamp': timestamp,
            'device_type': 'sprinkler'
        }
        ddb_data = json.loads(json.dumps(raw_data), parse_float=Decimal)
        table.put_item(
            Item=ddb_data
        )
        sprinkler_latitude += 5
        sprinkler_longitude += 5

    # data for sensors
    for i in range(20):
        raw_data = {
            # The PK and the sort keys are mandatory
            'device_id': f'Sensor{i}',
            'device_status': 'OFF',
            # Due to the schemaless nature the following keys are not required in the table definition
            'device_lat': sensor_latitude,
            'device_long': sensor_longitude,
            'device_timestamp': timestamp,
            'device_type': 'sensor'
        }
        ddb_data = json.loads(json.dumps(raw_data), parse_float=Decimal)
        table.put_item(
            Item=ddb_data
        )
        sensor_latitude += 1
        sensor_longitude += 1

#  function to add global secondary index in anomaly table


def create_gsi_anomaly_table():
    client = boto3.client('dynamodb', region_name='us-east-1')

    try:
        resp = client.update_table(
            TableName="anomaly_data",
            # Any attributes used in your new global secondary index must be declared in AttributeDefinitions
            AttributeDefinitions=[
                {
                    "AttributeName": "sensor_id",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "timestamp",
                    "AttributeType": "S"
                }
            ],
            # This is where you add, update, or delete any global secondary indexes on your table.
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        # You need to name your index and specifically refer to it when using it for queries.
                        "IndexName": "timestampIndex",
                        # Like the table itself, you need to specify the key schema for an index.
                        # For a global secondary index, you can use a simple or composite key schema.
                        "KeySchema": [
                            {
                                "AttributeName": "sensor_id",
                                "KeyType": "HASH"
                            },
                            {
                                "AttributeName": "timestamp",
                                "KeyType": "RANGE"
                            }
                        ],
                        # You can choose to copy only specific attributes from the original item into the index.
                        # You might want to copy only a few attributes to save space.
                        "Projection": {
                            "ProjectionType": "ALL"
                        },
                        # Global secondary indexes have read and write capacity separate from the underlying table.
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1,
                        }
                    }
                }
            ],
        )
        print("Secondary index added!")
    except Exception as e:
        print("Error updating table:")
        print(e)

# function to create SNS


def create_sns(sns_client):
    response = sns_client.create_topic(Name="weather_data_sns_topic")
    topic_arn = response["TopicArn"]
    # Create email subscription
    sub_response = sns_client.subscribe(
        TopicArn=topic_arn, Protocol="email", Endpoint="rekhacthomas@gmail.com")
    print(sub_response)


# function to test lambda code

def test_code_lambda():

    # sns
    sns_client = boto3.client('sns', region_name='us-east-1')
    # change required
    topic_arn = "arn:aws:sns:us-east-1:212546747799:weather_data_sns_topic"
    print("SNS starting")
    message = f"\n Hello, \n\n Weather data anomaly detected on  for sensors: ."
    # subject = f"{timestamp} Weather data anomaly detected for {sensor_id} and current OWM weather data. Please  turn on the sprinkler: {sprinkler_id}"
    sns_client.publish(TopicArn=topic_arn, Message=message)

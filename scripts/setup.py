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

def create_sprinkler_data_table(dynamodb):
    table = dynamodb.create_table(
        TableName='sprinkler_data',
        KeySchema=[
            {
                'AttributeName': 'sprinkler_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'sprinkler_id',
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

def insert_sprinkler_data(dynamodb):
    table = dynamodb.Table('sprinkler_data')
    latitude = 28.5355
    longitude = 77.3910
    timestamp = str(datetime.now())
    for i in range(1, 6):
        raw_data = {
            # The PK and the sort keys are mandatory
            'sprinkler_id': f'Sprinkler{i}',
            'sprinkler_status': 'OFF',
            # Due to the schemaless nature the following keys are not required in the table definition
            'sprinkler_lat': latitude,
            'sprinkler_long': longitude,
            'timestamp': timestamp
        }
        ddb_data = json.loads(json.dumps(raw_data), parse_float=Decimal)
        table.put_item(
            Item=ddb_data
        )
        latitude += 5
        longitude += 5


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

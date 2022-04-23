import boto3
from decimal import Decimal
import json
from datetime import datetime

# object to create dynamodb & sns client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
sns_client = boto3.client("sns", region_name="us-east-1")


# function to create aggregate table

def create_aggregate_data_table():

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

def create_anomaly_data_table():

    table = dynamodb.create_table(
        TableName='anomaly_data',
        KeySchema=[
            {
                'AttributeName': 'data_type',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'data_type',
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

def create_sprinkler_data_table():
    table = dynamodb.create_table(
        TableName='sprinkler_data',
        KeySchema=[
            {
                'AttributeName': 'sprinkler_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'sprinkler_id',
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


# function to insert data into sprinkler table

def insert_sprinkler_data():
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
            'latitude': latitude,
            'longitude': longitude,
            'timestamp': timestamp
        }
        ddb_data = json.loads(json.dumps(raw_data), parse_float=Decimal)
        table.put_item(
            Item=ddb_data
        )
        latitude += 1
        longitude += 1


# function to create SNS

def create_sns():
    response = sns_client.create_topic(Name="weather_data_sns_topic")
    topic_arn = response["TopicArn"]
    # Create email subscription
    sub_response = sns_client.subscribe(
        TopicArn=topic_arn, Protocol="email", Endpoint="rekhacthomas@gmail.com")
    print(sub_response)


# function to test lambda code

def test_code_lambda():

    sprinkler_table_name = 'sprinkler_data'
    sprinkler_table = boto3.resource(
        'dynamodb').Table(sprinkler_table_name)
    update_resp = sprinkler_table.update_item(
        Key={
            # sprinkler_id, latitude, longitude, status
            'sprinkler_id': 'Sprinkler2'
        },
        UpdateExpression='set sprinkler_status = :val1',
        ExpressionAttributeValues={
            ':val1': 'ON'
        },
        ReturnValues="UPDATED_NEW"
    )
    print(update_resp)

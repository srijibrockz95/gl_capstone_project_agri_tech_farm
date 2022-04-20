
from unicodedata import decimal
from urllib import response
import boto3
from decimal import Decimal


class setup:

    # Place this code in main.py
    # dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    # sns_client = boto3.client("sns", region_name="us-east-1")

    # Create anomaly table

    def create_anomaly_data_table(self, dynamodb):

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
    # Create aggregate_data table

    def create_sprinkler_data_table(self, dynamodb):
        table = dynamodb.create_table(
            TableName='sprinkler_data',
            KeySchema=[
                {
                    'AttributeName': 'sprinkler_id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'status',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'sprinkler_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'status',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

    def insert_sprinkler_data(self, dynamodb):
        table = dynamodb.Table('sprinkler_data')
        latitude = 98.9
        longitude = 100.5
        for i in range(5):
            table.put_item(
                Item={
                    # The PK and the sort keys are mandatory
                    'sprinkler_id': f'sprinkler_{i}',
                    'status': 'OFF',
                    # Due to the schemaless nature the following keys are not required in the table definition
                    'latitude': str(latitude),
                    'longitude': str(longitude)
                }
            )
            latitude += 1
            longitude += 1

    # Create SNS
    # sns_client = boto3.client("sns", region_name="us-east-1")
    def create_sns(self):
        sns_client = boto3.client("sns", region_name="us-east-1")
        response = sns_client.create_topic(Name="weather_data_sns_topic")
        topic_arn = response["TopicArn"]
        # Create email subscription
        sub_response = sns_client.subscribe(
            TopicArn=topic_arn, Protocol="email", Endpoint="rekhacthomas@gmail.com")
        subscription_arn = response["sub_response"]

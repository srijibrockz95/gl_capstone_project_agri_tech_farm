
from unicodedata import decimal
from urllib import response
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
import json


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

                # },
                # {
                #     'AttributeName': 'sprinkler_status',
                #     'KeyType': 'RANGE'
                # }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'sprinkler_id',
                    'AttributeType': 'S'
                }
                # },
                # {
                #     'AttributeName': 'sprinkler_status',
                #     'AttributeType': 'S'
                # }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Table status:", table.table_status)

    def insert_sprinkler_data(self, dynamodb):
        table = dynamodb.Table('sprinkler_data')
        latitude = 28.5355
        longitude = 77.3910
        for i in range(1, 6):
            raw_data = {
                # The PK and the sort keys are mandatory
                'sprinkler_id': f'Sprinkler{i}',
                'sprinkler_status': 'OFF',
                # Due to the schemaless nature the following keys are not required in the table definition
                'latitude': latitude,
                'longitude': longitude
            }
            ddb_data = json.loads(json.dumps(raw_data), parse_float=Decimal)
            table.put_item(
                Item=ddb_data
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
        # subscription_arn = response["sub_response"]

    def test_code_lambda(self):
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

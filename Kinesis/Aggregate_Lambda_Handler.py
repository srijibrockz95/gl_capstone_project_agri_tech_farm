from datetime import datetime
import json
from urllib import response
import boto3
import base64
from pprint import pprint


def lambda_handler(event, context):
    # Dynamodb
    tablename = 'aggregate_data'
    table = boto3.resource('dynamodb').Table(tablename)

    for record in event['records']:
        data = base64.b64decode(record['data'])
        data = str(data, 'utf-8')
        readings = json.loads(data)
        sprinklerid = readings['SPRINKLER_ID']
        sensor_id = readings['SENSOR_ID']
        sensor_timestamp = readings['SENSOR_TIMESTAMP']
        avg_temp = float(readings['AVG_TEMPERATURE'])
        max_temp = float(readings['MAX_TEMPERATURE'])
        min_temp = float(readings['MIN_TEMPERATURE'])
        avg_moisture = float(readings['AVG_MOISTURE'])
        max_moisture = float(readings['MAX_MOISTURE'])
        min_moisture = float(readings['MIN_MOISTURE'])
        sensor_lat = float(readings['SENSOR_LAT'])
        sensor_long = float(readings['SENSOR_LONG'])
        table.put_item(Item={'sprinkler_id': sprinklerid, 'sensor_id': sensor_id, 'sensor_timestamp': sensor_timestamp,
                             'avg_temp': str(avg_temp), 'max_temp': str(max_temp),
                             'min_temp': str(min_temp), 'avg_moisture': str(avg_moisture), 'max_moisture': str(max_moisture),
                             'min_moisture': str(min_moisture), 'sensor_lat': str(sensor_lat), 'sensor_long': str(sensor_long)})

    print("calling off method")
    change_sprinkler_status_off()


def change_sprinkler_status_off():
    # first scan sprinkler table and get all data
    print("in the method")
    table_name = 'sprinkler_data'
    sprinkler_table = boto3.resource('dynamodb').Table(table_name)
    # sns
    sns_client = boto3.client('sns', region_name='us-east-1')
    # change required
    topic_arn = "arn:aws:sns:us-east-1:212546747799:weather_data_sns_topic"
    # iot-core
    iot_client = boto3.client(
        'iot-data', region_name='us-east-1', verify=False)
    # loop through all sprinklers and check if the db timestamp is >= 10 mins
    response = sprinkler_table.scan()
    for item in response['Items']:
        # convert database string datetime to datetime datatype

        print(f"sprinkler_timestamp: {item['sprinkler_timestamp']}")

        db_datetime = (datetime.fromisoformat(item['sprinkler_timestamp']))

        current_time = datetime.now()
        print(f"Current_time: {current_time}")

        # (compare with current time)
        duration = current_time-db_datetime
        duration_in_seconds = duration.total_seconds()
        minutes = divmod(duration_in_seconds, 60)[0]
        print(f"minutes: {minutes}")
        if minutes >= 0:
            # if yes, turn oFF sprinkler, SNS, IoT MQTT
            # while turning off update sprinkler_status and timestamp.
            # so need to delete the record and insert with new values as timestamp is sort key.
            print("dynamodb update starting")
            current_datetime = str(datetime.now())
            sprinkler_table.update_item(
                Key={
                    'sprinkler_id': item['sprinkler_id']

                },
                UpdateExpression='SET sprinkler_status = :val1, sprinkler_timestamp = :val2',
                ExpressionAttributeValues={
                    ':val1': 'OFF',
                    ':val2': current_datetime

                })

            # send sns notification
            print("SNS starting")
            message = f"\n Hello, \n\n Please turn OFF the sprinkler: {item['sprinkler_id']}."
            sns_client.publish(TopicArn=topic_arn, Message=message)
            print("sns published. check email")

            # publish to iot core
            # # chanage required for topic. need to check
            print("MQTT starting")
            notification = {"message": message}
            response = iot_client.publish(
                topic='weather_data', payload=json.dumps(notification))

            print(response)

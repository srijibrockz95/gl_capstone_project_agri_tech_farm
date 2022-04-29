from datetime import datetime
import json
from urllib import response
import boto3
import base64
from pprint import pprint
from boto3.dynamodb.conditions import Key, Attr

# first scan sprinkler table and get all data
table_name = 'device_data'
device_table = boto3.resource('dynamodb').Table(table_name)
anomaly_table_name = 'anomaly_data'
anomaly_table = boto3.resource('dynamodb').Table(anomaly_table_name)
# sns
sns_client = boto3.client('sns', region_name='us-east-1')
# change required
topic_arn = "arn:aws:sns:us-east-1:212546747799:weather_data_sns_topic"
# iot-core
iot_client = boto3.client(
    'iot-data', region_name='us-east-1', verify=False)


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
        print("Inserted to Aggregate table")
    sprinkler_sensor_status_off()


def update_device_status(device_id):
    print("device_data status update starting")
    current_datetime = str(datetime.now())
    device_table.update_item(
        Key={
            'device_id': device_id

        },
        UpdateExpression='SET device_status = :val1, device_timestamp = :val2',
        ExpressionAttributeValues={
            ':val1': 'OFF',
            ':val2': current_datetime

        })
    print("device_data table updated")


def sprinkler_sensor_status_off():

    # loop through all sprinklers and check if the db timestamp is >= 10 mins
    response = device_table.scan(
        FilterExpression=Attr('device_type').eq('sprinkler'))
    for item in response['Items']:
        # convert database string datetime to datetime datatype
        print(f"device_timestamp: {item['device_timestamp']}")
        db_datetime = (datetime.fromisoformat(item['device_timestamp']))
        current_time = datetime.now()
        print(f"current_timestamp: {current_time}")
        # (compare with current time)
        duration = current_time-db_datetime
        duration_in_seconds = duration.total_seconds()
        minutes = divmod(duration_in_seconds, 60)[0]
        print(f"minutes: {minutes}")
        if minutes >= 0:
            # if yes, turn OFF sprinkler, sensor alarm, SNS, IoT MQTT
            update_device_status(item['device_id'])
            # Get all sensors in anomaly attached to this sprinkler from the anomaly table and turn off alarm
            anomaly_response = anomaly_table.scan(
                FilterExpression=Attr('device_id').eq(item['device_id']))
            anomaly_sensors = []
            for record in anomaly_response['Items']:
                anomaly_sensors.append(record['sensor_id'])
            anomaly_sensors = set(anomaly_sensors)

            for sensor in anomaly_sensors:
                # check if this sensor status is ON.then update to off.
                sensor_resp = device_table.scan(
                    FilterExpression=Attr('device_id').eq('sensor'))
                for item in sensor_resp['Items']:
                    if item['device_status'] == 'ON':
                        update_device_status(sensor)

            # send sns notification
            print("SNS starting")
            message = f"\n Hello, \n\n Please turn OFF the sprinkler: {item['device_id']}."
            sns_client.publish(TopicArn=topic_arn, Message=message)
            print("sns published. check email")

            # publish to iot core
            # # chanage required for topic. need to check
            print("MQTT starting")
            notification = {"message": message}
            response = iot_client.publish(
                topic='weather_data', qos=0, payload=json.dumps(notification))
            print("MQTT published")

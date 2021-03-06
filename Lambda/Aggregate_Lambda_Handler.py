from datetime import datetime, timedelta
import time
import json
from urllib import response
import boto3
import base64
from pprint import pprint
from boto3.dynamodb.conditions import Key, Attr


# first scan sprinkler table and get all data
table_name = 'device_state'
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
sensor_timestamp = ""


def lambda_handler(event, context):
    # Dynamodb
    tablename = 'aggregate_data'
    table = boto3.resource('dynamodb').Table(tablename)
    global sensor_timestamp

    # insert all records to aggregate data table
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

        table.put_item(Item={'sprinkler_id': sprinklerid, 'sensor_id': sensor_id, 'sensor_timestamp': sensor_timestamp,
                             'avg_temp': str(avg_temp), 'max_temp': str(max_temp),
                             'min_temp': str(min_temp), 'avg_moisture': str(avg_moisture), 'max_moisture': str(max_moisture),
                             'min_moisture': str(min_moisture)})
        print("Inserted to Aggregate table")
    sprinkler_sensor_status_off()
    sensor_timestamp = ""


# Update device
def update_device_status(device_id):
    print("device_data status update starting for ", device_id)
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
    print("device_data table updated for", device_id)


def Diff(li1, li2):
    return list(set(li1) - set(li2)) + list(set(li2) - set(li1))


def sprinkler_sensor_status_off():
    print("Inside method")
    print("sensor timestamp", sensor_timestamp)
    timestamp_of_the_event = (datetime.fromisoformat(sensor_timestamp))
    # subtract 2 mins
    two_minute = timedelta(minutes=2)
    timestamp_twomins_before = str(timestamp_of_the_event - two_minute)
    sprinklers = []
    sensors = []
    device_sprinklers = []
    device_sensors = []
    print("Test1")
    print("timestamp 2 mins before: ", timestamp_twomins_before)
    # get master list of sprinklers and sensors from table
    devices_response = device_table.scan()
    for device in devices_response['Items']:
        if device['device_type'] == 'sprinkler' and device['device_status'] == 'ON':
            device_sprinklers.append(device['device_id'])
        elif device['device_type'] == 'sensor' and device['device_status'] == 'ON':
            device_sensors.append(device['device_id'])
    print("Test2")
    # get list of anomaly records 2 mins before the current timestamp
    # response = anomaly_table.query(IndexName="timestampIndex",
    #                                KeyConditionExpression=Key('timestamp').gte(timestamp_twomins_before))

    if len(device_sensors) != 0:
        response = anomaly_table.scan(FilterExpression=Attr(
            'timestamp').eq(sensor_timestamp))
        print(f"Anomaly records Count from 2 mins: {response['Count']}")
        # get the list of anomaly sprinklers and sensors. They do not have to turn off.
        # sprinklers and sensors not in this list has to be off
        for item in response['Items']:
            print("sprinkler_id: ", item)
            if(item['sensor_id'] != 'owm'):
                sprinklers.append(item['sprinkler_id'])
                sensors.append(item['sensor_id'])

        sprinklers = list(set(sprinklers))
        print("Test3")
        # handle sensor turn off
        sensor_turn_off_list = Diff(device_sensors, sensors)
        print("List of sensors to turn off: ", sensor_turn_off_list)
        if len(sensor_turn_off_list) > 0:
            for se in sensor_turn_off_list:
                update_device_status(se)
        print("Test4")
        # handle sprinkler off
        sprinkler_turn_off_list = Diff(device_sprinklers, sprinklers)
        print("List of sprinklers to turn off: ", sprinkler_turn_off_list)
        if(len(sprinkler_turn_off_list) > 0):
            for sp in sprinkler_turn_off_list:
                update_device_status(sp)
                print("Test5")
                # send sns notification
                print("SNS starting")
                message = f"\n Hello, \n\n Please turn OFF the sprinkler: {sp}."
                sns_client.publish(TopicArn=topic_arn, Message=message)
                print("sns published. check email")

                # publish to iot core
                # # chanage required for topic. need to check
                print("MQTT starting")
                notification = {"message": message}
                response = iot_client.publish(
                    topic='weather_data', qos=0, payload=json.dumps(notification))
                print("MQTT published")
                print("Test6")
    else:
        print("No sensors and sprinklers with status ON in device data table")

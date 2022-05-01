from datetime import datetime
import json
import base64
from pprint import pprint
from urllib import response
import boto3
from pyowm import OWM
from boto3.dynamodb.conditions import Key, Attr
from boto3 import resource
from decimal import Decimal

# Dynamodb
anomaly_table_name = 'anomaly_data'
anomaly_table = boto3.resource('dynamodb').Table(anomaly_table_name)
device_table_name = 'device_data'
device_table = boto3.resource('dynamodb').Table(device_table_name)
# owm
owm = OWM('12d6e473b26dddd50e95a73e1ce0a648')
# 104bb94d45e91d4f3a7d97053708757b
# c0adabdd390870a2ae001b8c2ba65496
# ba63b2274f39f96837a0e2613ee0bec1

# sns
sns_client = boto3.client('sns', region_name='us-east-1')
# change required
topic_arn = "arn:aws:sns:us-east-1:212546747799:weather_data_sns_topic"

# iot-core
iot_client = boto3.client(
    'iot-data', region_name='us-east-1', verify=False)
i = 1
sprinklers = []
timestamp = ""


def lambda_handler(event, context):
    global sprinklers
    global timestamp
    try:
        for record in event['records']:
            data = base64.b64decode(record['data'])
            data = str(data, 'utf-8')
            readings = json.loads(data)
            # get anomaly data
            sprinkler_id = readings['SPRINKLER_ID']
            sprinklers.append(sprinkler_id)
            sensor_id = readings['SENSOR_ID']
            # for debugging
            print(f"sprinkler_id: {sprinkler_id}")
            print(f"sensor_id: {sensor_id}")
            timestamp = readings['SENSOR_TIMESTAMP']
            temperature = str(readings['AVG_TEMPERATURE'])
            moisture = str(readings['AVG_MOISTURE'])
            sensor_lat = float(readings['SENSOR_LAT'])
            sensor_long = float(readings['SENSOR_LONG'])

            # get sprinkler lat,long and status values
            sprinkler_data = get_sprinkler_data(sprinkler_id)
            sprinkler_lat = sprinkler_data[0]['device_lat']
            sprinkler_long = sprinkler_data[0]['device_long']
            sprinkler_status = sprinkler_data[0]['device_status']
            sprinkler_timestamp = sprinkler_data[0]['device_timestamp']
            # # get owm weather data
            mgr = owm.weather_manager()
            print(f"weather mgr: {mgr}")
            one_call = mgr.one_call(
                lat=float(sprinkler_lat), lon=float(sprinkler_long))
            print(f"one_call: {one_call}")
            current_data = json.dumps(one_call.current.__dict__)
            pprint(current_data)
            owm_humidity = one_call.current.humidity
            owm_temperature = one_call.current.temperature('celsius')['temp']
            print(f"owm_temperature: {owm_temperature}")
            print(f"owm_humidity: {owm_humidity}")
            # # ignore seconds. considering only minutes.
            owm_timestamp = timestamp
            # owm_temperature = 25
            # owm_humidity = 30
            # owm anomaly check and processes followed
            if owm_temperature >= 20 or owm_humidity <= 60:
                owm_alert_flag = True
                print(f"owm_alert_flag: {owm_alert_flag}")

                # insert both anomaly data from sensor and owm in dynamodb
                sensor_anomaly = {'sprinkler_id': sprinkler_id, 'sensor_id': sensor_id, 'timestamp': timestamp,
                                  'temperature': temperature, 'moisture': moisture, 'sensor_lat': sensor_lat, 'sensor_long': sensor_long}
                owm_anomaly = {'sensor_id': 'owm', 'timestamp': owm_timestamp,
                               'temperature': temperature, 'humidity': owm_humidity}
                print(f"owm_anomaly: {owm_anomaly}")
                print(f"sensor_anomaly: {sensor_anomaly}")

                # insert both(sensors and owm) anomaly data to dynamodb
                ddb_sensor_anomaly_data = json.loads(
                    json.dumps(sensor_anomaly), parse_float=Decimal)
                ddb_owm_anomaly_data = json.loads(
                    json.dumps(owm_anomaly), parse_float=Decimal)
                anomaly_table.put_item(Item=ddb_sensor_anomaly_data)
                print(
                    f'Sensor anomaly inserted')
                anomaly_table.put_item(Item=ddb_owm_anomaly_data)
                print(
                    f'OWM anomaly inserted')

                # turn on sensor alarm in device table
                update_device_status(sensor_id)

            else:
                print(
                    f"No anomaly in OWM data for the timestamp: {owm_timestamp}")
    except Exception as e:
        print(e)
    process_anomaly()


def get_sprinkler_data(sprinkler_id):
    response = device_table.query(
        KeyConditionExpression=Key('device_id').eq(sprinkler_id))
    return response['Items']


def update_device_status(device_id):
    print("device_data status update starting")
    current_datetime = str(datetime.now())
    device_table.update_item(
        Key={
            'device_id': device_id

        },
        UpdateExpression='SET device_status = :val1, device_timestamp = :val2',
        ExpressionAttributeValues={
            ':val1': 'ON',
            ':val2': current_datetime

        })
    print("device_data table updated")


def process_anomaly():
    global sprinklers
    global timestamp
    sprinklers = set(sprinklers)
    print(sprinklers)
    for sprinkler_id in sprinklers:
        # query anomaly table by timestamp and sprinkler_id
        # get the distinct sensor_ids count from the query result.
        # if the record count is >= 3, send sns, publish to iot, and turn sprinkler ON in db.
        scan_resp = anomaly_table.scan(FilterExpression=Attr('timestamp').eq(
            timestamp) & Attr('sprinkler_id').eq(sprinkler_id))
        owm_resp = anomaly_table.query(KeyConditionExpression=Key(
            'sensor_id').eq('owm') & Key('timestamp').eq(timestamp))
        # =Attr('timestamp').eq(
        # timestamp) & Attr('sensor_id').eq('owm'))
        owm_anomaly = '\n '.join(str(item) for item in owm_resp['Items'])
        sensors_in_alarm = []
        print(f"Count: {scan_resp['Count']}")
        if scan_resp['Count'] >= 3:
            # check sensor ids are different
            for item in scan_resp['Items']:
                if item['sensor_id'] not in sensors_in_alarm:
                    sensors_in_alarm.append(item['sensor_id'])
            alarm_sensors_list = ', '.join(str(x) for x in sensors_in_alarm)
        if(len(sensors_in_alarm) >= 3):
            anomaly_data_for_3_sensors = '\n '.join(
                str(item) for item in scan_resp['Items'])
            print(f'List of sensors in alarm: {alarm_sensors_list}')
            # send sns notification
            print("SNS starting")
            message = f"\n Hello, \n\n Weather data anomaly detected for {sprinkler_id} on {timestamp} for sensors: {alarm_sensors_list}.\n\n {anomaly_data_for_3_sensors}\n\n OWM Anomaly \n\n {json.dumps(owm_anomaly)}"
            # subject = f"{timestamp} Weather data anomaly detected for {sensor_id} and current OWM weather data. Please  turn on the sprinkler: {sprinkler_id}"
            sns_client.publish(TopicArn=topic_arn, Message=message)
            print("sns published. check email")

            # update sprinkler status in sprinkler table.
            update_device_status(sprinkler_id)

            # publish to iot core
            # # chanage required for topic. need to check
            message = f"Please turn ON {sprinkler_id}. Anomaly detected in OWM data and for sensors: {alarm_sensors_list} \nTimestamp : {timestamp}"
            notification = {"message": message}
            response = iot_client.publish(
                topic='weather_data', qos=0, payload=json.dumps(notification))

            print(response)
        else:
            print(
                f"{sprinkler_id} does not have 50% or more sensors with anomaly data.")
    sprinklers = []
    timestamp = ""

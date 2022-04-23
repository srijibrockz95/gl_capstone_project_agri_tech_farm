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


def Anomaly_handler(event, context):
    # Dynamodb
    anomaly_table_name = 'anomaly_data'
    anomaly_table = boto3.resource('dynamodb').Table(anomaly_table_name)
    sprinkler_table_name = 'sprinkler_data'
    sprinkler_table = boto3.resource('dynamodb').Table(sprinkler_table_name)
    # owm
    owm = OWM('12d6e473b26dddd50e95a73e1ce0a648')

    # sns
    sns_client = boto3.client('sns', region_name='us-east-1')
    # change required
    topic_arn = "arn:aws:sns:us-east-1:212546747799:weather_data_sns_topic"

    # iot-core
    # import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
    iot_client = boto3.client(
        'iot-data', region_name='us-east-1', verify=False)
    i = 0
    try:

        for record in event['records']:
            print(f"record no: {i}")
            i += 1
            data = base64.b64decode(record['data'])
            data = str(data, 'utf-8')
            readings = json.loads(data)
            # get anomaly data
            sprinkler_id = readings['SPRINKLER_ID']
            print(f"sprinkler_id: {sprinkler_id}")
            sensor_id = readings['SENSOR_ID']
            timestamp = readings['SENSOR_TIMESTAMP']
            temperature = str(readings['AVG_TEMPERATURE'])
            moisture = str(readings['AVG_MOISTURE'])
            lat = float(readings['LAT'])
            long = float(readings['LONG'])
            # get sprinkler lat,long and status values
            response = sprinkler_table.query(
                KeyConditionExpression=Key('sprinkler_id').eq(sprinkler_id))
            sprinkler_data = response['Items']
            # pprint(sprinkler_data)
            lat = sprinkler_data[0]['latitude']
            long = sprinkler_data[0]['longitude']
            sprinkler_status = sprinkler_data[0]['sprinkler_status']

            print(f"lat and long: {float(lat)},{float(long)}")

            # get owm weather data
            mgr = owm.weather_manager()
            print(f"weather mgr: {mgr}")
            one_call = mgr.one_call(lat=float(lat), lon=float(long))
            print(f"one_call: {one_call}")
            current_data = json.dumps(one_call.current.__dict__)

            pprint(current_data)
            owm_humidity = one_call.current.humidity
            owm_temperature = one_call.current.temperature('celsius')['temp']

            print(f"owm_temperature: {owm_temperature}")
            print(f"owm_humidity: {owm_humidity}")
            # ignore seconds. considering only minutes.
            owm_timestamp = timestamp
            owm_temperature = 25
            owm_humidity = 30
            # owm anomaly check and processes followed
            if owm_temperature >= 20 or owm_humidity <= 60:
                owm_alert_flag = True
                print(f"owm_alert_flag: {owm_alert_flag}")

                # insert both anomaly data from sensor and owm in dynamodb
                sensor_anomaly = {'data_type': 'sensor_anomaly', 'sprinkler_id': sprinkler_id, 'sensor_id': sensor_id, 'timestamp': timestamp,
                                  'temperature': temperature, 'moisture': moisture, 'lat': lat, 'long': long}
                owm_anomaly = {'data_type': 'owm_anomaly', 'timestamp': owm_timestamp,
                               'temperature': temperature, 'humidity': owm_humidity}
                print(f"owm_anomaly: {owm_anomaly}")
                print(f"sensor_anomaly: {sensor_anomaly}")

                # insert both(sensors and owm) anomaly data to dynamodb
                ddb_sensor_anomaly_data = json.loads(
                    json.dumps(sensor_anomaly), parse_float=Decimal)
                ddb_owm_anomaly_data = json.loads(
                    json.dumps(owm_anomaly), parse_float=Decimal)
                anomaly_table.put_item(Item=ddb_sensor_anomaly_data)
                anomaly_table.put_item(Item=ddb_owm_anomaly_data)

                # send sns notification
                print("SNS starting")
                message = f"\n Hello, \n\n Weather data anomaly detected on {timestamp} for {sensor_id}.\n\n  {json.dumps(sensor_anomaly)}\n\n  {json.dumps(owm_anomaly)}"
                subject = f"{timestamp} Weather data anomaly detected for {sensor_id} and current OWM weather data. Please  turn on the sprinkler: {sprinkler_id}"
                sns_client.publish(TopicArn=topic_arn, Message=message)
                print("sns published. check email")

                # update sprinkler status in sprinkler table.
                update_resp = sprinkler_table.update_item(
                    Key={
                        'sprinkler_id': sprinkler_id
                    },
                    UpdateExpression='SET sprinkler_status = :val1',
                    ExpressionAttributeValues={
                        ':val1': 'ON'
                    }
                )
                print(update_resp)

                # publish to iot core
                # # chanage required for topic. need to check
                response = iot_client.publish(
                    topic='weather_data', payload=json.dumps(sensor_anomaly))

                print(response)

            else:
                print(f"No anomaly in OWM data: {owm_anomaly}")
    except Exception as e:
        print(e)

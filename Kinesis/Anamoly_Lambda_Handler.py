from datetime import datetime
import json
import base64
from pprint import pprint
from urllib import response
import boto3
from pyowm import OWM
from boto3.dynamodb.conditions import Key, Attr
from boto3 import resource


def lambda_handler(event, context):
    # Dynamodb
    anomaly_table_name = 'anomaly_data'
    anomaly_table = boto3.resource('dynamodb').Table(anomaly_table_name)
    sprinkler_table_name = 'sprinkler_data'
    sprinkler_table = boto3.resource('dynamodb').Table(sprinkler_table_name)
    # owm
    owm = OWM('0f8b321c68552dff33eeb5625f971c39')
    mgr = owm.weather_manager()

    # sns
    sns_client = boto3.client('sns', region_name='us-east-1')
    # change required
    topic_arn = "arn:aws:sns:us-east-1:108664094679:weather_data_sns_topic"

    # iot-core
    iot_client = boto3.client('iot-data', region_name='us-east-1')
    try:

        for record in event['records']:
            data = base64.b64decode(record['data'])
            data = str(data, 'utf-8')
            readings = json.loads(data)
            pprint(readings, sort_dicts=False)
            sprinkler_id = readings['sprinklerid']
            sensor_id = readings['sensor_id']
            timestamp = readings['sensor_timestamp']
            temperature = str(readings['avg_temperature'])
            moisture = str(readings['avg_moisture'])

            # get sprinkler lat and long
            filtering_exp = Key(sprinkler_id).eq(sprinkler_id)
            response = sprinkler_table.query(
                FilterExpression=Attr('sprinkler_id').eq(sprinkler_id))
            sprinkler_data = response['Items']
            # change required
            lat = sprinkler_data[0]['latitude']
            long = sprinkler_data[0]['longitude']
            sprinkler_status = sprinkler_data[0]['status']

            # owm weather data
            one_call = mgr.one_call(lat=lat, lon=long)
            # current_data = json.dumps(one_call.current.__dict__)
            # pprint(current_data)
            owm_humidity = one_call.current.humidity
            owm_temperature = one_call.current.temperature('celsius')['temp']
            # ignore seconds. considering only minutes.
            owm_timestamp = datetime.now

            if owm_temperature >= 20 or owm_humidity <= 60:
                owm_alert_flag = True
                # insert both anomaly data from sensor and owm in dynamodb
                sensor_anomaly = {'data_type': 'sensor_anomaly', 'sprinkler_id': sprinkler_id, 'sensor_id': sensor_id, 'timestamp': timestamp,
                                  'temperature': temperature, 'moisture': moisture}
                owm_anomaly = {'data_type': 'owm_anomaly', 'timestamp': owm_timestamp,
                               'temperature': temperature, 'humidity': owm_humidity}

                anomaly_table.put_item(Item={'data_type': 'sensor_anomaly', 'sprinkler_id': sprinkler_id, 'sensor_id': sensor_id, 'timestamp': timestamp,
                                             'temperature': temperature, 'moisture': moisture})
                anomaly_table.put_item(Item={'data_type': 'owm_anomaly', 'timestamp': owm_timestamp,
                                             'temperature': temperature, 'humidity': owm_humidity})
                # send sns notification
                sns_client.publish(TopicArn=topic_arn, Message=f"Weather data anomaly detected on {timestamp} for {sensor_id}.\n {sensor_anomaly}\n {owm_anomaly}",
                                   Subject=f"{timestamp} Weather data anomaly detected for {sensor_id} and current OWM weather data. Please  turn on the sprinkler: {sprinkler_id}")
                # publish to iot core
                # chanage required for topic. need to check
                response = iot_client.publish(topic=f'weather_data/{sprinkler_id}', qos=1, payload={
                    json.dumps(sensor_anomaly), json.dumps(owm_anomaly)})

                # update sprinkler status in sprinkler table.
                update_resp = sprinkler_table.update_item(
                    Key={
                        # sprinkler_id, latitude, longitude, status
                        'sprinkler_id': sprinkler_id,
                        'status': sprinkler_status
                    },
                    UpdateExpression='SET status = :val1',
                    ExpressionAttributeValues={
                        ':val1': 'ON'
                    }
                )
                print(update_resp)
            else:
                print(f"No anomaly in OWM data: {owm_anomaly}")
    except Exception as e:
        print(f'e')

import json
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
        pprint(readings, sort_dicts=False)
        sprinklerid = readings['SPRINKLER_ID']
        sensor_id = readings['SENSOR_ID']
        sensor_timestamp = readings['SENSOR_TIMESTAMP']
        avg_temp = float(readings['AVG_TEMPERATURE'])
        max_temp = float(readings['MAX_TEMPERATURE'])
        min_temp = float(readings['MIN_TEMPERATURE'])
        avg_moisture = float(readings['AVG_MOISTURE'])
        max_moisture = float(readings['MAX_MOISTURE'])
        min_moisture = float(readings['MIN_MOISTURE'])
        lat = float(readings['LAT'])
        long = float(readings['LONG'])
        table.put_item(Item={'sprinkler_id': sprinklerid, 'sensor_id': sensor_id, 'sensor_timestamp': sensor_timestamp,
                             'avg_temp': str(avg_temp), 'max_temp': str(max_temp),
                             'min_temp': str(min_temp), 'avg_moisture': str(avg_moisture), 'max_moisture': str(max_moisture),
                             'min_moisture': str(min_moisture), 'lat': str(lat), 'long': str(long)})


def change_sprinkler_status():
    # first scan sprinkler table and get all data
    # loop through all sprinklers and check if the db timestamp is >= 10 mins
    # (compare with current time)
    # if yes, turn oFF sprinkler
    # while turning off update sprinkler_status and timestamp.
    # so need to delete the record and insert with new values as timestamp is sort key.

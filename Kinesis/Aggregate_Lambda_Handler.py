import json
import boto3
import base64
from pprint import pprint

def lambda_handler(event, context):
    # Dynamodb
    tablename = 'AggregatedData'
    table = boto3.resource('dynamodb').Table(tablename)
    for record in event['records']:
        data = base64.b64decode(record['data'])
        data = str(data, 'utf-8')
        readings = json.loads(data)
        pprint(readings, sort_dicts=False)
        sprinklerid = readings['SPRINKLERID']
        sensor_id = readings['SENSOR_ID']
        sensor_timestamp = readings['SENSOR_TIMESTAMP']
        avg_temp = float(readings['AVG_TEMPERATURE'])
        max_temp = float(readings['MAX_TEMPERATURE'])
        min_temp = float(readings['MIN_TEMPERATURE'])
        avg_moisture = float(readings['AVG_MOISTURE'])
        max_moisture = float(readings['MAX_MOISTURE'])
        min_moisture = float(readings['MIN_MOISTURE'])
        table.put_item(Item={'sprinklerid': sprinklerid, 'sensor_id': sensor_id, 'sensor_timestamp': sensor_timestamp,
                         'avg_temp': str(avg_temp), 'max_temp': str(max_temp),
                         'min_temp': str(min_temp), 'avg_moisture': str(avg_moisture), 'max_moisture': str(max_moisture),
                         'min_moisture': str(min_moisture)})

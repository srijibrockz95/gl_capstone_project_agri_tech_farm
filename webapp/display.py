from asyncore import read
from sqlite3 import Timestamp
from urllib import response
from flask import Flask, render_template
import boto3
from pprint import pprint
import json
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr

app = Flask(__name__)

dynamodb = boto3.resource('dynamodb', 'us-east-1')
sprinkler_headings = ("S.No", "Sprinkler", "Status", "Latitude",
                      "Longitude", "Timestamp")
sensor_headings = ("S.No", "Sensor", "Status",
                   "Latitude", "Longitude", "Timestamp")


@app.route('/')
def get_data():
    sprinkler_data = get_device_data('sprinkler')
    sensor_data = get_device_data('sensor')
    return render_template("table.html", sprinkler_headings=sprinkler_headings,
                           sprinkler_data=sprinkler_data, sensor_headings=sensor_headings, sensor_data=sensor_data)


def get_device_data(device_type):
    current_time = datetime.now()
    list_data = []
    tuple_data = ()
    anomaly_table = dynamodb.Table('device_state')
    response = anomaly_table.scan(
        FilterExpression=Attr('device_type').eq(device_type))
    for item in response['Items']:
        if(device_type == 'sensor'):
            sensor_name = item['device_id']
            sensor_order = int(sensor_name[6:]) + 1
            item_tuple = (sensor_order, sensor_name,
                          item['device_status'], item['device_lat'], item['device_long'], item['device_timestamp'], )
        else:
            sprinkler_name = item['device_id']
            sprinkler_order = int(sprinkler_name[9:])
            item_tuple = (sprinkler_order, sprinkler_name,
                          item['device_status'], item['device_lat'], item['device_long'], item['device_timestamp'], )
        list_data.append(item_tuple)
    tuple_data = tuple(list_data)
    return sorted(tuple_data)


app.run(debug=True)

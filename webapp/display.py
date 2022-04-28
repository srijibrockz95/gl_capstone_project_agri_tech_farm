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
sprinkler_headings = ("Sprinkler", "Status", "Latitude",
                      "Longitude", "Timestamp")
sensor_headings = ("Sensor", "Status", "Latitude", "Longitude", "Timestamp")

# data = (
#     ("Rolf", "Software Engineer", "$1000"),
#     ("Amy", "Software Engineer", "$2000"),
#     ("Bob", "Software Engineer", "$3000")
# )


@app.route('/hello')
def hello() -> str:
    return "Hello World from Flask"


@app.route('/')
def get_data():
    sprinkler_data = get_data('sprinkler')
    sensor_data = get_data('sensor')
    return render_template("table.html", sprinkler_headings=sprinkler_headings,
                           sprinkler_data=sprinkler_data, sensor_headings=sensor_headings, sensor_data=sensor_data)
    # , sensor_headings=sensor_headings, sensor_data=sensor_data
    # return "hello1"


def get_sensor_data_test():
    list_data = []
    tuple_data = ()
    thing_list = []
    with open('secure/provision/provisioning-data.json') as f:
        for jsonObj in f:
            thing_dict = json.loads(jsonObj)
            thing_list.append(thing_dict)

        for item in thing_list:
            item_tuple = (item['GroupName'],
                          item['ThingName'], "ON")
            list_data.append(item_tuple)
        tuple_data = tuple(list_data)
        return tuple_data


def get_sensor_data():
    current_time = datetime.now()
    list_data = []
    tuple_data = ()
    n = -2
    passed_time = current_time + timedelta(n)
    anomaly_table = dynamodb.Table('device_data')
    response = anomaly_table.scan(
        FilterExpression=Attr('device_type').eq('sensor'))
    for item in response['Items']:
        item_tuple = (item['device_id'],
                      item['device_status'], item['device_lat'], item['device_long'], item['device_timestamp'], )
        list_data.append(item_tuple)
    tuple_data = tuple(list_data)
    return tuple_data


def get_data(device_type):
    current_time = datetime.now()
    list_data = []
    tuple_data = ()
    n = -2
    passed_time = current_time + timedelta(n)
    anomaly_table = dynamodb.Table('device_data')
    response = anomaly_table.scan(
        FilterExpression=Attr('device_type').eq(device_type))
    for item in response['Items']:
        item_tuple = (item['device_id'],
                      item['device_status'], item['device_lat'], item['device_long'], item['device_timestamp'], )
        list_data.append(item_tuple)
    tuple_data = tuple(list_data)
    return tuple_data
    # sprinkler_table = dynamodb.Table('sprinkler_data')
    # response = sprinkler_table.scan()
    # list_data = []
    # tuple_data = ()
    # for item in response['Items']:
    #     item_tuple = (item['sprinkler_id'],
    #                   item['sprinkler_timestamp'], item['sprinkler_status'])
    #     list_data.append(item_tuple)
    # tuple_data = tuple(list_data)

    # return tuple_data


app.run(debug=True)

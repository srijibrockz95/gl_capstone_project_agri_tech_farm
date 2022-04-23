from config import *
import threading
import time
import json
import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
import random
import datetime
import boto3
import sched
import pprint
from cloudpathlib import CloudPath

# Define ENDPOINT, TOPIC, RELATOVE DIRECTORY for CERTIFICATE AND KEYS
import schedule as schedule



# AWS class to create number of objects (devices)
class AWS():
    # Constructor that accepts client id that works as device id and file names for different devices
    # This method will obviosuly be called while creating the instance
    # It will create the MQTT client for AWS using the credentials
    # Connect operation will make sure that connection is established between the device and AWS MQTT
    def __init__(self, client, group, certificate, private_key,lat, long):
        self.client_id = client
        self.device_id = client
        self.lat = lat
        self.long = long
        self.sprinkler_id = group
        self.cert_path = PATH_TO_CERT + "/" + certificate
        self.pvt_key_path = PATH_TO_KEY + "/" + private_key
        self.root_path = PATH_TO_ROOT_CA
        self.myAWSIoTMQTTClient = AWSIoTPyMQTT.AWSIoTMQTTClient(self.client_id)
        self.myAWSIoTMQTTClient.configureEndpoint(ENDPOINT, MQTT_PORT)
        self.myAWSIoTMQTTClient.configureCredentials(self.root_path, self.pvt_key_path, self.cert_path)
        self._connect()

    # Connect method to establish connection with AWS IoT core MQTT
    def _connect(self):
        self.myAWSIoTMQTTClient.connect()


    # This method will publish the data on MQTT
    # Before publishing we are confiuguring message to be published on MQTT
    def publish(self):
            print('Begin Publish')
            # Iterating through the items in device configuration dictionary, every second
            message = {}
            temp_value = float(random.normalvariate(21, 1.5))
            temp_value = round(temp_value, 1)
            moisture_value = float(random.normalvariate(58, 1.5))
            moisture_value = round(moisture_value, 1)
            timestamp = str(datetime.datetime.now())
            message['sensor_id'] = self.device_id
            message['sprinkler_id'] = self.sprinkler_id
            message['sensor_timestamp'] = timestamp
            message['sensor_temperature'] = temp_value
            message['sensor_moisture'] = moisture_value
            message['sensor_lat'] = self.lat
            message['sensor_long'] = self.long
            messageJson = json.dumps(message)
            self.myAWSIoTMQTTClient.publish(TOPIC, messageJson, 1)
            print("Published: '" + json.dumps(message) + "' to the topic: " + TOPIC)

    def disconnect(self):
        self.myAWSIoTMQTTClient.disconnect()

# Main method with actual objects and method calling to publish the data in MQTT
# Again this is a minimal example that can be extended to incopporate more devices
# Also there can be different method calls as well based on the devices and their working.
if __name__ == '__main__':
    # Download the config file from the S3 bucket and save it in local/EC2
    cp = CloudPath("s3://" + BUCKET_NAME)
    cp.download_to(PATH_TO_CONFIG)
    thing_list = []
    config_file_path = PATH_TO_CONFIG + "/"+ PROVISION_FILE_NAME

    # Download the certificates from S3 bucket and save it in local/EC2
    # currently hashed as the certificates are not available in S3 yet. So reading from local path
    cp = CloudPath("s3://" + BUCKET_NAME)
    cp.download_to(PATH_TO_CERT)
    cp.download_to(PATH_TO_KEY)

    #Serialize the sensors listed in the config file
    with open(config_file_path) as configFile:
        for jsonObj in configFile:
            thing_dict = json.loads(jsonObj)
            thing_list.append(thing_dict)

    #Create device/sensor objects using the appropriate certificates and private keys
    for device in thing_list:
        print(device["ThingName"], type(device["ThingName"]))
        device["ThingName"] = AWS(device["ThingName"], device["GroupName"], device["ThingName"]+".pem.crt",
                                  device["ThingName"]+".pem.key", device["sensor_lat"], device["sensor_long"])


    # Publish to the same topic in a loop forever for all devices/sensors
    loopCount = 0
    scheduler = sched.scheduler(time.time, time.sleep)
    now = time.time()
    while True:
        try:
            for device in thing_list:
                scheduler.enterabs(now + loopCount, 1, device["ThingName"].publish)
            loopCount += 1
            scheduler.run()
        except KeyboardInterrupt:
            for device in thing_list:
                device["ThingName"].disconnect()

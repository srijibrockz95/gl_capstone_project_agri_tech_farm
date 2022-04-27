import json
import boto3
import sys
import time
import random
import datetime
import sched


KINESIS_DATA_STREAM = "StreamSource01"

kinesis_handle = boto3.client('kinesis', region_name="us-east-1")
kinesis_response = kinesis_handle.get_shard_iterator(
    StreamName=KINESIS_DATA_STREAM,
    ShardId='shardId-000000000000',
    ShardIteratorType='LATEST'
)


# Function to push data in the kinesis stream, partition key of kinesis stream is deviceid
devices = ['Sensor1', 'Sensor2', 'Sensor3', 'Sensor4', 'Sensor5']
sprinklers = ['Sprinkler1', 'Sprinkler2', 'Sprinkler3', 'Sprinkler4']


def publishDummyData(loopCount):
    message = {}
    temp_value = float(random.uniform(-20, 60))
    temp_value = round(temp_value, 1)
    moisture_value = float(random.uniform(0, 100))
    moisture_value = round(moisture_value, 1)
    timestamp = str(datetime.datetime.now())
    sprinkler = random.choice(sprinklers)
    message['sprinkler_id'] = sprinkler
    message['sensor_id'] = random.choice(devices)
    message['sensor_timestamp'] = timestamp
    message['sensor_temperature'] = temp_value
    message['sensor_moisture'] = moisture_value
    messageJson = json.dumps(message)
    print(messageJson)
    response = kinesis_handle.put_record(
        StreamName=KINESIS_DATA_STREAM, Data=messageJson, PartitionKey=sprinkler)
    # print(response)


today = datetime.date.today()


scheduler = sched.scheduler(time.time, time.sleep)

now = time.time()
loopCount = 0
# Pushing the data to a configuered kinesis stream.
print("Data push to kinesis stream started")

while True:
    try:
        scheduler.enterabs(now+loopCount, 1, publishDummyData, (loopCount,))
        loopCount += 1
        scheduler.run()
    except KeyboardInterrupt:
        break

print("Data push to kinesis stream has stopped")

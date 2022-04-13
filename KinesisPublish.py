import json
import boto3
import sys
import time
import random
import datetime
import sched


KINESIS_DATA_STREAM = "TestStream02"

kinesis_handle = boto3.client('kinesis', region_name = "us-east-1")


# Function to push data in the kinesis stream, partition key of kinesis stream is deviceid
devices=['Sensor1','Sensor2','Sensor3','Sensor4']

def publishDummyData(loopCount):
	message = {}
	value = float(random.normalvariate(99, 1.5))
	value = round(value, 1)
	timestamp = str(datetime.datetime.now())
	message['sprinklerid'] = 'Sprinkler1'
	message['deviceid'] = random.choice(devices)
	message['timestamp'] = timestamp
	message['Temperature'] = value
	message['Moisture'] = value
	messageJson = json.dumps(message)
	print(messageJson)
	response = kinesis_handle.put_record(StreamName=KINESIS_DATA_STREAM, Data = messageJson, PartitionKey="Sprinkler1")
	print(response)


today = datetime.date.today()


scheduler = sched.scheduler(time.time, time.sleep)

now = time.time()
loopCount = 0
# Pushing the data to a configuered kinesis stream.
print("Data push to kinesis stream started")

while True:
    try :
    	scheduler.enterabs(now+loopCount, 1, publishDummyData, (loopCount,))
    	loopCount += 1
    	scheduler.run()
    except KeyboardInterrupt:
        break

print("Data push to kinesis stream has stopped")



from setup import *
import time

# One time use while setting up tables.
# dynamodb = boto3.resource('dynamodb', 'us-east-1')
sns_client = boto3.client('sns', region_name='us-east-1')
# create_aggregate_data_table(dynamodb=dynamodb)
# create_anomaly_data_table(dynamodb=dynamodb)
# create_device_data_table(dynamodb=dynamodb)
# time.sleep(60)
# insert_device_data(dynamodb=dynamodb)
create_sns(sns_client=sns_client)
# create_gsi_anomaly_table()
print("Done..")

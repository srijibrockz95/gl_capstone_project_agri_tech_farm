import json
import base64
from pprint import pprint

def lambda_handler(event, context):
    for record in event['records']:
        data = base64.b64decode(record['data'])
        data = str(data, 'utf-8')
        readings = json.loads(data)
        pprint(readings, sort_dicts=False)

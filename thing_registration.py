################################################### Connecting to AWS
import boto3

import json
################################################### Create random name for things
import random
import string

################################################### Parameters for Thing
from urllib3.connectionpool import xrange

thingArn = ''
thingId = ''
#thingName = ''.join([random.choice(string.ascii_letters + string.digits) for n in xrange(15)])
defaultPolicyName = 'Group-core-policy'


###################################################

def createThing():
    global thingClient
    data_JSON = """
    {
    	"mapping" : {
        "sprinkler1" : ["sensor1","sensor2","sensor3","sensor4","sensor5"],
        "sprinkler2" : ["sensor6","sensor7","sensor8","sensor9","sensor10"]
        }
    }
    """

    config = json.loads(data_JSON)
    print("json loaded to config file")
    for data in config:
        if data == 'mapping':
            for devices in config.values(): #Looping through all the mappings
                for sprinkler in devices.keys(): #Looping through each sprinkler
                    for sensor in devices.values(): #Looping through the list of mapped sensors
                        for i in sensor:
                            print(sprinkler,i, type(sprinkler), type(i))
                            thingName = ''.join([sprinkler+"-"+i])
                            thingResponse = thingClient.create_thing(
                                thingName=thingName
                            )
                            data = json.loads(json.dumps(thingResponse, sort_keys=False, indent=4))
                            for element in data:
                                if element == 'thingArn':
                                    thingArn = data['thingArn']
                                elif element == 'thingId':
                                    thingId = data['thingId']
                                    createCertificate(thingName)


def createCertificate(thingName_for_certificates):
    thingName = thingName_for_certificates
    global thingClient
    certResponse = thingClient.create_keys_and_certificate(
        setAsActive=True
    )
    data = json.loads(json.dumps(certResponse, sort_keys=False, indent=4))
    for element in data:
        if element == 'certificateArn':
            certificateArn = data['certificateArn']
        elif element == 'keyPair':
            PublicKey = data['keyPair']['PublicKey']
            PrivateKey = data['keyPair']['PrivateKey']
        elif element == 'certificatePem':
            certificatePem = data['certificatePem']
        elif element == 'certificateId':
            certificateId = data['certificateId']
    sensorPublicKey = thingName+"-"+"PublicKey"
    with open(sensorPublicKey, 'w') as outfile:
        outfile.write(PublicKey)
    sensorPrivateKey = thingName + "-" + "PrivateKey"
    with open(sensorPrivateKey, 'w') as outfile:
        outfile.write(PrivateKey)
    sensorcertificatePem = thingName + "-" + "CertificatePem"
    with open(sensorcertificatePem, 'w') as outfile:
        outfile.write(certificatePem)

    response = thingClient.attach_policy(
        policyName=defaultPolicyName,
        target=certificateArn
    )
    response = thingClient.attach_thing_principal(
        thingName=thingName,
        principal=certificateArn
    )


thingClient = boto3.client('iot')
createThing()
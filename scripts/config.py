import sys
import logging

# Local directory paths and file names
PROVISION_FILE_NAME = "provisioning-data.json"
POLICY_FILE_NAME = "general_policy.json"
PROVISIONING_TEMPLATE = 'provisioning-template.json'

PATH_TO_PROVISION= '../secure/provision/'+PROVISION_FILE_NAME
PATH_TO_POLICY = '../secure/policy/'+POLICY_FILE_NAME
PATH_TO_PROVISIONING_TEMPLATE = '../secure/provision/' + PROVISIONING_TEMPLATE

# AWS IoT Core Region
IOT_CORE_REGION = "us-east-1"

# AWS IoT Core - Parameters used for creating thing(s) and thing type
THING_TYPE_NAME = "soil_sensor"
THING_NAME_PREFIX = "Sensor"
THING_COUNT = 20
THING_GROUP_COUNT = 5
THING_GROUP_NAME_PREFIX = "Sprinkler"
THING_GROUP_BASE_ARN = "arn:aws:iot:us-east-1:212546747799:thinggroup/"
THING_BASE_ARN = "arn:aws:iot:us-east-1:212546747799:thing/"

# AWS IoT Core - Parameters used for the MQTT Connection
ENDPOINT = "a1df0xo7q9cvqr-ats.iot.us-east-1.amazonaws.com"
PATH_TO_ROOT_CA = "../secure/ca/AmazonRootCA1.pem"
PATH_TO_CERT = "../secure/certificates"
PATH_TO_CONFIG = "../secure/provision"
PATH_TO_KEY = "../secure/keys/private"
MQTT_PORT = 8883
TOPIC = "iot/agritech"


# AWS IoT Core - Parameter used for creating certificates(s)
SET_CERT_UNIQUE = True

# AWS S3 - Paremeters used to set up a  S3 bucket 
S3_REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::212546747799:role/capstoneagritechfarmrole1"
BUCKET_NAME = "capstoneagritechfarm20"


OBJ_PROVISION_FILE = PROVISION_FILE_NAME

# Logger Settings
logger_aws_iot_core = logging.getLogger('example_logger')
logger_aws_iot_core.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler_aws_iot_core = logging.StreamHandler(sys.stdout)
log_format_aws_iot_core = logging.Formatter('%(asctime)s - %(levelname)s - AWS-IOT-CORE - %(message)s',datefmt='%H:%M:%S')
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
handler_aws_iot_core.setFormatter(log_format_aws_iot_core)
handler.setFormatter(log_format)
logger_aws_iot_core.addHandler(handler_aws_iot_core)




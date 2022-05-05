from things_registration_utils import *
from sys import exit


if __name__ == "__main__":

    # # Step 0.0 : Reset/Delete all the existing things, thing groups,
    # # certificates and policies registered in AWS IoT Core
    aws_iot_core_reset()

    # # Step 0.1 : Reset/Delete all the existing buckets and their contents in AWS S3
    aws_s3_reset()

    # # Step 1: Create a provision file
    create_provision_file()

    # # Step 2: Configure the s3 bucket
    aws_s3_config()

    # # Step 3: Create things, thing groups, thing type in AWS Iot Core
    status = aws_iot_core_create_bulk_things()
    if not status:
        exit()

    # # Step 4: Adding things to thing groups
    add_thing_to_thing_group()

    # # Step 5: Create certificates in AWS Iot Core
    aws_iot_core_create_certificates()

    # # Step 6: Create policy
    aws_iot_core_create_policy()

    # # Step 7: Attach everything
    aws_iot_core_attach_certificates()

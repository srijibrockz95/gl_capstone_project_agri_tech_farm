#!/bin/bash
sudo apt update
sudo apt install python3-pip
sudo apt-get install awscli
pip3 install --upgrade awscli
pip install AWSIoTPythonSDK
pip install boto3
pip install cloudpathlib
pip install schedule
aws configure
python3 soil_sensor_publish.py


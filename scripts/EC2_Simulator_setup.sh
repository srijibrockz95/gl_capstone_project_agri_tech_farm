#!/bin/bash
sudo apt update
sudo apt install python3-pip
sudo apt-get install awscli
pip3 install --upgrade awscli
pip install AWSIoTPythonSDK
pip install boto3
pip install cloudpathlib
pip install schedule

curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
sudo apt-get install -y nodejs build-essential
sudo npm install -g --unsafe-perm node-red
node-red

aws configure
python3 soil_sensor_publish.py


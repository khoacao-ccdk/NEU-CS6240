#!/bin/bash
set -x -e

sudo yum install -y python3-pip

# Download requirements.txt from S3
wget https://cs6240-config-cody.s3.us-west-2.amazonaws.com/requirements.txt

# Install dependencies from downloaded requirements.txt

sudo pip3 install -r requirements.txt
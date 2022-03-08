#!/bin/bash

# update code
aws lambda update-function-code --function-name Worker --architectures arm64\
  --zip-file fileb://target/pixels-lambda-0.1.0-SNAPSHOT.jar

# change configuration
#aws lambda update-function-configuration --function-name Worker  --handler Worker

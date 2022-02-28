#!/bin/bash

# update code
aws lambda update-function-code --function-name javaLambdaFuncAccessS3 \
  --zip-file fileb://target/blank-java-1.0-SNAPSHOT.jar

# change configuration
aws lambda update-function-configuration --handler Worker

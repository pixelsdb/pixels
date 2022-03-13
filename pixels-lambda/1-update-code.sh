#!/bin/bash

# update code
#aws lambda update-function-code --function-name Worker --architectures arm64\
#  --s3-bucket "tiannan-test"  --s3-key "pixels-lambda-0.1.0-SNAPSHOT.jar"\
#  --zip-file fileb://target/pixels-lambda-0.1.0-SNAPSHOT.jar

# change configuration
#aws lambda update-function-configuration --function-name Worker  --handler Worker

# upload jar to s3
aws s3 cp ./target/pixels-lambda-0.1.0-SNAPSHOT.jar s3://tiannan-test/pixels-lambda-0.1.0-SNAPSHOT.jar
# update the jar file directly from s3
aws lambda update-function-code --function-name Worker --architectures arm64\
  --s3-bucket "tiannan-test"  --s3-key "pixels-lambda-0.1.0-SNAPSHOT.jar"

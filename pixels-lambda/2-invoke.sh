#!/bin/bash
aws lambda invoke --function-name javaLambdaFuncAccessS3 \
  --payload '{ "bucketName":"pixels-tpch-customer-v-0-order", "fileName": "20220213140252_0.pxl" }' \
  --cli-binary-format raw-in-base64-out \  #encoding stuff
  response.json #output file

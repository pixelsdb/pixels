#!/bin/bash
aws lambda invoke --function-name Worker \
  --payload '{ "fileName":["pixels-tpch-orders-v-0-order/20220306043322_0.pxl"],
   "cols":["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate"]}' \
  --cli-binary-format raw-in-base64-out response.json #output file

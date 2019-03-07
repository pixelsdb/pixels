#!/bin/bash

echo "using PIXELS_HOME=$PIXELS_HOME"

java -Dio.netty.leakDetection.level=advanced -Drole=main -jar $PIXELS_HOME/pixels-daemon-0.1.0-SNAPSHOT-full.jar coordinator

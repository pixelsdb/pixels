# Pixels Index Rockset

### Main Features
This is a single point index implementation based on Rockset. It is the cloud version of RocksDBIndex and is used to add indexes for data stored in S3 buckets on AWS EC2 instances.

### Build Instructions
1. Configure the bucketName, s3Prefix, localDbPath, persistentCachePath, persistentCacheSizeGB, readOnly in `pixels-common/src/main/resources/pixels.properties`.

2. RocksetIndex depends on the corresponding C++ module for compilation. Before compiling the Java project, you need to first compile the C++ module to generate `libRocksetJni.so`. For detailed build instructions, see `pixels/cpp/pixels-index/pixels-index-rockset/README.md`.

3. Compile the project using `mvn clean install`.

4. Use `RocksetIndexProvider.createInstance(SinglePointIndex.Scheme.rockset)` to create a `RocksetIndex`.

5. The indexing service must be called through `IndexServer`. It is important to note that when compiling, you need to use `-Djava.library.path` to include the `libRocksetJni.so` file. Below is an example of a script to start the service:
```cpp
#!/bin/bash
# IndexServer startup script
# Usage: ./start-index-server.sh [PORT] [LOG_LEVEL]

# Default parameters
PIXELS_HOME="$HOME/pixels"

# Dependency check
check_dependencies() {
    if ! command -v java &> /dev/null; then
        echo "Error: Java environment not detected"
        exit 1
    fi
}

# Classpath builder (adjust as needed)
build_classpath() {
    CP="$PIXELS_HOME/pixels-daemon/target/pixels-daemon-0.2.0-SNAPSHOT-full.jar"
    CP="$CP:$PIXELS_HOME/pixels-common/target/pixels-common-0.2.0-SNAPSHOT.jar"

    if [[ $CP != *"grpc"* ]]; then
        CP="$CP:$(find ~/.m2 -name 'grpc-netty-shaded-*.jar' | head -1)"
    fi

    echo "$CP"
}

# Main startup logic
start_server() {
    mkdir -p "$PIXELS_HOME/pixels-daemon/logs"
    mkdir -p "$PIXELS_HOME/pixels-daemon/run"

    echo "Startup parameters:"
    echo "Port: $PORT"
    echo "PIXELS_HOME: $PIXELS_HOME"
    echo "Log level: $LOG_LEVEL"

    java -cp $(build_classpath) \
        -Djava.library.path=$PIXELS_HOME/lib \
        -Dlog4j.configurationFile=$PIXELS_HOME/conf/log4j2.xml \
        -Dio.pixelsdb.pixels.daemon.index.log.level=$LOG_LEVEL \
        io.pixelsdb.pixels.daemon.index.IndexServerMain_set \
        --port $PORT \
        2>&1 | tee -a $PIXELS_HOME/pixels-daemon/logs/index-server_set.log &

    SERVER_PID=$!
    echo "IndexServer started (PID: $SERVER_PID)"
    echo $SERVER_PID > $PIXELS_HOME/pixels-daemon/run/index-server.pid
}

# Execution entry
main() {
    if [ $# -lt 2 ]; then
        echo "Usage: $0 [port] [log level]"
        echo "Error: Missing arguments. Port and log level are required."
        exit 1
    fi

    PORT=$1
    LOG_LEVEL=$2 
    
    check_dependencies
    start_server

    sleep 3
    if ! ps -p $SERVER_PID > /dev/null; then
        echo "Error: Server failed to start. Check log: $PIXELS_HOME/pixels-daemon/logs/index-server_set.log"
        exit 1
    fi

    echo "Server successfully started on port $PORT"
    echo "Tail log: tail -f $PIXELS_HOME/pixels-daemon/logs/index-server_set.log"
}
main
```
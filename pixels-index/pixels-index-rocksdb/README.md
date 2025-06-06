# Pixels Index RocksDB

### Main Features
This is a secondary index implementation based on RocksDB. The secondary index is used to store the mapping between data and RowId. Combined with the MainIndex, it forms a complete indexing service, which is invoked by gRPC.

### Build Instructions
1. Configure the RocksDB path in `pixels-common/src/main/resources/pixels.properties`.

2. Compile the project using `mvn clean install`.

3. Use `RocksDBIndexProvider.createInstance(SecondaryIndex.Scheme.rocksdb)` to create a `RocksDBIndex`.

4. The indexing service must be called through `IndexServer`. Below is an example of a script to start the service:
```cpp
#!/bin/bash
# IndexServer startup script
# Usage: ./start-index-server.sh [port] [log level]

PIXELS_HOME="$HOME/pixels"

# Check Dependency
check_dependencies() {
    if ! command -v java &> /dev/null; then
        echo "Error: Java environment not detected"
        exit 1
    fi
}

# Classpath construction (adjust based on actual project structure)
build_classpath() {
    # Use fat jar (make sure mvn package has been run)
    CP="$PIXELS_HOME/pixels-daemon/target/pixels-daemon-0.2.0-SNAPSHOT-full.jar"
    CP="$CP:$PIXELS_HOME/pixels-common/target/pixels-common-0.2.0-SNAPSHOT.jar"

    # Add explicit gRPC path (if not included in fat jar)
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
    echo "Log level: $LOG_LEVEL"

    java -cp $(build_classpath) \
        -Dlog4j.configurationFile=$PIXELS_HOME/conf/log4j2.xml \
        -Dio.pixelsdb.pixels.daemon.index.log.level=$LOG_LEVEL \
        io.pixelsdb.pixels.daemon.index.IndexServerMain \
        --port $PORT \
        2>&1 | tee -a $PIXELS_HOME/pixels-daemon/logs/index-server.log &

    SERVER_PID=$!
    echo "IndexServer started (PID: $SERVER_PID)"
    echo $SERVER_PID > $PIXELS_HOME/pixels-daemon/run/index-server.pid
}

# Execution flow
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

    # Wait 3 seconds to check if the service is alive
    sleep 3
    if ! ps -p $SERVER_PID > /dev/null; then
        echo "Error: Service failed to start. Check log at $PIXELS_HOME/pixels-daemon/logs/index-server.log"
        exit 1
    fi

    echo "Service started successfully, listening on port $PORT"
    echo "For detailed logs: tail -f $PIXELS_HOME/pixels-daemon/logs/index-server.log"
}
main
```
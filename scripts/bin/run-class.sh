#!/usr/bin/env bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-role role] [-name serviceName] [-daemon] classname [opts]"
  exit 1
fi

# get base dir
base_dir=$(dirname $0)

if [ "x$PIXELS_HOME" = "x" ]; then
  export PIXELS_HOME="$base_dir/../"
else
  base_dir=$PIXELS_HOME
fi

# add local jars into classpath
for file in "$base_dir"/pixels-daemon-*.jar;
do
  CLASSPATH="$CLASSPATH":"$file"
done

# add external jars under PIXELS_HOME/lib/
# please put libraries such as mysql_connector in this directory.
for file in "$base_dir"/lib/*.jar
do
  CLASSPATH="$CLASSPATH":"$file"
done

# Generic jvm settings
if [ -z "$PIXELS_OPTS" ]; then
  PIXELS_OPTS=""
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Set JVM options file path or use default JVM options
JVM_OPTIONS_FILE="./jvm.config"
if [ -z "$PIXELS_JVM_OPTS" ]; then
  if [ -e "$JVM_OPTIONS_FILE" ]; then
    PIXELS_JVM_OPTS=$(tr '\n' ' ' < "$JVM_OPTIONS_FILE")
  else
    echo "jvm.config is not found, using default jvm configurations"
    PIXELS_JVM_OPTS="-Xmx1024M -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
  fi
fi

echo "JVM options: ${PIXELS_JVM_OPTS}"

# Parse commands
while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -role)
      DAEMON_ROLE=$2
      shift 2
      ;;
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=${base_dir}/logs/$DAEMON_NAME.out
      shift 2
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

echo "role: $DAEMON_ROLE, name: $DAEMON_NAME, mode: $DAEMON_MODE"

PIXELS_OPTS="-Dio.netty.leakDetection.level=advanced"
if [ "x$DAEMON_ROLE" = "x" ]; then
  PIXELS_OPTS=$PIXELS_OPTS
else
  PIXELS_OPTS=$PIXELS_OPTS" -Drole=$DAEMON_ROLE"
fi

if [ "x$DAEMON_NAME" = "x" ]; then
  PIXELS_OPTS=$PIXELS_OPTS
else
  PIXELS_OPTS=$PIXELS_OPTS" -Dname=$DAEMON_NAME"
fi

echo "PIXELS OPTS: "$PIXELS_OPTS

NUMA_INTERLEAVE=""
if type numactl >/dev/null 2>&1; then
  NUMA_INTERLEAVE="numactl --interleave=all"
fi

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  echo "$DAEMON_NAME running in the daemon mode."
  nohup ${NUMA_INTERLEAVE} ${JAVA} ${PIXELS_JVM_OPTS} -cp ${CLASSPATH} ${PIXELS_OPTS} "$@" > ${CONSOLE_OUTPUT_FILE} 2>&1 < /dev/null &
else
  exec ${NUMA_INTERLEAVE} ${JAVA} ${PIXELS_JVM_OPTS} -cp ${CLASSPATH} ${PIXELS_OPTS} "$@"
fi

#!/usr/bin/env bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-operation operation] [-role role] [-daemon] classname [opts]"
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
for file in "$base_dir"/bin/pixels-daemon-*.jar;
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

# Which java to use. Prefer an explicit valid JAVA_HOME, then PATH, then common node-local JDK locations.
JAVA=""
if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
  JAVA="$JAVA_HOME/bin/java"
elif JAVA="$(command -v java 2>/dev/null)"; then
  echo "Using Java from PATH: $JAVA"
else
  for candidate in "$HOME"/opt/jdk-*/bin/java "$HOME"/.sdkman/candidates/java/current/bin/java /usr/lib/jvm/*/bin/java; do
    if [ -x "$candidate" ]; then
      JAVA="$candidate"
      echo "Using node-local Java: $JAVA"
      break
    fi
  done
  if [ -z "$JAVA" ]; then
    echo "ERROR: Java not found. Set JAVA_HOME, add java to PATH, or install a JDK under HOME/opt or /usr/lib/jvm." >&2
    exit 1
  fi
fi

# Resolve the Java home on this node so JNI dependencies (for example libjawt.so) are discoverable.
PIXELS_JAVA_HOME=$(dirname "$(dirname "$(readlink -f "$JAVA")")")
export JAVA_HOME="$PIXELS_JAVA_HOME"
export PATH="$PIXELS_JAVA_HOME/bin:$PATH"
if [ -d "$PIXELS_JAVA_HOME/lib" ]; then
  export LD_LIBRARY_PATH="$PIXELS_JAVA_HOME/lib:$PIXELS_JAVA_HOME/lib/server:$LD_LIBRARY_PATH"
fi

# Parse commands
while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -operation)
      OPERATION=$2
      shift 2
      ;;
    -role)
      DAEMON_ROLE=$2
      CONSOLE_OUTPUT_FILE=${base_dir}/logs/$DAEMON_ROLE.out
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

if [ $OPERATION = "stop" ]; then
  echo "using simple jvm configuration for stop operation"
  PIXELS_JVM_OPTS="-Xmx1024M -server -XX:+UseG1GC"
else
  # Set JVM options file path or use default JVM options
  JVM_OPTIONS_FILE="$PIXELS_HOME/etc/$DAEMON_ROLE-jvm.config"
  if [ -z "$PIXELS_JVM_OPTS" ]; then
    if [ -e "$JVM_OPTIONS_FILE" ]; then
      PIXELS_JVM_OPTS=$(tr '\n' ' ' < "$JVM_OPTIONS_FILE")
    else
      echo "$DAEMON_ROLE-jvm.config is not found in $PIXELS_HOME/etc, using default jvm configuration"
      PIXELS_JVM_OPTS="-Xmx1024M -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
    fi
  fi
fi

echo "JVM options: ${PIXELS_JVM_OPTS}"

PIXELS_OPTS="-Dio.netty.leakDetection.level=advanced"
if [ "x$OPERATION" = "x" ]; then
  PIXELS_OPTS=$PIXELS_OPTS
else
  PIXELS_OPTS=$PIXELS_OPTS" -Doperation=$OPERATION"
fi

if [ "x$DAEMON_ROLE" = "x" ]; then
  PIXELS_OPTS=$PIXELS_OPTS
else
  PIXELS_OPTS=$PIXELS_OPTS" -Drole=$DAEMON_ROLE"
fi

echo "PIXELS OPTS: "$PIXELS_OPTS

NUMA_INTERLEAVE=""
if type numactl >/dev/null 2>&1; then
  NUMA_INTERLEAVE="numactl --interleave=all"
fi

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  echo "$DAEMON_ROLE running in the daemon mode."
  nohup ${NUMA_INTERLEAVE} ${JAVA} ${PIXELS_JVM_OPTS} -cp ${CLASSPATH} ${PIXELS_OPTS} "$@" > ${CONSOLE_OUTPUT_FILE} 2>&1 < /dev/null &
else
  exec ${NUMA_INTERLEAVE} ${JAVA} ${PIXELS_JVM_OPTS} -cp ${CLASSPATH} ${PIXELS_OPTS} "$@"
fi

#!/usr/bin/env bash

mvn install

if [ $? -ne 0 ]; then
  echo "ERROR: Maven package failed. Stop installing Pixels."
  exit 1
fi

if [ -z "$PIXELS_HOME" ]; then
  echo "ERROR: PIXELS_HOME is not set. Stop installing Pixels."
  exit 1
else
  echo "PIXELS_HOME is '$PIXELS_HOME'"
fi

# remove the last '/', this is optional, but makes the output look better
if [[ "$PIXELS_HOME" == *"/" ]]
then
    PIXELS_HOME=${PIXELS_HOME::-1}
fi

mkdir -p $PIXELS_HOME/bin
mkdir -p $PIXELS_HOME/sbin
mkdir -p $PIXELS_HOME/etc
mkdir -p $PIXELS_HOME/lib
mkdir -p $PIXELS_HOME/listener
mkdir -p $PIXELS_HOME/logs
mkdir -p $PIXELS_HOME/var

echo "Installing scripts..."

CP_SBIN=0

if [ -z "$(ls -A $PIXELS_HOME/sbin)" ]; then
  CP_SBIN=1
else
  read -p "'$PIXELS_HOME/sbin' not empty, override?[y/n]" -n 1 -r
  echo # move to a new line
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    CP_SBIN=1
  fi
fi

if [ $CP_SBIN -eq 1 ]; then
  cp -v ./scripts/sbin/* $PIXELS_HOME/sbin
fi

CP_BIN=0

if [ -z "$(ls -A $PIXELS_HOME/bin)" ]; then
  CP_BIN=1
else
  read -p "'$PIXELS_HOME/bin' not empty, override?[y/n]" -n 1 -r
  echo # move to a new line
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    CP_BIN=1
  fi
fi

if [ $CP_BIN -eq 1 ]; then
  cp -v ./scripts/bin/* $PIXELS_HOME/bin
fi

echo "Installing pixels-daemons..."
cp -v ./pixels-daemon/target/pixels-daemon-*-full.jar $PIXELS_HOME/bin
echo "Installing pixels-cli..."
cp -v ./pixels-cli/target/pixels-cli-*-full.jar $PIXELS_HOME/sbin

if [ -z "$(ls -A $PIXELS_HOME/lib)" ]; then
  echo "$(
    tput setaf 1
    tput setab 7
  )Make sure to put the jdbc connector of MySQL into '$PIXELS_HOME/lib'!$(tput sgr 0)"
fi

echo "Installing config files..."
# copy the jvm and worker nodes config files
CP_ETC=0

if [ -z "$(ls -A $PIXELS_HOME/etc)" ]; then
  CP_ETC=1
else
  read -p "'$PIXELS_HOME/etc' not empty, override?[y/n]" -n 1 -r
  echo # move to a new line
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    CP_ETC=1
  fi
fi

if [ $CP_ETC -eq 1 ]; then
  cp -v ./scripts/etc/* $PIXELS_HOME/etc
fi

# find and copy pixels.properties
if [ -z "$(find $PIXELS_HOME/etc -name "pixels.properties")" ]; then
  cp -v ./pixels-common/src/main/resources/pixels.properties $PIXELS_HOME/etc
  echo "$(
    tput setaf 1
    tput setab 7
  )Make sure to modify '$PIXELS_HOME/etc/pixels.properties'!$(tput sgr 0)"
else
  read -p "'$PIXELS_HOME/etc/pixels.properties' exists, override?[y/n]" -n 1 -r
  echo # move to a new line
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    cp -v ./pixels-common/src/main/resources/pixels.properties $PIXELS_HOME/etc
  fi
fi

# find and copy pixels-cpp.properties
if [ -z "$(find $PIXELS_HOME/etc -name "pixels-cpp.properties")" ]; then
  cp -v ./cpp/pixels-cpp.properties $PIXELS_HOME/etc
  echo "$(
    tput setaf 1
    tput setab 7
  )Make sure to modify '$PIXELS_HOME/etc/pixels-cpp.properties'!$(tput sgr 0)"
else
  read -p "'$PIXELS_HOME/etc/pixels-cpp.properties' exists, override?[y/n]" -n 1 -r
  echo # move to a new line
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    cp -v ./cpp/pixels-cpp.properties $PIXELS_HOME/etc
  fi
fi

# prompt the post-install steps
echo "$(
  tput setaf 1
  tput setab 7
)You may need to install and configure MySQL and etcd. Please refer to README.$(tput sgr 0)"
echo "$(
  tput setaf 1
  tput setab 7
)See the README of pixels-presto/trino/hive to install a query engine.$(tput sgr 0)"


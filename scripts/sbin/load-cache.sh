#!/bin/bash
export ETCDCTL_API=3
HOST_1=localhost
ENDPOINTS=$HOST_1:2379

etcdctl --endpoints=$ENDPOINTS put layout_version $1

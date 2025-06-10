> # Pixels Docker Deployment Guide

This document describes the deployment of Pixels through Docker containerization, detailing operational procedures, service initialization within container instances, and annotated explanations of Dockerfile directives.

---

## Pixels ALL-IN-ONE Container Overview

This container operates in ALL-IN-ONE mode, enabling full Pixels service functionality within a single container instance without external dependencies.

**Base Image:** `Ubuntu 22.04`  
**Integrated Services:**

| Service  | Version  |
|----------|----------|
| Pixels   | v0.1.0   |
| MySQL    | v8.0     |
| Trino    | 466      |
| ETCD     | v3.3.4   |

**Dependencies:**

| Dependency           | Mandatory |
|----------------------|-----------|
| openjdk-23-jdk       | Yes       |
| openjdk-23-jre       | Yes       |
| python3              | Yes       |
| openssh-server       | Yes       |
| golang               | Yes       |

> Pre-installed utilities for development/debugging: `vim`, `tar`, `git`, `maven`, `wget`, `unzip`

**Minimum Host System Requirements:**

| Environment       | Specification               |
|-------------------|-----------------------------|
| CPU:              | 4 cores                     |
| RAM:              | 4GB                         |
| OS Version:       | Linux kernel 5.15 recommended (linux-generic), any distribution |

---

## Deployment Procedures

> Docker environment required. No specific version constraints.

```shell
# Recommended for users in mainland China due to network restrictions on DockerHub
# Option 1: Build from Dockerfile (requires zulu23.32.11-ca-jdk23.0.2-linux_amd64.deb in build directory)
docker build -t my-pixels:latest .

# Option 2: Import from provided .tar (faster and more stable)
docker load -i my-pixels.tar

# Launch container instance with port 8080 exposed for Trino debugging
docker run -p 8080:8080 -d --name pixels my-pixels tail -f /dev/null
# Access container shell
docker exec -it pixels /bin/bash
```

After container initialization, execute service startup commands (default container user: root). If daemon processes fail to start, configure necessary permissions such as passwordless SSH authentication:

```shell
# Start SSH service
service ssh start
# Initialize MySQL
service mysql start
# Launch ETCD
nohup ~/opt/etcd/start-etcd.sh &
# Navigate to Pixels directory
cd ~/opt/pixels/
# Start Pixels
./sbin/start-pixels.sh                 
# Prepare Trino initialization
cd ~/opt/trino-server
# Launch Trino
./bin/launcher start
# Connect via Trino CLI
./bin/trino --server localhost:8080 --catalog pixels
```

Successful service initialization is confirmed when executing `SHOW SCHEMAS;` in trino-cli:

```shell
    Schema       
-------------------- 
information_schema 
(1 row)
```

> Monitor Trino status via IP:8080

> Reference: [Start Pixels](https://github.com/pixelsdb/pixels/blob/master/docs/INSTALL.md#start-pixels)
>
> 

**MySQL Credentials:**

| username | password |
| -------- | -------- |
| root     | password |
| pixels   | password |

> ALL-IN-ONE configuration uses localhost for Trino connectivity with default ports. For advanced configurations, refer to [Trino Documentation](https://trino.io/docs/405/installation/deployment.html#configuring-trino).


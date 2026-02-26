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
docker build -f Dockerfile -t my-pixels ..

# Option 2: Import from provided .tar (faster and more stable)
docker load -i my-pixels.tar
```

### Launch Container

#### Environment Variables

Prepare the following environment variables for S3/MinIO connectivity:

| Variable              | Description                           |
|-----------------------|---------------------------------------|
| AWS_ACCESS_KEY_ID     | S3/MinIO access key                   |
| AWS_SECRET_ACCESS_KEY | S3/MinIO secret key                   |
| AWS_REGION            | S3/MinIO region                       |
| AWS_ENDPOINT_URL      | S3/MinIO endpoint URL                 |

#### Container Startup

Port `8080` is exposed for Trino web UI and debugging.

```shell
# Launch container with environment variables above
docker run -p 8080:8080 -itd \
  -e AWS_ACCESS_KEY_ID=x \
  -e AWS_SECRET_ACCESS_KEY=x \
  -e AWS_REGION=x \
  -e AWS_ENDPOINT_URL=x \
  --name pixels my-pixels
```

### Verify Installation

#### Check Service Logs

View container logs to confirm all services started successfully:

```shell
docker logs pixels
```

Expected output:
```shell
 * Starting OpenBSD Secure Shell server sshd               [ OK ]
 * Starting MySQL database server mysqld                   [ OK ]
 * Start Etcd ...                                          [  OK  ]
 * Start Pixels ...                                        [  OK  ]
 * Start Trino ...                                         [  OK  ]

=================================================
      Pixels is installed successfully!
=================================================
```

#### Access Container Shell

```shell
# Login to pixels container, you should in path ~/opt
docker exec -it pixels bash

# Connect via Trino CLI in pixels container
~/opt/trino-server/bin/trino --server localhost:8080 --catalog pixels
```

#### Confirm Service Initialization

Successful service initialization is confirmed when executing `SHOW SCHEMAS;` in trino-cli:

```shell
trino> SHOW SCHEMAS;
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


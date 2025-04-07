FROM ac2-registry.cn-hangzhou.cr.aliyuncs.com/ac2/base:ubuntu22.04

# JDK23 not available via apt, manual installation with downloaded deb package required
COPY zulu23.32.11-ca-jdk23.0.2-linux_amd64.deb /root/

# Reconfigure APT sources
RUN sed -i 's|http://mirrors.cloud.aliyuncs.com|http://mirrors.aliyun.com|g' /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse" > /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse" >> /etc/apt/sources.list && \

    apt-get update &&\
    apt-get install -y --no-install-recommends \
    git maven \
    python-is-python3 \
    wget tar unzip openssh-server vim \
    mysql-server golang \
    ca-certificates libmysqlclient-dev \
    # Additional dependency packages
    xz-utils \
    libx11-6 libxau6 libxcb1 libxdmcp6 \
    libxext6 libxi6 libxrender1 libxtst6 \
    # Install JDK with dependency resolution
    && dpkg -i /root/zulu23.32.11-ca-jdk23.0.2-linux_amd64.deb || true && \
    apt-get -f install -y \


# Prepare source code directory
&& mkdir /root/opt \

# Download and build Pixels source code
&& cd /root/opt/ \
&& git clone https://github.com/pixelsdb/pixels.git \
&& cd /root/opt/pixels/ \
&& mvn install \

# Execute Pixels installation script
&& echo 'export PIXELS_HOME=$HOME/opt/pixels/' >> /root/.bashrc \
&& . /root/.bashrc \
&& bash /root/opt/pixels/install.sh \

# Prepare MySQL Connector/J
&& cd /root/opt/pixels/lib \
&& wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \

# Clone and compile pixels-trino
&& cd /root/opt/ \
&& git clone https://github.com/pixelsdb/pixels-trino.git \
&& cd /root/opt/pixels-trino \
&& mvn package \

# Download and install Trino binaries
&& cd /root/opt \
&& wget https://repo1.maven.org/maven2/io/trino/trino-server/466/trino-server-466.tar.gz \
&& tar xzvf trino-server-466.tar.gz && rm -rf trino-server-466.tar.gz \
&& ln -s trino-server-466 trino-server \
&& cd /root/opt/trino-server/bin/ \
&& wget https://repo1.maven.org/maven2/io/trino/trino-cli/466/trino-cli-466-executable.jar \
&& mv trino-cli-466-executable.jar trino \
&& chmod +x trino \

# Configure Trino properties
&& mkdir /root/opt/data \
&& mkdir /root/opt/trino-server/etc \
# Node configuration
&& echo "node.environment=production" >> /root/opt/trino-server/etc/node.properties \
&& echo "node.id=ffffffff-ffff-ffff-ffff-ffffffffffff" >> /root/opt/trino-server/etc/node.properties \
&& echo "node.data-dir=/root/opt/data" >> /root/opt/trino-server/etc/node.properties \
# JVM configuration
&& echo "-server" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-Xmx16G" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:InitialRAMPercentage=80" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:MaxRAMPercentage=80" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:G1HeapRegionSize=32M" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:+ExplicitGCInvokesConcurrent" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:+ExitOnOutOfMemoryError" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:+HeapDumpOnOutOfMemoryError" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:-OmitStackTraceInFastThrow" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:ReservedCodeCacheSize=512M" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:PerMethodRecompilationCutoff=10000" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:PerBytecodeRecompilationCutoff=10000" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-Djdk.attach.allowAttachSelf=true" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-Djdk.nio.maxCachedBufferSize=2000000" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-Dfile.encoding=UTF-8" >> /root/opt/trino-server/etc/jvm.config \
&& echo "-XX:+EnableDynamicAgentLoading" >> /root/opt/trino-server/etc/jvm.config \

# Development environment configuration (modify for production)
&& echo "coordinator=true" >> /root/opt/trino-server/etc/config.properties \
# Set coordinator=false for worker nodes
&& echo "node-scheduler.include-coordinator=true" >> /root/opt/trino-server/etc/config.properties \
&& echo "http-server.http.port=8080" >> /root/opt/trino-server/etc/config.properties \
&& echo "discovery.uri=http://localhost:8080" >> /root/opt/trino-server/etc/config.properties \

# Log configuration
&& echo "io.trino=INFO" > /root/opt/trino-server/etc/log.properties \
# Catalog configuration
&& mkdir /root/opt/trino-server/etc/catalog \
&& echo "connector.name=jmx" > /root/opt/trino-server/etc/catalog/jmx.properties \

# Deploy Trino plugins: pixels-listener and pixels-connector
&& unzip -d /root/opt/trino-server/plugin /root/opt/pixels-trino/listener/target/pixels-trino-listener-*.zip \
&& unzip -d /root/opt/trino-server/plugin /root/opt/pixels-trino/connector/target/pixels-trino-connector-*.zip \
# Configure pixels.properties
&& echo "connector.name=pixels" >> /root/opt/trino-server/etc/catalog/pixels.properties \
&& echo "# Serverless configuration options: on, off, auto, session" >> /root/opt/trino-server/etc/catalog/pixels.properties \
&& echo "cloud.function.switch=off" >> /root/opt/trino-server/etc/catalog/pixels.properties \
&& echo "clean.intermediate.result=true" >> /root/opt/trino-server/etc/catalog/pixels.properties \
# Configure event listener
&& echo "event-listener.name=pixels-event-listener" >> /root/opt/trino-server/etc/event-listener.properties \
&& echo "enabled=true" >> /root/opt/trino-server/etc/event-listener.properties \
&& echo "listened.user.prefix=none" >> /root/opt/trino-server/etc/event-listener.properties \
&& echo "listened.schema=pixels" >> /root/opt/trino-server/etc/event-listener.properties \
&& echo "listened.query.type=SELECT" >> /root/opt/trino-server/etc/event-listener.properties \
&& echo "log.dir=/root/opt/pixels/listener/" >> /root/opt/trino-server/etc/event-listener.properties \
# Append JVM arguments
&& echo "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" >> /root/opt/trino-server/etc/jvm.config \
&& echo "--add-opens=java.base/java.nio=ALL-UNNAMED" >> /root/opt/trino-server/etc/jvm.config \

# Configure MySQL
&& cd /root \
# Initialize root user credentials (username: root, password: password)
&& echo "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';" >> /root/root.sql \
# Create pixels user and database
&& echo "CREATE USER 'pixels'@'%' IDENTIFIED BY 'password';" >> /root/user.sql \
&& echo "CREATE DATABASE pixels_metadata;" >> /root/user.sql \
&& echo "GRANT ALL PRIVILEGES ON pixels_metadata.* to 'pixels'@'%';" >> /root/user.sql \
&& echo "FLUSH PRIVILEGES;" >> /root/user.sql \

# Initialize MySQL service
&& usermod -d /var/lib/mysql/ mysql \
&& service mysql start \
&& while ! mysqladmin ping -hlocalhost --silent; do sleep 1; done  \
# Initialize root user
&& mysql < /root/root.sql \
# Create root credential file
&& echo "[client]" >> /root/root.cnf \
&& echo "user=root" >> /root/root.cnf \
&& echo "password=password" >> /root/root.cnf \
# Create pixels user credential file
&& echo "[client]" >> /root/pixels.cnf \
&& echo "user=pixels" >> /root/pixels.cnf \
&& echo "password=password" >> /root/pixels.cnf \
# Initialize metadata schema
&& mysql --defaults-file=root.cnf < /root/user.sql \
&& mysql --defaults-file=pixels.cnf < /root/opt/pixels/scripts/sql/metadata_schema.sql \

# Install and configure ETCD
&& cp -v /root/opt/pixels/scripts/tars/etcd-v3.3.4-linux-amd64.tar.xz /root/opt/ \
&& cd /root/opt \
&& tar xf etcd-v3.3.4-linux-amd64.tar.xz \
&& echo 'export ETCDCTL_API=3' >> /root/.bashrc \
&& echo 'export ETCD=$HOME/opt/etcd-v3.3.4-linux-amd64-bin' >> /root/.bashrc \
&& echo 'export PATH=$PATH:$ETCD' >> /root/.bashrc \
&& . /root/.bashrc && ln -s etcd-v3.3.4-linux-amd64-bin etcd \

# Cleanup to reduce image size
&& rm -rf /var/lib/apt/lists/* \
&& rm -rf /root/SRC_BASE

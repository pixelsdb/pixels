FROM ac2-registry.cn-hangzhou.cr.aliyuncs.com/ac2/base:ubuntu22.04

# Define mandatory env for entire pixels installation
ENV HOME=/root
ENV PIXELS_HOME=$HOME/opt/pixels JAVA_HOME=/usr/lib/current_java

# JDK23 not available via apt, manual installation with downloaded deb package required
# https://cdn.azul.com/zulu/bin/zulu23.32.11-ca-jdk23.0.2-linux_amd64.deb
COPY docker/zulu23.32.11-ca-jdk23.0.2-linux_amd64.deb $HOME/pixels_jdk.deb

# Reconfigure APT sources
RUN sed -i 's|http://mirrors.cloud.aliyuncs.com|http://mirrors.aliyun.com|g' /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy main restricted universe multiverse" > /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy-updates main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy-backports main restricted universe multiverse" >> /etc/apt/sources.list && \
    echo "deb http://mirrors.aliyun.com/ubuntu/ jammy-security main restricted universe multiverse" >> /etc/apt/sources.list && \

    apt-get update &&\
    apt-get install -y --no-install-recommends \
    git maven less g++ cmake make netcat-openbsd\
    python-is-python3 \
    wget tar unzip openssh-server vim \
    mysql-server golang \
    ca-certificates libmysqlclient-dev \
    # Additional dependency packages
    xz-utils \
    libx11-6 libxau6 libxcb1 libxdmcp6 \
    libxext6 libxi6 libxrender1 libxtst6 \
    # Install JDK with dependency resolution
    && dpkg -i $HOME/pixels_jdk.deb || true \
    && apt-get -f install -y \
    && rm -f $HOME/pixels_jdk.deb \


# Auto calculate java dictionary, use softlink to force JAVA_HOME
&& export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) \
&& ln -s ${JAVA_HOME} /usr/lib/current_java \

# SSH passwordless login used by pixels when worker start
&& ssh-keygen -t rsa -b 2048 -N "" -f $HOME/.ssh/id_rsa \
&& cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys \
&& printf "\nStrictHostKeyChecking no\nUserKnownHostsFile /dev/null\n" >> /etc/ssh/ssh_config \

# Download and build Pixels source code
&& mkdir $HOME/opt \
&& cd $HOME/opt/ \
&& git clone --depth 1 https://github.com/pixelsdb/pixels.git \

# Execute Pixels installation script
&& cd $PIXELS_HOME && mvn install \
&& bash $PIXELS_HOME/install.sh \

# Prepare MySQL Connector/J
&& cd $PIXELS_HOME/lib/ \
&& wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar \

# Clone and compile pixels-trino
&& cd $HOME/opt/ \
&& git clone --depth 1 https://github.com/pixelsdb/pixels-trino.git \
&& cd $HOME/opt/pixels-trino \
&& mvn package \

# Download and install Trino binaries
&& cd $HOME/opt \
&& wget https://repo1.maven.org/maven2/io/trino/trino-server/466/trino-server-466.tar.gz \
&& tar xzvf trino-server-466.tar.gz && rm -rf trino-server-466.tar.gz \
&& ln -s trino-server-466 trino-server \
&& cd $HOME/opt/trino-server/bin \
&& wget https://repo1.maven.org/maven2/io/trino/trino-cli/466/trino-cli-466-executable.jar \
&& mv trino-cli-466-executable.jar trino \
&& chmod +x trino \

# Configure Trino properties
&& mkdir $HOME/opt/data \
&& mkdir $HOME/opt/trino-server/etc \
# Node configuration
&& echo "node.environment=production" >> $HOME/opt/trino-server/etc/node.properties \
&& echo "node.id=ffffffff-ffff-ffff-ffff-ffffffffffff" >> $HOME/opt/trino-server/etc/node.properties \
&& echo "node.data-dir=$HOME/opt/data" >> $HOME/opt/trino-server/etc/node.properties \
# JVM configuration
&& echo "-server" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-Xmx16G" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:InitialRAMPercentage=80" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:MaxRAMPercentage=80" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:G1HeapRegionSize=32M" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:+ExplicitGCInvokesConcurrent" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:+ExitOnOutOfMemoryError" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:+HeapDumpOnOutOfMemoryError" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:-OmitStackTraceInFastThrow" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:ReservedCodeCacheSize=512M" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:PerMethodRecompilationCutoff=10000" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:PerBytecodeRecompilationCutoff=10000" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-Djdk.attach.allowAttachSelf=true" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-Djdk.nio.maxCachedBufferSize=2000000" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-Dfile.encoding=UTF-8" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "-XX:+EnableDynamicAgentLoading" >> $HOME/opt/trino-server/etc/jvm.config \

# Development environment configuration (modify for production)
&& echo "coordinator=true" >> $HOME/opt/trino-server/etc/config.properties \
# Set coordinator=false for worker nodes
&& echo "node-scheduler.include-coordinator=true" >> $HOME/opt/trino-server/etc/config.properties \
&& echo "http-server.http.port=8080" >> $HOME/opt/trino-server/etc/config.properties \
&& echo "discovery.uri=http://localhost:8080" >> $HOME/opt/trino-server/etc/config.properties \

# Log configuration
&& echo "io.trino=INFO" > $HOME/opt/trino-server/etc/log.properties \
# Catalog configuration
&& mkdir $HOME/opt/trino-server/etc/catalog \
&& echo "connector.name=jmx" > $HOME/opt/trino-server/etc/catalog/jmx.properties \

# Deploy Trino plugins: pixels-listener and pixels-connector
&& unzip -d $HOME/opt/trino-server/plugin $HOME/opt/pixels-trino/listener/target/pixels-trino-listener-*.zip \
&& unzip -d $HOME/opt/trino-server/plugin $HOME/opt/pixels-trino/connector/target/pixels-trino-connector-*.zip \
# Configure pixels.properties
&& echo "connector.name=pixels" >> $HOME/opt/trino-server/etc/catalog/pixels.properties \
&& echo "# Serverless configuration options: on, off, auto, session" >> $HOME/opt/trino-server/etc/catalog/pixels.properties \
&& echo "cloud.function.switch=off" >> $HOME/opt/trino-server/etc/catalog/pixels.properties \
&& echo "clean.intermediate.result=true" >> $HOME/opt/trino-server/etc/catalog/pixels.properties \
# Configure event listener
&& echo "event-listener.name=pixels-event-listener" >> $HOME/opt/trino-server/etc/event-listener.properties \
&& echo "enabled=true" >> $HOME/opt/trino-server/etc/event-listener.properties \
&& echo "listened.user.prefix=none" >> $HOME/opt/trino-server/etc/event-listener.properties \
&& echo "listened.schema=pixels" >> $HOME/opt/trino-server/etc/event-listener.properties \
&& echo "listened.query.type=SELECT" >> $HOME/opt/trino-server/etc/event-listener.properties \
&& echo "log.dir=$HOME/opt/pixels/listener/" >> $HOME/opt/trino-server/etc/event-listener.properties \
# Append JVM arguments
&& echo "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" >> $HOME/opt/trino-server/etc/jvm.config \
&& echo "--add-opens=java.base/java.nio=ALL-UNNAMED" >> $HOME/opt/trino-server/etc/jvm.config \

# Configure MySQL
&& cd $HOME \
# Initialize root user credentials (username: root, password: password)
&& echo "ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';" >> $HOME/root.sql \
# Create pixels user and database
&& echo "CREATE USER 'pixels'@'%' IDENTIFIED BY 'password';" >> $HOME/user.sql \
&& echo "CREATE DATABASE pixels_metadata;" >> $HOME/user.sql \
&& echo "GRANT ALL PRIVILEGES ON pixels_metadata.* to 'pixels'@'%';" >> $HOME/user.sql \
&& echo "FLUSH PRIVILEGES;" >> $HOME/user.sql \

# Initialize MySQL service
&& usermod -d /var/lib/mysql/ mysql \
&& service mysql start \
&& while ! mysqladmin ping -hlocalhost --silent; do sleep 1; done  \
# Initialize root user
&& mysql < $HOME/root.sql \
# Create root credential file
&& echo "[client]" >> $HOME/root.cnf \
&& echo "user=root" >> $HOME/root.cnf \
&& echo "password=password" >> $HOME/root.cnf \
# Create pixels user credential file
&& echo "[client]" >> $HOME/pixels.cnf \
&& echo "user=pixels" >> $HOME/pixels.cnf \
&& echo "password=password" >> $HOME/pixels.cnf \
# Initialize metadata schema
&& mysql --defaults-file=root.cnf < $HOME/user.sql \
&& mysql --defaults-file=pixels.cnf < $HOME/opt/pixels/scripts/sql/metadata_schema.sql \
&& service mysql stop \

# Install ETCD
&& cp -v $HOME/opt/pixels/scripts/tars/etcd-v3.3.4-linux-amd64.tar.xz $HOME/opt/ \
&& cd $HOME/opt \
&& tar xf etcd-v3.3.4-linux-amd64.tar.xz \
&& ln -s etcd-v3.3.4-linux-amd64-bin etcd \

# Cleanup to reduce image size
&& rm -rf /var/lib/apt/lists/* \
&& rm -rf $HOME/SRC_BASE \
&& rm -rf $HOME/.m2 

# configure ETCD
ENV ETCDCTL_API=3 ETCD=$HOME/opt/etcd 
ENV PATH=$PATH:$ETCD

# change workspace & startup
EXPOSE 8080
WORKDIR $HOME/opt
COPY scripts/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh 
ENTRYPOINT ["bash","-c","/entrypoint.sh"]

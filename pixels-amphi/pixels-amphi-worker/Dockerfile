FROM amd64/gcc:latest

# install dependencies
RUN apt-get update &&  \
    apt-get install -y \
    build-essential \
    autoconf \
    libtool \
    pkg-config \
    cmake \
    git \
    libssl-dev \
    libprotobuf-dev \
    protobuf-compiler \
    protobuf-compiler-grpc \
    libyaml-cpp-dev \
    libgrpc++-dev \
    libgrpc-dev \
    wget \
    unzip \
    && apt-get clean

ENV GRPC_RELEASE_TAG v1.54.0

# download duckdb binary
RUN echo "-- downloading duckdb" && \
    wget https://github.com/duckdb/duckdb/releases/download/v0.8.0/libduckdb-linux-amd64.zip \
    && unzip libduckdb-linux-amd64.zip -d /duckdb

COPY . /pixels-worker-amphi

WORKDIR /pixels-worker-amphi

# replace with cmake file for docker
RUN cp resources/CMakeLists.txt CMakeLists.txt

RUN make clean
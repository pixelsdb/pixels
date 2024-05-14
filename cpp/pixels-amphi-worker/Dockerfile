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
    libspdlog-dev \
    libyaml-cpp-dev \
    libgrpc++-dev \
    libgrpc-dev \
    wget \
    zip \
    unzip \
    && apt-get clean

ENV GRPC_RELEASE_TAG v1.54.0

# download duckdb binary
RUN echo "-- downloading duckdb" && \
    wget https://github.com/duckdb/duckdb/releases/download/v0.8.0/libduckdb-linux-amd64.zip \
    && unzip libduckdb-linux-amd64.zip -d /duckdb

# set up workspace
RUN mkdir /proto
WORKDIR /proto
RUN wget -O amphi.proto https://raw.githubusercontent.com/voidforall/pixels/pixels-worker-amphi/proto/amphi.proto
RUN wget -O metadata.proto https://raw.githubusercontent.com/voidforall/pixels/pixels-worker-amphi/proto/metadata.proto
COPY . /home/pixels-worker-amphi
WORKDIR /home/pixels-worker-amphi
RUN cp resources/CMakeLists_root.txt CMakeLists.txt
RUN cp resources/CMakeLists_worker.txt /home/pixels-worker-amphi/worker/CMakeLists.txt
RUN cp resources/CMakeLists_test.txt /home/pixels-worker-amphi/tests/CMakeLists.txt
RUN cp resources/CMakeLists_benchmark.txt /home/pixels-worker-amphi/benchmark/CMakeLists.txt
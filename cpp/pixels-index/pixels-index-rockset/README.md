# Pixels Index Rockset

### Main Features
This is the corresponding C++ module for `RocksetIndex`.
It implements the JNI methods for pixels-index-rockset to operate Rockset (RocksDB Cloud).

### Build Instructions
This module is built and tested with:

- GCC/G++ with C++11 support (version 4.8 or later)
- RocksDB Cloud `v6.7.3`
- AWS SDK for C++ installed under `/usr/local`

The RocksDB Cloud version is pinned in `CMakeLists.txt` as `ROCKSDB_CLOUD_GIT_TAG v6.7.3`.
Do not switch it to `master`: newer RocksDB headers are not ABI/API compatible with this module.

1. We download and install rocksdb-cloud automatically in the CMakeLists of pixels-index-rockset.
   However, the compilation process of rocksdb-cloud assumes that the AWS c++ SDK is installed in
   the default location of /usr/local. You can follow the steps listed here https://github.com/aws/aws-sdk-cpp (version 1.7.325 or later).
   Following the RocksDB Linux - Ubuntu instructions, install the prerequisite packages first:
```bash
sudo apt-get install \
  libgflags-dev \
  libsnappy-dev \
  zlib1g-dev \
  libbz2-dev \
  liblz4-dev \
  libzstd-dev \
  libcurl4-openssl-dev
```
   Or on CentOS / RHEL, prepare the prerequisites like this:
```bash
# GCC/G++ with C++11 support
sudo yum install gcc48-c++

# gflags
git clone https://github.com/gflags/gflags.git
cd gflags
git checkout v2.0
./configure && make && sudo make install
# If installed to /usr/local, export CPATH=/usr/local/include:$CPATH
# and LIBRARY_PATH=/usr/local/lib:$LIBRARY_PATH

# snappy / zlib / bzip2 / lz4 / optional ASAN
sudo yum install snappy snappy-devel
sudo yum install zlib zlib-devel
sudo yum install bzip2 bzip2-devel
sudo yum install lz4-devel
sudo yum install libasan

# zstd
wget https://github.com/facebook/zstd/archive/v1.1.3.tar.gz
mv v1.1.3.tar.gz zstd-1.1.3.tar.gz
tar zxvf zstd-1.1.3.tar.gz
cd zstd-1.1.3
make && sudo make install
```
   Here are the AWS SDK build and install commands for reference:
```bash
git clone https://github.com/aws/aws-sdk-cpp
cd aws-sdk-cpp
git checkout 1.11.578 ## we use this version by default
git submodule update --init --recursive
mkdir build && cd build
cmake .. \
-DCMAKE_BUILD_TYPE=Release \
-DBUILD_ONLY="s3;core;transfer;kinesis" \
-DENABLE_UNITY_BUILD=ON \
-DENABLE_TESTING=OFF \
-DBUILD_SHARED_LIBS=ON \
-DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build . --config=Release
sudo cmake --install . --config=Release
```

2. Use the CI/bootstrap build script. It supports Ubuntu and CentOS/RHEL-like systems, installs the documented system dependencies, installs AWS SDK for C++ under `/usr/local` when missing, and then runs the native build.
Provide Java separately in the environment before running it:
```bash
cd cpp/pixels-index/pixels-index-rockset
./ci-build.sh
```
The script checks and/or prepares:
- `cmake`, `make`, `git`
- `PIXELS_HOME`
- `JAVA_HOME` and JNI headers
- `gcc` and `g++` in `PATH`, with version `4.8` or later
- RocksDB build dependencies for Linux, including Ubuntu and CentOS/RHEL variants: `gflags`, `snappy`, `zlib`, `bzip2`, `lz4`, `zstd`, `libcurl`
- AWS SDK for C++ installed under `/usr/local`
After a successful build, it also installs `libpixels-index-rockset.so` and the required `librocksdb.so*` runtime libraries into `$PIXELS_HOME/lib`.

If you need to run the native build commands manually after the environment is ready, the script executes the equivalent workflow:
```bash
export CC=/path/to/gcc
export CXX=/path/to/g++
mkdir build && cd build
cmake .. # in the output of this command, check whether the correct JNI version is used
make
```

3. After completing the above steps, you can find the generated `libpixels-index-rockset.so` in the local `build/` directory.

4. If everything works correctly, you can find the installed `libpixels-index-rockset.so` and `librocksdb.so*` in the `$PIXELS_HOME/lib` directory.
This shared library is loaded by the Java code under `PIXELS_SRC/pixels-index/pixels-index-rockset/src/main/java/io/pixelsdb/pixels/index/rockset`.

You can verify that the expected RocksDB Cloud and LZ4 dependencies are linked with:
```bash
ldd "$PIXELS_HOME/lib/libpixels-index-rockset.so"
ldd build/deps/rocksdb_cloud-install/lib/librocksdb.so | grep lz4
```

If you modified the native methods in the Java JNI classes under
`PIXELS_SRC/pixels-index/pixels-index-rockset/src/main/java/io/pixelsdb/pixels/index/rockset/jni`,
regenerate the corresponding JNI header files using:
```bash
# enter the cpp source path of pixels-index-rockset
cd cpp/pixels-index/pixels-index-rockset/
# regenerate JNI header files from compiled Java classes
javac -h ./include/jni \
  ../../../pixels-index/pixels-index-rockset/src/main/java/io/pixelsdb/pixels/index/rockset/jni/*.java
```
And redo the above steps to build the `libpixels-index-rockset.so` library.

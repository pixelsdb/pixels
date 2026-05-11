# Pixels Index Rockset

### Main Features
This is the corresponding C++ module for `RocksetIndex`.
It implements the JNI methods for pixels-index-rockset to operate Rockset (RocksDB Cloud).

### Build Instructions
This module is built and tested with:

- GCC/G++ 10 (`/usr/bin/gcc-10` and `/usr/bin/g++-10`)
- RocksDB Cloud `v6.7.3`
- AWS SDK for C++ installed under `/usr/local`

The RocksDB Cloud version is pinned in `CMakeLists.txt` as `ROCKSDB_CLOUD_GIT_TAG v6.7.3`.
Do not switch it to `master`: newer RocksDB headers are not ABI/API compatible with this module.

1. We download and install rocksdb-cloud automatically in the CMakeLists of pixels-index-rockset.
   However, the compilation process of rocksdb-cloud assumes that the AWS c++ SDK is installed in
   the default location of /usr/local. You can follow the steps listed here https://github.com/aws/aws-sdk-cpp (version 1.7.325 or later).
   Here are the instructions for your reference:
```bash
sudo apt install gcc-10 g++-10 libcurl4-openssl-dev liblz4-dev
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

2. Enter the cpp source directory of pixels-index-rockset, 
ensure the `PIXELS_HOME` and `JAVA_HOME` environment variables are set correctly, then execute:
```bash
export CC=/usr/bin/gcc-10
export CXX=/usr/bin/g++-10
mkdir build && cd build
cmake .. # in the output of this command, check whether the correct JNI version is used
make
```

3. After completing the above steps, if everything works correctly, you can find the generated `libpixels-index-rockset.so` in the `$PIXELS_HOME/lib` directory.
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

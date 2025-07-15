# Pixels Index Rockset

### Main Features
This is the corresponding C++ module for `RocksetIndex`.
It implements the JNI methods for pixels-index-rockset to operate Rockset (RocksDB Cloud).

### Build Instructions
1. We download and install rocksdb-cloud automatically in the CMakeLists of pixels-index-rockset.
   However, the compilation process of rocksdb-cloud assumes that the AWS c++ SDK is installed in
   the default location of /usr/local. You can follow the steps listed here https://github.com/aws/aws-sdk-cpp (version 1.7.325 or later).
   Here are the instructions for your reference:
```bash
sudo apt install libcurl4-openssl-dev # libcurl is required by aws-sdk-cpp
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
mkdir build && cd build
cmake .. # in the output of this command, check whether the correct JNI version is used
make
```

3. After completing the above steps, if everything works correctly, you can find the generated `libpixels-index-rockset.so` in the `$PIXELS_HOME/lib` directory.
This shared library is to be loaded and bind to the Java code in `PIXELS_SRC/pixels-index/pixels-index-rockset/src/main/java/io/pixelsdb/pixels/index/rocksdb/RocksetIndex.java`.

If you modified the native methods in `PIXELS_SRC/pixels-index/pixels-index-rockset/src/main/java/io/pixelsdb/pixels/index/rocksdb/RocksetIndex.java`, regenerate the `RocksetJni.h` file using:
```bash
# enter the cpp source path of pixels-index-rockset
cd cpp/pixels-index/pixels-index-rockset/
#regenerate the JNI header file
javac -cp . -h ./include/ io/pixelsdb/pixels/index/rocksdb/RocksetIndex.java
# rename the header file
mv ./include/io_pixelsdb_pixels_index_rocksdb_RocksetIndex.h ./include/RocksetJni.h
```
And redo the above steps to build the `libpixels-index-rockset.so` library.
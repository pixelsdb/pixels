# Pixels Index Rockset

### Main Features
This is the corresponding C++ module for `RocksetIndex`, used to build `libRocksetJni.so` to facilitate Java development in the project. The main functions include `createInstance`, `get`, `put`, `delete`, and `close` operations for Rockset.

### Build Instructions
1. The compilation process assumes that the AWS c++ SDK is installed in
the default location of /usr/local. You can follow the steps listed
here https://github.com/aws/aws-sdk-cpp (version 1.7.325 or later)
to install the c++ AWS sdk.
2. `mkdir build && cd build`
3. `cmake ..`
4. `make`
5. After completing the above steps, if everything works correctly, you should see the generated `libRocksetJni.so` in the `$PIXELS_HOME/lib` directory.
6. If you modify the native methods in `pixels/pixels-index/pixels-index-rockset/src/main/java/io/pixelsdb/pixels/index/rocksdb/RocksetIndex.java`, you need to regenerate the `io_pixelsdb_pixels_index_rocksdb_RocksetIndex.h` file using:
`javac -cp . -h /home/ubuntu/pixels/cpp/pixels-index/pixels-index-rockset io/pixelsdb/pixels/index/rocksdb/RocksetIndex.java`

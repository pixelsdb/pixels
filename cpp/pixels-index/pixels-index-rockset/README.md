# Pixels Index Rockset

### Main Features
This is the corresponding C++ module for `RocksetIndex`, used to build `libRocksetJni.so` to facilitate Java development in the project. The main functions include `createInstance`, `get`, `put`, `delete`, and `close` operations for Rockset.

### Build Instructions
1. `mkdir build && cd build`
2. `cmake ..`
3. `make`
4. After completing the above steps, if everything works correctly, you should see the generated `libRocksetJni.so` in the `$PIXELS_HOME/lib` directory.
5. If you modify the native methods in `pixels/pixels-index/pixels-index-rockset/src/main/java/io/pixelsdb/pixels/index/rocksdb/RocksetIndex.java`, you need to regenerate the `io_pixelsdb_pixels_index_rocksdb_RocksetIndex.h` file using:
`javac -cp . -h /home/ubuntu/pixels/cpp/pixels-index/pixels-index-rockset io/pixelsdb/pixels/index/rocksdb/RocksetIndex.java`

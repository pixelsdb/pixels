cmake_minimum_required(VERSION 3.10)
project(pixels_rockset_jni)

# 设置RocksDB-Cloud路径
set(ROCKSDB_CLOUD_DIR /home/ubuntu/home/eliza/rocksdb-cloud)

# 包含路径
include_directories(
    ${JNI_INCLUDE_DIRS}
    ${ROCKSDB_CLOUD_DIR}/include     # RocksDB-Cloud头文件
    ${CMAKE_CURRENT_SOURCE_DIR}       # 当前cpp目录
)

# 生成动态库
add_library(RocksetJni SHARED RocksetJni.cpp)

# 链接库
target_link_libraries(RocksetJni
    ${ROCKSDB_CLOUD_DIR}/build/librocksdb.so
    z curl pthread  # RocksDB的依赖库
)
cmake_minimum_required(VERSION 3.10)
project(pixels_rockset_jni)

include(ExternalProject)
include(ProcessorCount)

# 0. Get core count
ProcessorCount(CORES)
if(CORES EQUAL 0)
    set(CORES 1)
endif()

# 1. Check and set PIXELS_HOME
set(PIXELS_HOME $ENV{PIXELS_HOME})
if(NOT PIXELS_HOME)
    message(FATAL_ERROR "PIXELS_HOME environment variable not set")
endif()

# 2. Find JNI dependency (automatically locates JAVA_HOME/include and platform-specific headers)
find_package(JNI REQUIRED)
if (NOT JNI_FOUND)
    message(FATAL_ERROR "JNI not found! Please ensure JAVA_HOME is set")
else()
    message(STATUS "Found JNI: ${JNI_INCLUDE_DIRS}")
endif()

# 3. Configure rocksdb-cloud as external project
set(ROCKSDB_CLOUD_GIT_REPOSITORY git@github.com:rockset/rocksdb-cloud.git)
set(ROCKSDB_CLOUD_GIT_TAG v6.7.3)
set(ROCKSDB_CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_C_COMPILER=/usr/bin/gcc-10
    -DCMAKE_CXX_COMPILER=/usr/bin/g++-10
    -DWITH_GFLAGS=OFF
    -DWITH_TESTS=OFF
    -DWITH_BENCHMARK_TOOLS=OFF
    -DWITH_CORE_TOOLS=OFF
    -DWITH_TOOLS=OFF
    -DFAIL_ON_WARNINGS=OFF
    -DUSE_RTTI=1
    -DCMAKE_CXX_FLAGS=-frtti\ -DUSE_AWS
    -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/deps/rocksdb_cloud-install
    -DWITH_JNI=OFF
    -DWITH_SNAPPY=OFF
    -DWITH_ZLIB=ON
    -DWITH_BZ2=OFF
    -DWITH_LZ4=ON
    -DWITH_ZSTD=OFF
    -DWITH_LIBURING=ON
    -DWITH_AWS=ON
)

# 4. Download and build rocksdb-cloud
ExternalProject_Add(rocksdb_cloud
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/deps
    GIT_REPOSITORY ${ROCKSDB_CLOUD_GIT_REPOSITORY}
    GIT_TAG ${ROCKSDB_CLOUD_GIT_TAG}
    GIT_SHALLOW true
    UPDATE_COMMAND ""
    PATCH_COMMAND ${CMAKE_COMMAND}
        -DROCKSDB_CLOUD_SOURCE_DIR=<SOURCE_DIR>
        -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/PatchRocksDBCloud.cmake
    CMAKE_ARGS ${ROCKSDB_CMAKE_ARGS}
    BUILD_BYPRODUCTS
        ${CMAKE_CURRENT_BINARY_DIR}/deps/rocksdb_cloud-install/lib/librocksdb${CMAKE_SHARED_LIBRARY_SUFFIX}
    INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/deps/rocksdb_cloud-install
    BUILD_COMMAND make -j${CORES}
)

# 5. Get the installation directory
ExternalProject_Get_Property(rocksdb_cloud INSTALL_DIR)
set(ROCKSDB_CLOUD_INCLUDE_DIR ${INSTALL_DIR}/include)
set(ROCKSDB_CLOUD_LIBRARY ${INSTALL_DIR}/lib/librocksdb${CMAKE_SHARED_LIBRARY_SUFFIX})
file(MAKE_DIRECTORY ${ROCKSDB_CLOUD_INCLUDE_DIR})

# 6. Create imported target for rocksdb
add_library(RocksDB::rocksdb SHARED IMPORTED GLOBAL)
set_target_properties(RocksDB::rocksdb PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_CLOUD_INCLUDE_DIR}"
    IMPORTED_LOCATION "${ROCKSDB_CLOUD_LIBRARY}"
)
add_dependencies(RocksDB::rocksdb rocksdb_cloud)

# 7. Include header directories
include_directories(
    ${JNI_INCLUDE_DIRS}              # JNI headers (jni.h, jni_md.h)
    ${ROCKSDB_CLOUD_INCLUDE_DIR}     # RocksDB-Cloud headers
    ${CMAKE_SOURCE_DIR}/include/jni
)

# 8. Collect all JNI cpp files
file(GLOB ROCKSET_JNI_SOURCES
    ${CMAKE_SOURCE_DIR}/lib/jni/*.cpp
)

message(STATUS "Rockset JNI sources: ${ROCKSET_JNI_SOURCES}")


# 9. Build pixels-index-rockset shared library
add_library(pixels-index-rockset SHARED
    ${ROCKSET_JNI_SOURCES}
)

add_dependencies(pixels-index-rockset rocksdb_cloud)

target_compile_definitions(pixels-index-rockset
    PRIVATE ROCKSDB_NAMESPACE=rocksdb
)

# 10. ink RocksDB and its dependencies
target_link_libraries(pixels-index-rockset
    RocksDB::rocksdb
    z
    lz4
    curl
    pthread
    dl
)

# 11. Install pixels-index-rockset to ${PIXELS_HOME}/lib
install(TARGETS pixels-index-rockset
        LIBRARY DESTINATION ${PIXELS_HOME}/lib)

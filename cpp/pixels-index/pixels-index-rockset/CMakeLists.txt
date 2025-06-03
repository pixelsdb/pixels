cmake_minimum_required(VERSION 3.10)
project(pixels_rockset_jni)
include(ExternalProject)

# 1. set PIXELS_HOME
set(PIXELS_HOME $ENV{PIXELS_HOME})
if(NOT PIXELS_HOME)
    message(FATAL_ERROR "PIXELS_HOME environment variable not set")
endif()

# 2. Find JNI dependency (automatically locates JAVA_HOME/include and platform-specific headers)
find_package(JNI REQUIRED)
if (NOT JNI_FOUND)
    message(FATAL_ERROR "JNI not found! Please ensure JAVA_HOME is set.")
else()
    message(STATUS "Found JNI: ${JNI_INCLUDE_DIRS}")
endif()

# 3. Configure rocksdb-cloud as external project
set(ROCKSDB_CLOUD_GIT_REPOSITORY https://github.com/rockset/rocksdb-cloud.git)
set(ROCKSDB_CLOUD_GIT_TAG v6.7.3)
set(ROCKSDB_CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=Release
    -DWITH_GFLAGS=OFF
    -DWITH_TESTS=OFF
    -DWITH_BENCHMARK_TOOLS=OFF
    -DWITH_CORE_TOOLS=OFF
    -DFAIL_ON_WARNINGS=OFF
    -DUSE_RTTI=1
    -DCMAKE_CXX_FLAGS=-frtti
    -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/deps/rocksdb_cloud-install
    -DWITH_JNI=OFF
    -DWITH_SNAPPY=OFF
    -DWITH_ZLIB=ON
    -DWITH_BZ2=OFF
    -DWITH_LZ4=OFF
    -DWITH_ZSTD=OFF
)

# 4. Download and build rocksdb-cloud
ExternalProject_Add(rocksdb_cloud
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/deps
    GIT_REPOSITORY ${ROCKSDB_CLOUD_GIT_REPOSITORY}
    GIT_TAG ${ROCKSDB_CLOUD_GIT_TAG}
    GIT_SHALLOW true
    UPDATE_COMMAND ""
    CMAKE_ARGS ${ROCKSDB_CMAKE_ARGS}
    BUILD_BYPRODUCTS
        ${CMAKE_CURRENT_BINARY_DIR}/deps/rocksdb_cloud-install/lib/librocksdb${CMAKE_SHARED_LIBRARY_SUFFIX}
    INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/deps/rocksdb_cloud-install
    BUILD_COMMAND make -j2
)

# 5. Configure AWS SDK for C++
set(AWS_SDK_GIT_REPOSITORY https://github.com/aws/aws-sdk-cpp.git)
set(AWS_SDK_GIT_TAG 1.11.579)

set(AWS_SDK_CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=Release
    -DBUILD_ONLY="s3;core;transfer;kinesis"
    -DENABLE_UNITY_BUILD=ON
    -DENABLE_TESTING=OFF
    -DBUILD_SHARED_LIBS=ON
    -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/deps/aws-sdk-install
)

# 6. Download and build AWS SDK for C++
ExternalProject_Add(aws_sdk
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/deps
    GIT_REPOSITORY ${AWS_SDK_GIT_REPOSITORY}
    GIT_TAG ${AWS_SDK_GIT_TAG}
    GIT_SHALLOW true
    UPDATE_COMMAND ""
    CMAKE_ARGS ${AWS_SDK_CMAKE_ARGS}
    INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/deps/aws-sdk-install
    BUILD_COMMAND make -j2
)

# 7. Get the installation directory
ExternalProject_Get_Property(rocksdb_cloud INSTALL_DIR)
set(ROCKSDB_CLOUD_INCLUDE_DIR ${INSTALL_DIR}/include)
set(ROCKSDB_CLOUD_LIBRARY ${INSTALL_DIR}/lib/librocksdb${CMAKE_SHARED_LIBRARY_SUFFIX})
file(MAKE_DIRECTORY ${ROCKSDB_CLOUD_INCLUDE_DIR})
ExternalProject_Get_Property(aws_sdk INSTALL_DIR)
set(AWS_SDK_INCLUDE_DIR ${INSTALL_DIR}/include)
set(AWS_SDK_LIBRARY_DIR ${INSTALL_DIR}/lib)
file(MAKE_DIRECTORY ${AWS_SDK_INCLUDE_DIR})

# 8. Create imported target for rocksdb
add_library(RocksDB::rocksdb SHARED IMPORTED GLOBAL)
set_target_properties(RocksDB::rocksdb PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_CLOUD_INCLUDE_DIR}"
    IMPORTED_LOCATION "${ROCKSDB_CLOUD_LIBRARY}"
)
add_dependencies(RocksDB::rocksdb rocksdb_cloud)

# 9. Create imported targets for AWS SDK
add_library(AWSSDK::core SHARED IMPORTED GLOBAL)
set_target_properties(AWSSDK::core PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${AWS_SDK_INCLUDE_DIR}"
    IMPORTED_LOCATION "${AWS_SDK_LIBRARY_DIR}/libaws-cpp-sdk-core${CMAKE_SHARED_LIBRARY_SUFFIX}"
)

add_library(AWSSDK::s3 SHARED IMPORTED GLOBAL)
set_target_properties(AWSSDK::s3 PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${AWS_SDK_INCLUDE_DIR}"
    IMPORTED_LOCATION "${AWS_SDK_LIBRARY_DIR}/libaws-cpp-sdk-s3${CMAKE_SHARED_LIBRARY_SUFFIX}"
)

add_library(AWSSDK::transfer SHARED IMPORTED GLOBAL)
set_target_properties(AWSSDK::transfer PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${AWS_SDK_INCLUDE_DIR}"
    IMPORTED_LOCATION "${AWS_SDK_LIBRARY_DIR}/libaws-cpp-sdk-transfer${CMAKE_SHARED_LIBRARY_SUFFIX}"
)

add_library(AWSSDK::kinesis SHARED IMPORTED GLOBAL)
set_target_properties(AWSSDK::kinesis PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${AWS_SDK_INCLUDE_DIR}"
    IMPORTED_LOCATION "${AWS_SDK_LIBRARY_DIR}/libaws-cpp-sdk-kinesis${CMAKE_SHARED_LIBRARY_SUFFIX}"
)

add_dependencies(AWSSDK::core aws_sdk)
add_dependencies(AWSSDK::s3 aws_sdk)
add_dependencies(AWSSDK::transfer aws_sdk)
add_dependencies(AWSSDK::kinesis aws_sdk)

# 7. Include header directories
include_directories(
    ${JNI_INCLUDE_DIRS}              # JNI headers (jni.h, jni_md.h)
    ${ROCKSDB_CLOUD_INCLUDE_DIR}     # RocksDB-Cloud headers
    ${AWS_SDK_INCLUDE_DIR}           # AWS SDK headers
    ${CMAKE_CURRENT_SOURCE_DIR}      # Current CPP directory
)

# 8. Build RocksetJni shared library
add_library(RocksetJni SHARED RocksetJni.cpp)

# 9. Link RocksDB and its dependencies
target_link_libraries(RocksetJni
    RocksDB::rocksdb
    z
    curl
    pthread
    dl  # Additional dependency often needed by RocksDB
)

# 10. install RocksetJni to ${PIXELS_HOME}/lib
install(TARGETS RocksetJni
        LIBRARY DESTINATION ${PIXELS_HOME}/lib)
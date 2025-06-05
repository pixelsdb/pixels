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
set(ROCKSDB_CLOUD_GIT_TAG master)
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
    CMAKE_ARGS ${ROCKSDB_CMAKE_ARGS}
    BUILD_BYPRODUCTS
        ${CMAKE_CURRENT_BINARY_DIR}/deps/rocksdb_cloud-install/lib/librocksdb${CMAKE_SHARED_LIBRARY_SUFFIX}
    INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/deps/rocksdb_cloud-install
    BUILD_COMMAND make -j2
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
)

# 8. Build RocksetJni shared library
add_library(RocksetJni SHARED RocksetJni.cpp)

# 9. Link RocksDB and its dependencies
target_link_libraries(RocksetJni
    RocksDB::rocksdb
    z
    curl
    pthread
    dl
)

# 10. install RocksetJni to ${PIXELS_HOME}/lib
install(TARGETS RocksetJni
        LIBRARY DESTINATION ${PIXELS_HOME}/lib)
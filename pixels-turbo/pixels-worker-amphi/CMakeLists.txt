cmake_minimum_required(VERSION 3.15)
project(pixels_worker_amphi)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_PREFIX_PATH "/opt/homebrew")

# find dependencies
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(DuckDB REQUIRED)
find_package(yaml-cpp REQUIRED)

add_subdirectory(worker)
add_subdirectory(tests)

add_executable(${PROJECT_NAME} main.cpp)

target_include_directories(${PROJECT_NAME} PRIVATE
        ${Protobuf_INCLUDE_DIRS}
        "${CMAKE_CURRENT_BINARY_DIR}/worker/generated"
        ${DuckDB_INCLUDE_DIRS}
        ${gRPC_INCLUDE_DIRS}
        worker/include
)

target_link_libraries(${PROJECT_NAME}
        worker
        ${Protobuf_LIBRARIES}
        duckdb
        gRPC::grpc++
        yaml-cpp
)

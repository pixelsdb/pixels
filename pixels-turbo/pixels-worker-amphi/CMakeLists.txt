cmake_minimum_required(VERSION 3.10)
project(pixels_worker_amphi)

set(CMAKE_CXX_STANDARD 17)

# find dependencies
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(duckdb REQUIRED)

# protobuf configuration
set(PROTO_FILES_PATH "../../proto")
file(COPY ${PROTO_FILES_PATH} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(GLOB_RECURSE PROTO_FILES "${CMAKE_CURRENT_BINARY_DIR}/proto/*.proto")
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_subdirectory(worker)

# add header directories
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${PROTO_GEN_PATH})
include_directories(${duckdb_INCLUDE_DIRS})
include_directories(${gRPC_INCLUDE_DIRS})

add_executable(${PROJECT_NAME} main.cpp ${PROTO_SRCS})

target_link_libraries(${PROJECT_NAME} ${Protobuf_LIBRARIES} duckdb gRPC::grpc++)

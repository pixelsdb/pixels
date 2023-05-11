cmake_minimum_required(VERSION 2.8.12)
project(pixels_worker_amphi)

set(CMAKE_CXX_STANDARD 17)
set(OPENSSL_ROOT_DIR "/opt/homebrew/opt/openssl@3")
set(PROTOBUF_ROOT_DIR "/opt/homebrew/opt/protobuf")
set(BUILD_FILES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/build")
set(PROTO_FILES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../proto")

find_package(gRPC REQUIRED)
find_package(OpenSSL REQUIRED)

if(APPLE)
    find_library(PROTOBUF_LIBRARY protobuf
            HINTS ${PROTOBUF_ROOT_DIR}
            PATH_SUFFIXES lib)
else()
    find_library(PROTOBUF_LIBRARY protobuf)
endif()

find_path(DUCKDB_INCLUDE_DIR duckdb.hpp
        HINTS ${DUCKDB_ROOT_DIR}
        PATH_SUFFIXES include)
find_library(DUCKDB_LIBRARY duckdb
        HINTS ${DUCKDB_ROOT_DIR}
        PATH_SUFFIXES lib)


file(GLOB PROTO_FILES ${PROTO_FILES_DIR}/*.proto)

add_executable(example main.cpp)

protobuf_generate(
        LANGUAGE cpp
        TARGET example
        PROTOS ${PROTO_FILES}
        OUT_VAR PROTO_CPP_FILES
        IMPORT_DIRS ${PROTO_FILES_DIR}
        GENERATE_EXTENSIONS ${BUILD_FILES_DIR}
)

target_sources(example PRIVATE ${PROTO_CPP_FILES})
target_include_directories(example PRIVATE ${DUCKDB_INCLUDE_DIR} ${Protobuf_INCLUDE_DIRS})
target_link_libraries(example
        PRIVATE
        ${DUCKDB_LIBRARY}
        gRPC::grpc++
        gRPC::grpc
        ${PROTOBUF_LIBRARY}
        )

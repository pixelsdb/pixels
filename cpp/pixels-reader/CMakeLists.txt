cmake_minimum_required(VERSION 3.16)
set(CMAKE_CXX_STANDARD 14)

project(pixels)

message("${CMAKE_MODULE_PATH}")

# protobuf library
find_package(Protobuf REQUIRED)

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(pixels-common)
add_subdirectory(pixels-core)
add_subdirectory(tests)


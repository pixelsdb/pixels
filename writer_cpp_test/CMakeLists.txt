cmake_minimum_required(VERSION 3.0)
project(pixels-cli)

set(CMAKE_CXX_STANDARD 17)
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)



set(ENV{PIXELS_SRC} "/home/whz/dev/pixels")
set(ENV{PIXELS_HOME} "/home/whz/dev/pixels")

if(POLICY CMP0057)
    cmake_policy(SET CMP0057 NEW)
endif()

if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

find_package(Boost REQUIRED COMPONENTS program_options regex)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
else()
    message(FATAL_ERROR "Could not find Boost")
endif()

# 查找Protobuf库，如果找不到则报错
find_package(Protobuf REQUIRED)
# 将Protobuf的包含目录添加到项目的包含目录中
include_directories(${Protobuf_INCLUDE_DIRS})



include_directories(${CMAKE_CURRENT_BINARY_DIR}/pixels-common/liburing/src/include)
include_directories(pixels-cli/include)
include_directories(pixels-core/include)
include_directories(pixels-common/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(pixels-common)
add_subdirectory(pixels-core)

set(pixels_cli_cxx
        pixels-cli/main.cpp
        pixels-cli/lib/executor/LoadExecutor.cpp
        pixels-cli/lib/load/Parameters.cpp
        pixels-cli/lib/load/PixelsConsumer.cpp)
add_executable(pixels-cli ${pixels_cli_cxx})


target_link_libraries(pixels-cli Boost::program_options Boost::regex pixels-core pixels-common ${Protobuf_LIBRARIES})
cmake_minimum_required(VERSION 3.0)
project(pixels-cli)

set(CMAKE_CXX_STANDARD 17)
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)

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

set(pixels_cli_cxx
        main.cpp
        lib/executor/LoadExecutor.cpp
        lib/load/Parameters.cpp
        lib/load/PixelsConsumer.cpp)
add_executable(pixels-cli ${pixels_cli_cxx})
include_directories(include)
include_directories(../pixels-core/include)
include_directories(../pixels-common/include)
target_link_libraries(pixels-cli Boost::program_options Boost::regex pixels-core pixels-common)
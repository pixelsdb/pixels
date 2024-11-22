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

find_package(Boost REQUIRED COMPONENTS program_options)
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
# target_link_libraries(pixels-cli Boost::program_options pixels-core pixels-common)

include(ExternalProject)
ExternalProject_Add(liburing
        GIT_REPOSITORY git@github.com:axboe/liburing.git
        GIT_TAG liburing-2.2
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}
        SOURCE_DIR "liburing"
        CONFIGURE_COMMAND ""
        INSTALL_COMMAND ""
        BUILD_COMMAND make -j
        BUILD_IN_SOURCE true
)

add_dependencies(pixels-cli liburing)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/liburing/src/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/liburing/src)
target_link_libraries(pixels-cli Boost::program_options
        ${CMAKE_SOURCE_DIR}/../build/release/extension/pixels/pixels-core/libpixels-core.a
        ${CMAKE_SOURCE_DIR}/../build/release/extension/pixels/pixels-common/libpixels-common.a
        ${CMAKE_CURRENT_BINARY_DIR}/liburing/src/liburing.a)
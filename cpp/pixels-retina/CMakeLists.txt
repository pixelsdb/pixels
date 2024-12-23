cmake_minimum_required(VERSION 3.10)
project(DeleteTrackerNative)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find Java and JNI
find_package(Java REQUIRED)
find_package(JNI REQUIRED)

# Include directories
include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})
include_directories(${CMAKE_SOURCE_DIR}/include)

# Source files
set(SOURCES
    lib/visibility/Visibility_JNI.cpp
    lib/visibility/Visibility.cpp)

# Create shared library
add_library(VisibilityNative SHARED ${SOURCES})

# Set the target properties
set_target_properties(VisibilityNative PROPERTIES
    OUTPUT_NAME "VisibilityNative"
    LIBRARY_OUTPUT_DIRECTORY "$ENV{PIXELS_HOME}/lib")

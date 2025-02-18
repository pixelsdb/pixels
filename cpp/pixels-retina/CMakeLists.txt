cmake_minimum_required(VERSION 3.10)
project(pixels-retina)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find Java and JNI
find_package(Java REQUIRED)
find_package(JNI REQUIRED)

# Find gtest
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.12.1
)
FetchContent_MakeAvailable(googletest)
enable_testing()

# Include directories
include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})
include_directories(${CMAKE_SOURCE_DIR}/include)

# Source files
set(SOURCES
        lib/Retina.cpp
        lib/Visibility.cpp
)

# Create shared library
add_library(pixels-retina SHARED ${SOURCES})

# Set the target properties
set_target_properties(pixels-retina PROPERTIES
        OUTPUT_NAME "pixels-retina"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

# Ensure the library is placed in the PIXELS_HOME directory
install(TARGETS pixels-retina
        LIBRARY DESTINATION $ENV{PIXELS_HOME}/lib)

# Add the test executable
add_executable(visibility_tests test/VisibilityTest.cpp)
add_executable(retina_tests test/RetinaTest.cpp)

# Link the test executable with the library
target_link_libraries(visibility_tests
    pixels-retina
    GTest::gtest_main
)
target_link_libraries(retina_tests
    pixels-retina
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(visibility_tests)
gtest_discover_tests(retina_tests)

# Set build type to Debug if not specified
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif ()

# Add debug flags
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
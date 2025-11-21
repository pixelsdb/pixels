cmake_minimum_required(VERSION 3.10)
project(pixels-retina)

if(POLICY CMP0177)
    cmake_policy(SET CMP0177 NEW)
endif()

# Set the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find Java and JNI
find_package(Java REQUIRED)
find_package(JNI REQUIRED)

# Disable gtest installation
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)

# Find gtest
include(FetchContent)
FetchContent_Declare(
    googletest
    # if network error, try to download the zip file from the internet
    # and unzip it to the third_party/googletest directory
    # and uncomment the following line
    # SOURCE_DIR ${CMAKE_SOURCE_DIR}/third_party/googletest

    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.17.0
)
FetchContent_MakeAvailable(googletest)
enable_testing()

# Include directories
include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})
include_directories(${CMAKE_SOURCE_DIR}/include)

# Source files
set(SOURCES
        lib/TileVisibility.cpp
        lib/RGVisibility.cpp
        lib/RGVisibilityJni.cpp
)

# Create shared library
add_library(pixels-retina SHARED ${SOURCES})

# Set the target properties
set_target_properties(pixels-retina PROPERTIES
        OUTPUT_NAME "pixels-retina"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
)

target_link_libraries(pixels-retina
    ${JNI_LIBRARIES}
)

# Ensure the library is placed in the PIXELS_HOME directory
install(TARGETS pixels-retina
        LIBRARY DESTINATION $ENV{PIXELS_HOME}/lib)

# Add the test executable
add_executable(tile_visibility_tests test/TileVisibilityTest.cpp)
add_executable(rg_visibility_tests test/RGVisibilityTest.cpp)

# Link the test executable with the library
target_link_libraries(tile_visibility_tests
    pixels-retina
    GTest::gtest_main
)
target_link_libraries(rg_visibility_tests
    pixels-retina
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(tile_visibility_tests)
gtest_discover_tests(rg_visibility_tests)

# Set build type to Debug if not specified
# if (NOT CMAKE_BUILD_TYPE)
#     set(CMAKE_BUILD_TYPE Debug)
# endif ()

if (RETINA_SIMD)
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag("-mavx2" COMPILER_SUPPORTS_AVX2)
    if(COMPILER_SUPPORTS_AVX2)
        message(STATUS "AVX2 supported: enabling AVX2 code path")
        target_compile_options(pixels-retina PRIVATE
                -mavx2
        )
        target_compile_definitions(pixels-retina PRIVATE RETINA_SIMD)
    else()
        message(STATUS "AVX2 not supported: falling back to scalar code path")
    endif ()
endif()


# Add debug flags
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")

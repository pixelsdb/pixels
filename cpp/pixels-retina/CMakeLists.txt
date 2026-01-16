cmake_minimum_required(VERSION 3.10)
project(pixels-retina)

if(POLICY CMP0177)
    cmake_policy(SET CMP0177 NEW)
endif()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(Java REQUIRED)
find_package(JNI REQUIRED)

# --- Jemalloc Configuration (Shared with Prefix) ---
option(ENABLE_JEMALLOC "Enable jemalloc with pixels_ prefix" OFF)
include(ExternalProject)

if(ENABLE_JEMALLOC)
    add_definitions(-DDISABLE_RETINA_SIMD)
    set(JEMALLOC_INSTALL_DIR "${CMAKE_BINARY_DIR}/jemalloc_install")
    set(JEMALLOC_LIB_PATH "${JEMALLOC_INSTALL_DIR}/lib/libjemalloc.so")
    set(JEMALLOC_INCLUDE_DIR "${JEMALLOC_INSTALL_DIR}/include")

    # --- ADD THIS LINE TO FIX THE ERROR ---
    file(MAKE_DIRECTORY ${JEMALLOC_INCLUDE_DIR})
    # --------------------------------------

    ExternalProject_Add(jemalloc_ext
            URL https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2
            CONFIGURE_COMMAND <SOURCE_DIR>/configure
            --prefix=${JEMALLOC_INSTALL_DIR}
            --with-jemalloc-prefix=pixels_
            --enable-prof
            --enable-stats
            --enable-shared
            --disable-initial-exec-tls
            "EXTRA_CFLAGS=-fPIC"
            BUILD_COMMAND make -j
            INSTALL_COMMAND make install
            BUILD_BYPRODUCTS ${JEMALLOC_LIB_PATH}
    )

    add_library(jemalloc_lib SHARED IMPORTED GLOBAL)
    set_target_properties(jemalloc_lib PROPERTIES
            IMPORTED_LOCATION ${JEMALLOC_LIB_PATH}
            INTERFACE_INCLUDE_DIRECTORIES ${JEMALLOC_INCLUDE_DIR}
    )
    add_dependencies(jemalloc_lib jemalloc_ext)

    add_definitions(-DENABLE_JEMALLOC)
    add_definitions(-DJEMALLOC_MANGLE)
endif()

# --- GTest Configuration ---
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
include(FetchContent)
FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.17.0
)
FetchContent_MakeAvailable(googletest)
enable_testing()

include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})
include_directories(${CMAKE_SOURCE_DIR}/include)

set(SOURCES
        lib/EpochManager.cpp
        lib/TileVisibility.cpp
        lib/RGVisibility.cpp
        lib/RGVisibilityJni.cpp
)

add_library(pixels-retina SHARED ${SOURCES})

set_target_properties(pixels-retina PROPERTIES
        OUTPUT_NAME "pixels-retina"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
)

target_link_libraries(pixels-retina PRIVATE ${JNI_LIBRARIES})

if(ENABLE_JEMALLOC)
    # 动态链接 jemalloc，运行时会寻找 libjemalloc.so
    target_link_libraries(pixels-retina PRIVATE jemalloc_lib pthread dl)
    if(UNIX)
        set_target_properties(pixels-retina PROPERTIES LINK_FLAGS "-Wl,--export-dynamic")
    endif()
endif()

# 安装
install(TARGETS pixels-retina LIBRARY DESTINATION $ENV{PIXELS_HOME}/lib)

if(ENABLE_JEMALLOC)
    message(STATUS "Jemalloc install dir: ${JEMALLOC_INSTALL_DIR}")
    add_custom_command(TARGET pixels-retina POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E make_directory $ENV{PIXELS_HOME}/lib
            COMMAND /bin/sh -c "cp -d ${JEMALLOC_INSTALL_DIR}/lib/libjemalloc.so* $ENV{PIXELS_HOME}/lib/"
            COMMENT "Copying all jemalloc library files and symlinks to PIXELS_HOME/lib"
    )
endif()

# --- Tests ---
add_executable(tile_visibility_tests test/TileVisibilityTest.cpp)
add_executable(rg_visibility_tests test/RGVisibilityTest.cpp)
add_executable(visibility_perf_tests test/VisibilityPerformanceTest.cpp)

target_link_libraries(tile_visibility_tests pixels-retina GTest::gtest_main)
target_link_libraries(rg_visibility_tests pixels-retina GTest::gtest_main)
target_link_libraries(visibility_perf_tests pixels-retina GTest::gtest_main)

if(ENABLE_JEMALLOC)
    target_link_libraries(tile_visibility_tests jemalloc_lib)
    target_link_libraries(rg_visibility_tests jemalloc_lib)
    target_link_libraries(visibility_perf_tests jemalloc_lib)
endif()

include(GoogleTest)
gtest_discover_tests(tile_visibility_tests)
gtest_discover_tests(rg_visibility_tests)
gtest_discover_tests(visibility_perf_tests)

# --- SIMD & Debug ---
if (RETINA_SIMD)
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag("-mavx2" COMPILER_SUPPORTS_AVX2)
    if(COMPILER_SUPPORTS_AVX2)
        target_compile_options(pixels-retina PRIVATE -mavx2)
        target_compile_definitions(pixels-retina PRIVATE RETINA_SIMD)
    endif()
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
cmake_minimum_required(VERSION 3.19)

set(CMAKE_CXX_STANDARD 20)
set(TARGET_NAME pixels)
set(DCMAKE_EXPORT_COMPILE_COMMANDS=1)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})

add_definitions(-DDUCKDB_EXTENSION_LIBRARY)

include_directories(include)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(EXTENSION_SOURCES
        pixels-duckdb/pixels_extension.cpp
        pixels-duckdb/PixelsScanFunction.cpp
)

add_subdirectory(pixels-common)
add_subdirectory(pixels-core)
add_subdirectory(pixels-cli)
add_subdirectory(third-party/googletest)
add_subdirectory(tests)

include_directories(pixels-common/include)
include_directories(pixels-core/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/pixels-common/liburing/src/include)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME}
        pixels-common
        pixels-core
)

# Add the subdirectory that contains the build_loadable_extension definition
message("duckdb export set: ${DUCKDB_EXPORT_SET}" )
message("TARGET NAME: ${TARGET_NAME} EXTENSION NAME: ${EXTENSION_NAME}")

install(
        TARGETS ${EXTENSION_NAME} pixels-core pixels-common
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

cmake_minimum_required(VERSION 3.16)
set(CMAKE_CXX_STANDARD 14)
# Set extension name here
set(TARGET_NAME pixels)
set(DCMAKE_EXPORT_COMPILE_COMMANDS=1)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(include)

set(EXTENSION_SOURCES
        PixelsExtension.cpp
        PixelsScanFunction.cpp
)
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory(pixels-common)
add_subdirectory(pixels-core)
 add_subdirectory(pixels-cli)
add_subdirectory(third-party/googletest)
add_subdirectory(tests)

include_directories(pixels-common/include)
include_directories(pixels-core/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/pixels-common/liburing/src/include)



target_link_libraries(
        ${EXTENSION_NAME}
        pixels-common
        pixels-core
)


# Add the subdirectory that contains the build_loadable_extension definition

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

message("duckdb export set: ${DUCKDB_EXPORT_SET}" )

install(
        TARGETS ${EXTENSION_NAME} pixels-core pixels-common
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME pixels)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(include)

set(EXTENSION_SOURCES
        pixels_extension.cpp
        PixelsScanFunction.cpp
        )
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})


include_directories(pixels-reader/pixels-common/include)
include_directories(pixels-reader/pixels-core/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/pixels-reader)

add_subdirectory(pixels-reader)


target_link_libraries(
        ${EXTENSION_NAME}
        pixels-common
        pixels-core
)

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME} pixels-core pixels-common
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

project(pixels-core)

file(GLOB_RECURSE pixels_core_cxx
        "lib/*.cpp"
        "include/*.h"
)

add_library(pixels-core ${pixels_core_cxx})

target_link_libraries(
        pixels-core
        pixels-common
)
SET(CMAKE_CXX_FLAGS "-mavx2")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../pixels-common/liburing/src/include)
include_directories(../pixels-common/include)
include_directories(include)
include_directories(include/writer)

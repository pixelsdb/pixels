add_executable(
        IntegerWriterTest
        IntegerWriterTest.cpp
)

add_executable(
        PixelsWriterTest
        PixelsWriterTest.cpp
)

if (CMAKE_BUILD_TYPE MATCHES "Debug")
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -fsanitize=undefined -fsanitize=address")
    target_link_options(IntegerWriterTest BEFORE PUBLIC -fsanitize=undefined PUBLIC -fsanitize=address)
    target_link_options(PixelsWriterTest BEFORE PUBLIC -fsanitize=undefined PUBLIC -fsanitize=address)
endif ()

target_link_libraries(
        IntegerWriterTest
        PRIVATE
        gtest_main
        duckdb
        pixels-core
)

target_link_libraries(
        PixelsWriterTest
        PRIVATE
        gtest_main
        duckdb
        pixels-core
)

set(GTEST_DIR "${PROJECT_SOURCE_DIR}/third-party/googletest")
include_directories(${GTEST_DIR}/googletest/include)
include_directories(${PROJECT_SOURCE_DIR}/pixels-core/include)
include_directories(${PROJECT_SOURCE_DIR}/pixels-common/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../pixels-common/liburing/src/include)
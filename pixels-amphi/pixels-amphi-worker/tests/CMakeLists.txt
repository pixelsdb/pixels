project(tests)

include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/b796f7d44681514f58a683a3a71ff17c94edb0c1.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
        unit_tests
        unit_tests.cpp
)

target_include_directories(
        unit_tests PRIVATE
        GoogleTest
        ../worker/include
        "${CMAKE_CURRENT_BINARY_DIR}/../worker/generated"
        ${DuckDB_INCLUDE_DIRS}
)
target_link_libraries(
        unit_tests
        GTest::gtest_main
        worker
        duckdb
        yaml-cpp
        spdlog::spdlog
)
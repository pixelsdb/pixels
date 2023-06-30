project(benchmark)

add_executable(
        benchmark
        benchmark.cpp
)

target_include_directories(
        benchmark PRIVATE
        ../worker/include
        "${CMAKE_CURRENT_BINARY_DIR}/../worker/generated"
        ${DuckDB_INCLUDE_DIRS}
)
target_link_libraries(
        benchmark
        worker
        duckdb
        yaml-cpp
        spdlog::spdlog
        ${AWSSDK_LINK_LIBRARIES}
        nlohmann_json::nlohmann_json
)
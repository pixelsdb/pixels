project(pixels-cli)

set(CMAKE_CXX_STANDARD 17)

include(ExternalProject)
include(ProcessorCount)

# Get core count
ProcessorCount(CORES)
if(CORES EQUAL 0)
    set(CORES 1)
endif()

# List of source files for the executable
set(pixels_cli_cxx
        main.cpp
        lib/executor/LoadExecutor.cpp
        lib/load/Parameters.cpp
        lib/load/PixelsConsumer.cpp
)

# Create the executable
add_executable(pixels-cli ${pixels_cli_cxx})

# Include directories
include_directories(include)
include_directories(../pixels-core/include)
include_directories(../pixels-common/include)

# Link libraries
target_link_libraries(pixels-cli duckdb)
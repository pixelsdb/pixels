project(pixels-cli)

set(CMAKE_CXX_STANDARD 17)

# boost-dev
include(ExternalProject)
ExternalProject_Add(boost_regex
		GIT_REPOSITORY git@github.com:boostorg/regex.git
		GIT_TAG boost-1.70.0
		PREFIX ${CMAKE_CURRENT_BINARY_DIR}
		SOURCE_DIR "boost_regex"
		CONFIGURE_COMMAND ""
		INSTALL_COMMAND ""
		BUILD_COMMAND ""
		BUILD_IN_SOURCE false
		)

ExternalProject_Add(boost_program_options
		GIT_REPOSITORY git@github.com:boostorg/program_options.git
		GIT_TAG boost-1.70.0
		PREFIX ${CMAKE_CURRENT_BINARY_DIR}
		SOURCE_DIR "boost_program_options"
		CONFIGURE_COMMAND ""
		INSTALL_COMMAND ""
		BUILD_COMMAND ""
		BUILD_IN_SOURCE false
		)

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)

if(POLICY CMP0057)
    cmake_policy(SET CMP0057 NEW)
endif()

if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()

set(pixels_cli_cxx
        main.cpp
        lib/executor/LoadExecutor.cpp
        lib/load/Parameters.cpp
        lib/load/PixelsConsumer.cpp)
add_executable(pixels-cli ${pixels_cli_cxx})
include_directories(include)
include_directories(../pixels-core/include)
include_directories(../pixels-common/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/boost_regex/include)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/boost_program_options/include)
target_link_libraries(pixels-cli pixels-core pixels-common duckdb)
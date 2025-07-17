project(pixels-cli)

set(CMAKE_CXX_STANDARD 17)

include(ExternalProject)
include(ProcessorCount)

# get core count
ProcessorCount(CORES)
if(CORES EQUAL 0)
    set(CORES 1)
endif()

# boost-dev
set(BOOST_LIBRARIES "program_options,regex")
set(BOOST_BOOTSTRAP_COMMAND ./bootstrap.sh --with-libraries=${BOOST_LIBRARIES})
set(BOOST_BUILD_TOOL ./b2)
set(BOOST_CXXFLAGS "cxxflags=-std=c++11")
set(BOOST_GIT_REPOSITORY git@github.com:boostorg/boost.git)
set(BOOST_GIT_TAG boost-1.74.0)
set(BOOST_GIT_SUBMODULES
        libs/headers libs/regex libs/program_options libs/algorithm
        # The primary dependencies for algorithm, program_options, and regex:
        libs/any libs/bind libs/config libs/core libs/detail libs/function libs/iterator libs/lexical_cast
        libs/smart_ptr libs/static_assert libs/throw_exception libs/tokenizer libs/type_traits libs/assert
        libs/concept_check libs/container_hash libs/integer libs/mpl libs/predef libs/preprocessor libs/conversion
        libs/function_types libs/fusion libs/optional libs/utility libs/move libs/typeof libs/tuple libs/io
        libs/type_index libs/array libs/container libs/math libs/numeric/conversion libs/range libs/intrusive
        libs/atomic libs/lambda libs/mp11 libs/winapi libs/exception libs/unordered
        # The tools required to build boost:
        tools/auto_index tools/bcp tools/boost_install tools/boostbook tools/boostdep
        tools/build tools/check_build tools/cmake tools/docca tools/inspect tools/litre tools/quickbook)

# download and compile boost libraries
ExternalProject_Add(boost
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/deps
        GIT_REPOSITORY ${BOOST_GIT_REPOSITORY}
        GIT_TAG ${BOOST_GIT_TAG}
        GIT_SUBMODULES ${BOOST_GIT_SUBMODULES}
        GIT_SUBMODULES_RECURSE true
        GIT_SHALLOW true
        SOURCE_DIR "boost"
        BUILD_IN_SOURCE true
        UPDATE_COMMAND ${BOOST_BOOTSTRAP_COMMAND}
        CONFIGURE_COMMAND ./b2 headers
        BUILD_COMMAND ${BOOST_BUILD_TOOL} stage
        ${BOOST_CXXFLAGS}
        threading=multi
        variant=release
        link=static
        -j${CORES}
        INSTALL_COMMAND ""
        # logging
        LOG_CONFIGURE true
        LOG_BUILD true
        LOG_INSTALL true
)

ExternalProject_Get_Property(boost SOURCE_DIR)
set(BOOST_INCLUDE_DIR ${SOURCE_DIR})
set(BOOST_LIBRARY_PREFIX ${SOURCE_DIR}/stage/lib/${CMAKE_STATIC_LIBRARY_PREFIX})
# boost algorithm is a header only library, no need to add dependency
# add boost program_options library
add_library(Boost::program_options STATIC IMPORTED GLOBAL)
set_property(TARGET Boost::program_options PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})
set_property(TARGET Boost::program_options PROPERTY IMPORTED_LOCATION ${BOOST_LIBRARY_PREFIX}boost_program_options${CMAKE_STATIC_LIBRARY_SUFFIX})
add_dependencies(Boost::program_options boost)
# add boost regex library
add_library(Boost::regex STATIC IMPORTED GLOBAL)
set_property(TARGET Boost::regex PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${BOOST_INCLUDE_DIR})
set_property(TARGET Boost::regex PROPERTY IMPORTED_LOCATION ${BOOST_LIBRARY_PREFIX}boost_regex${CMAKE_STATIC_LIBRARY_SUFFIX})
add_dependencies(Boost::regex boost)
unset(SOURCE_DIR)

set(pixels_cli_cxx
        main.cpp
        lib/executor/LoadExecutor.cpp
        lib/load/Parameters.cpp
        lib/load/PixelsConsumer.cpp)

add_executable(pixels-cli ${pixels_cli_cxx})
include_directories(include)
include_directories(../pixels-core/include)
include_directories(../pixels-common/include)
target_link_libraries(pixels-cli
        Boost::program_options Boost::regex duckdb pixels-core)
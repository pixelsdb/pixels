#project(tests)
#
#include(FetchContent)
#FetchContent_Declare(
#        googletest
#        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
#)
#
#set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
#FetchContent_MakeAvailable(googletest)
#
#enable_testing()
#
#
#add_executable(
#        unit_tests
#        unit_tests.cpp)
#
#target_link_libraries(
#        unit_tests
#        GTest::gtest_main
#        pixels-common
#        pixels-core
#)
#
#include(GoogleTest)
#include_directories(../pixels-core/include)
#include_directories(../pixels-common/include)
#gtest_discover_tests(unit_tests)

add_subdirectory(writer)
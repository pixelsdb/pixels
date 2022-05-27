cmake_minimum_required(VERSION 3.14)
# GoogleTest requires at least C++11
set(CMAKE_CXX_STANDARD 20)

# include(FetchContent)
# FetchContent_Declare(
#   googletest
#   URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
# )
# # For Windows: Prevent overriding the parent project's compiler/linker settings
# set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
# FetchContent_MakeAvailable(googletest)

find_library(ROCKS_DB_PATH rocksdb)

find_package(GTest REQUIRED)
enable_testing()
include_directories(${GTEST_INCLUDE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/include)

add_definitions(-DCOMPILE_UNIT_TESTS)

find_package(log4cxx 0.11)
find_package(fmt)

add_executable(
  buffer_test
  buffer_test.cc
)

target_link_libraries(
  buffer_test
  ${GTEST_BOTH_LIBRARIES}
  ${ROCKS_DB_PATH}
  # retina
  mq
  memory
  util
  port
  log4cxx
  fmt::fmt
)

add_executable(
  version_test
  version_test.cc
)

target_link_libraries(
  version_test
  ${GTEST_BOTH_LIBRARIES}
  # retina
  mq
  memory
  util
  port
  log4cxx
  fmt::fmt
)

add_executable(
  index_test
  index_test.cc
)

target_link_libraries(
  index_test
  ${GTEST_BOTH_LIBRARIES}
  ${ROCKS_DB_PATH}
  # retina
  mq
  memory
  util
  port
  log4cxx
  fmt::fmt
)

add_executable(
  retina_test
  retina_test.cc
)

target_link_libraries(
  retina_test
  ${GTEST_BOTH_LIBRARIES}
  ${ROCKS_DB_PATH}
  # retina
  mq
  memory
  util
  port
  log4cxx
  fmt::fmt
)

# include(GoogleTest)
# gtest_discover_tests(BufferTest)
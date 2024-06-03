cmake_minimum_required(VERSION 3.14)
project(my_project)

set(CMAKE_CXX_STANDARD 20)

include_directories(include)
add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(memory)
add_subdirectory(util)
add_subdirectory(port)
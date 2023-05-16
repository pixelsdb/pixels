project(worker)

set(worker_cxx src/utils/constants.cpp)

include_directories(include)

add_library(worker ${worker_cxx})

target_link_libraries(worker)
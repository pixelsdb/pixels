aux_source_directory(. MQ_LIB_SRCS)
include_directories(${CMAKE_SOURCE_DIR}/include)
add_library(mq STATIC ${MQ_LIB_SRCS})
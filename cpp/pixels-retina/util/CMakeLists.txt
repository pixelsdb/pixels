aux_source_directory(. UTIL_LIB_SRCS)
include_directories(${CMAKE_SOURCE_DIR}/include)
add_library(util STATIC ${UTIL_LIB_SRCS})
# target_include_directories(RetinaService PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

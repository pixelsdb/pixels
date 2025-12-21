project(pixels-common)

set(CMAKE_CXX_STANDARD 17)

include(ExternalProject)


file(GLOB_RECURSE pixels_common_cxx
		"lib/physical/*.cpp"
		"lib/physical/*.h"
		"lib/exception/*.cpp"
		"lib/exception/*.h"
		"lib/utils/*.cpp"
		"lib/utils/*.h"
		"lib/profiler/*.cpp"
		"lib/profiler/*.h"
		"include/physical/*.h"
		"include/profiler/*.h"
		"include/utils/*.h"
		"include/physical/BufferPool/*.h"
		"lib/MergedRequest.cpp"
)

include_directories(include)

if(NOT DEFINED ENV{PIXELS_SRC})
	message(FATAL_ERROR "You must set PIXELS_SRC environment variable. The value should be set to the Pixels base directory.")
endif()

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS $ENV{PIXELS_SRC}/proto/pixels.proto)

add_library(pixels-common ${pixels_common_cxx} ${PROTO_SRCS} ${PROTO_HDRS})

# liburing
set(LIBURING_GIT_REPOSITORY git@github.com:axboe/liburing.git)
set(LIBURING_GIT_TAG liburing-2.2)
set(LIBURING_BUILD_COMMAND make -j)

ExternalProject_Add(liburing
		PREFIX ${CMAKE_CURRENT_BINARY_DIR}/deps
		GIT_REPOSITORY ${LIBURING_GIT_REPOSITORY}
		GIT_TAG ${LIBURING_GIT_TAG}
		SOURCE_DIR "liburing"
		CONFIGURE_COMMAND ""
		INSTALL_COMMAND ""
		BUILD_COMMAND ${LIBURING_BUILD_COMMAND}
		BUILD_IN_SOURCE true
		)

add_dependencies(pixels-common liburing)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/liburing/src/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/liburing/src)
message(${CMAKE_CURRENT_BINARY_DIR}/liburing/src)
target_link_libraries(pixels-common
        ${Protobuf_LIBRARIES}
		${CMAKE_CURRENT_BINARY_DIR}/liburing/src/liburing.a)

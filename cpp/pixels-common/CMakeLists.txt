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

set(FBS_SRC "${CMAKE_CURRENT_SOURCE_DIR}/../../flatbuffers/pixels.fbs")
set(GEN_HEADER_DIR "${CMAKE_CURRENT_BINARY_DIR}/flatbuffers_out")
set(FBS_HEADER "${GEN_HEADER_DIR}/pixels_generated.h")

file(MAKE_DIRECTORY "${GEN_HEADER_DIR}")

add_custom_command(
		OUTPUT "${FBS_HEADER}"
		COMMAND $<TARGET_FILE:flatc>
				--cpp
				--gen-object-api
				-o "${GEN_HEADER_DIR}"
				"${FBS_SRC}"
		DEPENDS flatc "${FBS_SRC}"
		COMMENT "Running FlatBuffers compiler (flatc) with --gen-object-api on ${FBS_SRC}"
		VERBATIM
)

add_custom_target(pixels_fb_gen DEPENDS "${FBS_HEADER}")

add_library(pixels-common ${pixels_common_cxx} "${FBS_HEADER}")
add_dependencies(pixels-common pixels_fb_gen)

target_include_directories(pixels-common PUBLIC
		$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
		$<BUILD_INTERFACE:${GEN_HEADER_DIR}>
		$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../third-party/flatbuffers/include>
)

set(LIBURING_GIT_REPOSITORY https://github.com/axboe/liburing.git)
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

set(LIBURING_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/liburing/src/include)
set(LIBURING_LIB ${CMAKE_CURRENT_BINARY_DIR}/liburing/src/liburing.a)

target_include_directories(pixels-common PRIVATE ${LIBURING_INCLUDE_DIR})

target_link_libraries(pixels-common
		PRIVATE
		${LIBURING_LIB}
		pthread
)

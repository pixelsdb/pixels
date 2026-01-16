project(pixels-common)

set(CMAKE_CXX_STANDARD 17)

include(ExternalProject)

set(pixels_common_cxx
		lib/physical/storage/LocalFS.cpp
		lib/physical/storage/LocalFSProvider.cpp
		lib/physical/storage/PhysicalLocalWriter.cpp
		lib/physical/PhysicalWriterOption.cpp
		lib/physical/Status.cpp
		lib/physical/Storage.cpp
		lib/physical/FilePath.cpp
		lib/physical/natives/PixelsRandomAccessFile.cpp
		lib/physical/natives/DirectRandomAccessFile.cpp
		lib/physical/natives/ByteBuffer.cpp
		lib/physical/io/PhysicalLocalReader.cpp
		lib/physical/StorageFactory.cpp
		lib/physical/Request.cpp
		lib/physical/RequestBatch.cpp
		lib/physical/scheduler/NoopScheduler.cpp
		lib/physical/SchedulerFactory.cpp
		lib/exception/InvalidArgumentException.cpp
		lib/utils/Constants.cpp
		lib/utils/String.cpp
		include/physical/natives/DirectIoLib.h
		lib/physical/natives/DirectIoLib.cpp
		include/utils/ConfigFactory.h
		lib/utils/ConfigFactory.cpp
		include/physical/MergedRequest.h
		include/physical/scheduler/SortMergeScheduler.h
		lib/physical/scheduler/SortMergeScheduler.cpp
		lib/MergedRequest.cpp
		include/profiler/TimeProfiler.h
		lib/profiler/TimeProfiler.cpp
		include/profiler/CountProfiler.h
		lib/profiler/CountProfiler.cpp
		include/profiler/AbstractProfiler.h
		include/physical/allocator/Allocator.h
		include/physical/allocator/OrdinaryAllocator.h
		lib/physical/allocator/OrdinaryAllocator.cpp
		include/physical/allocator/BufferPoolAllocator.h
		lib/physical/allocator/BufferPoolAllocator.cpp
		include/physical/BufferPool.h
		lib/physical/BufferPool.cpp
		include/physical/natives/DirectUringRandomAccessFile.h
		lib/physical/natives/DirectUringRandomAccessFile.cpp
		include/utils/ColumnSizeCSVReader.h
		lib/utils/ColumnSizeCSVReader.cpp
		include/physical/StorageArrayScheduler.h
		lib/physical/StorageArrayScheduler.cpp
		include/physical/natives/ByteOrder.h
)

set(FBS_SRC "${CMAKE_CURRENT_SOURCE_DIR}/../../proto/pixels.fbs")
set(GEN_HEADER_DIR "${CMAKE_CURRENT_BINARY_DIR}/flatbuffers_out")
set(FBS_HEADER "${GEN_HEADER_DIR}/pixels_generated.h")

file(MAKE_DIRECTORY ${GEN_HEADER_DIR})

add_custom_command(
		OUTPUT "${FBS_HEADER}"
		COMMAND flatc --cpp --gen-object-api -o "${GEN_HEADER_DIR}" "${FBS_SRC}"
		DEPENDS "${FBS_SRC}"
		COMMENT "Running FlatBuffers compiler (flatc) with --gen-object-api on ${FBS_SRC}"
		VERBATIM
)

add_custom_target(pixels_fb_gen DEPENDS "${FBS_HEADER}")

add_library(pixels-common ${pixels_common_cxx} "${FBS_HEADER}")
add_dependencies(pixels-common pixels_fb_gen)

target_include_directories(pixels-common PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${GEN_HEADER_DIR}>
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
		pthread  # liburing usually depends on pthread
)

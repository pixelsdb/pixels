project(pixels-common)

set(pixels_common_cxx
        lib/physical/storage/LocalFS.cpp
        lib/physical/Storage.cpp
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
        lib/MergedRequest.cpp include/profiler/TimeProfiler.h
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
        )

include_directories(include)

if(NOT DEFINED ENV{PIXELS_SRC})
	message(FATAL_ERROR "You must set PIXELS_SRC environment variable. The value should be set to the Pixels base directory.")
endif()

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS $ENV{PIXELS_SRC}/proto/pixels.proto)

add_library(pixels-common ${pixels_common_cxx} ${PROTO_SRCS} ${PROTO_HDRS})

# liburing
include(ExternalProject)
ExternalProject_Add(liburing
		GIT_REPOSITORY https://github.com/axboe/liburing
		GIT_TAG liburing-2.2
		PREFIX ${CMAKE_CURRENT_BINARY_DIR}
		SOURCE_DIR "liburing"
		CONFIGURE_COMMAND ""
		INSTALL_COMMAND ""
		BUILD_COMMAND make -j
		BUILD_IN_SOURCE true
		)

add_dependencies(pixels-common liburing)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/liburing/src/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/liburing/src)
message(${CMAKE_CURRENT_BINARY_DIR}/liburing/src)
target_link_libraries(pixels-common
        ${Protobuf_LIBRARIES}
		${CMAKE_CURRENT_BINARY_DIR}/liburing/src/liburing.a
        )

# find_package(Java REQUIRED)
# find_package(JNI REQUIRED)
# include_directories(${JNI_INCLUDE_DIRS})
find_library(ROCKS_DB_PATH rocksdb)
find_package(Threads REQUIRED)

# This branch assumes that gRPC and all its dependencies are already installed
# on this system, so they can be located by find_package().

# Find Protobuf installation
# Looks for protobuf-config.cmake file installed by Protobuf's cmake installation.
set(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
set(_REFLECTION gRPC::grpc++_reflection)
if(CMAKE_CROSSCOMPILING)
find_program(_PROTOBUF_PROTOC protoc)
else()
set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
endif()

# Find gRPC installation
# Looks for gRPCConfig.cmake file installed by gRPC's cmake installation.
find_package(gRPC CONFIG REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(_GRPC_GRPCPP gRPC::grpc++)
if(CMAKE_CROSSCOMPILING)
find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
else()
set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
endif()

## Retina proto
# Proto file
get_filename_component(retina_proto "../../../proto/retina.proto" ABSOLUTE)
get_filename_component(retina_proto_dir "${retina_proto}" DIRECTORY)

# Generated sources
set(retina_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/retina.pb.cc")
set(retina_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/retina.pb.h")
set(retina_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/retina.grpc.pb.cc")
set(retina_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/retina.grpc.pb.h")
add_custom_command(
      OUTPUT "${retina_proto_srcs}" "${retina_proto_hdrs}" "${retina_grpc_srcs}" "${retina_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${retina_proto_dir}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${retina_proto}"
      DEPENDS "${retina_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# retina_grpc_proto
add_library(retina_grpc_proto
  ${retina_grpc_srcs}
  ${retina_grpc_hdrs}
  ${retina_proto_srcs}
  ${retina_proto_hdrs})
target_link_libraries(retina_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

aux_source_directory(. DIR_LIB_SRCS)
include_directories(${CMAKE_SOURCE_DIR}/include)

add_subdirectory(mq)
# add_library(retina STATIC ${DIR_LIB_SRCS})
# target_include_directories(RetinaService PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# add_library(retina SHARED "RetinaServiceImpl.cpp" "VersionManager.cpp")

find_package(log4cxx 0.11)
find_package(fmt)

# Targets route_guide_(client|server)
foreach(_target
  retina_client retina_server)
  add_executable(${_target}
    "${_target}.cc")
  target_link_libraries(${_target}
    retina_grpc_proto
   # retina
    mq
    memory
    util
    port
    log4cxx
    fmt::fmt
    ${ROCKS_DB_PATH}
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})
endforeach()

# # Targets route_guide_(client|server)
# foreach(_target
#   retina_writer_client)
#   add_executable(${_target}
#     "${_target}.cc")
#   target_link_libraries(${_target}
#     retina_grpc_proto
#    # retina
#     mq
#     memory
#     util
#     port
#     log4cxx
#     fmt::fmt
#     ${ROCKS_DB_PATH}
#     ${_REFLECTION}
#     ${_GRPC_GRPCPP}
#     ${_PROTOBUF_LIBPROTOBUF})
# endforeach()
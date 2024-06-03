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
get_filename_component(rg_proto "../proto/retina_service.proto" ABSOLUTE)
get_filename_component(rg_proto_path "${rg_proto}" PATH)

# Generated sources
set(rg_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/retina_service.pb.cc")
set(rg_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/retina_service.pb.h")
set(rg_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/retina_service.grpc.pb.cc")
set(rg_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/retina_service.grpc.pb.h")
add_custom_command(
      OUTPUT "${rg_proto_srcs}" "${rg_proto_hdrs}" "${rg_grpc_srcs}" "${rg_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${rg_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${rg_proto}"
      DEPENDS "${rg_proto}")


## Retina Writer Proto
# Proto file
get_filename_component(rg_writer_proto "../proto/retina_writer.proto" ABSOLUTE)
get_filename_component(rg_writer_proto_path "${rg_writer_proto}" PATH)

# Generated sources
set(rg_writer_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/retina_writer.pb.cc")
set(rg_writer_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/retina_writer.pb.h")
set(rg_writer_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/retina_writer.grpc.pb.cc")
set(rg_writer_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/retina_writer.grpc.pb.h")
add_custom_command(
      OUTPUT "${rg_writer_proto_srcs}" "${rg_writer_proto_hdrs}" "${rg_writer_grpc_srcs}" "${rg_writer_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${rg_writer_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${rg_writer_proto}"
      DEPENDS "${rg_writer_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# rg_grpc_proto
add_library(rg_grpc_proto
  ${rg_grpc_srcs}
  ${rg_grpc_hdrs}
  ${rg_proto_srcs}
  ${rg_proto_hdrs})
target_link_libraries(rg_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# rg_writer_grpc_proto
add_library(rg_writer_grpc_proto
  ${rg_writer_grpc_srcs}
  ${rg_writer_grpc_hdrs}
  ${rg_writer_proto_srcs}
  ${rg_writer_proto_hdrs})
target_link_libraries(rg_writer_grpc_proto
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
    rg_grpc_proto
    rg_writer_grpc_proto
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
#     rg_grpc_proto
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





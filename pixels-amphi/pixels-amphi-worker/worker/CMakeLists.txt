project(worker)

find_package(gRPC CONFIG REQUIRED)

set(worker_cxx
        src/db/duckdb_manager.cpp
        src/grpc/coordinate_query_client.cpp
        src/grpc/metadata_client.cpp
        src/grpc/transpile_sql_client.cpp
        src/grpc/trino_query_client.cpp
        src/exception/grpc_exception.cpp
        src/exception/grpc_coordinator_query_exception.cpp
        src/exception/grpc_metadata_exception.cpp
        src/exception/grpc_transpile_exception.cpp
        src/exception/grpc_trino_query_exception.cpp
        src/exception/grpc_unavailable_exception.cpp
        src/utils/constants.cpp
        src/utils/aws_utils.cpp)

set(PROTO_FILES_DIR "${CMAKE_CURRENT_LIST_DIR}/../proto")
set(PROTO_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_OUTPUT_DIR})

# Proto files
get_filename_component(amphi_proto "../proto/amphi.proto" ABSOLUTE)
get_filename_component(amphi_proto_path "${amphi_proto}" PATH)
get_filename_component(metadata_proto "../proto/metadata.proto" ABSOLUTE)
get_filename_component(metadata_proto_path "${metadata_proto}" PATH)

# Generated sources
set(AMPHI_PROTO_SRCS "${PROTO_OUTPUT_DIR}/amphi.pb.cc")
set(AMPHI_PROTO_HDRS "${PROTO_OUTPUT_DIR}/amphi.pb.h")
set(AMPHI_GRPC_SRCS "${PROTO_OUTPUT_DIR}/amphi.grpc.pb.cc")
set(AMPHI_GRPC_HDRS "${PROTO_OUTPUT_DIR}/amphi.grpc.pb.h")
set(METADATA_PROTO_SRCS "${PROTO_OUTPUT_DIR}/metadata.pb.cc")
set(METADATA_PROTO_HDRS "${PROTO_OUTPUT_DIR}/metadata.pb.h")
set(METADATA_GRPC_SRCS "${PROTO_OUTPUT_DIR}/metadata.grpc.pb.cc")
set(METADATA_GRPC_HDRS "${PROTO_OUTPUT_DIR}/metadata.grpc.pb.h")

get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)

# Custom command for amphi_proto
add_custom_command(
        OUTPUT "${AMPHI_PROTO_SRCS}" "${AMPHI_PROTO_HDRS}" "${AMPHI_GRPC_SRCS}" "${AMPHI_GRPC_HDRS}"
        COMMAND "protoc"
        --grpc_out "${PROTO_OUTPUT_DIR}"
        --cpp_out "${PROTO_OUTPUT_DIR}"
        -I "${amphi_proto_path}"
        --plugin=protoc-gen-grpc="${grpc_cpp_plugin_location}"
        "${amphi_proto}"
        DEPENDS "${amphi_proto}"
)

# Custom command for metadata_proto
add_custom_command(
        OUTPUT "${METADATA_PROTO_SRCS}" "${METADATA_PROTO_HDRS}" "${METADATA_GRPC_SRCS}" "${METADATA_GRPC_HDRS}"
        COMMAND "protoc"
        --grpc_out "${PROTO_OUTPUT_DIR}"
        --cpp_out "${PROTO_OUTPUT_DIR}"
        -I "${metadata_proto_path}"
        --plugin=protoc-gen-grpc="${grpc_cpp_plugin_location}"
        "${metadata_proto}"
        DEPENDS "${metadata_proto}"
)

add_library(worker
        ${worker_cxx}
        ${AMPHI_PROTO_SRCS}
        ${AMPHI_PROTO_HDRS}
        ${AMPHI_GRPC_SRCS}
        ${AMPHI_GRPC_HDRS}
        ${METADATA_PROTO_SRCS}
        ${METADATA_PROTO_HDRS}
        ${METADATA_GRPC_SRCS}
        ${METADATA_GRPC_HDRS})

# Include worker header files and generated *.pb.h files
target_include_directories(worker PRIVATE
        include
        "${CMAKE_CURRENT_BINARY_DIR}/generated")

target_link_libraries(
        worker
        ${Protobuf_LIBRARIES}
        gRPC::grpc++
)

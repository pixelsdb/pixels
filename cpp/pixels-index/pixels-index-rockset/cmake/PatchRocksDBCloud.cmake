set(ROCKSDB_CLOUD_CMAKE "${ROCKSDB_CLOUD_SOURCE_DIR}/CMakeLists.txt")
set(ROCKSDB_CLOUD_SOURCES
    "cloud/aws/aws_env.cc"
    "cloud/aws/aws_kafka.cc"
    "cloud/aws/aws_kinesis.cc"
    "cloud/aws/aws_retry.cc"
    "cloud/aws/aws_s3.cc"
    "cloud/db_cloud_impl.cc"
    "cloud/cloud_env.cc"
    "cloud/cloud_env_impl.cc"
    "cloud/cloud_env_options.cc"
    "cloud/cloud_log_controller.cc"
    "cloud/manifest_reader.cc"
    "cloud/purge.cc"
    "cloud/cloud_manifest.cc"
    "cloud/cloud_storage_provider.cc"
    "db/db_impl/db_impl_remote_compaction.cc")

file(READ "${ROCKSDB_CLOUD_CMAKE}" ROCKSDB_CMAKE_CONTENT)

foreach(ROCKSDB_CLOUD_SOURCE IN LISTS ROCKSDB_CLOUD_SOURCES)
    if(NOT ROCKSDB_CMAKE_CONTENT MATCHES "${ROCKSDB_CLOUD_SOURCE}")
        string(APPEND ROCKSDB_CLOUD_SOURCE_LINES "\n        ${ROCKSDB_CLOUD_SOURCE}")
    endif()
endforeach()

if(ROCKSDB_CLOUD_SOURCE_LINES)
    string(REPLACE
        "        utilities/write_batch_with_index/write_batch_with_index_internal.cc"
        "        utilities/write_batch_with_index/write_batch_with_index_internal.cc${ROCKSDB_CLOUD_SOURCE_LINES}"
        ROCKSDB_CMAKE_CONTENT
        "${ROCKSDB_CMAKE_CONTENT}")
    message(STATUS "Patched rocksdb-cloud CMake source list with cloud sources")
endif()

if(NOT ROCKSDB_CMAKE_CONTENT MATCHES "aws-cpp-sdk-core")
    string(REPLACE
        "if(WIN32)\n  set(SYSTEM_LIBS"
        "list(APPEND THIRDPARTY_LIBS\n  aws-cpp-sdk-s3\n  aws-cpp-sdk-transfer\n  aws-cpp-sdk-kinesis\n  aws-cpp-sdk-core\n  curl)\n\nif(WIN32)\n  set(SYSTEM_LIBS"
        ROCKSDB_CMAKE_CONTENT
        "${ROCKSDB_CMAKE_CONTENT}")
    message(STATUS "Patched rocksdb-cloud CMake link libraries with AWS SDK dependencies")
endif()

file(WRITE "${ROCKSDB_CLOUD_CMAKE}" "${ROCKSDB_CMAKE_CONTENT}")

set(ROCKSDB_CLOUD_AWS_S3 "${ROCKSDB_CLOUD_SOURCE_DIR}/cloud/aws/aws_s3.cc")
file(READ "${ROCKSDB_CLOUD_AWS_S3}" ROCKSDB_AWS_S3_CONTENT)

string(REPLACE
    "std::make_shared<Aws::S3::S3Client>(creds, config)"
    "std::make_shared<Aws::S3::S3Client>(creds, config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent, true)"
    ROCKSDB_AWS_S3_CONTENT
    "${ROCKSDB_AWS_S3_CONTENT}")
string(REPLACE
    "std::make_shared<Aws::S3::S3Client>(config)"
    "std::make_shared<Aws::S3::S3Client>(config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent, true)"
    ROCKSDB_AWS_S3_CONTENT
    "${ROCKSDB_AWS_S3_CONTENT}")

file(WRITE "${ROCKSDB_CLOUD_AWS_S3}" "${ROCKSDB_AWS_S3_CONTENT}")

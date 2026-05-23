set(ROCKSDB_CLOUD_PATCH_DIR "${CMAKE_CURRENT_LIST_DIR}/patches")
set(ROCKSDB_CLOUD_PATCH_STAMP
    "${ROCKSDB_CLOUD_SOURCE_DIR}/.pixels-rocksdb-cloud-patched")

if(EXISTS "${ROCKSDB_CLOUD_PATCH_STAMP}")
    message(STATUS "rocksdb-cloud patches already applied")
    return()
endif()

set(ROCKSDB_CLOUD_PATCH_FILES
    "${ROCKSDB_CLOUD_PATCH_DIR}/0001-add-cloud-sources-and-aws-libs.patch"
    "${ROCKSDB_CLOUD_PATCH_DIR}/0002-enable-path-style-s3-client.patch"
    "${ROCKSDB_CLOUD_PATCH_DIR}/0003-include-cstdint-in-data-block-hash-index.patch")

foreach(ROCKSDB_CLOUD_PATCH_FILE IN LISTS ROCKSDB_CLOUD_PATCH_FILES)
    if(NOT EXISTS "${ROCKSDB_CLOUD_PATCH_FILE}")
        message(FATAL_ERROR "rocksdb-cloud patch file not found: ${ROCKSDB_CLOUD_PATCH_FILE}")
    endif()

    execute_process(
        COMMAND patch --batch -p1 --input "${ROCKSDB_CLOUD_PATCH_FILE}"
        WORKING_DIRECTORY "${ROCKSDB_CLOUD_SOURCE_DIR}"
        RESULT_VARIABLE ROCKSDB_CLOUD_PATCH_RESULT
        OUTPUT_VARIABLE ROCKSDB_CLOUD_PATCH_STDOUT
        ERROR_VARIABLE ROCKSDB_CLOUD_PATCH_STDERR)

    if(NOT ROCKSDB_CLOUD_PATCH_RESULT EQUAL 0)
        message(FATAL_ERROR
            "Failed to apply rocksdb-cloud patch: ${ROCKSDB_CLOUD_PATCH_FILE}\n"
            "stdout:\n${ROCKSDB_CLOUD_PATCH_STDOUT}\n"
            "stderr:\n${ROCKSDB_CLOUD_PATCH_STDERR}")
    endif()
endforeach()

file(WRITE "${ROCKSDB_CLOUD_PATCH_STAMP}" "patched\n")

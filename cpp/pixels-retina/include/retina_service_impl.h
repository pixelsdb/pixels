/*
 * Copyright 2017-2019 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
#pragma once
#include <unordered_map>

#include "buffer.h"
#include "index.h"
#ifndef COMPILE_UNIT_TESTS
#include "retina_writer_client.h"
#endif
#include "row_group_id_manager.h"
#include "shared_memory_allocator.h"
#include "version_manager.h"
#include "water_mark_manager.h"

/**
 * @author mzp0514
 * @date 27/05/2022
 */
class RetinaServiceImpl {
 public:
  RetinaServiceImpl() {
    query_mem_ =
        std::make_shared<stm::SharedMemory>("/dev/shm/query", 200 * 1024);
    query_mem_alloc_ =
        new SharedMemoryAllocator(query_mem_, 200 * 1024, 50 * 1024);

    flush_mem_ =
        std::make_shared<stm::SharedMemory>("/dev/shm/flush", 200 * 1024);
    flush_mem_alloc_ =
        new SharedMemoryAllocator(flush_mem_, 200 * 1024, 50 * 1024);

    version_mem_ =
        std::make_shared<stm::SharedMemory>("/dev/shm/version", 100 * 1024);
    version_mem_alloc_ =
        new SharedMemoryAllocator(flush_mem_, 100 * 1024, 10 * 1024);

    flush_writer_ = new Writer();
    query_writer_ = new Writer();
    version_writer_ = new Writer();

#ifndef COMPILE_UNIT_TESTS
    writer_client_ = new RetinaWriterClient(grpc::CreateChannel(
        "localhost:50052", grpc::InsecureChannelCredentials()));
#endif

    db_ = new Index();
  }

  /**
   * @brief Insert a row into buffer.
   *
   * @param schema_name
   * @param table_name
   * @param pk_id
   * @param values
   * @param types
   * @param timestamp
   * @param called_by_update
   */
  StatusCode Insert(std::string schema_name, std::string table_name, int pk_id,
                    std::vector<Slice>& values, std::vector<ValueType> types,
                    long timestamp, bool called_by_update = false) {
    std::string key = schema_name + ":" + table_name;
    Buffer* buffer = nullptr;

    // If a buffer for the table exist, directly append the row;
    // else apply for a rgid and create a new buffer.
    if (called_by_update || buffers_.find(key) != buffers_.end()) {
      buffer = buffers_[key];
      buffer->Append(values, pk_id, timestamp);
    } else {
      int rgid = RowGroupIdManager::next_rgid();
#ifndef COMPILE_UNIT_TESTS
      buffer = new Buffer(schema_name, table_name, rgid, types, flush_mem_,
                          flush_writer_, flush_mem_alloc_, query_writer_,
                          writer_client_, db_);
#else
      buffer = new Buffer(schema_name, table_name, rgid, types, flush_mem_,
                          flush_writer_, flush_mem_alloc_, query_writer_, db_);
#endif
      buffer->Append(values, pk_id, timestamp);
      buffers_[key] = buffer;
    }
    if (!called_by_update) {
      PushHighWaterMark(timestamp);
    }

    return StatusCode::kOk;
  }

  /**
   * @brief Delete a row from buffer.
   *        The implementation is to directly set the delete timestamp.
   *
   * @param schema_name
   * @param table_name
   * @param pk_id
   * @param pk
   * @param timestamp
   * @param called_by_update
   */
  StatusCode Delete(std::string schema_name, std::string table_name, int pk_id,
                    Slice& pk, long timestamp, bool called_by_update = false) {
    std::string key = schema_name + ":" + table_name;

    // First find the row in the buffer
    // if it is not in the buffer, look up the index and get the rgid:rid of it
    StatusCode s = buffers_[key]->Delete(pk, timestamp);
    if (s == StatusCode::kKeyNotExist) {
      std::string file_path, rgid;
      int rid;
      s = db_->get(schema_name, table_name, pk, file_path, rgid, rid);
      if (s == StatusCode::kOk) {
        VersionManager::SetDeleteIntent(key + ":" + rgid, rid, timestamp);
      }
    }

    if (s == StatusCode::kOk && !called_by_update) {
      PushHighWaterMark(timestamp);
    }

    return s;
  }

  /**
   * @brief Update a row.
   *        The implementation is to first delete the row and then append a new
   * row.
   *
   * @param schema_name
   * @param table_name
   * @param pk_id
   * @param values
   * @param timestamp
   */
  StatusCode Update(std::string schema_name, std::string table_name, int pk_id,
                    std::vector<Slice>& values, long timestamp) {
    StatusCode s =
        Delete(schema_name, table_name, pk_id, values[pk_id], timestamp, true);
    if (s == StatusCode::kOk) {
      Insert(schema_name, table_name, pk_id, values, std::vector<ValueType>(),
             timestamp, true);
      PushHighWaterMark(timestamp);
      return StatusCode::kOk;
    } else {
      return s;
    }
  }

  /**
   * @brief Get all the visible rowsin the buffer.
   *        The results will be written to the shared memory.
   *        Result format: [col number (int32)] [row number (int32)] [cols]
   *        Long column:
   *        [value type (int16)]
   *        [data length (unit: char) (int32)] [data]
   *        Binary column:
   *        [value type (int16)]
   *        [offset/length array length (unit: char) (int32)]
   *        [offset array] [length array]
   *        [data length (unit: char) (int32)] [data]
   *
   * @param schema_name
   * @param table_name
   * @param ts
   */
  StatusCode Query(std::string schema_name, std::string table_name, int rgid,
                   long ts, long& pos) {
    std::string key = schema_name + ":" + table_name;
    pos = query_mem_alloc_->Allocate();
    return buffers_[key]->Query(rgid, ts, query_mem_, pos);
  }

  /**
   * @brief Get the visibility of a rowgroup.
   *        The results will be written into the shared memory.
   *        result format:
   *        [uint64_t array length (unit: char)]
   *        [uint64_t array]
   *
   * @param schema_name
   * @param table_name
   * @param rgid
   * @param ts
   */
  StatusCode QueryVisibility(std::string schema_name, std::string table_name,
                             int rgid, long ts, long& pos) {
    std::string key =
        schema_name + ":" + table_name + ":" + std::to_string(rgid);
    pos = version_mem_alloc_->Allocate();
    return VersionManager::GetVisibility(key, ts, version_mem_, pos);
  }

  void ReleaseFlushMem(long pos) {
    std::cout << "Release flush mem..." << std::endl;
    flush_mem_alloc_->Release(pos);
  }

  void ReleaseQueryMem(long pos) {
    std::cout << "Release query mem..." << std::endl;
    query_mem_alloc_->Release(pos);
  }

  void ReleaseVersionMem(long pos) {
    std::cout << "Release version mem..." << std::endl;
    version_mem_alloc_->Release(pos);
  }

  std::shared_ptr<stm::SharedMemory> query_mem() { return query_mem_; }

  std::shared_ptr<stm::SharedMemory> flush_mem() { return flush_mem_; }

  std::shared_ptr<stm::SharedMemory> version_mem() { return version_mem_; }

 private:
  void PushHighWaterMark(int timestamp) {
    // write WAL
    // push ts to water mark service
    WaterMarkManager::set_hwm(timestamp);
  }

  std::unordered_map<std::string, Buffer*> buffers_;  // s_name:t_name -> buffer
  // VersionManager* vm;
  // MessageQueue mmapToFileSystem;
  // MessageQueue mmapToQuery;

  std::shared_ptr<stm::SharedMemory> query_mem_;
  SharedMemoryAllocator* query_mem_alloc_;
  std::shared_ptr<stm::SharedMemory> flush_mem_;
  SharedMemoryAllocator* flush_mem_alloc_;
  std::shared_ptr<stm::SharedMemory> version_mem_;
  SharedMemoryAllocator* version_mem_alloc_;

  Writer* flush_writer_;
  Writer* query_writer_;
  Writer* version_writer_;

#ifndef COMPILE_UNIT_TESTS
  RetinaWriterClient* writer_client_;
#endif

  Index* db_;  // schema:table:pk -> filepath:rgid:rid "s0:t0:key0", "0:0:0"

  static log4cxx::LoggerPtr logger;
};

log4cxx::LoggerPtr RetinaServiceImpl::logger(
    log4cxx::Logger::getLogger("RetinaServiceImpl"));

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

#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "const.h"
#include "flush_writer.h"
#include "index.h"
#include "inlineskiplist.h"
#include "log4cxx/basicconfigurator.h"
#include "log4cxx/helpers/exception.h"
#include "log4cxx/logger.h"
#include "memory/concurrent_arena.h"
#ifndef COMPILE_UNIT_TESTS
#include "retina_writer_client.h"
#endif
#include "rocksdb/comparator.h"
#include "rocksdb/memtablerep.h"
#include "row_group_id_manager.h"
#include "shared_memory_allocator.h"
#include "status.h"
#include "vectorized_row_batch.h"
#include "version_manager.h"
#include "water_mark_manager.h"

/**
 * @brief Used for skiplist internal key comparison.
 *
 */
class KeyComparator {
 public:
  using DecodedType = Slice;
  /**
   * @brief Deocode the internal skiplist key into user key.
   *
   * @param key
   * @return DecodedType
   */
  DecodedType decode_key(const char* key) const {
    return rocksdb::GetLengthPrefixedSlice(key);
  }

  /**
   * @brief Compare a and b. Return a negative value if a is less than b, 0 if
   * they are equal, and a positive value if a is greater than b
   *
   * @param prefix_len_key1
   * @param prefix_len_key2
   * @return int
   */
  int operator()(const char* prefix_len_key1,
                 const char* prefix_len_key2) const {
    return decode_key(prefix_len_key1).compare(decode_key(prefix_len_key2));
  }

  int operator()(const char* prefix_len_key, const DecodedType key) const {
    return decode_key(prefix_len_key).compare(key);
  }

  ~KeyComparator() {}
};

using SkipList = rocksdb::InlineSkipList<KeyComparator>;

/**
 * @brief Encode user key and value to internal key.
 *
 * @param key
 * @param value
 * @param list
 * @return const char*
 */
static const char* Encode(const Slice& key, int value, SkipList* list) {
  uint32_t key_size = static_cast<uint32_t>(key.size());
  const uint32_t encoded_len =
      rocksdb::VarintLength(key_size) + key_size + sizeof(int);
  char* buf = list->AllocateKey(encoded_len);
  char* p = rocksdb::EncodeVarint32(buf, key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  memcpy(p, &value, sizeof(int));
  assert((unsigned)(p + sizeof(int) - buf) == (unsigned)encoded_len);
  return buf;
}

/**
 * @brief Decode the internal skiplist key into user key and value.
 *
 * @param key
 * @param user_key
 * @param value
 */
static void Decode(const char* key, Slice& user_key, int& value) {
  uint32_t key_length = 0;
  const char* key_ptr = rocksdb::GetVarint32Ptr(key, key + 5, &key_length);
  user_key = Slice(key_ptr, key_length);
  value = *(int*)(key_ptr + key_length);
  std::cout << "decode: " << user_key.ToString() << " " << value << std::endl;
}

/**
 * @brief Deocode the internal skiplist key into value.
 *
 * @param key
 * @return int
 */
static int DecodeValue(const char* key) {
  uint32_t key_length = 0;
  const char* key_ptr = rocksdb::GetVarint32Ptr(key, key + 5, &key_length);
  return *(int*)(key_ptr + key_length);
}

/**
 * @brief A buffer is to store newly written delta data.
 *        A buffer corresponds to a row group for a table of one schema.
 *
 * @author mzp0514
 * @date 27/05/2022
 */
class Buffer {
 public:
#ifndef COMPILE_UNIT_TESTS
  Buffer(std::string schema_name, std::string table_name, int rgid,
         std::vector<ValueType>& columns,
         std::shared_ptr<stm::SharedMemory> flush_mem, Writer* flush_writer,
         SharedMemoryAllocator* flush_mem_alloc, Writer* query_writer,
         RetinaWriterClient* writer_client, Index* db) {
    schema_name_ = schema_name;
    table_name_ = table_name;
    rgid_ = rgid;
    columns_ = columns;
    num_cols_ = columns.size() + 1;
    cur_length_ = 0;
    max_row_size_ = min_row_size_ = 0;
    tot_size_ = sizeof(int) * 2 + num_cols_ * sizeof(short);
    for (auto col : columns) {
      switch (col) {
        case ValueType::BYTES:
          tot_size_ += sizeof(int);
          max_row_size_ += MAX_STRING_SIZE + 2 * sizeof(int);
          min_row_size_ += 1 + 2 * sizeof(int);
          break;
        case ValueType::LONG:
          max_row_size_ += sizeof(long);
          break;

        default:
          break;
      }
    }
    meta_size_ = tot_size_;
    flush_mem_ = flush_mem;
    flush_writer_ = flush_writer;
    flush_mem_alloc_ = flush_mem_alloc;
    writer_client_ = writer_client;
    db_ = db;
    batch_ = new VectorizedRowBatch(rgid, columns,
                                    MAX_BUFFER_SIZE / min_row_size_ + 1);
    arena_ = new rocksdb::ConcurrentArena();
    skip_list_ = new SkipList(cmp_, arena_);
  }
#endif

  Buffer(std::string schema_name, std::string table_name, int rgid,
         std::vector<ValueType>& columns,
         std::shared_ptr<stm::SharedMemory> flush_mem, Writer* flush_writer,
         SharedMemoryAllocator* flush_mem_alloc, Writer* query_writer,
         Index* db) {
    schema_name_ = schema_name;
    table_name_ = table_name;
    rgid_ = rgid;
    columns_ = columns;
    num_cols_ = columns.size() + 1;
    cur_length_ = 0;
    max_row_size_ = min_row_size_ = 0;
    tot_size_ = sizeof(int) * 2 + num_cols_ * sizeof(short);
    for (auto col : columns) {
      switch (col) {
        case ValueType::BYTES:
          tot_size_ += sizeof(int);
          max_row_size_ += MAX_STRING_SIZE + 2 * sizeof(int);
          min_row_size_ += 1 + 2 * sizeof(int);
          break;
        case ValueType::LONG:
          max_row_size_ += sizeof(long);
          break;

        default:
          break;
      }
    }
    meta_size_ = tot_size_;
    flush_mem_ = flush_mem;
    flush_writer_ = flush_writer;
    flush_mem_alloc_ = flush_mem_alloc;
    db_ = db;
    batch_ = new VectorizedRowBatch(rgid, columns,
                                    MAX_BUFFER_SIZE / min_row_size_ + 1);
    arena_ = new rocksdb::ConcurrentArena();
    skip_list_ = new SkipList(cmp_, arena_);
  }

  ~Buffer() {
    delete skip_list_;
    delete arena_;
    delete batch_;
  }

  StatusCode Append(std::vector<Slice> values, int pk_id, long ts) {
    // std::scoped_lock guard(mu_);
    int index = cur_length_++;
    // Append into batch
    batch_->Append(index, values, pk_id, ts);

    // Calculate memory usage
    int row_size = 0;
    for (int i = 0; i < columns_.size(); i++) {
      row_size += values[i].size_;
      if (columns_[i] == ValueType::BYTES) {
        row_size += 2 * sizeof(int);
      }
    }
    tot_size_ += row_size;

    // Insert into skip list
    const char* key = Encode(values[pk_id], index, skip_list_);
    skip_list_->InsertConcurrently(key);

    // If the buffer is full, flush
    if (ShouldFlush()) {
      std::scoped_lock guard(mu_);
      if (ShouldFlush()) {
        Flush();
      }
    }
    return StatusCode::kOk;
  }

  bool ShouldFlush() const {
    // std::cout << "check should flush..." << tot_size_ << " " << max_row_size_
    //           << " " << tot_size_ + max_row_size_ << std::endl;
    LOG4CXX_DEBUG(logger, "check should flush..." << tot_size_ << " "
                                                  << max_row_size_ << " "
                                                  << tot_size_);
    return tot_size_ + max_row_size_ >= MAX_BUFFER_SIZE;
  }

  StatusCode Delete(Slice pk, long ts) {
    // std::scoped_lock guard(mu_);

    SkipList::Iterator iter(skip_list_);
    iter.Seek(pk.data());

    // key found
    if (iter.Valid() && cmp_(iter.key(), pk.data()) == 0) {
      int rid = DecodeValue(iter.key());
      batch_->SetDeleteTimestamp(rid, ts);
    } else {
      return StatusCode::kKeyNotExist;
    }
    return StatusCode::kOk;
  }

  StatusCode Query(int rgid, long ts, std::shared_ptr<stm::SharedMemory> mem,
                   long pos) {
    // std::scoped_lock guard(mu_);
    VectorizedRowBatch* batch_to_query = nullptr;
    if (rgid == batch_->rgid()) {
      batch_to_query = batch_;
    } else {
      std::lock_guard<std::mutex> lock(batches_mutex_);
      auto it = immutable_batches_.find(rgid);
      if (it != immutable_batches_.end()) {
        batch_to_query = it->second;
      } else {
        return StatusCode::kRgFlushed;
      }
    }

    batch_to_query->IncrementQueryRef();
    auto t = std::packaged_task<void()>(std::bind(
        &VectorizedRowBatch::FilteredFlush, batch_to_query, mem, pos, ts));
    auto callback = [batch_to_query]() { batch_to_query->DecrementQueryRef(); };
    WriteTask task = {std::move(t), callback};
    std::future<void> f = t.get_future();

    query_writer_->Submit(task);

    f.get();

    return StatusCode::kOk;
  }

  void Flush() {
    // File path generation mechanism to be decided
    std::string file_path = schema_name_ + "|" + table_name_;

    int* indices = batch_->indices();
    int i = 0;
    LOG4CXX_DEBUG(logger, "prepare to flush...");
    std::cout << "start to flush..." << std::endl;
    SkipList::Iterator iter(skip_list_);
    iter.SeekToFirst();
    while (iter.Valid()) {
      Slice key;
      int rid;
      Decode(iter.key(), key, rid);

      // Insert to db
      db_->put(schema_name_, table_name_, key, file_path, rgid_, i);

      // Construct sorted indices
      indices[i++] = rid;

      iter.Next();
    }

    // Create version
    std::cout << "Create version..." << std::endl;
    long* delete_ts = batch_->delete_timestamps();
    long lwm = WaterMarkManager::lwm();
    VersionManager::AddVersion(
        schema_name_ + ":" + table_name_ + ":" + std::to_string(rgid_),
        delete_ts, lwm, indices, cur_length_);

    // FlushTask task(flush_mem_, 0, batch_, false);
    std::cout << "Request pos..." << std::endl;
    long pos = flush_mem_alloc_->Allocate();
    std::cout << "Allocate " << pos << std::endl;

    batch_->set_to_flush(true);
    auto t = std::packaged_task<void()>(
        std::bind(&VectorizedRowBatch::SortedFlush, batch_, flush_mem_, pos));
    auto callback =
        std::bind(&Buffer::FlushCallBack, this, rgid_, pos, file_path);
    WriteTask task = {std::move(t), callback};  // wrap the function
    // std::future<void> f = t.get_future();     // get a future
    flush_writer_->Submit(task);

    Swap();
  }

  void FlushCallBack(int rgid, long pos, std::string file_path) {
    std::cout << "Flush callback called..." << std::endl;
#ifndef COMPILE_UNIT_TESTS
    // Call the pixels writer to flush the data (java side)
    writer_client_->Flush(schema_name_, table_name_, rgid, pos, file_path);
    std::cout << "Flushed into shared mem, waiting for response..."
              << std::endl;
#endif

    immutable_batches_[rgid]->set_to_flush(false);

    // Remove and release unused batch
    {
      std::lock_guard<std::mutex> lock(batches_mutex_);
      auto it = immutable_batches_.begin();
      while (it != immutable_batches_.end()) {
        if (!it->second->to_flush() && it->second->query_ref_cnt() == 0) {
          delete it->second;
          it = immutable_batches_.erase(it);
        } else {
          ++it;
        }
      }
    }

    // Release shared memory slot
    flush_mem_alloc_->Release(pos);
    std::cout << "Flush callback finished..." << std::endl;
  }

  int num_cols() { return num_cols_; };

  int cur_length() { return cur_length_; };

  Index* db() { return db_; }

 private:
  // Meta data
  std::string schema_name_;
  std::string table_name_;
  int rgid_;

  // Storage
  std::vector<ValueType> columns_;
  int num_cols_;
  std::atomic<int> cur_length_;
  std::atomic<int> tot_size_;
  int meta_size_;
  int max_row_size_;
  int min_row_size_;
  VectorizedRowBatch* batch_;

  std::unordered_map<int, VectorizedRowBatch*>
      immutable_batches_;  // batches to be flushed
  std::mutex batches_mutex_;

  // IPC
  std::shared_ptr<stm::SharedMemory> flush_mem_;
  SharedMemoryAllocator* flush_mem_alloc_;
  Writer* flush_writer_;
  Writer* query_writer_;
#ifndef COMPILE_UNIT_TESTS
  RetinaWriterClient* writer_client_;
#endif
  std::mutex mu_;
  // std::condition_variable cond;
  // std::mutex flush_mu;

  // Skip list
  rocksdb::ConcurrentArena* arena_;
  KeyComparator cmp_;
  SkipList* skip_list_;

  // Index (global use)
  Index* db_;

  static log4cxx::LoggerPtr logger;

  /**
   * @brief Swap old data structures with new ones
   *
   */
  void Swap() {
    LOG4CXX_DEBUG(logger, "start to swap...");
    std::cout << "start to swap..." << std::endl;

    cur_length_ = 0;
    tot_size_ = meta_size_;

    immutable_batches_[rgid_] = batch_;
    rgid_ = RowGroupIdManager::next_rgid();
    batch_ = new VectorizedRowBatch(rgid_, columns_,
                                    MAX_BUFFER_SIZE / min_row_size_ + 1);

    rocksdb::ConcurrentArena* old_arena = arena_;
    arena_ = new rocksdb::ConcurrentArena();
    delete old_arena;

    SkipList* old_list = skip_list_;
    skip_list_ = new SkipList(cmp_, arena_);
    delete old_list;

    LOG4CXX_DEBUG(logger, "swap finished...");
    std::cout << "swap finished" << std::endl;
  }
};

log4cxx::LoggerPtr Buffer::logger(log4cxx::Logger::getLogger("Buffer"));

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
#include <fmt/color.h>
#include <fmt/core.h>
#include <fmt/ostream.h>

#include <cassert>
#include <iostream>

#include "log4cxx/logger.h"
#include "rocksdb/db.h"
#include "status.h"
#include "util/string_util.h"

/**
 * @author mzp0514
 * @date 27/05/2022
 */

/**
 * @brief Wrapper for rocksdb. kv: schema_name:table_name:primary_key ->
 * file_path:rgid:rid
 *
 */
class Index {
 public:
  Index() {
    options_.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options_, "/tmp/retina", &db_);
    LOG4CXX_INFO_FMT(logger, "open db: {}", status.ToString());
    assert(status.ok());
  }

  ~Index() {
    LOG4CXX_INFO(logger, "close db...");
    if (!db_) {
      delete db_;
      db_ = nullptr;
    }
  }

  Index(const Index&) = delete;
  Index& operator=(const Index&) = delete;

  void put(rocksdb::Slice& key, std::string& file_path, int& rgid, int& rid) {
    std::string value =
        file_path + ":" + std::to_string(rgid) + ":" + std::to_string(rid);
    LOG4CXX_DEBUG_FMT(logger, "put: {} {}", key.ToString(), value);
    rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), key, value);
    LOG4CXX_DEBUG_FMT(logger, "put res: {}", s.ToString());
  }

  void get(rocksdb::Slice& key, std::string& file_path, std::string& rgid,
           int& rid) {
    std::string value;
    LOG4CXX_DEBUG_FMT(logger, "put: {}", key.ToString());
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
    std::vector<std::string> elems = rocksdb::StringSplit(value, ':');
    file_path = elems[0];
    rgid = elems[1];
    rid = std::stoi(elems[2]);
  }

  void put(std::string& schema_name, std::string& table_name,
           rocksdb::Slice& key, std::string& file_path, int& rgid, int& rid) {
    std::string internal_key =
        schema_name + ":" + table_name + ":" + key.ToString();
    std::string value =
        file_path + ":" + std::to_string(rgid) + ":" + std::to_string(rid);
    LOG4CXX_DEBUG_FMT(logger, "put: {} {}", internal_key, value);
    // std::cout << "put: " << internal_key << " " << value << std::endl;
    rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), internal_key, value);
    LOG4CXX_DEBUG_FMT(logger, "put res: {}", s.ToString());
    // std::cout << "put res: " << s.ToString() << std::endl;
  }

  StatusCode get(std::string& schema_name, std::string& table_name,
                 rocksdb::Slice& key, std::string& file_path, std::string& rgid,
                 int& rid) {
    std::string internal_key =
        schema_name + ":" + table_name + ":" + key.ToString();
    std::string value;
    LOG4CXX_DEBUG_FMT(logger, "get: {}", internal_key);
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), internal_key, &value);
    if (s.ok()) {
      std::vector<std::string> elems = rocksdb::StringSplit(value, ':');
      file_path = elems[0];
      rgid = elems[1];
      rid = std::stoi(elems[2]);
      LOG4CXX_DEBUG_FMT(logger, "get res: {}", s.ToString());
      return StatusCode::kOk;
    } else if (s.IsNotFound()) {
      return StatusCode::kKeyNotExist;
    } else {
      return StatusCode::kIndexError;
    }
  }

  std::string get(rocksdb::Slice key) {
    std::string value;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
    return value;
  }

  std::string get(std::string& schema_name, std::string& table_name,
                  rocksdb::Slice key) {
    std::string internal_key =
        schema_name + ":" + table_name + ":" + key.ToString();
    std::string value;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), internal_key, &value);
    return value;
  }

 private:
  rocksdb::DB* db_;
  rocksdb::Options options_;

  static log4cxx::LoggerPtr logger;
};

log4cxx::LoggerPtr Index::logger(log4cxx::Logger::getLogger("Index"));
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
#include "buffer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>

#include "test_utils.h"

/**
 * @author mzp0514
 * @date 27/05/2022
 */

std::shared_ptr<stm::SharedMemory> query_mem =
    std::make_shared<stm::SharedMemory>("/dev/shm/query", 40960);
std::shared_ptr<stm::SharedMemory> flush_mem =
    std::make_shared<stm::SharedMemory>("/dev/shm/flush", 40960);
std::shared_ptr<stm::SharedMemory> version_mem =
    std::make_shared<stm::SharedMemory>("/dev/shm/version", 40960);

Index* db = new Index();

class BufferTest : public ::testing::Test {
 protected:
  std::vector<Slice> make_tuple(long long_vals[], int long_len,
                                std::string str_vals[], int str_len) {
    std::vector<Slice> tuple;
    for (int i = 0; i < long_len; i++) {
      tuple.push_back(Slice((char*)(long_vals + i), sizeof(long)));
    }

    for (int i = 0; i < str_len; i++) {
      tuple.push_back(Slice(str_vals[i]));
    }
    return tuple;
  }

  void SetUp() override {
    log4cxx::BasicConfigurator::configure();

    std::vector<ValueType> cols;
    for (int i = 0; i < 5; i++) {
      cols.push_back(ValueType::LONG);
    }
    for (int i = 0; i < 2; i++) {
      cols.push_back(ValueType::BYTES);
    }

    Writer* flush_writer = new Writer();
    Writer* query_writer = new Writer();
    SharedMemoryAllocator* flush_mem_alloc =
        new SharedMemoryAllocator(flush_mem, 40960, 4096);
    // db = new Index("s0", "t0");
    buf_ = new Buffer(schema_name_, table_name_, 0, cols, flush_mem,
                      flush_writer, flush_mem_alloc, query_writer, db);

    buf_->Append(make_tuple(long_vals_[0], 5, str_vals_[0], 2), pk_,
                 ver_vals_[0]);
    buf_->Append(make_tuple(long_vals_[1], 5, str_vals_[1], 2), pk_,
                 ver_vals_[1]);
    buf_->Append(make_tuple(long_vals_[2], 5, str_vals_[2], 2), pk_,
                 ver_vals_[2]);
  }

  void TearDown() override {
    std::cout << "test tear down" << std::endl;
    if (buf_) {
      delete buf_;
      buf_ = nullptr;
    }
    // if (db) {
    //   delete db;
    //   db = nullptr;
    // }
  }

  long long_vals_[3][5] = {
      {3, 2, 3, 4, 5}, {1, 7, 8, 9, 10}, {2, 12, 13, 14, 15}};
  std::string str_vals_[3][2] = {
      {"AB", "CDE"}, {"FGHI", "J"}, {"KLMNOPQ", "RSTUVWXYZ"}};
  long sorted_long_vals_[3][5] = {
      {1, 7, 8, 9, 10}, {2, 12, 13, 14, 15}, {3, 2, 3, 4, 5}};
  std::string sorted_str_vals_[3][2] = {
      {"FGHI", "J"}, {"KLMNOPQ", "RSTUVWXYZ"}, {"AB", "CDE"}};
  long ver_vals_[3] = {0, 1, 2};
  long sorted_ver_vals_[3] = {1, 2, 0};
  int pk_ = 0;
  Buffer* buf_;
  // Index* db;
  std::string schema_name_ = "s0";
  std::string table_name_ = "t0";
};

TEST_F(BufferTest, TESTQUERY1) {
  // std::shared_ptr<stm::SharedMemory> mem_tmp =
  // std::make_shared<stm::SharedMemory>("/dev/shm/test", 4096);
  auto mem_tmp = query_mem;
  ASSERT_EQ(buf_->num_cols(), 8);
  ASSERT_EQ(buf_->cur_length(), 3);

  long pos = 0;
  buf_->Query(0, 5, query_mem, pos);

  sleep(1);

  // read col num
  pos = 0;
  int col_num = mem_tmp->getInt(pos);
  ASSERT_EQ(col_num, buf_->num_cols());
  pos += sizeof(int);

  // long cols
  long long_vals_tmp[5][3];

  for (int i = 0; i < 5; i++) {
    ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
    ASSERT_EQ(type, ValueType::LONG);
    pos += sizeof(short);

    int len = mem_tmp->getInt(pos);
    ASSERT_EQ(len / sizeof(long), 3);
    pos += sizeof(int);

    mem_tmp->getBytes(pos, (stm::byte*)long_vals_tmp[i], 0, len);
    pos += len;

    for (int j = 0; j < len / sizeof(long); j++) {
      ASSERT_EQ(long_vals_tmp[i][j], long_vals_[j][i]);
    }
  }

  // str cols
  std::string str_vals_tmp[2][3];
  for (int i = 0; i < 2; i++) {
    ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
    ASSERT_EQ(type, ValueType::BYTES);
    pos += sizeof(short);

    int len = mem_tmp->getInt(pos);
    ASSERT_EQ(len / sizeof(int), 3);
    pos += sizeof(int);

    int starts[len / sizeof(int)];
    int lens[len / sizeof(int)];
    mem_tmp->getBytes(pos, (stm::byte*)starts, 0, len);
    pos += len;
    mem_tmp->getBytes(pos, (stm::byte*)lens, 0, len);
    pos += len;

    int data_len = mem_tmp->getInt(pos);
    pos += sizeof(int);
    for (int j = 0; j < len / sizeof(int); j++) {
      char tmp[lens[j] + 5];
      mem_tmp->getBytes(pos, (stm::byte*)tmp, 0, lens[j]);
      pos += lens[j];
      str_vals_tmp[i][j].assign(tmp, lens[j]);
      ASSERT_EQ(str_vals_tmp[i][j], str_vals_[j][i]);
    }
  }

  // version col
  long ver_vals_tmp[3];

  ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
  ASSERT_EQ(type, ValueType::LONG);
  pos += sizeof(short);

  int len = mem_tmp->getInt(pos);
  ASSERT_EQ(len / sizeof(long), 3);
  pos += sizeof(int);

  mem_tmp->getBytes(pos, (stm::byte*)ver_vals_tmp, 0, len);
  pos += len;

  for (int j = 0; j < len / sizeof(long); j++) {
    ASSERT_EQ(ver_vals_tmp[j], ver_vals_[j]);
  }
}

TEST_F(BufferTest, TESTQUERY2) {
  // std::shared_ptr<stm::SharedMemory> mem_tmp =
  // std::make_shared<stm::SharedMemory>("/dev/shm/test", 4096);
  auto mem_tmp = query_mem;
  ASSERT_EQ(buf_->num_cols(), 8);
  ASSERT_EQ(buf_->cur_length(), 3);

  long pos = 0;
  buf_->Query(0, 1, query_mem, pos);

  sleep(1);

  // read col num
  pos = 0;
  int col_num = mem_tmp->getInt(pos);
  ASSERT_EQ(col_num, buf_->num_cols());
  pos += sizeof(int);

  // long cols
  long long_vals_tmp[5][2];

  for (int i = 0; i < 5; i++) {
    ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
    ASSERT_EQ(type, ValueType::LONG);
    pos += sizeof(short);

    int len = mem_tmp->getInt(pos);
    ASSERT_EQ(len / sizeof(long), 2);
    pos += sizeof(int);

    mem_tmp->getBytes(pos, (stm::byte*)long_vals_tmp[i], 0, len);
    pos += len;

    for (int j = 0; j < len / sizeof(long); j++) {
      ASSERT_EQ(long_vals_tmp[i][j], long_vals_[j][i]);
    }
  }

  // str cols
  std::string str_vals_tmp[2][2];
  for (int i = 0; i < 2; i++) {
    ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
    ASSERT_EQ(type, ValueType::BYTES);
    pos += sizeof(short);

    int len = mem_tmp->getInt(pos);
    ASSERT_EQ(len / sizeof(int), 2);
    pos += sizeof(int);

    int starts[len / sizeof(int)];
    int lens[len / sizeof(int)];
    mem_tmp->getBytes(pos, (stm::byte*)starts, 0, len);
    pos += len;
    mem_tmp->getBytes(pos, (stm::byte*)lens, 0, len);
    pos += len;

    int data_len = mem_tmp->getInt(pos);
    pos += sizeof(int);
    for (int j = 0; j < len / sizeof(int); j++) {
      char tmp[lens[j] + 5];
      mem_tmp->getBytes(pos, (stm::byte*)tmp, 0, lens[j]);
      pos += lens[j];
      str_vals_tmp[i][j].assign(tmp, lens[j]);
      ASSERT_EQ(str_vals_tmp[i][j], str_vals_[j][i]);
    }
  }

  // version col
  long ver_vals_tmp[2];

  ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
  ASSERT_EQ(type, ValueType::LONG);
  pos += sizeof(short);

  int len = mem_tmp->getInt(pos);
  ASSERT_EQ(len / sizeof(long), 2);
  pos += sizeof(int);

  mem_tmp->getBytes(pos, (stm::byte*)ver_vals_tmp, 0, len);
  pos += len;

  for (int j = 0; j < len / sizeof(long); j++) {
    ASSERT_EQ(ver_vals_tmp[j], ver_vals_[j]);
  }
}

TEST_F(BufferTest, TESTFLUSH) {
  // std::shared_ptr<stm::SharedMemory> mem_tmp =
  // std::make_shared<stm::SharedMemory>("/dev/shm/test", 4096);
  auto mem_tmp = flush_mem;
  buf_->Flush();

  sleep(1);

  // read col num
  long pos = 0;
  int col_num = mem_tmp->getInt(pos);
  ASSERT_EQ(col_num, buf_->num_cols());
  pos += sizeof(int);

  long vals_tmp[5][3];
  for (int i = 0; i < 5; i++) {
    ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
    ASSERT_EQ(type, ValueType::LONG);
    pos += sizeof(short);

    long len = mem_tmp->getInt(pos);
    ASSERT_EQ(len / sizeof(long), 3);
    pos += sizeof(int);

    mem_tmp->getBytes(pos, (stm::byte*)vals_tmp[i], 0, len);
    pos += len;

    for (int j = 0; j < len / sizeof(long); j++) {
      ASSERT_EQ(vals_tmp[i][j], sorted_long_vals_[j][i]);
    }
  }

  std::string str_vals_tmp[2][3];
  for (int i = 0; i < 2; i++) {
    ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
    ASSERT_EQ(type, ValueType::BYTES);
    pos += sizeof(short);

    int len = mem_tmp->getInt(pos);
    ASSERT_EQ(len / sizeof(int), 3);
    pos += sizeof(int);

    int starts[len / sizeof(int)];
    int lens[len / sizeof(int)];
    mem_tmp->getBytes(pos, (stm::byte*)starts, 0, len);
    pos += len;
    mem_tmp->getBytes(pos, (stm::byte*)lens, 0, len);
    pos += len;

    int data_len = mem_tmp->getInt(pos);
    pos += sizeof(int);
    for (int j = 0; j < len / sizeof(int); j++) {
      char tmp[lens[j] + 5];
      mem_tmp->getBytes(pos, (stm::byte*)tmp, 0, lens[j]);
      pos += lens[j];
      str_vals_tmp[i][j].assign(tmp, lens[j]);
      ASSERT_EQ(str_vals_tmp[i][j], sorted_str_vals_[j][i]);
    }
  }

  // version col
  long ver_vals_tmp[3];

  ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
  ASSERT_EQ(type, ValueType::LONG);
  pos += sizeof(short);

  int len = mem_tmp->getInt(pos);
  ASSERT_EQ(len / sizeof(long), 3);
  pos += sizeof(int);

  mem_tmp->getBytes(pos, (stm::byte*)ver_vals_tmp, 0, len);
  pos += len;

  for (int j = 0; j < len / sizeof(long); j++) {
    ASSERT_EQ(ver_vals_tmp[j], sorted_ver_vals_[j]);
  }

  std::string file_path;
  std::string rgid;
  int rid;
  std::cout << "here " << db << std::endl;
  for (int i = 0; i < 3; i++) {
    char key_buf[8];
    memcpy(key_buf, (char*)&sorted_long_vals_[i][pk_], sizeof(long));
    Slice key(key_buf, sizeof(long));
    db->get(schema_name_, table_name_, key, file_path, rgid, rid);
    ASSERT_EQ(file_path, schema_name_ + "|" + table_name_);
    ASSERT_EQ(rgid, "0");
    ASSERT_EQ(rid, i);
  }

  pos = 0;
  VersionManager::GetVisibility(schema_name_ + ":" + table_name_ + ":" + rgid,
                                5, version_mem, pos);
  pos = 0;
  len = version_mem->getInt(pos);
  ASSERT_EQ(len, 1024 >> 3);
  pos += sizeof(int);
  char* vis_buf = new char[len];
  version_mem->getBytes(pos, (stm::byte*)vis_buf, 0, len);

  char visibility_res[1024 >> 3];
  memset(visibility_res, 255, sizeof(visibility_res));
  for (int i = 0; i < len; i++) {
    ASSERT_EQ(visibility_res[i], vis_buf[i]);
  }
}
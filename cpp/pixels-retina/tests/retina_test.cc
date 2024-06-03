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
#include <gtest/gtest.h>

#include <algorithm>
#include <iostream>

#include "retina_service_impl.h"

/**
 * @author mzp0514
 * @date 27/05/2022
 */

const int row_num = 791;

std::string makeFixedLength(const int i, const int length) {
  std::ostringstream ostr;

  if (i < 0) ostr << '-';

  ostr << std::setfill('0') << std::setw(length) << (i < 0 ? -i : i);

  return ostr.str();
}

class RetinaTest : public ::testing::Test {
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
    service = new RetinaServiceImpl();
    for (int i = 0; i < 5; i++) {
      cols.push_back(ValueType::LONG);
    }
    for (int i = 0; i < 2; i++) {
      cols.push_back(ValueType::BYTES);
    }
    int u = 0;
    for (int k = 0; k < 4; k++) {
      for (int i = 0; i < row_num; i++) {
        indices[k][i] = i;
        for (int j = 0; j < 2; j++) {
          str_vals_[k][i][j] = makeFixedLength(u, 4);
        }
        for (int j = 0; j < 5; j++) {
          long_vals_[k][i][j] = u;
        }
        ver_vals_[k][i] = u;
        u++;
      }

      std::sort(indices[k], indices[k] + row_num, [&](int i, int j) {
        return str_vals_[k][i][0] < str_vals_[k][j][0];
      });
    }
  }

  void TearDown() override {
    std::cout << "Test tear down" << std::endl;
    if (service) {
      delete service;
      service = nullptr;
    }
  }

  std::vector<ValueType> cols;

  long long_vals_[4][row_num][5];
  std::string str_vals_[4][row_num][2];
  long ver_vals_[4][row_num];
  int indices[4][row_num];
  int pk_ = 6;
  RetinaServiceImpl* service;

  std::string schema_name_ = "s0";
  std::string table_name_ = "t0";
};

TEST_F(RetinaTest, TEST) {
  auto mem_tmp = service->flush_mem();
  for (int k = 0; k < 4; k++) {
    auto long_vals_k = long_vals_[k];
    auto str_vals_k = str_vals_[k];
    auto ver_vals_k = ver_vals_[k];

    for (int i = 0; i < row_num; i++) {
      std::vector<Slice> values =
          make_tuple(long_vals_k[i], 5, str_vals_k[i], 2);
      service->Insert(schema_name_, table_name_, pk_, values, cols,
                      ver_vals_k[i]);
    }

    sleep(1);

    // read col num
    long pos = 0;
    int col_num = mem_tmp->getInt(pos);
    pos += sizeof(int);

    // read row num
    int row_num = mem_tmp->getInt(pos);
    pos += sizeof(int);

    // long cols
    std::cout << "start to test long ..." << std::endl;
    long long_vals_tmp[5][row_num];
    for (int i = 0; i < 5; i++) {
      ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
      ASSERT_EQ(type, ValueType::LONG);
      pos += sizeof(short);

      int len = mem_tmp->getInt(pos);
      ASSERT_EQ(len / sizeof(long), row_num);
      pos += sizeof(int);

      mem_tmp->getBytes(pos, (stm::byte*)long_vals_tmp[i], 0, len);
      pos += len;

      for (int j = 0; j < len / sizeof(long); j++) {
        ASSERT_EQ(long_vals_tmp[i][j], long_vals_k[indices[k][j]][i]);
      }
    }
    std::cout << "start to test strs..." << std::endl;
    // str cols
    std::string str_vals_tmp[2][row_num];
    for (int i = 0; i < 2; i++) {
      ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
      ASSERT_EQ(type, ValueType::BYTES);
      pos += sizeof(short);
      std::cout << "=" << std::endl;
      int len = mem_tmp->getInt(pos);
      ASSERT_EQ(len / sizeof(int), row_num);
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
        ASSERT_EQ(str_vals_tmp[i][j], str_vals_k[indices[k][j]][i]);
      }
    }
    std::cout << "start to test version..." << std::endl;
    // version col
    long ver_vals_tmp[row_num];

    ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
    ASSERT_EQ(type, ValueType::LONG);
    pos += sizeof(short);

    int len = mem_tmp->getInt(pos);
    ASSERT_EQ(len / sizeof(long), row_num);
    pos += sizeof(int);

    mem_tmp->getBytes(pos, (stm::byte*)ver_vals_tmp, 0, len);
    pos += len;

    for (int j = 0; j < len / sizeof(long); j++) {
      ASSERT_EQ(ver_vals_tmp[j], ver_vals_k[indices[k][j]]);
    }

    service->ReleaseFlushMem(0);
  }

  int timeshift = 4096;
  for (int k = 0; k < 4; k++) {
    auto long_vals_k = long_vals_[k];
    auto str_vals_k = str_vals_[k];
    auto ver_vals_k = ver_vals_[k];

    for (int i = 0; i < row_num; i++) {
      std::vector<Slice> values =
          make_tuple(long_vals_k[i], 5, str_vals_k[i], 2);
      service->Update(schema_name_, table_name_, pk_, values,
                      ver_vals_k[i] + timeshift);
    }

    sleep(1);

    // read col num
    long pos = 0;
    int col_num = mem_tmp->getInt(pos);
    pos += sizeof(int);

    // read row num
    int row_num = mem_tmp->getInt(pos);
    pos += sizeof(int);

    // long cols
    std::cout << "start to test long ..." << std::endl;
    long long_vals_tmp[5][row_num];
    for (int i = 0; i < 5; i++) {
      ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
      ASSERT_EQ(type, ValueType::LONG);
      pos += sizeof(short);

      int len = mem_tmp->getInt(pos);
      ASSERT_EQ(len / sizeof(long), row_num);
      pos += sizeof(int);

      mem_tmp->getBytes(pos, (stm::byte*)long_vals_tmp[i], 0, len);
      pos += len;

      for (int j = 0; j < len / sizeof(long); j++) {
        ASSERT_EQ(long_vals_tmp[i][j], long_vals_k[indices[k][j]][i]);
      }
    }
    std::cout << "start to test strs..." << std::endl;
    // str cols
    std::string str_vals_tmp[2][row_num];
    for (int i = 0; i < 2; i++) {
      ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
      ASSERT_EQ(type, ValueType::BYTES);
      pos += sizeof(short);
      std::cout << "=" << std::endl;
      int len = mem_tmp->getInt(pos);
      ASSERT_EQ(len / sizeof(int), row_num);
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

        ASSERT_EQ(str_vals_tmp[i][j], str_vals_k[indices[k][j]][i]);
      }
    }
    std::cout << "start to test version..." << std::endl;
    // version col
    long ver_vals_tmp[row_num];

    ValueType type = static_cast<ValueType>(mem_tmp->getShort(pos));
    ASSERT_EQ(type, ValueType::LONG);
    pos += sizeof(short);

    int len = mem_tmp->getInt(pos);
    ASSERT_EQ(len / sizeof(long), row_num);
    pos += sizeof(int);

    mem_tmp->getBytes(pos, (stm::byte*)ver_vals_tmp, 0, len);
    pos += len;

    for (int j = 0; j < len / sizeof(long); j++) {
      ASSERT_EQ(ver_vals_tmp[j], ver_vals_k[indices[k][j]] + timeshift);
    }

    service->ReleaseFlushMem(0);
  }

  ASSERT_EQ(VersionManager::vms_.size(), 8);
  auto version_mem = service->version_mem();
  int query_ts = 2 * row_num + timeshift - 1;
  uint64_t visibility[] = {0,          0,          UINT64_MAX, UINT64_MAX,
                           UINT64_MAX, UINT64_MAX, UINT64_MAX, UINT64_MAX};

  for (int k = 0; k < 8; k++) {
    std::cout << k << std::endl;
    long pos = 0;
    service->QueryVisibility(schema_name_, table_name_, k, query_ts, pos);
    long pos_cp = pos;
    int len = version_mem->getInt(pos);
    pos += sizeof(int);
    uint64_t buf[len / sizeof(uint64_t)];
    version_mem->getBytes(pos, (stm::byte*)buf, 0, len);
    service->ReleaseVersionMem(pos_cp);

    for (int i = 0; i < row_num; i++) {
      ASSERT_EQ((buf[i / 64] >> (i % 64)) & 1ull,
                (visibility[k] >> (i % sizeof(uint64_t))) & 1ull);
    }
  }
}
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
#include "index.h"

#include <gtest/gtest.h>

#include <iostream>

/**
 * @author mzp0514
 * @date 27/05/2022
 */

TEST(IndexTest, TEST) {
  Index* index = new Index();
  long buf[24];
  buf[0] = 0;
  buf[1] = 1;
  buf[2] = 2;
  rocksdb::Slice key0((char*)buf, sizeof(long));
  std::string file_path = "s0:t1";
  int rgid = 0, rid = 0;
  index->put(key0, file_path, rgid, rid);

  rocksdb::Slice key1((char*)(buf + 1), sizeof(long));
  rgid = 1, rid = 2;
  index->put(key1, file_path, rgid, rid);

  rocksdb::Slice key2((char*)(buf + 2), sizeof(long));
  rgid = 1, rid = 3;
  index->put(key2, file_path, rgid, rid);

  ASSERT_EQ(index->get(key0), "s0:t1:0:0");
  ASSERT_EQ(index->get(key1), "s0:t1:1:2");
  ASSERT_EQ(index->get(key2), "s0:t1:1:3");

  delete index;
}
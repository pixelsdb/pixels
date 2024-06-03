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
#include <vector>

#include "const.h"
#include "log4cxx/logger.h"
#include "vector/binary_column_vector.h"
#include "vector/long_column_vector.h"

/**
 * @author mzp0514
 * @date 27/05/2022
 */
class VectorizedRowBatch {
 public:
  VectorizedRowBatch(int rgid, std::vector<ValueType>& columns,
                     int max_row_num) {
    rgid_ = rgid;
    num_cols_ = columns.size() + 1;
    cols_ =
        std::vector<ColumnVector*>(num_cols_, nullptr);  // add version column
    for (int i = 0; i < num_cols_ - 1; i++) {
      switch (columns[i]) {
        case ValueType::LONG:
          cols_[i] = new LongColumnVector(max_row_num);
          break;

        case ValueType::BYTES:
          cols_[i] = new BinaryColumnVector(max_row_num);
          break;

        default:
          cols_[i] = new LongColumnVector(max_row_num);
          break;
      }
    }

    cols_[num_cols_ - 1] = new LongColumnVector(MAX_BUFFER_SIZE);
    cur_length_ = 0;
  }

  void Append(int index, std::vector<Slice> values, int pk, long ts) {
    assert(values.size() == num_cols_ - 1);
    std::cout << "append row: ";
    for (int i = 0; i < values.size(); i++) {
      cols_[i]->Add(index, values[i]);
      if (cols_[i]->value_type() == ValueType::BYTES) {
        std::cout << values[i].ToString() << " " << values[i].size() << " ";
      } else if (cols_[i]->value_type() == ValueType::LONG) {
        std::cout << *(long*)values[i].data() << " ";
      }
    }
    std::cout << std::endl;
    Slice version_val = Slice((char*)&ts, sizeof(long));
    cols_[num_cols_ - 1]->Add(index, version_val);
    cur_length_ = std::max(index + 1, cur_length_);
  }

  void SetDeleteTimestamp(int rid, long ts) { delete_timestamps_[rid] = ts; }

  void FilteredFlush(std::shared_ptr<stm::SharedMemory> mem, long pos,
                     long ts) {
    LOG4CXX_DEBUG(logger, "start to filtered flush...");
    // filter out invisible rows
    long* version_col = (long*)cols_[num_cols_ - 1]->buffer();
    int len = 0;
    for (int i = 0; i < cur_length_; i++) {
      if (version_col[i] <= ts &&
          (!delete_timestamps_[i] || ts < delete_timestamps_[i])) {
        indices_[len++] = i;
      }
    }
    mem->setInt(pos, num_cols_);  // col number
    pos += sizeof(int);

    mem->setInt(pos, len);  // row number
    pos += sizeof(int);

    for (auto col : cols_) {
      col->FilteredFlush(mem, pos, indices_, len);
    }
  }

  void SortedFlush(std::shared_ptr<stm::SharedMemory> mem, long pos) {
    LOG4CXX_DEBUG(logger, "start to sorted flush...");
    std::cout << "start to sorted flush..." << std::endl;
    mem->setInt(pos, num_cols_);  // col number
    pos += sizeof(int);

    mem->setInt(pos, cur_length_);  // row number
    pos += sizeof(int);

    for (auto col : cols_) {
      col->SortedFlush(mem, pos, indices_);
    }

    std::cout << "sorted flush finished" << std::endl;
  }

  long* delete_timestamps() { return delete_timestamps_; }

  int* indices() { return indices_; }

  int rgid() const { return rgid_; }

  bool to_flush() const { return to_flush_; }

  int query_ref_cnt() const { return query_ref_cnt_; }

  void set_to_flush(bool to_flush) { to_flush_ = to_flush; }

  void IncrementQueryRef() { query_ref_cnt_++; }

  void DecrementQueryRef() { query_ref_cnt_--; }

 private:
  int rgid_;
  int num_cols_;                     // number of columns
  std::vector<ColumnVector*> cols_;  // a vector for each column
  int cur_length_;

  int indices_[MAX_BUFFER_SIZE];  // sorted/needed indices

  long delete_timestamps_[MAX_BUFFER_SIZE];

  bool to_flush_ = false;
  std::atomic<int> query_ref_cnt_ = 0;

  static log4cxx::LoggerPtr logger;
};

log4cxx::LoggerPtr VectorizedRowBatch::logger(
    log4cxx::Logger::getLogger("VectorizedRowBatch"));

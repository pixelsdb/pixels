/*
 * Copyright 2023 PixelsDB.
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

/*
 * @author liyu
 * @create 2023-03-15
 */
#ifndef PIXELS_PIXELSREADEROPTION_H
#define PIXELS_PIXELSREADEROPTION_H

#include <iostream>
#include <string>
#include <vector>
#include "duckdb/planner/table_filter.hpp"

class PixelsReaderOption
{
 public:
  PixelsReaderOption();

  void setIncludeCols(const std::vector<std::string> &columnNames);

  std::vector<std::string> getIncludedCols();

  void setSkipCorruptRecords(bool s);

  bool isSkipCorruptRecords();

  void setQueryId(long qId);

  void setBatchSize(int batchSize);

  void setEnabledFilterPushDown(bool enabledFilterPushDown);

  bool isEnabledFilterPushDown();

  long getQueryId();

  void setRGRange(int start, int len);

  void setFilter(duckdb::TableFilterSet *filter);

  duckdb::TableFilterSet *getFilter();

  int getRGStart();

  int getRGLen();

  int getBatchSize() const;

  void setTolerantSchemaEvolution(bool t);

  bool isTolerantSchemaEvolution();

  void setEnableEncodedColumnVector(bool enabled);

  bool isEnableEncodedColumnVector();

 private:
  std::vector<std::string> includedCols;
  duckdb::TableFilterSet *filter;
  // TODO: pixelsPredicate
  bool skipCorruptRecords;
  bool tolerantSchemaEvolution;     // this may lead to column missing due to schema evolution
  bool enableEncodedColumnVector;   // whether read encoded column vectors directly when possible
  bool enableFilterPushDown;        // if filter pushDown is enabled
  long queryId;
  int batchSize;
  int rgStart;
  int rgLen;
};
#endif //PIXELS_PIXELSREADEROPTION_H

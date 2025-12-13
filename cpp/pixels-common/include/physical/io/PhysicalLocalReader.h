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
 * @create 2023-02-27
 */
#ifndef PIXELS_READER_PHYSICALLOCALREADER_H
#define PIXELS_READER_PHYSICALLOCALREADER_H

#include "physical/PhysicalReader.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/natives/DirectUringRandomAccessFile.h"
#include "physical/storage/LocalFS.h"
#include <atomic>
#include <iostream>
#include <unordered_set>

class PhysicalLocalReader : public PhysicalReader
{
public:
  PhysicalLocalReader(std::shared_ptr <Storage> storage, std::string path);

  std::shared_ptr <ByteBuffer> readFully(int length) override;

  std::shared_ptr <ByteBuffer> readFully(int length, std::shared_ptr <ByteBuffer> bb) override;

  std::shared_ptr <ByteBuffer> readAsync(int length, std::shared_ptr <ByteBuffer> bb, int index,int ringIndex,int startOffset);

  void readAsyncSubmit(std::unordered_map<int,uint32_t> sizes,std::unordered_set<int> ringIndex);

  void readAsyncComplete(std::unordered_map<int,uint32_t> sizes,std::unordered_set<int> ringIndex);

  void readAsyncSubmitAndComplete(uint32_t size,std::unordered_set<int> ringIndex);

  void close() override;

  long getFileLength() override;

  void seek(long desired) override;

  long readLong() override;

  int readInt() override;

  char readChar() override;

  std::string getName() override;

  void addRingIndex(int ringIndex);

  std::unordered_set<int>& getRingIndexes();

  std::unordered_map<int,uint32_t> getRingIndexCountMap();

  void setRingIndexCountMap(std::unordered_map<int,uint32_t> ringIndexCount);

private:
  std::shared_ptr <LocalFS> local;
  std::string path;
  long id;
  std::atomic<int> numRequests;
  std::atomic<int> asyncNumRequests;
  std::shared_ptr <PixelsRandomAccessFile> raf;
  // usr for bufferpool
  bool isBufferValid;
  std::unordered_set<std::shared_ptr<PixelsRandomAccessFile>> ringIndexes;
  std::unordered_set<int> ring_index_vector;
  std::unordered_map<int, uint32_t> ringIndexCountMap;
};

#endif //PIXELS_READER_PHYSICALLOCALREADER_H

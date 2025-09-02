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
 * @create 2023-05-25
 */
#ifndef DUCKDB_BUFFERPOOL_H
#define DUCKDB_BUFFERPOOL_H

#include <iostream>
#include <vector>
#include "physical/natives/ByteBuffer.h"
#include <memory>
#include "physical/natives/DirectIoLib.h"
#include "exception/InvalidArgumentException.h"
#include "utils/ColumnSizeCSVReader.h"
#include <map>

// when allocating buffer pool, we use the size of the first pxl file. Consider that
// the remaining pxl file has larger size than the first file, we allocate some extra
// size (10MB) to each column.
// TODO: how to evaluate the maximal pool size
#define EXTRA_POOL_SIZE 3*1024*1024

class DirectUringRandomAccessFile;
// This class is global class. The variable is shared by each thread
class BufferPool
{
public:
    static void
    Initialize(std::vector <uint32_t> colIds, std::vector <uint64_t> bytes, std::vector <std::string> columnNames);

    static std::shared_ptr <ByteBuffer> GetBuffer(uint32_t colId);

    static int64_t GetBufferId(uint32_t index);

    static void Switch();

    static void Reset();

private:
    BufferPool() = default;

    static thread_local int colCount;
    static thread_local std::map<uint32_t, uint64_t>
    nrBytes;
    static thread_local bool isInitialized;
    static thread_local std::map<uint32_t, std::shared_ptr < ByteBuffer>>
    buffers[2];
    static thread_local std::shared_ptr <DirectIoLib> directIoLib;
    static thread_local int currBufferIdx;
    static thread_local int nextBufferIdx;
    friend class DirectUringRandomAccessFile;
};
#endif // DUCKDB_BUFFERPOOL_H

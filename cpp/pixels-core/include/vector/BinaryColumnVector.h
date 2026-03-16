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
 * @create 2023-03-17
 */
#ifndef PIXELS_BINARYCOLUMNVECTOR_H
#define PIXELS_BINARYCOLUMNVECTOR_H

#include "vector/ColumnVector.h"
#include "vector/VectorizedRowBatch.h"
#include "PixelsTypes.h" 
#include <string>
#include <vector>

/**
 * BinaryColumnVector 
 * 作用：存储字符串或二进制数据的引用。原本项目同时保存duckdb的string和std的string，有点和稀泥，这里先改为仅保存std的string
 * 后续要实现一个类似duckdb的string_t结构体，包含指针和长度，来避免std::string的内存分配和复制开销。
 */
// BinaryColumnVector.h

class BinaryColumnVector : public ColumnVector
{
public:
    BinaryColumnVector(uint64_t len, bool encoding);
    ~BinaryColumnVector();

    void close() override;

    void setRef(int elementNum, uint8_t *const &sourceBuf, int start, int length);
    void setVal(int elementNum, uint8_t *sourceBuf, int start, int length);
    void ensureSize(uint64_t size, bool preserveData) override;
    void add(std::string &value);
    void add(uint8_t *v, int len);
    void *current() override;
    const std::string &getValue(idx_t i) const;
    bool isNullAt(idx_t i) const;
    std::vector<std::string> str_vec;   // 唯一字符串存储
};

#endif //PIXELS_BINARYCOLUMNVECTOR_H

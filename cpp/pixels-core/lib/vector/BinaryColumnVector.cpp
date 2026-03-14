#include "vector/BinaryColumnVector.h"
#include <cstdlib> // 用于 posix_memalign 和 free
#include <cstring> // 用于 memcpy

// 1. 构造函数修改：分配String 空间
BinaryColumnVector::BinaryColumnVector(uint64_t len, bool encoding)
    : ColumnVector(len, encoding)
{
    str_vec.resize(len);
    memoryUsage += sizeof(std::string) * len;
}

void BinaryColumnVector::close()
{
    if (!closed)
    {
        ColumnVector::close();
        str_vec.clear();
        str_vec.shrink_to_fit();
        closed = true;
    }
}

// 2. setRef 修改：直接填充指针和长度
void BinaryColumnVector::setRef(int elementNum,
                                uint8_t *const &sourceBuf,
                                int start,
                                int length)
{
    if (elementNum >= (int)this->length)
    {
        ensureSize(elementNum + 1, true);
    }

    if (elementNum >= writeIndex)
    {
        writeIndex = elementNum + 1;
    }

    str_vec[elementNum] = std::string(
        reinterpret_cast<char *>(sourceBuf + start),
        length);

    isNull[elementNum] = false;
}

// 3. setVal 修改
void BinaryColumnVector::setVal(int elementNum,
                                uint8_t *sourceBuf,
                                int start,
                                int length)
{
    str_vec[elementNum] = std::string(
        reinterpret_cast<char *>(sourceBuf + start),
        length);

    isNull[elementNum] = false;
}

// 4. ensureSize 修改：扩容逻辑适配
void BinaryColumnVector::ensureSize(uint64_t size, bool preserveData)
{
    if (length >= size)
        return;

    if (preserveData)
    {
        str_vec.resize(size);
    }
    else
    {
        std::vector<std::string> new_vec(size);
        str_vec.swap(new_vec);
    }

    memoryUsage += sizeof(std::string) * (size - length);

    resize(size);  // 更新基类 length
}

// --- 以下函数保持原有逻辑，已自动适配 PixelsString ---

BinaryColumnVector::~BinaryColumnVector()
{
    if (!closed)
    {
        BinaryColumnVector::close();
    }
}

void *BinaryColumnVector::current()
{
    if (readIndex >= str_vec.size())
        return nullptr;

    return (void *)&str_vec[readIndex];
}

void BinaryColumnVector::add(std::string &value)
{
    if (writeIndex >= (int)length)
    {
        ensureSize(writeIndex == 0 ? 1 : writeIndex * 2, true);
    }

    str_vec[writeIndex++] = value;
}

void BinaryColumnVector::add(uint8_t *v, int len)
{
    if (writeIndex >= (int)length)
    {
        ensureSize(writeIndex == 0 ? 1 : writeIndex * 2, true);
    }

    str_vec[writeIndex++] =
        std::string(reinterpret_cast<char *>(v), len);
}

const std::string &BinaryColumnVector::getValue(idx_t i) const
{
    return str_vec[i];
}

bool  BinaryColumnVector::isNullAt(idx_t i) const
{
    return isNull[i];
}
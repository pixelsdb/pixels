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
 * @create 2023-04-19
 */
#include "physical/natives/DirectIoLib.h"


DirectIoLib::DirectIoLib(int fsBlockSize)
{
    this->fsBlockSize = fsBlockSize;
    this->fsBlockNotMask = ~((long) fsBlockSize - 1);
}

std::shared_ptr <ByteBuffer> DirectIoLib::allocateDirectBuffer(long size)
{
    int toAllocate = blockEnd(size) + (size == 1 ? 0 : fsBlockSize);
    uint8_t *directBufferPointer;
    posix_memalign((void **) &directBufferPointer, fsBlockSize, toAllocate);
    auto directBuffer = std::make_shared<ByteBuffer>(directBufferPointer, toAllocate, false);
    return directBuffer;
}

std::shared_ptr <ByteBuffer> DirectIoLib::read(int fd, long fileOffset,
                                               std::shared_ptr <ByteBuffer> directBuffer, long length)
{
    // the file will be read from blockStart(fileOffset), and the first fileDelta bytes should be ignored.
    long fileOffsetAligned = blockStart(fileOffset);
    long toRead = blockEnd(fileOffset + length) - blockStart(fileOffset);
    ssize_t ret = pread(fd, directBuffer->getPointer(), toRead, fileOffsetAligned);
    if (ret == -1) {
        perror("pread failed");
        throw InvalidArgumentException("DirectIoLib::read: pread fail. ");
    }
    auto bb = std::make_shared<ByteBuffer>(*directBuffer,
                                           fileOffset - fileOffsetAligned, length);
    return bb;
}


long DirectIoLib::blockStart(long value)
{
    return (value & fsBlockNotMask);
}

long DirectIoLib::blockEnd(long value)
{
    return (value + fsBlockSize - 1) & fsBlockNotMask;
}


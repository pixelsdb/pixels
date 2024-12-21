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
 * @create 2023-03-02
 */
#ifndef PIXELS_DIRECTRANDOMACCESSFILE_H
#define PIXELS_DIRECTRANDOMACCESSFILE_H

#include "physical/natives/PixelsRandomAccessFile.h"
#include "physical/natives/ByteBuffer.h"
#include "physical/natives/DirectIoLib.h"
#include <fcntl.h>
#include <unistd.h>
#include "profiler/TimeProfiler.h"
#include "physical/allocator/OrdinaryAllocator.h"

class DirectRandomAccessFile : public PixelsRandomAccessFile
{
public:
    explicit DirectRandomAccessFile(const std::string &file);

    void close() override;

    std::shared_ptr <ByteBuffer> readFully(int len) override;

    std::shared_ptr <ByteBuffer> readFully(int len, std::shared_ptr <ByteBuffer> bb) override;

    long length() override;

    void seek(long off) override;

    long readLong() override;

    char readChar() override;

    int readInt() override;

private:
    void populatedBuffer();

    std::shared_ptr <Allocator> allocator;
    std::vector <std::shared_ptr<ByteBuffer>> largeBuffers;
    /* smallDirectBuffer align to blockSize. smallBuffer adds the offset to smallDirectBuffer. */
    std::shared_ptr <ByteBuffer> smallBuffer;
    std::shared_ptr <ByteBuffer> smallDirectBuffer;
    bool bufferValid;
    long len;
protected:
    int fd;
    long offset;
    std::shared_ptr <DirectIoLib> directIoLib;
    bool enableDirect;
    int fsBlockSize;
};
#endif //PIXELS_DIRECTRANDOMACCESSFILE_H

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
#ifndef DUCKDB_DIRECTIOLIB_H
#define DUCKDB_DIRECTIOLIB_H

/**
 * Mapping Linux I/O functions to native methods.
 * Partially referenced the implementation of Jaydio (https://github.com/smacke/jaydio),
 * which is implemented by Stephen Macke and licensed under Apache 2.0.
 * <p>
 * Created at: 02/02/2023
 * Author: Liangyong Yu
 */

#include "utils/ConfigFactory.h"
#include "physical/natives/ByteBuffer.h"
#include <iostream>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include "liburing.h"
#include "liburing/io_uring.h"


struct uringData
{
    int idx;
    ByteBuffer *bb;
};


class DirectIoLib
{
public:
    /**
     * the start address/size of direct buffer is the multiple of block Size
     */
    DirectIoLib(int fsBlockSize);

    std::shared_ptr <ByteBuffer> allocateDirectBuffer(long size);

    std::shared_ptr <ByteBuffer> read(int fd, long fileOffset, std::shared_ptr <ByteBuffer> directBuffer, long length);

    long blockStart(long value);

    long blockEnd(long value);

    int getBlockSize() const {
        return fsBlockSize;
    };

private:
    int fsBlockSize;
    long fsBlockNotMask;
};

#endif // DUCKDB_DIRECTIOLIB_H

//
// Created by yuly on 19.04.23.
//

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


struct uringData {
	int idx;
	ByteBuffer * bb;
};


class DirectIoLib {
public:
	/**
     * the start address/size of direct buffer is the multiple of block Size
	 */
	DirectIoLib(int fsBlockSize);
	std::shared_ptr<ByteBuffer> allocateDirectBuffer(long size);
	std::shared_ptr<ByteBuffer> read(int fd, long fileOffset, std::shared_ptr<ByteBuffer> directBuffer, long length);
	long blockStart(long value);
	long blockEnd(long value);
private:
	int fsBlockSize;
	long fsBlockNotMask;
};

#endif // DUCKDB_DIRECTIOLIB_H

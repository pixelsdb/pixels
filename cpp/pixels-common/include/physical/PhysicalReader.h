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
#ifndef PIXELS_READER_PHYSICALREADER_H
#define PIXELS_READER_PHYSICALREADER_H

#include <string>
#include <iostream>
#include "physical/natives/ByteBuffer.h"
class PhysicalReader {
public:
    virtual long getFileLength() = 0;
    virtual void seek(long desired) = 0;
    virtual std::shared_ptr<ByteBuffer> readFully(int length) = 0;
	virtual std::shared_ptr<ByteBuffer> readFully(int length, std::shared_ptr<ByteBuffer> bb) = 0;
//    virtual void readFully(char * buffer) = 0;
//    virtual void readFully(char * buffer, int offset, int length) = 0;
    virtual std::string getName() = 0;
    /**
     * If direct I/O is supported, {@link #readFully(int)} will directly read from the file
     * without going through the OS cache. This is currently supported on LocalFS.
     *
     * @return true if direct read is supported.
     */
    virtual bool supportsDirect() {
        return false;
    }

    /**
     * @return true if readAsync is supported.
     */
    virtual bool supportsAsync() {
        return false;
    }

    /**
     * readAsync does not affect the position of this reader, and is not affected by seek().
     * @param offset
     * @param length
     * @return
     * @throws IOException
     */
     // TODO: default CompletableFuture<ByteBuffer> readAsync(long offset, int length) throws IOException

    virtual long readLong() = 0;
    virtual int readInt() = 0;
    virtual char readChar() = 0;
//    virtual int readInt() = 0;
    virtual void close() = 0;

//    virtual std::string getPath() = 0;

    /**
    * Get the last domain in path.
    * @return
    */
//    virtual std::string getName() = 0;

    /**
     * For a file or object in the storage, it may have one or more
     * blocks. Each block has its unique id. This method returns the
     * block id of the current block that is been reading.
     *
     * For local fs, each file has only one block id, which is also
     * the file id.
     *
     * <p>Note: Storage.getFileId() assumes that each file or object
     * only has one block. In this case, the file id is the same as
     * the block id here.</p>
     * @return
     * @throws IOException
     */
//    virtual long getBlockId() = 0;

    /**
     * @return the scheme of the backed physical storage.
     */
    // TODO: Storage.Scheme getStorageScheme();

    /**
     * @return the number of read requests sent to the storage.
     */
//    virtual int getNumReadRequests() = 0;
};

#endif //PIXELS_READER_PHYSICALREADER_H

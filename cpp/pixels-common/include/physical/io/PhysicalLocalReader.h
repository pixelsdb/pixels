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
#include "physical/storage/LocalFS.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "physical/natives/DirectUringRandomAccessFile.h"
#include <iostream>
#include <atomic>


class PhysicalLocalReader: public PhysicalReader {
public:
    PhysicalLocalReader(std::shared_ptr<Storage> storage, std::string path);
    std::shared_ptr<ByteBuffer> readFully(int length) override;
	std::shared_ptr<ByteBuffer> readFully(int length, std::shared_ptr<ByteBuffer> bb) override;
	std::shared_ptr<ByteBuffer> readAsync(int length, std::shared_ptr<ByteBuffer> bb, int index);
	void readAsyncSubmit(uint32_t size);
	void readAsyncComplete(uint32_t size);
	void readAsyncSubmitAndComplete(uint32_t size);
    void close() override;
    long getFileLength() override;
    void seek(long desired) override;
    long readLong() override;
    int readInt() override;
    char readChar() override;
    std::string getName() override;
private:
    std::shared_ptr<LocalFS> local;
    std::string path;
    long id;
    std::atomic<int> numRequests;
	std::atomic<int> asyncNumRequests;
	std::shared_ptr<PixelsRandomAccessFile> raf;

};

#endif //PIXELS_READER_PHYSICALLOCALREADER_H

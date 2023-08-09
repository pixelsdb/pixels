//
// Created by liyu on 2/27/23.
//

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

//
// Created by yuliangyong on 2023-03-02.
//

#ifndef PIXELS_DIRECTRANDOMACCESSFILE_H
#define PIXELS_DIRECTRANDOMACCESSFILE_H

#include "physical/natives/PixelsRandomAccessFile.h"
#include "physical/natives/ByteBuffer.h"
#include "physical/natives/DirectIoLib.h"
#include <fcntl.h>
#include <unistd.h>
#include "profiler/TimeProfiler.h"
#include "physical/allocator/OrdinaryAllocator.h"

class DirectRandomAccessFile: public PixelsRandomAccessFile {
public:
    explicit DirectRandomAccessFile(const std::string& file);
    void close() override;
    std::shared_ptr<ByteBuffer> readFully(int len) override;
	std::shared_ptr<ByteBuffer> readFully(int len, std::shared_ptr<ByteBuffer> bb) override;
    long length() override;
    void seek(long off) override;
    long readLong() override;
    char readChar() override;
    int readInt() override;
private:
    void populatedBuffer();
	std::shared_ptr<Allocator> allocator;
    std::vector<std::shared_ptr<ByteBuffer>> largeBuffers;
	/* smallDirectBuffer align to blockSize. smallBuffer adds the offset to smallDirectBuffer. */
    std::shared_ptr<ByteBuffer> smallBuffer;
	std::shared_ptr<ByteBuffer> smallDirectBuffer;
    bool bufferValid;
	long len;
protected:
	int fd;
	long offset;
	std::shared_ptr<DirectIoLib> directIoLib;
	bool enableDirect;
	int fsBlockSize;
};
#endif //PIXELS_DIRECTRANDOMACCESSFILE_H

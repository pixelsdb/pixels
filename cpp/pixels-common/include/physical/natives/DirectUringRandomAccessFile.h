//
// Created by liyu on 5/28/23.
//

#ifndef DUCKDB_DIRECTURINGRANDOMACCESSFILE_H
#define DUCKDB_DIRECTURINGRANDOMACCESSFILE_H

#include "liburing.h"
#include "liburing/io_uring.h"
#include "physical/natives/DirectRandomAccessFile.h"
#include "exception/InvalidArgumentException.h"
#include "DirectIoLib.h"
#include "physical/BufferPool.h"
class DirectUringRandomAccessFile: public DirectRandomAccessFile {
public:
	explicit DirectUringRandomAccessFile(const std::string& file);
	static void RegisterBuffer(std::vector<std::shared_ptr<ByteBuffer>> buffers);
    static void RegisterBufferFromPool(std::vector<uint32_t> colIds);
	static void Initialize();
	static void Reset();
	std::shared_ptr<ByteBuffer> readAsync(int length, std::shared_ptr<ByteBuffer> buffer, int index);
	void readAsyncSubmit(int size);
	void readAsyncComplete(int size);
	~DirectUringRandomAccessFile();
private:
	static thread_local struct io_uring * ring;
	static thread_local bool isRegistered;
	static thread_local struct iovec * iovecs;
	static thread_local uint32_t iovecSize;
};
#endif // DUCKDB_DIRECTURINGRANDOMACCESSFILE_H

//
// Created by liyu on 5/21/23.
//

#ifndef DUCKDB_BUFFERPOOLALLOCATOR_H
#define DUCKDB_BUFFERPOOLALLOCATOR_H

#include "Allocator.h"

class BufferPoolAllocator: public Allocator {
public:
	BufferPoolAllocator();
	~BufferPoolAllocator();
	std::shared_ptr<ByteBuffer> allocate(int size) override;
	void reset() override;
private:
	long maxSize;
	std::shared_ptr<ByteBuffer> buffer;
};
#endif // DUCKDB_BUFFERPOOLALLOCATOR_H

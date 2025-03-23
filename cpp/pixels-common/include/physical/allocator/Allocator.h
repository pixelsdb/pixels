//
// Created by liyu on 5/21/23.
//

#ifndef DUCKDB_ALLOCATOR_H
#define DUCKDB_ALLOCATOR_H

#include <iostream>
#include <memory>
#include <physical/natives/ByteBuffer.h>

class Allocator {
public:
	virtual void reset() = 0;
	virtual std::shared_ptr<ByteBuffer> allocate(int size) = 0;
};
#endif // DUCKDB_ALLOCATOR_H

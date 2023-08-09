//
// Created by liyu on 5/21/23.
//

#ifndef DUCKDB_ORDINARYALLOCATOR_H
#define DUCKDB_ORDINARYALLOCATOR_H

#include "physical/allocator/Allocator.h"

class OrdinaryAllocator: public Allocator {
public:
	OrdinaryAllocator() = default;
	std::shared_ptr<ByteBuffer> allocate(int size) override;
	void reset() override {};
};
#endif // DUCKDB_ORDINARYALLOCATOR_H

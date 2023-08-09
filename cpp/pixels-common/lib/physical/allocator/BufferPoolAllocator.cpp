//
// Created by liyu on 5/21/23.
//
#include "physical/allocator/BufferPoolAllocator.h"

void BufferPoolAllocator::reset() {
	buffer->resetPosition();
}

BufferPoolAllocator::BufferPoolAllocator() {
	// 100M. This value is a temporary value
	maxSize = 100 * 1024 * 1024;
	buffer = std::make_shared<ByteBuffer>(maxSize);

}

std::shared_ptr<ByteBuffer> BufferPoolAllocator::allocate(int size) {
	auto bb = std::make_shared<ByteBuffer>(*buffer, buffer->getReadPos(), size);
	int roundSize;
	int remainder = size % 4096;
	if(remainder != 0) {
		roundSize = size + 4096 - remainder;
	} else {
		roundSize = size;
	}
	buffer->setReadPos(buffer->getReadPos() + roundSize);
	return bb;
}

BufferPoolAllocator::~BufferPoolAllocator() {
}

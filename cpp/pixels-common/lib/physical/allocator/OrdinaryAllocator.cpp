//
// Created by liyu on 5/21/23.
//
#include "physical/allocator/OrdinaryAllocator.h"


std::shared_ptr<ByteBuffer> OrdinaryAllocator::allocate(int size) {
	auto * buffer = new uint8_t[size];
	auto bb = std::make_shared<ByteBuffer>(buffer, static_cast<uint32_t>(size));
	return bb;
}

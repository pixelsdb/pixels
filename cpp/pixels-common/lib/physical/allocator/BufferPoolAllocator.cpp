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
 * @create 2023-05-21
 */
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

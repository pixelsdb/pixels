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

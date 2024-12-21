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
 * @create 2023-03-07
 */
#ifndef PIXELS_SCHEDULER_H
#define PIXELS_SCHEDULER_H
#include "physical/PhysicalReader.h"
#include "physical/RequestBatch.h"
#include "profiler/TimeProfiler.h"

class Scheduler {
public:
    /**
      * Execute a batch of read requests, and return the future of the completion of
      * all the requests.
      * @param reader
      * @param batch
      * @param queryId
      */
	virtual std::vector<std::shared_ptr<ByteBuffer>> executeBatch(std::shared_ptr<PhysicalReader> reader, RequestBatch batch, long queryId) = 0;
    virtual std::vector<std::shared_ptr<ByteBuffer>> executeBatch(std::shared_ptr<PhysicalReader> reader,
	                                                              RequestBatch batch, std::vector<std::shared_ptr<ByteBuffer>> reuseBuffers, long queryId) = 0;
};
#endif //PIXELS_SCHEDULER_H

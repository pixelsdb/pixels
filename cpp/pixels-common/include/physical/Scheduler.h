//
// Created by liyu on 3/7/23.
//

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

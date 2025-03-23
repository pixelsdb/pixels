//
// Created by yuly on 01.05.23.
//

#ifndef DUCKDB_SORTMERGESCHEDULER_H
#define DUCKDB_SORTMERGESCHEDULER_H

#include "physical/Scheduler.h"
#include "physical/MergedRequest.h"
#include<algorithm>
#include "exception/InvalidArgumentException.h"

class SortMergeScheduler : public Scheduler {
    // TODO: logger
public:
    static Scheduler * Instance();
	std::vector<std::shared_ptr<MergedRequest>> sortMerge(RequestBatch batch, long queryId);
	std::vector<std::shared_ptr<ByteBuffer>> executeBatch(std::shared_ptr<PhysicalReader> reader,
	                                                                          RequestBatch batch, long queryId) override;
	std::vector<std::shared_ptr<ByteBuffer>> executeBatch(std::shared_ptr<PhysicalReader> reader, RequestBatch batch,
	                                                      std::vector<std::shared_ptr<ByteBuffer>> reuseBuffers, long queryId) override;


private:
    SortMergeScheduler();
    static Scheduler * instance;


};

#endif //DUCKDB_SORTMERGESCHEDULER_H

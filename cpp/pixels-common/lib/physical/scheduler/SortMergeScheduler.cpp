//
// Created by yuly on 01.05.23.
//

#include "physical/scheduler/SortMergeScheduler.h"
#include "utils/ConfigFactory.h"
#include "exception/InvalidArgumentException.h"

Scheduler * SortMergeScheduler::instance = nullptr;

Scheduler * SortMergeScheduler::Instance() {
    if(instance == nullptr) {
        instance = new SortMergeScheduler();
    }
    return instance;
}


std::vector<std::shared_ptr<ByteBuffer>> SortMergeScheduler::executeBatch(std::shared_ptr<PhysicalReader> reader,
                                                                     RequestBatch batch, long queryId) {
	return executeBatch(reader, batch, {}, queryId);
}


std::vector<std::shared_ptr<ByteBuffer>> SortMergeScheduler::executeBatch(std::shared_ptr<PhysicalReader> reader, RequestBatch batch,
                                                      std::vector<std::shared_ptr<ByteBuffer>> reuseBuffers, long queryId) {
    if(batch.getSize() < 0) {
        return std::vector<std::shared_ptr<ByteBuffer>>{};
    }
    auto mergeRequests = sortMerge(batch, queryId);
    std::vector<std::shared_ptr<ByteBuffer>> bbs;
    for(auto merged : mergeRequests) {
        reader->seek(merged->getStart());
        auto buffer = reader->readFully(merged->getLength());
        auto separateBuffers = merged->complete(buffer);
        bbs.insert(bbs.end(), separateBuffers.begin(), separateBuffers.end());
    }
    return bbs;
}

SortMergeScheduler::SortMergeScheduler() {

}

std::vector<std::shared_ptr<MergedRequest>> SortMergeScheduler::sortMerge(RequestBatch batch, long queryId) {
    auto requests = batch.getRequests();
    std::sort(requests.begin(), requests.end(), [](const Request& lhs, const Request& rhs) {
        return lhs.start < rhs.start;
    });

    std::vector<std::shared_ptr<MergedRequest>> mergedRequests;
    auto mr1 = std::make_shared<MergedRequest>(requests.at(0));
    auto mr2 = mr1;
    for(int i = 1; i < batch.getSize(); i++) {
        mr2 = mr1->merge(requests.at(i));
        if(mr1 == mr2) {
            continue;
        }
        mergedRequests.emplace_back(mr1);
        mr1 = mr2;
    }
    mergedRequests.emplace_back(mr2);
    return mergedRequests;
}

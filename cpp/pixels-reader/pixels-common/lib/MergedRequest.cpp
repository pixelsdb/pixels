//
// Created by yuly on 01.05.23.
//

#include "physical/MergedRequest.h"

std::shared_ptr<MergedRequest> MergedRequest::merge(Request curr) {
    if (curr.start < this->end)
    {
        throw InvalidArgumentException("MergedRequest: Can not merge backward request.");
    }
    if (curr.queryId != this->queryId)
    {
        throw InvalidArgumentException("MergedRequest: Can not merge requests from different queries (transactions).");
    }
    long gap = curr.start - this->end;
    if(gap <= maxGap && this->length + gap + curr.length <= std::numeric_limits<int>::max()) {
        this->offsets.emplace_back(this->length + (int) gap);
        this->lengths.emplace_back(curr.length);
        this->length += gap + curr.length;
        this->end = curr.start + curr.length;
        this->size++;
        return shared_from_this();
    }
    return std::make_shared<MergedRequest>(curr);
}

MergedRequest::MergedRequest(Request first) {
    this->queryId = first.queryId;
    this->start = first.start;
    this->end = first.start + first.length;
    this->maxGap = std::stoi(ConfigFactory::Instance().getProperty("read.request.merge.gap"));
    this->offsets.emplace_back(0);
    this->lengths.emplace_back(first.length);
    this->length = first.length;
    this->size = 1;
}

// when the data has been read, split the merged buffer to original buffer
std::vector<std::shared_ptr<ByteBuffer>> MergedRequest::complete(std::shared_ptr<ByteBuffer> buffer) {
    std::vector<std::shared_ptr<ByteBuffer>> bbs;
    for(int i = 0; i < this->size; i++) {
        auto bb = std::make_shared<ByteBuffer>(*buffer,
                                               offsets.at(i),
                                               lengths.at(i));
        bbs.emplace_back(bb);
    }
    return bbs;
}

long MergedRequest::getStart() {
    return start;
}

int MergedRequest::getLength() {
    return length;
}

int MergedRequest::getSize() {
    return size;
}

long MergedRequest::getQueryId() {
    return queryId;
}







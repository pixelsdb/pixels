//
// Created by liyu on 3/7/23.
//

#include "physical/RequestBatch.h"

RequestBatch::RequestBatch(int capacity) {
    if(capacity <= 0) {
        throw std::runtime_error("Request batch capacity: " + std::to_string(capacity));
    }
    requests.reserve(capacity);
    size = 0;
}

RequestBatch::RequestBatch() {
    requests = std::vector<Request>();
    size = 0;
}

int RequestBatch::getSize() {
    return size;
}

std::vector<Request> RequestBatch::getRequests() {
    return requests;
}

//std::vector<std::future<ByteBuffer *>> * RequestBatch::getPromises() {}() {
//    return &pro;
//}

void RequestBatch::add(uint64_t queryId, uint64_t start, uint64_t length, int64_t bufferId) {
    Request request = Request(queryId, start, length, bufferId);
    requests.push_back(request);
    size++;
}

void RequestBatch::add(Request request) {
    requests.push_back(request);
    size++;
}

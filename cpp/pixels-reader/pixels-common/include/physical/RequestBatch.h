//
// Created by liyu on 3/7/23.
//

#ifndef PIXELS_REQUESTBATCH_H
#define PIXELS_REQUESTBATCH_H

#include "physical/Request.h"
#include <iostream>
#include <vector>
#include <future>
#include "physical/natives/ByteBuffer.h"

class RequestBatch {
public:
    RequestBatch();
    explicit RequestBatch(int capacity);
    void add(uint64_t queryId, uint64_t start, uint64_t length, int64_t bufferId = -1);
    void add(Request request);
    int getSize();
    std::vector<Request> getRequests();
//    std::vector<std::promise<ByteBuffer *>> * getPromises();
private:
    int size;
    std::vector<Request> requests;
//    std::vector<std::promise<ByteBuffer *>> promises;

};

#endif //PIXELS_REQUESTBATCH_H

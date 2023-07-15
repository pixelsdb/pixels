//
// Created by yuly on 01.05.23.
//

#ifndef DUCKDB_MERGEDREQUEST_H
#define DUCKDB_MERGEDREQUEST_H

#include "physical/Request.h"
#include <iostream>
#include <memory>
#include "exception/InvalidArgumentException.h"
#include "utils/ConfigFactory.h"
#include "physical/natives/ByteBuffer.h"
#include <limits>
#include <vector>

class MergedRequest: public std::enable_shared_from_this<MergedRequest> {
public:
    MergedRequest(Request first);
    std::shared_ptr<MergedRequest> merge(Request curr);
    std::vector<std::shared_ptr<ByteBuffer>> complete(std::shared_ptr<ByteBuffer> buffer);
    long getStart();
    int getLength();
    int getSize();
    long getQueryId();
private:
    long queryId;
    long start;
    long end;
    int length; // the length of merged request
    int size;   // the number of sub-requests
    int maxGap;
    std::vector<int> offsets; // the starting offset of the sub-requests in the response of the merged request
    std::vector<int> lengths; // the length of sub-requests
};
#endif //DUCKDB_MERGEDREQUEST_H

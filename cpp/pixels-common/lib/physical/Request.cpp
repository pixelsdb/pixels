//
// Created by liyu on 3/7/23.
//

#include "physical/Request.h"


Request::Request(uint64_t queryId_, uint64_t start_, uint64_t length_, int64_t bufferId) {
    queryId = queryId_;
    start = start_;
    length = length_;
    this->bufferId = bufferId;
}

int Request::hashCode() {
    return (int) ((start << 32) >> 32);
}

int Request::comparedTo(Request o) {
    return start == o.start;
}

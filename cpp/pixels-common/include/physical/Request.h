//
// Created by liyu on 3/7/23.
//

#ifndef PIXELS_REQUEST_H
#define PIXELS_REQUEST_H

#include <cstdint>
#include <iostream>

class Request {
  public:
    int64_t bufferId;
    uint64_t queryId;
    uint64_t start;
    uint64_t length;
    Request(uint64_t queryId_, uint64_t start_, uint64_t length_,
            int64_t bufferId = -1);
    int hashCode();
    int comparedTo(Request o);
};
#endif // PIXELS_REQUEST_H

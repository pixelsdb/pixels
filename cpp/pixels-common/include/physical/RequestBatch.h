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

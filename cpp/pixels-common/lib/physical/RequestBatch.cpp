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
#include "physical/RequestBatch.h"

RequestBatch::RequestBatch(int capacity)
{
    if (capacity <= 0)
    {
        throw std::runtime_error("Request batch capacity: " + std::to_string(capacity));
    }
    requests.reserve(capacity);
    size = 0;
}

RequestBatch::RequestBatch()
{
    requests = std::vector<Request>();
    size = 0;
}

int RequestBatch::getSize()
{
    return size;
}

std::vector <Request> RequestBatch::getRequests()
{
    return requests;
}

//std::vector<std::future<ByteBuffer *>> * RequestBatch::getPromises() {}() {
//    return &pro;
//}

void RequestBatch::add(uint64_t queryId, uint64_t start, uint64_t length, int64_t bufferId)
{
    Request request = Request(queryId, start, length, bufferId);
    requests.push_back(request);
    size++;
}

void RequestBatch::add(Request request)
{
    requests.push_back(request);
    size++;
}


Request& RequestBatch::getRequest(int index)
{
    if (index < 0 || index >= size) {
        throw std::out_of_range("RequestBatch::getRequest: index out of range");
    }
    return requests[index];
}
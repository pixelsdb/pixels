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
 * @create 2023-05-01
 */
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

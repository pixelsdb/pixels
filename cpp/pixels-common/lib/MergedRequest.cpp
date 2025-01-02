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
#include "physical/MergedRequest.h"

std::shared_ptr <MergedRequest> MergedRequest::merge(Request curr)
{
    if (curr.start < this->end)
    {
        throw InvalidArgumentException("MergedRequest: Can not merge backward request.");
    }
    if (curr.queryId != this->queryId)
    {
        throw InvalidArgumentException("MergedRequest: Can not merge requests from different queries (transactions).");
    }
    long gap = curr.start - this->end;
    if (gap <= maxGap && this->length + gap + curr.length <= std::numeric_limits<int>::max())
    {
        this->offsets.emplace_back(this->length + (int) gap);
        this->lengths.emplace_back(curr.length);
        this->length += gap + curr.length;
        this->end = curr.start + curr.length;
        this->size++;
        return shared_from_this();
    }
    return std::make_shared<MergedRequest>(curr);
}

MergedRequest::MergedRequest(Request first)
{
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
std::vector <std::shared_ptr<ByteBuffer>> MergedRequest::complete(std::shared_ptr <ByteBuffer> buffer)
{
    std::vector <std::shared_ptr<ByteBuffer>> bbs;
    for (int i = 0; i < this->size; i++)
    {
        auto bb = std::make_shared<ByteBuffer>(*buffer,
                                               offsets.at(i),
                                               lengths.at(i));
        bbs.emplace_back(bb);
    }
    return bbs;
}

long MergedRequest::getStart()
{
    return start;
}

int MergedRequest::getLength()
{
    return length;
}

int MergedRequest::getSize()
{
    return size;
}

long MergedRequest::getQueryId()
{
    return queryId;
}







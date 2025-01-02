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
#include "physical/scheduler/SortMergeScheduler.h"
#include "utils/ConfigFactory.h"
#include "exception/InvalidArgumentException.h"

Scheduler *SortMergeScheduler::instance = nullptr;

Scheduler *SortMergeScheduler::Instance()
{
    if (instance == nullptr)
    {
        instance = new SortMergeScheduler();
    }
    return instance;
}


std::vector <std::shared_ptr<ByteBuffer>> SortMergeScheduler::executeBatch(std::shared_ptr <PhysicalReader> reader,
                                                                           RequestBatch batch, long queryId)
{
    return executeBatch(reader, batch, {}, queryId);
}


std::vector <std::shared_ptr<ByteBuffer>>
SortMergeScheduler::executeBatch(std::shared_ptr <PhysicalReader> reader, RequestBatch batch,
                                 std::vector <std::shared_ptr<ByteBuffer>> reuseBuffers, long queryId)
{
    if (batch.getSize() < 0)
    {
        return std::vector < std::shared_ptr < ByteBuffer >> {};
    }
    auto mergeRequests = sortMerge(batch, queryId);
    std::vector <std::shared_ptr<ByteBuffer>> bbs;
    for (auto merged: mergeRequests)
    {
        reader->seek(merged->getStart());
        auto buffer = reader->readFully(merged->getLength());
        auto separateBuffers = merged->complete(buffer);
        bbs.insert(bbs.end(), separateBuffers.begin(), separateBuffers.end());
    }
    return bbs;
}

SortMergeScheduler::SortMergeScheduler()
{

}

std::vector <std::shared_ptr<MergedRequest>> SortMergeScheduler::sortMerge(RequestBatch batch, long queryId)
{
    auto requests = batch.getRequests();
    std::sort(requests.begin(), requests.end(), [](const Request &lhs, const Request &rhs) {
        return lhs.start < rhs.start;
    });

    std::vector <std::shared_ptr<MergedRequest>> mergedRequests;
    auto mr1 = std::make_shared<MergedRequest>(requests.at(0));
    auto mr2 = mr1;
    for (int i = 1; i < batch.getSize(); i++)
    {
        mr2 = mr1->merge(requests.at(i));
        if (mr1 == mr2)
        {
            continue;
        }
        mergedRequests.emplace_back(mr1);
        mr1 = mr2;
    }
    mergedRequests.emplace_back(mr2);
    return mergedRequests;
}

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
 * @create 2023-03-08
 */
#include "physical/scheduler/NoopScheduler.h"
#include "exception/InvalidArgumentException.h"
#include "physical/io/PhysicalLocalReader.h"
#include <unordered_set>

Scheduler* NoopScheduler::instance = nullptr;

Scheduler* NoopScheduler::Instance()
{
    if (instance == nullptr)
    {
        instance = new NoopScheduler();
    }
    return instance;
}

std::vector<std::shared_ptr<ByteBuffer>>
NoopScheduler::executeBatch(std::shared_ptr<PhysicalReader> reader,
                            RequestBatch batch, long queryId)
{
    return executeBatch(reader, batch, {}, queryId);
}

std::vector<std::shared_ptr<ByteBuffer>> NoopScheduler::executeBatch(
    std::shared_ptr<PhysicalReader> reader, RequestBatch batch,
    std::vector<std::shared_ptr<ByteBuffer>> reuseBuffers, long queryId)
{
    auto requests = batch.getRequests();
    std::vector<std::shared_ptr<ByteBuffer>> results;
    results.resize(batch.getSize());
    if (ConfigFactory::Instance().boolCheckProperty("localfs.enable.async.io") &&
        reuseBuffers.size() > 0)
    {
        // async read
        auto localReader = std::static_pointer_cast<PhysicalLocalReader>(reader);
        std::unordered_set<int> ring_index_set = localReader->getRingIndexes();
        std::unordered_map<int, uint32_t> ringIndexCountMap;

        for (int i = 0; i < batch.getSize(); i++)
        {
            Request request = requests[i];
            if (request.length > reuseBuffers.at(i)->size())
            {
                throw InvalidArgumentException(
                    "The error is not here; need to pay attention to the previous "
                    "critical section\n");
            }
            results.at(i) = localReader->readAsync(request.length, reuseBuffers.at(i),
                                                   request.bufferId,
                                                   request.ring_index, request.start);
            if (ring_index_set.find(request.ring_index) == ring_index_set.end())
            {
                ring_index_set.insert(request.ring_index);
                localReader->addRingIndex(request.ring_index);
            }
            ringIndexCountMap[request.ring_index]++;
        }
        localReader->readAsyncSubmit(ringIndexCountMap, ring_index_set);
        localReader->setRingIndexCountMap(ringIndexCountMap);
    }
    else
    {
        // sync read
        for (int i = 0; i < batch.getSize(); i++)
        {
            Request request = requests[i];
            reader->seek(request.start);
            if (reuseBuffers.size() > 0)
            {
                results.at(i) = reader->readFully(request.length, reuseBuffers.at(i));
            }
            else
            {
                results.at(i) = reader->readFully(request.length);
            }
        }
    }
    return results;
}

NoopScheduler::~NoopScheduler()
{
    delete instance;
    instance = nullptr;
}

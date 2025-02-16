/*
 * Copyright 2025 PixelsDB.
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
#include "Retina.h"
#include <stdexcept>
#include <cstring>
#include <thread>
#include <chrono>

Retina::Retina(uint64_t rgRecordNum)
    : numVisibilities((rgRecordNum + VISIBILITY_RECORD_CAPACITY - 1) / VISIBILITY_RECORD_CAPACITY)
{
    flag.store(0);
    visibilities = new Visibility[numVisibilities];
}

Retina::~Retina()
{
    delete[] visibilities;
}

void Retina::beginAccess()
{
    while (true)
    {
        uint32_t v = flag.load(std::memory_order_acquire);
        uint32_t accessCount = v & ACCESS_MASK;

        if (accessCount >= MAX_ACCESS_COUNT)
        {
            throw std::runtime_error("Reaches the max concurrent access count.");
        }

        if ((v & GC_MASK) > 0 ||
            !flag.compare_exchange_strong(v, v + ACCESS_INC, std::memory_order_acq_rel))
        {
            // We failed to get gc lock or increase access count.
            if ((v & GC_MASK) > 0)
            {
                // if there is an existing gc, sleep for 10ms.
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            continue;
        }
        break;
    }
}

void Retina::endAccess()
{
    uint32_t v = flag.load(std::memory_order_acquire);
    while((v & ACCESS_MASK) > 0)
    {
        if (flag.compare_exchange_strong(v, v - ACCESS_INC, std::memory_order_acq_rel))
        {
            break;
        }
        v = flag.load(std::memory_order_acquire);
    }
}

void Retina::garbageCollect(uint64_t timestamp)
{
    // Set the gc flag.
    flag.store(flag.load(std::memory_order_relaxed) | GC_MASK, std::memory_order_relaxed);
    
    // Wait for all access to end.
    while (true)
    {
        uint32_t v = flag.load(std::memory_order_relaxed);
        if ((v & ACCESS_MASK) == 0)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Garbage collect.
    for (uint64_t i = 0; i < numVisibilities; i++)
    {
        visibilities[i].garbageCollect(timestamp);
    }

    // Clear the gc flag.
    flag.store(flag.load(std::memory_order_relaxed) & ~GC_MASK, std::memory_order_relaxed);
}

Visibility* Retina::getVisibility(uint64_t rowId) const
{
    uint64_t visibilityIndex = rowId / VISIBILITY_RECORD_CAPACITY;
    if (visibilityIndex >= numVisibilities)
    {
        throw std::runtime_error("Row id is out of range.");
    }
    return &visibilities[visibilityIndex];
}

void Retina::deleteRecord(uint64_t rowId, uint64_t timestamp)
{
    try
    {
        Visibility* visibility = getVisibility(rowId);
        visibility->deleteRecord(rowId % VISIBILITY_RECORD_CAPACITY, timestamp);
    }
    catch (const std::runtime_error& e)
    {
        throw std::runtime_error("Failed to delete record: " + std::string(e.what()));
    }
}

uint64_t* Retina::getVisibilityBitmap(uint64_t timestamp)
{
    uint64_t* bitmap = new uint64_t[numVisibilities * BITMAP_SIZE_PER_VISIBILITY];
    memset(bitmap, 0, numVisibilities * BITMAP_SIZE_PER_VISIBILITY * sizeof(uint64_t));

    for (uint64_t i = 0; i < numVisibilities; i++)
    {
        visibilities[i].getVisibilityBitmap(timestamp, bitmap + i * BITMAP_SIZE_PER_VISIBILITY);
    }

    return bitmap;
}

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
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring>
#include <random>
#include <vector>
#include <bitset>

#include "Visibility.h"

#ifndef GET_BITMAP_BIT
#define GET_BITMAP_BIT(bitmap, rowId) \
  (((bitmap)[(rowId) / 64] >> ((rowId) % 64)) & 1ULL)
#endif

static const int NUM_READ_THREADS = 3;
static const int TOTAL_DELETES    = 25;

std::atomic<bool> g_stop{false};

void readerThreadFunc(Visibility* vis, int threadId)
{
    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> distSleep(1, 10);

    while (!g_stop.load(std::memory_order_acquire))
    {
        // Generate a random timestamp in [0, 2000)
        uint64_t ts = rng() % 2000;

        // Prepare output bitmap
        uint64_t outBmp[4];
        std::memset(outBmp, 0, 4 * sizeof(uint64_t));

        // Call getVisibilityBitmap
        vis->getVisibilityBitmap(ts, outBmp);

        // Occasionally print row10, row20 for demonstration
        static thread_local int counter = 0;
        if (++counter % 50 == 0)
        {
            // print binary visibility bitmap for all rows
            std::cout << "[Reader#" << threadId << "] ts=" << ts << " -> ";
            for (int i = 0; i < 4; i++) {
                std::cout << std::bitset<64>(outBmp[i]) << " ";
            }
            std::cout << std::endl;
        }

        // Random sleep to simulate concurrent reads
        std::this_thread::sleep_for(std::chrono::milliseconds(distSleep(rng)));
    }
    std::cout << "[Reader#" << threadId << "] stopping.\n";
}

// Writer thread function: performs deleteRecord with increasing timestamps
void writerThreadFunc(Visibility* vis)
{
    // rowIds: 10, 20, 30, ... up to 10 + (TOTAL_DELETES-1)*10
    // timestamps: starting at 1 and increment by 10 each time
    uint64_t ts = 1;
    for (int i = 0; i < TOTAL_DELETES; i++)
    {
        uint8_t rowId = static_cast<uint8_t>(10 + i * 10);
        vis->deleteRecord(rowId, ts);

        std::cout << "[Writer] deleteRecord(rowId=" << (int)rowId
                  << ", ts=" << ts << ")\n";

        ts += 10;  // Increment timestamp for next delete

        // Sleep a bit to let readers observe incremental changes
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    // After all deletes, signal readers to stop
    g_stop.store(true, std::memory_order_release);
    std::cout << "[Writer] finished.\n";
}

int main()
{
    Visibility vis;

    // Start one writer thread
    std::thread wthread(writerThreadFunc, &vis);

    // Start multiple reader threads
    std::vector<std::thread> readers;
    for (int i = 0; i < NUM_READ_THREADS; i++)
    {
        readers.emplace_back(std::thread(readerThreadFunc, &vis, i));
    }

    // Wait for writer to finish
    wthread.join();
    // Wait for all readers to stop
    for (auto &t : readers)
    {
        t.join();
    }

    std::cout << "[Main] All threads finished.\n";

    // Final check: call getVisibilityBitmap with a large timestamp
    // to ensure all deleted rows are indeed marked as deleted (1).
    uint64_t outBmp[4];
    vis.getVisibilityBitmap(999999, outBmp); // Large timestamp

    // We expect rowIds: 10, 20, 30, ... 10+(TOTAL_DELETES-1)*10
    for (int i = 0; i < TOTAL_DELETES; i++)
    {
        uint8_t rowId = static_cast<uint8_t>(10 + i * 10);
        std::cout << "Final rowId=" << (int)rowId
                  << " -> " << GET_BITMAP_BIT(outBmp, rowId) << std::endl;
    }

    return 0;
}
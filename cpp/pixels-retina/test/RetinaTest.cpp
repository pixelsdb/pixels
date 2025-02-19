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
#define ROW_COUNT 25600
#define VISIBILITIES_NUM (ROW_COUNT + 256 - 1) / 256
#define BITMAP_SIZE (VISIBILITIES_NUM * 4)
#define INVALID_BITS_COUNT (-ROW_COUNT & 255)

#include "gtest/gtest.h"
#include "Retina.h"

#include <bitset>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <random>
#include <cstring>
#include <sstream>

bool RETINA_TEST_DEBUG = false;

class RetinaTest : public ::testing::Test {
protected:
    void SetUp() override {
        retina = new Retina(ROW_COUNT);
    }
    
    void TearDown() override {
        delete retina;
    }

    Retina* retina;
};

TEST_F(RetinaTest, BasicDeleteAndVisibility) {
    uint64_t timestamp1 = 100;
    uint64_t timestamp2 = 200;

    retina->deleteRecord(5, timestamp1);
    retina->deleteRecord(10, timestamp1);
    retina->deleteRecord(15, timestamp2);
    retina->garbageCollect(timestamp1);

    uint64_t* bitmap1 = retina->getVisibilityBitmap(timestamp1);
    EXPECT_EQ(bitmap1[0], 0b0000010000100000);
    delete[] bitmap1;

    uint64_t* bitmap2 = retina->getVisibilityBitmap(timestamp2);
    EXPECT_EQ(bitmap2[0], 0b1000010000100000);
    delete[] bitmap2;
}

TEST_F(RetinaTest, MultiThread) {
    struct DeleteRecord {
        uint64_t timestamp;
        uint64_t rowId;
        DeleteRecord(uint64_t timestamp, uint64_t rowId) : timestamp(timestamp), rowId(rowId) {}
    };

    std::vector<DeleteRecord> deleteHistory;
    std::mutex historyMutex;
    std::mutex printMutex;
    std::atomic<bool> running{true};
    std::atomic<uint64_t> MaxTimestamp{0};
    std::atomic<uint64_t> MinTimestamp{0};
    std::atomic<int> verificationCount{0};
    
    auto printError = [&](const std::string& msg) {
        std::lock_guard<std::mutex> lock(printMutex);
        ADD_FAILURE() << msg;
    };

    auto verifyBitmap = [&](uint64_t timestamp, const uint64_t* bitmap) {
        uint64_t expectedBitmap[BITMAP_SIZE] = {0};
        std::vector<DeleteRecord> historySnapshot;
        
        {
            std::lock_guard<std::mutex> lock(historyMutex);
            historySnapshot = deleteHistory;
        }
        
        for (const auto& record : historySnapshot) {
            if (record.timestamp <= timestamp) {
                uint64_t bitmapIndex = record.rowId / 64;
                uint64_t bitOffset = record.rowId % 64;
                expectedBitmap[bitmapIndex] |= (1ULL << bitOffset);
            }
        }

        for (size_t i = 0; i < BITMAP_SIZE; i++) {
            if (bitmap[i] != expectedBitmap[i]) {
                if (RETINA_TEST_DEBUG) {
                    std::stringstream ss;
                    ss << "Bitmap verification failed at timestamp " << timestamp << "\n";
                    ss << "Bitmap segment " << i << " (rows " << (i*64) << "-" << (i*64+63) << "):\n";
                    ss << "Actual:   " << std::bitset<64>(bitmap[i]) << "\n";
                    ss << "Expected: " << std::bitset<64>(expectedBitmap[i]) << "\n\n";
                    ss << "Delete history up to timestamp " << timestamp << ":\n";
                    for (const auto& record : historySnapshot) {
                        if (record.timestamp <= timestamp) {
                            ss << "- Timestamp " << record.timestamp << ": deleted row " << record.rowId << "\n";
                        }
                    }
                    printError(ss.str());
                }
                return false;
            }
        }
        
        verificationCount++;
        return true;
    };

    auto deleteThread = std::thread([&]() {
        uint64_t timestamp = 1;
        std::random_device rd;
        std::mt19937 gen(rd());
        
        std::vector<uint64_t> remainingRows;
        for (uint64_t i = 0; i < ROW_COUNT; i++) {
            remainingRows.push_back(i);
        }

        while (!remainingRows.empty() && running) {
            std::uniform_int_distribution<size_t> indexDist(0, remainingRows.size() - 1);
            size_t index = indexDist(gen);
            uint64_t rowId = remainingRows[index];

            remainingRows[index] = remainingRows.back();
            remainingRows.pop_back();
 
            retina->deleteRecord(rowId, timestamp);

            {
                std::lock_guard<std::mutex> lock(historyMutex);
                deleteHistory.emplace_back(timestamp, rowId);
            }

            MaxTimestamp.store(timestamp);
            timestamp++;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        if (RETINA_TEST_DEBUG) {
            std::lock_guard<std::mutex> lock(printMutex);
            std::cout << "Delete thread completed: deleted " << deleteHistory.size() 
                      << " rows with max timestamp " << (timestamp-1) << std::endl;
        }

        running.store(false);
    });

    auto gcThread = std::thread([&]() {
        uint64_t gcTs = 0;
        while (running) {
            gcTs += 10;
            if (gcTs <= MinTimestamp.load()) {
                retina->garbageCollect(gcTs);
                if (RETINA_TEST_DEBUG) {
                    std::lock_guard<std::mutex> lock(printMutex);
                    std::cout << "GC thread completed: GCed up to timestamp " << gcTs << std::endl;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    std::vector<std::thread> getThreads;
    for (int i = 0; i < 100; i++) {
        getThreads.emplace_back([&, i]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            int localVerificationCount = 0;
            
            while (running) {
                uint64_t maxTs = MaxTimestamp.load();
                uint64_t minTs = MinTimestamp.load();
                if (maxTs == 0 || minTs > maxTs) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                
                std::uniform_int_distribution<uint64_t> tsDist(minTs, maxTs);
                uint64_t queryTs = tsDist(gen);
                uint64_t* bitmap = retina->getVisibilityBitmap(queryTs);

                EXPECT_TRUE(verifyBitmap(queryTs, bitmap));

                delete[] bitmap;
                localVerificationCount++;
                MinTimestamp.fetch_add(1);
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }

            if (RETINA_TEST_DEBUG) {
                std::lock_guard<std::mutex> lock(printMutex);
                std::cout << "Get thread " << i << " completed: performed " 
                          << localVerificationCount << " verifications" << std::endl;
            }
        });
    }

    deleteThread.join();
    gcThread.join();
    for (auto& t : getThreads) {
        t.join();
    }

    uint64_t* finalBitmap = retina->getVisibilityBitmap(MaxTimestamp.load());
    uint64_t* expectedFinalBitmap = new uint64_t[BITMAP_SIZE]();
    std::memset(expectedFinalBitmap, 0xFF, sizeof(uint64_t) * BITMAP_SIZE);
    if (INVALID_BITS_COUNT != 0) {
        for (size_t i = ROW_COUNT; i < ROW_COUNT + INVALID_BITS_COUNT; i++) {
            expectedFinalBitmap[i / 64] &= ~(1ULL << (i % 64));
        }
    }
    
    EXPECT_TRUE(verifyBitmap(MaxTimestamp.load(), finalBitmap));
    
    delete[] finalBitmap;
    delete[] expectedFinalBitmap;
}
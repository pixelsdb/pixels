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
#include <atomic>
#include <bitset>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <thread>
#include <vector>
#include <mutex>
#include <sstream>

#include "gtest/gtest.h"
#include "TileVisibility.h"

#define BITMAP_SIZE 4
#ifndef GET_BITMAP_BIT
#define GET_BITMAP_BIT(bitmap, rowId)                                          \
    (((bitmap)[(rowId) / 64] >> ((rowId) % 64)) & 1ULL)
#endif

bool VISIBILITY_TEST_DEBUG = true;

class TileVisibilityTest : public ::testing::Test {
protected:
    void SetUp() override {
        v = new TileVisibility();
    }

    void TearDown() override {
        delete v;
    }

    bool checkBitmap(const uint64_t* actual, const uint64_t* expected, int size = BITMAP_SIZE) {
        for (int i = 0; i < size; i++) {
            if (actual[i] != expected[i]) {
                if (VISIBILITY_TEST_DEBUG) {
                    std::cout << "bits between " << (64 * i) << " and " << (64 * i + 63)
                          << " are not as expected\n";
                    std::cout << "actual: " << std::bitset<64>(actual[i]) << std::endl;
                    std::cout << "expect: " << std::bitset<64>(expected[i]) << std::endl;
                }
                return false;
            }
        }
        return true;
    }

    TileVisibility* v;
};

TEST_F(TileVisibilityTest, BaseFunction) {
    v->deleteTileRecord(1, 100);
    v->deleteTileRecord(2, 101);

    uint64_t actualBitmap[BITMAP_SIZE] = {0};
    uint64_t expectedBitmap[BITMAP_SIZE] = {0};

    v->getTileVisibilityBitmap(50, actualBitmap);
    EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));

    v->getTileVisibilityBitmap(100, actualBitmap);
    SET_BITMAP_BIT(expectedBitmap, 1);
    EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));

    v->getTileVisibilityBitmap(101, actualBitmap);
    SET_BITMAP_BIT(expectedBitmap, 2);
    EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));

    v->collectTileGarbage(101);
    v->getTileVisibilityBitmap(101, actualBitmap);
    EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));
}

TEST_F(TileVisibilityTest, DeleteRecord) {
    uint64_t actualBitmap[BITMAP_SIZE] = {0};
    uint64_t expectedBitmap[BITMAP_SIZE] = {0};

    for (int i = 0; i < 256; i++) {
        v->deleteTileRecord(i, i + 100);
        SET_BITMAP_BIT(expectedBitmap, i);
        v->getTileVisibilityBitmap(i + 100, actualBitmap);
        EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));
    }
}

TEST_F(TileVisibilityTest, GarbageCollect) {
    for (int i = 0; i < 100; i++) {
        v->deleteTileRecord(i, i + 100);
    }
    v->collectTileGarbage(150);
    uint64_t actualBitmap[BITMAP_SIZE] = {0};
    uint64_t expectedBitmap[BITMAP_SIZE] = {0};

    v->getTileVisibilityBitmap(150, actualBitmap);
    for (int i = 0; i <= 50; i++) {
        SET_BITMAP_BIT(expectedBitmap, i);
    }
    EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));
    for (int i = 51; i < 100; i++) {
        SET_BITMAP_BIT(expectedBitmap, i);
    }
    v->collectTileGarbage(200);
    v->getTileVisibilityBitmap(200, actualBitmap);
    EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));
}

TEST_F(TileVisibilityTest, MultiThread) {
    struct DeleteRecord {
        uint64_t timestamp;
        uint32_t rowId;
        DeleteRecord(uint64_t timestamp, uint32_t rowId) : timestamp(timestamp), rowId(rowId) {}
    };

    std::vector<DeleteRecord> deleteHistory;
    std::mutex historyMutex;
    std::mutex printMutex;
    std::atomic<bool> running{true};
    std::atomic<uint64_t> currentMaxTimestamp{0};
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
                SET_BITMAP_BIT(expectedBitmap, record.rowId);
            }
        }
        
        for (int i = 0; i < BITMAP_SIZE; i++) {
            if (bitmap[i] != expectedBitmap[i]) {
                if (VISIBILITY_TEST_DEBUG) {
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
        
        std::vector<uint32_t> remainingRows;
        for (uint32_t i = 0; i < 256; i++) {
            remainingRows.push_back(i);
        }

        while (!remainingRows.empty() && running) {
            std::uniform_int_distribution<size_t> indexDist(0, remainingRows.size() - 1);
            size_t index = indexDist(gen);
            uint32_t rowId = remainingRows[index];

            remainingRows[index] = remainingRows.back();
            remainingRows.pop_back();

            {
                std::lock_guard<std::mutex> lock(historyMutex);
                v->deleteTileRecord(rowId, timestamp);
                deleteHistory.emplace_back(timestamp, rowId);
            }

            currentMaxTimestamp.store(timestamp);
            timestamp++;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        if (VISIBILITY_TEST_DEBUG) {
            std::lock_guard<std::mutex> lock(printMutex);
            std::cout << "Delete thread completed: deleted " << deleteHistory.size() 
                      << " rows with max timestamp " << (timestamp-1) << std::endl;
        }

        running.store(false);
    });

    std::vector<std::thread> getThreads;
    for (int i = 0; i < 10000; i++) {
        getThreads.emplace_back([&, i]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            int localVerificationCount = 0;
            
            while (running) {
                uint64_t maxTs = currentMaxTimestamp.load();
                if (maxTs == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                
                std::uniform_int_distribution<uint64_t> tsDist(0, maxTs);
                uint64_t queryTs = tsDist(gen);
                
                uint64_t actualBitmap[BITMAP_SIZE] = {0};
                v->getTileVisibilityBitmap(queryTs, actualBitmap);
                
                EXPECT_TRUE(verifyBitmap(queryTs, actualBitmap));
                localVerificationCount++;
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            
            if (VISIBILITY_TEST_DEBUG) {
                std::lock_guard<std::mutex> lock(printMutex);
                std::cout << "Get thread " << i << " completed: performed " 
                          << localVerificationCount << " verifications" << std::endl;
            }
        });
    }

    deleteThread.join();
    for (auto& t : getThreads) {
        t.join();
    }

    uint64_t finalBitmap[BITMAP_SIZE] = {0};
    v->getTileVisibilityBitmap(currentMaxTimestamp.load(), finalBitmap);
    uint64_t expectedFinalBitmap[BITMAP_SIZE];
    std::memset(expectedFinalBitmap, 0xFF, sizeof(expectedFinalBitmap));
    
    EXPECT_TRUE(checkBitmap(finalBitmap, expectedFinalBitmap));
}

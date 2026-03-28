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
#include "RGVisibility.h"

#include <bitset>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <random>
#include <cstring>
#include <sstream>

bool RETINA_TEST_DEBUG = true;

class RGVisibilityTest : public ::testing::Test {
protected:
    void SetUp() override {
        rgVisibility = new RGVisibilityInstance(ROW_COUNT);
    }
    
    void TearDown() override {
        delete rgVisibility;
    }

    RGVisibilityInstance* rgVisibility;
};

TEST_F(RGVisibilityTest, BasicDeleteAndVisibility) {
    uint64_t timestamp1 = 100;
    uint64_t timestamp2 = 200;

    rgVisibility->deleteRGRecord(5, timestamp1);
    rgVisibility->deleteRGRecord(10, timestamp1);
    rgVisibility->deleteRGRecord(15, timestamp2);
    rgVisibility->collectRGGarbage(timestamp1);

    uint64_t* bitmap1 = rgVisibility->getRGVisibilityBitmap(timestamp1);
    EXPECT_EQ(bitmap1[0], 0b0000010000100000);
    delete[] bitmap1;

    uint64_t* bitmap2 = rgVisibility->getRGVisibilityBitmap(timestamp2);
    EXPECT_EQ(bitmap2[0], 0b1000010000100000);
    delete[] bitmap2;
}

TEST_F(RGVisibilityTest, MultiThread) {
    struct DeleteRecord {
        uint64_t timestamp;
        uint32_t rowId;
        DeleteRecord(uint64_t timestamp, uint32_t rowId) : timestamp(timestamp), rowId(rowId) {}
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
        
        std::vector<uint32_t> remainingRows;
        for (uint32_t i = 0; i < ROW_COUNT; i++) {
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
                rgVisibility->deleteRGRecord(rowId, timestamp);
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
                rgVisibility->collectRGGarbage(gcTs);
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
                uint64_t* bitmap = rgVisibility->getRGVisibilityBitmap(queryTs);

                EXPECT_TRUE(verifyBitmap(queryTs, bitmap));

                delete[] bitmap;
                localVerificationCount++;
                MinTimestamp.fetch_add(1, std::memory_order_relaxed);
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

    uint64_t* finalBitmap = rgVisibility->getRGVisibilityBitmap(MaxTimestamp.load());
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

// =====================================================================
// gcSnapshotBitmap correctness tests
//
// Core verification: the bitmap returned by collectRGGarbage (gcSnapshotBitmap)
// must be bitwise identical to getRGVisibilityBitmap called BEFORE GC.
//
// Why pre-GC reference matters:
//   getRGVisibilityBitmap traverses the full, unmodified deletion chain — it is
//   a completely independent computation from the GC code path.  Comparing
//   gcSnapshotBitmap with a post-GC getRGVisibilityBitmap is weaker because
//   both read from state that GC just modified; a bug that corrupts the compact
//   AND the snapshot identically would go undetected.
//
// Each test also verifies that post-GC queries still return correct results
// (regression check on the compact logic itself).
//
// Covers all three code paths in collectTileGarbage:
//   A — ts <= baseTimestamp (early return, no compaction)
//   B — chain exists but no full block compactable
//   C — one or more blocks compacted (with/without boundary block)
// =====================================================================

static void compareBitmaps(
    const uint64_t* actual, const uint64_t* expected, uint64_t size, uint64_t ts,
    const char* actualLabel, const char* expectedLabel)
{
    for (size_t i = 0; i < size; i++) {
        EXPECT_EQ(actual[i], expected[i])
            << "Word " << i << " (rows " << (i * 64) << "-" << (i * 64 + 63)
            << ") at ts=" << ts
            << "\n  " << actualLabel << ": " << std::bitset<64>(actual[i])
            << "\n  " << expectedLabel << ": " << std::bitset<64>(expected[i]);
    }
}

static void verifyGcSnapshot(
    RGVisibilityInstance* rgv, uint64_t ts,
    const uint64_t* preGcRef, const std::vector<uint64_t>& snapshot)
{
    uint64_t bitmapSize = rgv->getBitmapSize();
    ASSERT_EQ(snapshot.size(), bitmapSize);

    // Primary check: gcSnapshotBitmap must match the pre-GC ground truth
    compareBitmaps(snapshot.data(), preGcRef, bitmapSize, ts,
                   "gcSnapshot", "preGcRef");

    // Secondary check: post-GC query must also agree (compact regression)
    uint64_t* postGcRef = rgv->getRGVisibilityBitmap(ts);
    compareBitmaps(snapshot.data(), postGcRef, bitmapSize, ts,
                   "gcSnapshot", "postGcQuery");
    delete[] postGcRef;
}

// Path A: empty chain → all-zero snapshot; then repeat GC at same ts → early return A
TEST_F(RGVisibilityTest, GcSnapshot_EarlyReturnA) {
    // Empty chain: baseTimestamp=0, ts=100 > 0 → enters path B with null head
    uint64_t* preRef0 = rgVisibility->getRGVisibilityBitmap(100);
    std::vector<uint64_t> snap0 = rgVisibility->collectRGGarbage(100);
    verifyGcSnapshot(rgVisibility, 100, preRef0, snap0);
    delete[] preRef0;
    for (auto w : snap0) {
        EXPECT_EQ(w, 0ULL);
    }

    // Add deletes and compact to advance baseTimestamp
    rgVisibility->deleteRGRecord(5, 100);
    rgVisibility->deleteRGRecord(10, 100);
    rgVisibility->deleteRGRecord(15, 200);

    // First GC at ts=200 → compact all 3 items → baseTimestamp becomes 200
    uint64_t* preRef1 = rgVisibility->getRGVisibilityBitmap(200);
    std::vector<uint64_t> snap1 = rgVisibility->collectRGGarbage(200);
    verifyGcSnapshot(rgVisibility, 200, preRef1, snap1);
    delete[] preRef1;

    // Second GC at ts=200 → ts == baseTimestamp → true early return A
    uint64_t* preRef2 = rgVisibility->getRGVisibilityBitmap(200);
    std::vector<uint64_t> snap2 = rgVisibility->collectRGGarbage(200);
    verifyGcSnapshot(rgVisibility, 200, preRef2, snap2);
    delete[] preRef2;

    ASSERT_EQ(snap1.size(), snap2.size());
    for (size_t i = 0; i < snap1.size(); i++) {
        EXPECT_EQ(snap1[i], snap2[i]);
    }
}

// Path B: chain exists, head block straddles safeGcTs → no compactable block
TEST_F(RGVisibilityTest, GcSnapshot_EarlyReturnB) {
    // 5 items in one block: ts 1,2,3,8,10.  Block last ts=10 > safeGcTs=5
    rgVisibility->deleteRGRecord(0, 1);
    rgVisibility->deleteRGRecord(1, 2);
    rgVisibility->deleteRGRecord(2, 3);
    rgVisibility->deleteRGRecord(3, 8);
    rgVisibility->deleteRGRecord(4, 10);

    uint64_t* preRef = rgVisibility->getRGVisibilityBitmap(5);
    std::vector<uint64_t> snapshot = rgVisibility->collectRGGarbage(5);
    verifyGcSnapshot(rgVisibility, 5, preRef, snapshot);
    delete[] preRef;

    // Rows 0,1,2 marked (ts ≤ 5); rows 3,4 not (ts 8,10 > 5)
    EXPECT_EQ(snapshot[0], 0b111ULL);
}

// Path B variant: all items in head block have ts > safeGcTs
TEST_F(RGVisibilityTest, GcSnapshot_EarlyReturnB_NoneMatch) {
    rgVisibility->deleteRGRecord(0, 10);
    rgVisibility->deleteRGRecord(1, 20);

    uint64_t* preRef = rgVisibility->getRGVisibilityBitmap(5);
    std::vector<uint64_t> snapshot = rgVisibility->collectRGGarbage(5);
    verifyGcSnapshot(rgVisibility, 5, preRef, snapshot);
    delete[] preRef;

    EXPECT_EQ(snapshot[0], 0ULL);
}

// Path C: one full block compacted + boundary block with mixed items
TEST_F(RGVisibilityTest, GcSnapshot_CompactWithBoundary) {
    // 10 items: rows 0-9, ts 1-10
    // Block 1 (8 items, ts 1-8): last ts=8 ≤ 9 → compactable
    // Block 2 (2 items, ts 9-10): boundary block
    for (uint32_t i = 0; i < 10; i++) {
        rgVisibility->deleteRGRecord(i, i + 1);
    }

    uint64_t* preRef = rgVisibility->getRGVisibilityBitmap(9);
    std::vector<uint64_t> snapshot = rgVisibility->collectRGGarbage(9);
    verifyGcSnapshot(rgVisibility, 9, preRef, snapshot);
    delete[] preRef;

    // Rows 0-8 marked (ts 1-9 ≤ 9), row 9 not (ts 10 > 9)
    EXPECT_EQ(snapshot[0], 0x1FFULL); // bits 0-8
}

// Path C: all blocks fully compacted, no remaining chain
TEST_F(RGVisibilityTest, GcSnapshot_CompactAllBlocks) {
    // Exactly 8 items fill one block: rows 0-7, ts 1-8
    for (uint32_t i = 0; i < 8; i++) {
        rgVisibility->deleteRGRecord(i, i + 1);
    }

    // safeGcTs=10 > all item ts → entire block compacted, newHead=null
    uint64_t* preRef = rgVisibility->getRGVisibilityBitmap(10);
    std::vector<uint64_t> snapshot = rgVisibility->collectRGGarbage(10);
    verifyGcSnapshot(rgVisibility, 10, preRef, snapshot);
    delete[] preRef;

    EXPECT_EQ(snapshot[0], 0xFFULL); // bits 0-7
}

// Path C: multiple blocks compacted before a boundary block
TEST_F(RGVisibilityTest, GcSnapshot_CompactMultiBlock) {
    // 20 items: rows 0-19, ts 1-20
    // Block 1 (ts 1-8), Block 2 (ts 9-16), Block 3 tail (ts 17-20)
    // safeGcTs=18: blocks 1,2 compacted, block 3 is boundary
    for (uint32_t i = 0; i < 20; i++) {
        rgVisibility->deleteRGRecord(i, i + 1);
    }

    uint64_t* preRef = rgVisibility->getRGVisibilityBitmap(18);
    std::vector<uint64_t> snapshot = rgVisibility->collectRGGarbage(18);
    verifyGcSnapshot(rgVisibility, 18, preRef, snapshot);
    delete[] preRef;

    // Rows 0-17 marked (ts 1-18 ≤ 18), rows 18-19 not
    EXPECT_EQ(snapshot[0], (1ULL << 18) - 1);
}

// Multiple deletes sharing the same timestamp (batch deletes)
TEST_F(RGVisibilityTest, GcSnapshot_SameTimestamp) {
    rgVisibility->deleteRGRecord(0, 5);
    rgVisibility->deleteRGRecord(1, 5);
    rgVisibility->deleteRGRecord(2, 5);
    rgVisibility->deleteRGRecord(3, 10);
    rgVisibility->deleteRGRecord(4, 10);

    uint64_t* preRef = rgVisibility->getRGVisibilityBitmap(5);
    std::vector<uint64_t> snapshot = rgVisibility->collectRGGarbage(5);
    verifyGcSnapshot(rgVisibility, 5, preRef, snapshot);
    delete[] preRef;

    EXPECT_EQ(snapshot[0], 0b111ULL);
}

// Deletes spanning multiple tiles (RETINA_CAPACITY=256 rows per tile)
TEST_F(RGVisibilityTest, GcSnapshot_CrossTile) {
    // Tile 0: rows 0-255          Tile 1: rows 256-511
    // Tile 2: rows 512-767
    rgVisibility->deleteRGRecord(5, 1);     // tile 0
    rgVisibility->deleteRGRecord(10, 2);    // tile 0
    rgVisibility->deleteRGRecord(260, 3);   // tile 1, localRow 4
    rgVisibility->deleteRGRecord(600, 4);   // tile 2, localRow 88
    rgVisibility->deleteRGRecord(100, 5);   // tile 0
    rgVisibility->deleteRGRecord(300, 6);   // tile 1, localRow 44

    uint64_t* preRef1 = rgVisibility->getRGVisibilityBitmap(4);
    std::vector<uint64_t> snapshot = rgVisibility->collectRGGarbage(4);
    verifyGcSnapshot(rgVisibility, 4, preRef1, snapshot);
    delete[] preRef1;

    // After GC at ts=4, also verify a higher ts sees more deletes
    uint64_t* preRef2 = rgVisibility->getRGVisibilityBitmap(6);
    std::vector<uint64_t> snap2 = rgVisibility->collectRGGarbage(6);
    verifyGcSnapshot(rgVisibility, 6, preRef2, snap2);
    delete[] preRef2;
}

// Progressive GC rounds with interleaved inserts
TEST_F(RGVisibilityTest, GcSnapshot_ProgressiveRounds) {
    // Phase 1: 20 deletes at ts 1-20
    for (uint32_t i = 0; i < 20; i++) {
        rgVisibility->deleteRGRecord(i, i + 1);
    }

    uint64_t* preRef1 = rgVisibility->getRGVisibilityBitmap(5);
    std::vector<uint64_t> snap1 = rgVisibility->collectRGGarbage(5);
    verifyGcSnapshot(rgVisibility, 5, preRef1, snap1);
    delete[] preRef1;

    uint64_t* preRef2 = rgVisibility->getRGVisibilityBitmap(12);
    std::vector<uint64_t> snap2 = rgVisibility->collectRGGarbage(12);
    verifyGcSnapshot(rgVisibility, 12, preRef2, snap2);
    delete[] preRef2;

    // Phase 2: 10 more deletes at ts 21-30
    for (uint32_t i = 20; i < 30; i++) {
        rgVisibility->deleteRGRecord(i, i + 1);
    }

    uint64_t* preRef3 = rgVisibility->getRGVisibilityBitmap(25);
    std::vector<uint64_t> snap3 = rgVisibility->collectRGGarbage(25);
    verifyGcSnapshot(rgVisibility, 25, preRef3, snap3);
    delete[] preRef3;

    // Final GC beyond all timestamps
    uint64_t* preRef4 = rgVisibility->getRGVisibilityBitmap(100);
    std::vector<uint64_t> snap4 = rgVisibility->collectRGGarbage(100);
    verifyGcSnapshot(rgVisibility, 100, preRef4, snap4);
    delete[] preRef4;

    // All 30 rows should be marked
    EXPECT_EQ(snap4[0], (1ULL << 30) - 1);
}

// Randomized: random deletes across all tiles, verify at each GC round
TEST_F(RGVisibilityTest, GcSnapshot_Randomized) {
    std::mt19937 gen(42);
    std::uniform_int_distribution<uint32_t> rowDist(0, ROW_COUNT - 1);
    std::vector<bool> deleted(ROW_COUNT, false);
    uint64_t ts = 1;
    uint64_t lastGcTs = 0;

    for (int round = 0; round < 10; round++) {
        for (int d = 0; d < 100; d++) {
            uint32_t rowId;
            do { rowId = rowDist(gen); } while (deleted[rowId]);
            deleted[rowId] = true;
            rgVisibility->deleteRGRecord(rowId, ts);
            ts++;
        }

        uint64_t gcTs = lastGcTs + 51;
        if (gcTs >= ts) gcTs = ts - 1;

        uint64_t* preRef = rgVisibility->getRGVisibilityBitmap(gcTs);
        std::vector<uint64_t> snapshot = rgVisibility->collectRGGarbage(gcTs);
        verifyGcSnapshot(rgVisibility, gcTs, preRef, snapshot);
        delete[] preRef;
        lastGcTs = gcTs;
    }

    // Final GC beyond all timestamps
    uint64_t* preRefFinal = rgVisibility->getRGVisibilityBitmap(ts + 100);
    std::vector<uint64_t> finalSnap = rgVisibility->collectRGGarbage(ts + 100);
    verifyGcSnapshot(rgVisibility, ts + 100, preRefFinal, finalSnap);
    delete[] preRefFinal;
}
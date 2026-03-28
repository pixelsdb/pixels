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

#ifndef RETINA_CAPACITY
#define RETINA_CAPACITY 256
#endif

#define BITMAP_SIZE BITMAP_WORDS(RETINA_CAPACITY)
#ifndef GET_BITMAP_BIT
#define GET_BITMAP_BIT(bitmap, rowId)                                          \
    (((bitmap)[(rowId) / 64] >> ((rowId) % 64)) & 1ULL)
#endif

bool VISIBILITY_TEST_DEBUG = true;

class TileVisibilityTest : public ::testing::Test {
protected:
    void SetUp() override {
        v = new TileVisibility<RETINA_CAPACITY>();
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

    void collectGarbage(uint64_t ts) {
        uint64_t buf[BITMAP_SIZE] = {0};
        v->collectTileGarbage(ts, buf);
    }

    TileVisibility<RETINA_CAPACITY>* v;
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

    collectGarbage(101);
    v->getTileVisibilityBitmap(101, actualBitmap);
    EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));
}

TEST_F(TileVisibilityTest, DeleteRecord) {
    uint64_t actualBitmap[BITMAP_SIZE] = {0};
    uint64_t expectedBitmap[BITMAP_SIZE] = {0};

    for (int i = 0; i < RETINA_CAPACITY; i++) {
        v->deleteTileRecord(i, i + 100);
        SET_BITMAP_BIT(expectedBitmap, i);
        v->getTileVisibilityBitmap(i + 100, actualBitmap);
        EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));
    }
}

TEST_F(TileVisibilityTest, GarbageCollect) {
    int count = RETINA_CAPACITY < 100 ? RETINA_CAPACITY : 100;
    for (int i = 0; i < count; i++) {
        v->deleteTileRecord(i, i + 100);
    }
    collectGarbage(150);
    uint64_t actualBitmap[BITMAP_SIZE] = {0};
    uint64_t expectedBitmap[BITMAP_SIZE] = {0};

    v->getTileVisibilityBitmap(150, actualBitmap);
    for (int i = 0; i <= 50 && i < count; i++) {
        SET_BITMAP_BIT(expectedBitmap, i);
    }
    EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap));
    for (int i = 51; i < count; i++) {
        SET_BITMAP_BIT(expectedBitmap, i);
    }
    collectGarbage(100 + count);
    v->getTileVisibilityBitmap(100 + count, actualBitmap);
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
        for (uint32_t i = 0; i < RETINA_CAPACITY; i++) {
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
    for (int i = 0; i < 100; i++) {
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

/**
 * ZeroSentinelInGarbageCollect — deterministic regression for Scenario 2 guard.
 *
 * The fix for the full-block race (Scenario 2) relies on treating item=0 as a
 * sentinel that marks uninitialised tail slots. makeDeleteIndex(rowId=0, ts=0)=0,
 * which is identical to the zero-initialised memory of a freshly-allocated block.
 *
 * Precondition enforced by TransService: all valid transaction timestamps are > 0,
 * so ts=0 can never represent a real deletion and is safe to use as a sentinel.
 *
 * This test simulates the exact item value produced by the race without requiring
 * concurrent execution:
 *   1. Fill BLOCK_CAPACITY-1 slots with valid (rowId, ts) pairs.
 *   2. Insert makeDeleteIndex(0,0)=0 into the last slot — the same value a
 *      zero-initialised slot in a new block would have during the race window.
 *   3. Run GC: without the fix, extractTimestamp(0)=0 ≤ ts would SET_BITMAP_BIT(0).
 *              with the fix, `if (item == 0) break` stops before touching bit 0.
 *
 * Failure mode WITHOUT fix: bits 0..BLOCK_CAPACITY-2 set (bit 0 is spurious).
 * Pass condition WITH fix:  bits 1..BLOCK_CAPACITY-2 set, bit 0 NOT set.
 */
TEST_F(TileVisibilityTest, ZeroSentinelInGarbageCollect) {
    // Fill slots 0..BLOCK_CAPACITY-2 with valid items (rows 1..7, ts 1..7)
    for (uint16_t i = 1; i < DeleteIndexBlock::BLOCK_CAPACITY; i++) {
        v->deleteTileRecord(i, static_cast<uint64_t>(i));
    }
    // Insert the sentinel value (row=0, ts=0 → item=0) into the final slot.
    // This replicates the zero-initialised items[1..7] that GC would encounter
    // during the Scenario-2 race if tailUsed were stale at BLOCK_CAPACITY.
    v->deleteTileRecord(0, 0);

    collectGarbage(100);

    uint64_t actualBitmap[BITMAP_SIZE] = {0};
    v->getTileVisibilityBitmap(100, actualBitmap);

    // Rows 1..(BLOCK_CAPACITY-1) should be deleted; row 0 must NOT be set.
    uint64_t expectedBitmap[BITMAP_SIZE] = {0};
    for (uint16_t i = 1; i < DeleteIndexBlock::BLOCK_CAPACITY; i++) {
        SET_BITMAP_BIT(expectedBitmap, i);
    }
    EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap))
        << "Row 0 must not be set: item==0 sentinel guard must stop GC "
           "before processing zero-initialised (or ts=0) slots";
}

/**
 * ConcurrentGCAndFirstInsert — targets Scenario 1 (empty-list path race).
 *
 * Race condition:
 *   deleteTileRecord (empty list) does:
 *     1. tail.CAS(nullptr → newBlk)
 *     2. currentVersion.CAS(oldVer → newVer with head=newBlk)   ← head now visible
 *     3. tailUsed.store(1)                                       ← window: 2 done, 3 not yet
 *
 *   If collectTileGarbage runs between steps 2 and 3:
 *     - blk = newVer->head = newBlk  (reachable)
 *     - count = tailUsed = 0         (not yet updated)
 *     - BEFORE FIX: items[count-1] = items[size_t(-1)] → size_t underflow → UB / crash
 *     - AFTER FIX:  count==0 guard breaks out safely; no crash.
 *
 * NOTE on test reliability: the race window is between two adjacent atomic operations
 * (currentVersion.CAS at line ~99 and tailUsed.store at line ~102 in deleteTileRecord).
 * This is too narrow to trigger reliably with OS-level scheduling alone; the test is
 * therefore a probabilistic stress test rather than a deterministic reproducer.  For
 * guaranteed detection, compile with AddressSanitizer + ThreadSanitizer or add a
 * -DENABLE_TEST_HOOKS build flag that injects a sleep between the two operations.
 *
 * The primary value of this test is as a no-crash regression guard: if the count==0
 * guard is removed, a crash (size_t underflow → OOB array access) will eventually
 * surface under sustained concurrent load even if it is not triggered every run.
 */
TEST_F(TileVisibilityTest, ConcurrentGCAndFirstInsert) {
    constexpr int TRIALS = 200;

    for (int trial = 0; trial < TRIALS; trial++) {
        delete v;
        v = new TileVisibility<RETINA_CAPACITY>();

        std::atomic<bool> deleteStarted{false};
        std::atomic<bool> gcDone{false};

        // GC thread: spin-waits until the delete thread has signalled it started,
        // then immediately fires GC to maximise the chance of hitting the race window.
        auto gcThread = std::thread([&]() {
            while (!deleteStarted.load(std::memory_order_acquire)) {}
            collectGarbage(1000);
            gcDone.store(true, std::memory_order_release);
        });

        // Delete thread: signals start, then inserts the very first item (row=5, ts=100).
        // Row 0 is intentionally never deleted so we can use bit 0 as a spurious-set
        // canary in the companion scenario-2 test.
        deleteStarted.store(true, std::memory_order_release);
        v->deleteTileRecord(5, 100);

        gcThread.join();

        // After both operations complete, GC with a ts that covers the inserted item
        // and verify the bitmap is exactly {row 5 deleted}.
        collectGarbage(1000);
        uint64_t actualBitmap[BITMAP_SIZE] = {0};
        v->getTileVisibilityBitmap(1000, actualBitmap);

        uint64_t expectedBitmap[BITMAP_SIZE] = {0};
        SET_BITMAP_BIT(expectedBitmap, 5);

        EXPECT_TRUE(checkBitmap(actualBitmap, expectedBitmap))
            << "Trial " << trial << ": bitmap incorrect after concurrent first-insert + GC";
    }
}

/**
 * ConcurrentGCAndBlockTransition — targets Scenario 2 (full-block path race).
 *
 * Race condition:
 *   deleteTileRecord (old tail block is full) does:
 *     1. curTail->next.CAS(nullptr → newBlk)
 *     2. tail.CAS(curTail → newBlk)    ← tail now points to new block
 *     3. tailUsed.store(1)             ← window: 2 done, 3 not yet
 *
 *   If collectTileGarbage runs between steps 2 and 3:
 *     - blk == tail (newBlk), count = tailUsed = BLOCK_CAPACITY (stale old value, 8)
 *     - items[0] is the real insertion; items[1..BLOCK_CAPACITY-1] are zero-initialised
 *     - BEFORE FIX: extractTimestamp(0)=0 ≤ ts → SET_BITMAP_BIT(extractRowId(0)=0)
 *                   → bit 0 spuriously set in baseBitmap (persistent data corruption)
 *     - AFTER FIX:  item==0 guard breaks the inner loop; no spurious bit 0.
 *
 * Strategy: pre-fill exactly BLOCK_CAPACITY items (one full block) with ts values
 * that GC will compact, then concurrently fire GC and the (BLOCK_CAPACITY+1)-th
 * insert that triggers new-block creation.  Row 0 is never deleted; if the bug fires,
 * getTileVisibilityBitmap will report bit 0 set even though row 0 was never deleted.
 *
 * NOTE on test reliability: identical narrow-window caveat as ConcurrentGCAndFirstInsert.
 * The ZeroSentinelInGarbageCollect test above is the deterministic companion that
 * verifies the item==0 guard logic directly without requiring concurrent execution.
 */
TEST_F(TileVisibilityTest, ConcurrentGCAndBlockTransition) {
    constexpr uint64_t GC_TS = 1000;
    // Number of concurrent trials; more iterations → higher probability of hitting the race.
    constexpr int TRIALS = 500;

    std::atomic<bool> spuriousRow0{false};

    for (int trial = 0; trial < TRIALS && !spuriousRow0.load(); trial++) {
        delete v;
        v = new TileVisibility<RETINA_CAPACITY>();

        // Pre-fill exactly BLOCK_CAPACITY (8) items so the next insert triggers
        // the full-block → new-block code path.  Use rows 1..8 (never row 0).
        for (size_t i = 0; i < DeleteIndexBlock::BLOCK_CAPACITY; i++) {
            v->deleteTileRecord(static_cast<uint16_t>(i + 1), i + 1);
        }

        std::atomic<bool> insertReady{false};

        // GC thread: waits for the insert thread to be about to create the new block,
        // then fires GC immediately to race with tail/tailUsed update.
        auto gcThread = std::thread([&]() {
            while (!insertReady.load(std::memory_order_acquire)) {}
            collectGarbage(GC_TS);
        });

        // Insert thread: signal then insert the (BLOCK_CAPACITY+1)-th item to force
        // new-block creation.  Row 0 is the canary — never intentionally deleted.
        insertReady.store(true, std::memory_order_release);
        v->deleteTileRecord(10, DeleteIndexBlock::BLOCK_CAPACITY + 1);

        gcThread.join();

        // Run one more clean GC to ensure everything that should be compacted is.
        collectGarbage(GC_TS);

        // Check the canary: bit 0 must be 0 because row 0 was never deleted.
        uint64_t bitmap[BITMAP_SIZE] = {0};
        v->getTileVisibilityBitmap(GC_TS, bitmap);

        if (GET_BITMAP_BIT(bitmap, 0)) {
            spuriousRow0.store(true);
            ADD_FAILURE() << "Trial " << trial
                          << ": bit 0 spuriously set in bitmap — "
                          << "stale tailUsed race bug triggered (Scenario 2)";
        }
    }

    EXPECT_FALSE(spuriousRow0.load())
        << "Row 0 was spuriously marked deleted by GC processing "
           "zero-initialised slots of a newly created tail block.";
}

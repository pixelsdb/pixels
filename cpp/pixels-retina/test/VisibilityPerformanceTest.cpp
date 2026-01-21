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

#include "gtest/gtest.h"
#include "RGVisibility.h"

#include <chrono>
#include <vector>
#include <thread>
#include <atomic>
#include <random>
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <numeric>

// Ensure RETINA_CAPACITY is defined for testing
#ifndef RETINA_CAPACITY
#define RETINA_CAPACITY 256
#endif

// Using the pre-defined alias from your header files
// If not defined elsewhere, ensure: using RGVisibilityInstance = RGVisibility<RETINA_CAPACITY>;

// Configurable test parameters
#ifndef PERF_SMALL_DATASET
#define PERF_SMALL_DATASET 1024
#endif

#ifndef PERF_MEDIUM_DATASET
#define PERF_MEDIUM_DATASET 102400
#endif

#ifndef PERF_LARGE_DATASET
#define PERF_LARGE_DATASET 1024000
#endif

#ifndef PERF_NUM_OPERATIONS
#define PERF_NUM_OPERATIONS 10000
#endif

#ifndef PERF_NUM_THREADS
#define PERF_NUM_THREADS 8
#endif

using namespace std::chrono;

// Performance statistics helper class
class PerfStats {
public:
    void addSample(double microseconds) {
        samples.push_back(microseconds);
    }

    void calculate() {
        if (samples.empty()) return;

        std::sort(samples.begin(), samples.end());

        min = samples.front();
        max = samples.back();
        avg = std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size();

        p50 = samples[samples.size() * 50 / 100];
        p95 = samples[samples.size() * 95 / 100];
        p99 = samples[samples.size() * 99 / 100];
    }

    void print(const std::string& name) const {
        std::cout << "\n=== " << name << " (Capacity: " << RETINA_CAPACITY << ") ===" << std::endl;
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "  Count:   " << samples.size() << std::endl;
        std::cout << "  Min:     " << min << " μs" << std::endl;
        std::cout << "  Max:     " << max << " μs" << std::endl;
        std::cout << "  Average: " << avg << " μs" << std::endl;
        std::cout << "  P50:     " << p50 << " μs" << std::endl;
        std::cout << "  P95:     " << p95 << " μs" << std::endl;
        std::cout << "  P99:     " << p99 << " μs" << std::endl;

        if (!samples.empty()) {
            double totalSeconds = std::accumulate(samples.begin(), samples.end(), 0.0) / 1000000.0;
            double throughput = samples.size() / totalSeconds;
            std::cout << "  Throughput: " << std::fixed << std::setprecision(0)
                      << throughput << " ops/sec" << std::endl;
        }
    }

    double getAvg() const { return avg; }
    size_t getCount() const { return samples.size(); }

private:
    std::vector<double> samples;
    double min = 0, max = 0, avg = 0, p50 = 0, p95 = 0, p99 = 0;
};

// Performance test base class
class VisibilityPerfTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::cout << "\n" << std::string(80, '=') << std::endl;
    }

    void TearDown() override {
        std::cout << std::string(80, '=') << std::endl;
    }

    // Timing helper function
    template<typename Func>
    double measureMicroseconds(Func&& func) {
        auto start = high_resolution_clock::now();
        func();
        auto end = high_resolution_clock::now();
        return duration_cast<microseconds>(end - start).count();
    }
};

// ============================================================================
// Single-threaded performance tests
// ============================================================================

TEST_F(VisibilityPerfTest, DeleteRecordPerformance_SmallDataset) {
    const uint64_t ROW_COUNT = PERF_SMALL_DATASET;
    const uint64_t NUM_OPS = std::min((uint64_t)PERF_NUM_OPERATIONS, ROW_COUNT);

    std::cout << "Testing deleteRGRecord() performance with " << ROW_COUNT << " rows" << std::endl;

    RGVisibilityInstance* rgVisibility = new RGVisibilityInstance(ROW_COUNT);
    PerfStats stats;

    for (uint64_t i = 0; i < NUM_OPS; i++) {
        uint64_t timestamp = i + 1;
        uint32_t rowId = i % ROW_COUNT;

        double elapsed = measureMicroseconds([&]() {
            rgVisibility->deleteRGRecord(rowId, timestamp);
        });

        stats.addSample(elapsed);
    }

    stats.calculate();
    stats.print("deleteRGRecord() - Small Dataset");

    delete rgVisibility;
}

TEST_F(VisibilityPerfTest, DeleteRecordPerformance_MediumDataset) {
    const uint64_t ROW_COUNT = PERF_MEDIUM_DATASET;
    const uint64_t NUM_OPS = std::min((uint64_t)PERF_NUM_OPERATIONS, ROW_COUNT);

    std::cout << "Testing deleteRGRecord() performance with " << ROW_COUNT << " rows" << std::endl;

    RGVisibilityInstance* rgVisibility = new RGVisibilityInstance(ROW_COUNT);
    PerfStats stats;

    for (uint64_t i = 0; i < NUM_OPS; i++) {
        uint64_t timestamp = i + 1;
        uint32_t rowId = i % ROW_COUNT;

        double elapsed = measureMicroseconds([&]() {
            rgVisibility->deleteRGRecord(rowId, timestamp);
        });

        stats.addSample(elapsed);
    }

    stats.calculate();
    stats.print("deleteRGRecord() - Medium Dataset");

    delete rgVisibility;
}

TEST_F(VisibilityPerfTest, DeleteRecordPerformance_LargeDataset) {
    const uint64_t ROW_COUNT = PERF_LARGE_DATASET;
    const uint64_t NUM_OPS = std::min((uint64_t)PERF_NUM_OPERATIONS, ROW_COUNT);

    std::cout << "Testing deleteRGRecord() performance with " << ROW_COUNT << " rows" << std::endl;

    RGVisibilityInstance* rgVisibility = new RGVisibilityInstance(ROW_COUNT);
    PerfStats stats;

    for (uint64_t i = 0; i < NUM_OPS; i++) {
        uint64_t timestamp = i + 1;
        uint32_t rowId = i % ROW_COUNT;

        double elapsed = measureMicroseconds([&]() {
            rgVisibility->deleteRGRecord(rowId, timestamp);
        });

        stats.addSample(elapsed);
    }

    stats.calculate();
    stats.print("deleteRGRecord() - Large Dataset");

    delete rgVisibility;
}

TEST_F(VisibilityPerfTest, GetBitmapPerformance) {
    const uint64_t ROW_COUNT = PERF_MEDIUM_DATASET;
    const uint64_t NUM_DELETES = 1000;

    std::cout << "Testing getRGVisibilityBitmap() performance" << std::endl;

    RGVisibilityInstance* rgVisibility = new RGVisibilityInstance(ROW_COUNT);

    // Perform some delete operations first
    for (uint64_t i = 0; i < NUM_DELETES; i++) {
        rgVisibility->deleteRGRecord(i, i + 1);
    }

    PerfStats stats;

    for (int i = 0; i < PERF_NUM_OPERATIONS; i++) {
        uint64_t timestamp = (i % NUM_DELETES) + 1;

        double elapsed = measureMicroseconds([&]() {
            uint64_t* bitmap = rgVisibility->getRGVisibilityBitmap(timestamp);
            delete[] bitmap;
        });

        stats.addSample(elapsed);
    }

    stats.calculate();
    stats.print("getRGVisibilityBitmap()");

    delete rgVisibility;
}

TEST_F(VisibilityPerfTest, CollectGarbagePerformance) {
    const uint64_t ROW_COUNT = PERF_MEDIUM_DATASET;
    const uint64_t NUM_DELETES = 10000;

    std::cout << "Testing collectRGGarbage() performance" << std::endl;

    RGVisibilityInstance* rgVisibility = new RGVisibilityInstance(ROW_COUNT);

    // Perform many delete operations first
    for (uint64_t i = 0; i < NUM_DELETES; i++) {
        rgVisibility->deleteRGRecord(i % ROW_COUNT, i + 1);
    }

    PerfStats stats;

    for (int i = 0; i < 100; i++) {
        uint64_t gcTimestamp = (i + 1) * 100;

        double elapsed = measureMicroseconds([&]() {
            rgVisibility->collectRGGarbage(gcTimestamp);
        });

        stats.addSample(elapsed);
    }

    stats.calculate();
    stats.print("collectRGGarbage()");

    delete rgVisibility;
}

// ============================================================================
// Multi-threaded performance tests
// ============================================================================

TEST_F(VisibilityPerfTest, ConcurrentDeletePerformance) {
    const uint64_t ROW_COUNT = PERF_MEDIUM_DATASET;
    const int NUM_THREADS = PERF_NUM_THREADS;
    const int OPS_PER_THREAD = PERF_NUM_OPERATIONS / NUM_THREADS;

    std::cout << "Testing concurrent deleteRGRecord() with "
              << NUM_THREADS << " threads" << std::endl;

    RGVisibilityInstance* rgVisibility = new RGVisibilityInstance(ROW_COUNT);
    std::vector<PerfStats> threadStats(NUM_THREADS);
    std::atomic<uint64_t> timestamp{0};

    auto start = high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&, t]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<uint32_t> dist(0, ROW_COUNT - 1);

            for (int i = 0; i < OPS_PER_THREAD; i++) {
                uint64_t ts = timestamp.fetch_add(1) + 1;
                uint32_t rowId = dist(gen);

                auto opStart = high_resolution_clock::now();
                rgVisibility->deleteRGRecord(rowId, ts);
                auto opEnd = high_resolution_clock::now();

                double elapsed = duration_cast<microseconds>(opEnd - opStart).count();
                threadStats[t].addSample(elapsed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end = high_resolution_clock::now();
    double totalTime = duration_cast<microseconds>(end - start).count();

    // Calculate statistics for all threads
    for (auto& stats : threadStats) {
        stats.calculate();
    }

    std::cout << "\nConcurrent Delete Performance:" << std::endl;
    std::cout << "  Threads:        " << NUM_THREADS << std::endl;
    std::cout << "  Total Ops:      " << NUM_THREADS * OPS_PER_THREAD << std::endl;
    std::cout << "  Total Time:     " << std::fixed << std::setprecision(2)
              << totalTime / 1000.0 << " ms" << std::endl;
    std::cout << "  Throughput:     " << std::fixed << std::setprecision(0)
              << (NUM_THREADS * OPS_PER_THREAD) / (totalTime / 1000000.0)
              << " ops/sec" << std::endl;

    for (int t = 0; t < NUM_THREADS; t++) {
        std::cout << "  Thread " << t << " - Avg latency: "
                  << std::fixed << std::setprecision(2)
                  << threadStats[t].getAvg() << " μs" << std::endl;
    }

    delete rgVisibility;
}

TEST_F(VisibilityPerfTest, ConcurrentReadPerformance) {
    const uint64_t ROW_COUNT = PERF_MEDIUM_DATASET;
    const int NUM_THREADS = PERF_NUM_THREADS;
    const int OPS_PER_THREAD = PERF_NUM_OPERATIONS / NUM_THREADS;

    std::cout << "Testing concurrent getRGVisibilityBitmap() with "
              << NUM_THREADS << " threads" << std::endl;

    RGVisibilityInstance* rgVisibility = new RGVisibilityInstance(ROW_COUNT);

    // Perform some delete operations in advance
    const uint64_t NUM_DELETES = 1000;
    for (uint64_t i = 0; i < NUM_DELETES; i++) {
        rgVisibility->deleteRGRecord(i % ROW_COUNT, i + 1);
    }

    std::vector<PerfStats> threadStats(NUM_THREADS);

    auto start = high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&, t]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<uint64_t> dist(1, NUM_DELETES);

            for (int i = 0; i < OPS_PER_THREAD; i++) {
                uint64_t ts = dist(gen);

                auto opStart = high_resolution_clock::now();
                uint64_t* bitmap = rgVisibility->getRGVisibilityBitmap(ts);
                delete[] bitmap;
                auto opEnd = high_resolution_clock::now();

                double elapsed = duration_cast<microseconds>(opEnd - opStart).count();
                threadStats[t].addSample(elapsed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end = high_resolution_clock::now();
    double totalTime = duration_cast<microseconds>(end - start).count();

    for (auto& stats : threadStats) {
        stats.calculate();
    }

    std::cout << "\nConcurrent Read Performance:" << std::endl;
    std::cout << "  Threads:        " << NUM_THREADS << std::endl;
    std::cout << "  Total Ops:      " << NUM_THREADS * OPS_PER_THREAD << std::endl;
    std::cout << "  Total Time:     " << std::fixed << std::setprecision(2)
              << totalTime / 1000.0 << " ms" << std::endl;
    std::cout << "  Throughput:     " << std::fixed << std::setprecision(0)
              << (NUM_THREADS * OPS_PER_THREAD) / (totalTime / 1000000.0)
              << " ops/sec" << std::endl;

    for (int t = 0; t < NUM_THREADS; t++) {
        std::cout << "  Thread " << t << " - Avg latency: "
                  << std::fixed << std::setprecision(2)
                  << threadStats[t].getAvg() << " μs" << std::endl;
    }

    delete rgVisibility;
}

TEST_F(VisibilityPerfTest, MixedWorkloadPerformance) {
    const uint64_t ROW_COUNT = PERF_MEDIUM_DATASET;
    const int NUM_WRITE_THREADS = PERF_NUM_THREADS / 2;
    const int NUM_READ_THREADS = PERF_NUM_THREADS / 2;
    const int OPS_PER_THREAD = PERF_NUM_OPERATIONS / PERF_NUM_THREADS;

    std::cout << "Testing mixed read/write workload with "
              << NUM_WRITE_THREADS << " writers and "
              << NUM_READ_THREADS << " readers" << std::endl;

    RGVisibilityInstance* rgVisibility = new RGVisibilityInstance(ROW_COUNT);
    std::atomic<uint64_t> timestamp{0};

    std::vector<PerfStats> writeStats(NUM_WRITE_THREADS);
    std::vector<PerfStats> readStats(NUM_READ_THREADS);

    auto start = high_resolution_clock::now();

    // Start write threads
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_WRITE_THREADS; t++) {
        threads.emplace_back([&, t]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<uint32_t> dist(0, ROW_COUNT - 1);

            for (int i = 0; i < OPS_PER_THREAD; i++) {
                uint64_t ts = timestamp.fetch_add(1) + 1;
                uint32_t rowId = dist(gen);

                auto opStart = high_resolution_clock::now();
                rgVisibility->deleteRGRecord(rowId, ts);
                auto opEnd = high_resolution_clock::now();

                double elapsed = duration_cast<microseconds>(opEnd - opStart).count();
                writeStats[t].addSample(elapsed);
            }
        });
    }

    // Start read threads
    for (int t = 0; t < NUM_READ_THREADS; t++) {
        threads.emplace_back([&, t]() {
            std::random_device rd;
            std::mt19937 gen(rd());

            for (int i = 0; i < OPS_PER_THREAD; i++) {
                uint64_t maxTs = timestamp.load();
                if (maxTs == 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(10));
                    continue;
                }

                std::uniform_int_distribution<uint64_t> dist(1, maxTs);
                uint64_t ts = dist(gen);

                auto opStart = high_resolution_clock::now();
                uint64_t* bitmap = rgVisibility->getRGVisibilityBitmap(ts);
                delete[] bitmap;
                auto opEnd = high_resolution_clock::now();

                double elapsed = duration_cast<microseconds>(opEnd - opStart).count();
                readStats[t].addSample(elapsed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end = high_resolution_clock::now();
    double totalTime = duration_cast<microseconds>(end - start).count();

    // Calculate statistics
    for (auto& stats : writeStats) {
        stats.calculate();
    }
    for (auto& stats : readStats) {
        stats.calculate();
    }

    std::cout << "\nMixed Workload Performance:" << std::endl;
    std::cout << "  Write Threads:  " << NUM_WRITE_THREADS << std::endl;
    std::cout << "  Read Threads:   " << NUM_READ_THREADS << std::endl;
    std::cout << "  Total Time:     " << std::fixed << std::setprecision(2)
              << totalTime / 1000.0 << " ms" << std::endl;

    size_t totalWrites = 0, totalReads = 0;
    for (auto& stats : writeStats) {
        totalWrites += stats.getCount();
    }
    for (auto& stats : readStats) {
        totalReads += stats.getCount();
    }

    std::cout << "  Total Writes:   " << totalWrites << std::endl;
    std::cout << "  Total Reads:    " << totalReads << std::endl;
    std::cout << "  Write Throughput: " << std::fixed << std::setprecision(0)
              << totalWrites / (totalTime / 1000000.0) << " ops/sec" << std::endl;
    std::cout << "  Read Throughput:  " << std::fixed << std::setprecision(0)
              << totalReads / (totalTime / 1000000.0) << " ops/sec" << std::endl;

    delete rgVisibility;
}

// ============================================================================
// Scalability tests - Testing scalability with different thread counts
// ============================================================================

TEST_F(VisibilityPerfTest, ScalabilityTest) {
    const uint64_t ROW_COUNT = PERF_MEDIUM_DATASET;
    const int TOTAL_OPS = 50000;

    std::cout << "Testing scalability with different thread counts" << std::endl;
    std::cout << std::string(60, '-') << std::endl;
    std::cout << std::setw(10) << "Threads"
              << std::setw(15) << "Time (ms)"
              << std::setw(20) << "Throughput (ops/s)"
              << std::setw(15) << "Speedup" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    double baselineTime = 0;

    for (int numThreads : {1, 2, 4, 8, 16}) {
        RGVisibilityInstance* rgVisibility = new RGVisibilityInstance(ROW_COUNT);
        std::atomic<uint64_t> timestamp{0};

        int opsPerThread = TOTAL_OPS / numThreads;

        auto start = high_resolution_clock::now();

        std::vector<std::thread> threads;
        for (int t = 0; t < numThreads; t++) {
            threads.emplace_back([&]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<uint32_t> dist(0, ROW_COUNT - 1);

                for (int i = 0; i < opsPerThread; i++) {
                    uint64_t ts = timestamp.fetch_add(1) + 1;
                    uint32_t rowId = dist(gen);
                    rgVisibility->deleteRGRecord(rowId, ts);
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        auto end = high_resolution_clock::now();
        double totalTime = duration_cast<microseconds>(end - start).count();
        double throughput = (numThreads * opsPerThread) / (totalTime / 1000000.0);

        if (numThreads == 1) {
            baselineTime = totalTime;
        }

        double speedup = baselineTime / totalTime;

        std::cout << std::setw(10) << numThreads
                  << std::setw(15) << std::fixed << std::setprecision(2) << totalTime / 1000.0
                  << std::setw(20) << std::fixed << std::setprecision(0) << throughput
                  << std::setw(15) << std::fixed << std::setprecision(2) << speedup << "x"
                  << std::endl;

        delete rgVisibility;
    }

    std::cout << std::string(60, '-') << std::endl;
}
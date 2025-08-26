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
 * @create 2023-03-07
 */
#ifndef PIXELS_PIXELSRECORDREADERIMPL_H
#define PIXELS_PIXELSRECORDREADERIMPL_H

#include "PixelsRecordReader.h"
#include "physical/PhysicalReader.h"
#include "vector/VectorizedRowBatch.h"
#include "physical/Scheduler.h"
#include "physical/SchedulerFactory.h"
#include "pixels-common/pixels.pb.h"
#include "PixelsFooterCache.h"
#include "reader/PixelsReaderOption.h"
#include "utils/String.h"
#include "TypeDescription.h"
#include "reader/ColumnReader.h"
#include "reader/ColumnReaderBuilder.h"
#include "profiler/TimeProfiler.h"
#include "physical/BufferPool.h"
#include "physical/natives/DirectUringRandomAccessFile.h"
#include "PixelsFilter.h"

class ChunkId
{
public:
    uint32_t rowGroupId;
    uint32_t columnId;
    uint64_t offset;
    uint64_t length;

    ChunkId() = default;

    ChunkId(int rgId, int cId, uint64_t off, uint64_t len)
    {
        rowGroupId = rgId;
        columnId = cId;
        offset = off;
        length = len;
    }
};

class PixelsRecordReaderImpl : public PixelsRecordReader
{
public:
    explicit PixelsRecordReaderImpl(std::shared_ptr <PhysicalReader> reader,
                                    const pixels::proto::PostScript &pixelsPostScript,
                                    const pixels::proto::Footer &pixelsFooter,
                                    const PixelsReaderOption &opt,
                                    std::shared_ptr <PixelsFooterCache> pixelsFooterCache
    );

    void asyncReadComplete(int requestSize);

    std::shared_ptr <VectorizedRowBatch> readBatch(bool reuse) override;

    std::shared_ptr <TypeDescription> getResultSchema() override;

    bool read();

    std::shared_ptr <PixelsBitMask> getFilterMask();

    bool isEndOfFile() override;

    ~PixelsRecordReaderImpl();

    void close() override;

private:
    std::vector <int64_t> bufferIds;

    void prepareRead();

    void checkBeforeRead();

    std::shared_ptr <VectorizedRowBatch> createEmptyEOFRowBatch(int size);

    void UpdateRowGroupInfo();


    static std::mutex mutex_;
    std::shared_ptr <PhysicalReader> physicalReader;
    pixels::proto::Footer footer;
    pixels::proto::PostScript postScript;
    std::shared_ptr <PixelsFooterCache> footerCache;
    PixelsReaderOption option;
    duckdb::TableFilterSet *filter;
    long queryId;
    int ring_index;
    int RGStart;
    int RGLen;
    bool everRead;
    bool everPrepareRead;
    int targetRGNum;
    int curRGIdx;
    int curRowInRG;
    int batchSize;
    int curRowInStride;
    std::string fileName;
    bool endOfFile;
    int curRGRowCount;
    bool enabledFilterPushDown;
    std::shared_ptr <PixelsBitMask> filterMask;
    std::shared_ptr <pixels::proto::RowGroupFooter> curRGFooter;
    std::vector <std::shared_ptr<pixels::proto::ColumnEncoding>> curEncoding;
    std::vector<int> curChunkBufferIndex;
    std::vector <std::shared_ptr<pixels::proto::ColumnChunkIndex>> curChunkIndex;
    /**
     * Columns included by reader option; if included, set true
     */
    std::vector<bool> includedColumns;
    /**
     * Target row groups to read after matching reader option,
     * each element represents a row group id.
     */
    std::vector<int> targetRGs;

    // buffers of each chunk in this file, arranged by chunk's row group id and column id
    std::vector <std::shared_ptr<ByteBuffer>> chunkBuffers;
    // column readers for each target columns
    std::vector <std::shared_ptr<ColumnReader>> readers;
    std::vector <uint32_t> targetColumns;
    std::vector <uint32_t> resultColumns;
    std::vector<bool> resultColumnsEncoded;
    bool enableEncodedVector;
    std::vector <std::shared_ptr<pixels::proto::RowGroupFooter>> rowGroupFooters;

    int includedColumnNum; // the number of columns to read
    std::vector <std::shared_ptr<pixels::proto::Type>> includedColumnTypes;

    std::shared_ptr <TypeDescription> fileSchema;
    std::shared_ptr <TypeDescription> resultSchema;
    std::shared_ptr <VectorizedRowBatch> resultRowBatch;
    int asyncReadRequestNum{0};
};
#endif //PIXELS_PIXELSRECORDREADERIMPL_H

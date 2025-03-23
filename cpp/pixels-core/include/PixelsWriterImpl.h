/*
 * Copyright 2024 PixelsDB.
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

//
// Created by gengdy on 24-11-25.
//

#ifndef PIXELS_PIXELSWRITERIMPL_H
#define PIXELS_PIXELSWRITERIMPL_H

#include "PixelsWriter.h"
#include "physical/PhysicalWriter.h"
#include "writer/PixelsWriterOption.h"
#include "writer/ColumnWriter.h"
#include "utils/ConfigFactory.h"
#include "stats/StatsRecorder.h"
#include "pixels-common/pixels.pb.h"
#include "vector/VectorizedRowBatch.h"
#include <unicode/timezone.h>
#include <unicode/unistr.h>
#include <unicode/locid.h>

class PixelsWriterImpl : public PixelsWriter {
public:
    PixelsWriterImpl(std::shared_ptr<TypeDescription> schema, int pixelsStride, int rowGroupSize,
                     const std::string &targetFilePath, int blockSize, bool blockPadding,
                     EncodingLevel encodingLevel, bool nullsPadding,bool partitioned, int compressionBlockSize);
    bool addRowBatch(std::shared_ptr<VectorizedRowBatch> rowBatch) override;
    void writeColumnVectors(std::vector<std::shared_ptr<ColumnVector>> &columnVectors, int rowBatchSize);
    void writeRowGroup();
    void writeFileTail();
    void close() override;

private:
    /**
     * The number of bytes that the start offset of each column chunk is aligned to.
     */
    static const int CHUNK_ALIGNMENT;
    /**
     * The byte buffer padded to each column chunk for alignment.
     */
    static const std::vector<uint8_t> CHUNK_PADDING_BUFFER;

    std::shared_ptr<TypeDescription> schema;
    int rowGroupSize;
    pixels::proto::CompressionKind compressionKind;
    int compressionBlockSize;
    // std::unique_ptr<icu::TimeZone> timeZone;
    std::shared_ptr<PixelsWriterOption> columnWriterOption;
    std::vector<std::shared_ptr<ColumnWriter>> columnWriters;
    std::vector<StatsRecorder> fileColStatRecorders;
    std::int64_t fileContentLength;
    int fileRowNum;
    std::int64_t writtenBytes = 0;
    std::int64_t curRowGroupOffset = 0;
    std::int64_t curRowGroupFooterOffset = 0;
    std::int64_t curRowGroupNumOfRows = 0;
    int curRowGroupDataLength = 0;
    bool haseValueIsSet = false;
    int currHashValue = 0;
    bool partitioned;
    std::vector<pixels::proto::RowGroupInformation> rowGroupInfoList;
    std::vector<pixels::proto::RowGroupStatistic> rowGroupStatisticList;
    std::shared_ptr<PhysicalWriter> physicalWriter;
    std::vector<std::shared_ptr<TypeDescription>> children;

};
#endif //PIXELS_PIXELSWRITERIMPL_H

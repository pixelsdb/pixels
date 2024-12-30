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

/*
 * @author gengdy
 * @create 2024-11-9
 */
#ifndef PIXELS_COLUMNWRITER_H
#define PIXELS_COLUMNWRITER_H

#include "TypeDescription.h"
#include "physical/natives/ByteBuffer.h"
#include "pixels-common/pixels.pb.h"
#include <cmath>
#include <cmath>
#include "duckdb.h"
#include "duckdb/common/types/vector.hpp"
#include "PixelsFilter.h"
#include "writer/PixelsWriterOption.h"
#include "stats/StatsRecorder.h"


class ColumnWriter
{
public:
    ColumnWriter(std::shared_ptr <TypeDescription> type, std::shared_ptr <PixelsWriterOption> writerOption);
//    virtual ~ColumnWriter() = default;
    /**
     * Write values from input buffers
     *
     */
    virtual int write(std::shared_ptr <ColumnVector> columnVector, int length) = 0;

    virtual std::vector <uint8_t> getColumnChunkContent() const;

    virtual int getColumnChunkSize() const;

    virtual bool decideNullsPadding(std::shared_ptr <PixelsWriterOption> writerOption) = 0;

    virtual pixels::proto::ColumnChunkIndex getColumnChunkIndex();

    virtual std::shared_ptr <pixels::proto::ColumnChunkIndex> getColumnChunkIndexPtr();

    virtual pixels::proto::ColumnEncoding getColumnChunkEncoding();

    virtual void reset();

    virtual void flush();

    virtual void close();

    // virtual
    virtual void newPixel();

private:
    static const int ISNULL_ALIGNMENT;
    static const std::vector <uint8_t> ISNULL_PADDING_BUFFER;

    std::shared_ptr <pixels::proto::ColumnChunkIndex> columnChunkIndex{};
    std::shared_ptr <pixels::proto::ColumnStatistic> columnChunkStat{};

    int lastPixelPosition = 0;
    int curPixelPosition = 0;

    std::shared_ptr <ByteBuffer> isNullStream;
protected:
    const int pixelStride;
    const EncodingLevel encodingLevel;
    int curPixelIsNullIndex = 0;
    std::shared_ptr <ByteBuffer> outputStream;
    int curPixelEleIndex = 0;
//std::unique_ptr<Encoder> encoder;
    StatsRecorder pixelStatRecorder;
    StatsRecorder columnChunkStatRecorder;
    bool hasNull = false;
    const bool nullsPadding;
    int curPixelVectorIndex = 0;
    const ByteOrder byteOrder;
    std::vector<bool> isNull{};
};
#endif //PIXELS_COLUMNWRITER_H

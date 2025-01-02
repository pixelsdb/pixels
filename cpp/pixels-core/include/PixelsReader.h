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
 * @create 2023-03-06
 */
#ifndef PIXELS_PIXELSREADER_H
#define PIXELS_PIXELSREADER_H

/**
 * Pixels file reader.
 * This interface is for reading pixels content as
 * {@link VectorizedRowBatch}.
 *
 * @author liangyong
 */

#include "physical/storage/LocalFS.h"
#include "physical/PhysicalReader.h"
#include "reader/PixelsRecordReader.h"
#include "reader/PixelsReaderOption.h"
#include "PixelsVersion.h"

typedef ::google::protobuf::RepeatedPtrField<::pixels::proto::ColumnStatistic>
        ColumnStatisticList;

typedef ::google::protobuf::RepeatedPtrField<::pixels::proto::RowGroupInformation>
        RowGroupInfoList;

typedef ::google::protobuf::RepeatedPtrField<::pixels::proto::RowGroupStatistic>
        RowGroupStatList;

class PixelsReader
{
public:
    /**
     * Get a <code>PixelsRecordReader</code>
     *
     * @return record reader
     */
    virtual std::shared_ptr <PixelsRecordReader> read(PixelsReaderOption option) = 0;

    virtual std::shared_ptr <TypeDescription> getFileSchema() = 0;

    virtual PixelsVersion::Version getFileVersion() = 0;

    virtual long getNumberOfRows() = 0;

    virtual pixels::proto::CompressionKind getCompressionKind() = 0;

    virtual long getCompressionBlockSize() = 0;

    virtual long getPixelStride() = 0;

    virtual std::string getWriterTimeZone() = 0;

    virtual int getRowGroupNum() = 0;

    virtual bool isPartitioned() = 0;

    virtual ColumnStatisticList getColumnStats() = 0;

    virtual pixels::proto::ColumnStatistic getColumnStat(std::string columnName) = 0;

    virtual RowGroupInfoList getRowGroupInfos() = 0;

    virtual pixels::proto::RowGroupInformation getRowGroupInfo(int rowGroupId) = 0;

    virtual pixels::proto::RowGroupStatistic getRowGroupStat(int rowGroupId) = 0;

    virtual RowGroupStatList getRowGroupStats() = 0;

    virtual void close() = 0;

};

#endif //PIXELS_PIXELSREADER_H

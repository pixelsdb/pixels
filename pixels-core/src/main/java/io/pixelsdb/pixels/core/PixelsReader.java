/*
 * Copyright 2017-2019 PixelsDB.
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
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Pixels file reader.
 * This interface is for reading pixels content as
 * {@link VectorizedRowBatch}.
 *
 * @author guodong
 */
public interface PixelsReader
        extends Closeable
{
    /**
     * Get a <code>PixelsRecordReader</code>
     *
     * @return record reader
     */
    PixelsRecordReader read(PixelsReaderOption option) throws IOException;

    /**
     * Get version of the Pixels file
     *
     * @return version number
     */
    PixelsVersion getFileVersion();

    /**
     * Get the number of rows of the file
     *
     * @return num of rows
     */
    long getNumberOfRows();

    /**
     * Get the compression codec used in this file
     *
     * @return compression codec
     */
    PixelsProto.CompressionKind getCompressionKind();

    /**
     * Get the compression block size
     *
     * @return compression block size
     */
    long getCompressionBlockSize();

    /**
     * Get the pixel stride
     *
     * @return pixel stride
     */
    long getPixelStride();

    /**
     * Get the writer's time zone
     *
     * @return time zone
     */
    String getWriterTimeZone();

    /**
     * Get schema of this file
     *
     * @return schema
     */
    TypeDescription getFileSchema();

    /**
     * Get the number of row groups in this file
     *
     * @return row group num
     */
    int getRowGroupNum();

    /**
     * Get file level statistics of each column
     *
     * @return array of column stat
     */
    List<PixelsProto.ColumnStatistic> getColumnStats();

    /**
     * Get file level statistic of the specified column
     *
     * @param columnName column name
     * @return column stat
     */
    PixelsProto.ColumnStatistic getColumnStat(String columnName);

    /**
     * Get row group footer
     *
     * @param rowGroupId row group id
     * @return row group footer
     * @throws java.io.IOException
     */
    PixelsProto.RowGroupFooter getRowGroupFooter(int rowGroupId)
            throws IOException;

    /**
     * Get information of all row groups
     *
     * @return array of row group information
     */
    List<PixelsProto.RowGroupInformation> getRowGroupInfos();

    /**
     * Get information of specified row group
     *
     * @param rowGroupId row group id
     * @return row group information
     */
    PixelsProto.RowGroupInformation getRowGroupInfo(int rowGroupId);

    /**
     * Get statistics of the specified row group
     *
     * @param rowGroupId row group id
     * @return row group statistics
     */
    PixelsProto.RowGroupStatistic getRowGroupStat(int rowGroupId);

    /**
     * Get statistics of all row groups
     *
     * @return row groups statistics
     */
    List<PixelsProto.RowGroupStatistic> getRowGroupStats();

    // Just for test
    public PixelsProto.PostScript getPostScript();


    // Just for test
    public PixelsProto.Footer getFooter();

    /**
     * Cleanup and release resources
     *
     * @throws java.io.IOException
     */
    @Override
    void close()
            throws IOException;
}

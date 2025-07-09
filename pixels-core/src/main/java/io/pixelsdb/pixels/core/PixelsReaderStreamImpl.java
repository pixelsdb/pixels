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
package io.pixelsdb.pixels.core;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.exception.PixelsFileMagicInvalidException;
import io.pixelsdb.pixels.core.exception.PixelsFileVersionInvalidException;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.reader.PixelsRecordReaderStreamImpl;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static io.pixelsdb.pixels.common.utils.Constants.FILE_MAGIC;

import static java.util.Objects.requireNonNull;

/**
 * Pixels stream reader default implementation.
 *
 * @author huasiy, jasha64
 */
@NotThreadSafe
public class PixelsReaderStreamImpl implements PixelsReader
{
    /**
     * The number of bytes that the start offset of each column chunk is aligned to.
     */
    private TypeDescription fileSchema;
    private final PhysicalReader physicalReader;
    private final PixelsStreamProto.StreamHeader streamHeader;
    private PixelsRecordReader recordReader = null;

    public PixelsReaderStreamImpl(String endpoint) throws Exception
    {
        throw new NotImplementedException();
    }

    public PixelsReaderStreamImpl(int port) throws Exception
    {
        throw new NotImplementedException();
    }

    public PixelsReaderStreamImpl(String endpoint, boolean partitioned, int numPartitions)
    {
        throw new NotImplementedException();
    }

    private PixelsReaderStreamImpl(TypeDescription fileSchema,
                                   PhysicalReader physicalReader,
                                   PixelsStreamProto.StreamHeader streamHeader)
    {
        this.fileSchema = fileSchema;
        this.physicalReader = physicalReader;
        this.streamHeader = streamHeader;
    }

    public static class Builder
    {
        private Storage builderStorage = null;
        private String builderPath = null;
        private TypeDescription builderSchema = null;

        private Builder()
        {
        }

        public Builder setStorage(Storage storage)
        {
            this.builderStorage = storage;
            return this;
        }

        public Builder setPath(String path)
        {
            this.builderPath = requireNonNull(path);
            return this;
        }

        public PixelsReader build()
                throws IllegalArgumentException, IOException
        {
            // check arguments
            if (builderStorage == null || builderPath == null)
            {
                throw new IllegalArgumentException("Missing argument to build PixelsReader");
            }
            PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(builderStorage, builderPath);
            // get stream header
            byte[] magic = new byte[FILE_MAGIC.length()];
            fsReader.readFully(magic);
            String fileMagic = new String(magic, StandardCharsets.US_ASCII);
            if (!fileMagic.contentEquals(FILE_MAGIC))
            {
                throw new PixelsFileMagicInvalidException(fileMagic);
            }
            int headerLength = 0;
            headerLength = fsReader.readInt(ByteOrder.BIG_ENDIAN);
            ByteBuffer streamHeaderBuffer = fsReader.readFully(headerLength);
            PixelsStreamProto.StreamHeader header = PixelsStreamProto.StreamHeader.parseFrom(streamHeaderBuffer);

            // check file MAGIC and file version
            int fileVersion = header.getVersion();
            fileMagic = header.getMagic();
            if (!PixelsVersion.matchVersion(fileVersion))
            {
                throw new PixelsFileVersionInvalidException(fileVersion);
            }
            if (!fileMagic.contentEquals(FILE_MAGIC))
            {
                throw new PixelsFileMagicInvalidException(fileMagic);
            }

            this.builderSchema = TypeDescription.createSchema(header.getTypesList());
            return new PixelsReaderStreamImpl(this.builderSchema, fsReader, header);
        }
    }

    public static Builder newBuilder() { return new Builder(); }

    /**
     * Get a <code>PixelsRecordReader</code>
     * Only support one record reader now.
     * @return record reader
     */
    @Override
    public PixelsRecordReader read(PixelsReaderOption option) throws IOException
    {
        if (recordReader != null)
        {
            throw new IOException("only one record reader is allowed");
        }
        recordReader = new PixelsRecordReaderStreamImpl(physicalReader, streamHeader, option);
        return recordReader;
    }

    /**
     * Get version of the Pixels file
     *
     * @return version number
     */
    @Override
    public PixelsVersion getFileVersion() { return PixelsVersion.from(this.streamHeader.getVersion()); }

    /**
     * Unsupported: In streaming mode, the number of rows cannot be determined in advance.
     */
    @Override
    public long getNumberOfRows()
    {
        throw new UnsupportedOperationException("getNumberOfRows is not supported in a stream");
    }

    /**
     * Get the compression codec used in this file. Currently unused and thus unsupported
     */
    @Override
    public PixelsProto.CompressionKind getCompressionKind()
    {
        throw new UnsupportedOperationException("getCompressionKind is currently not supported");
    }

    /**
     * Get the compression block size. Currently unused and thus unsupported
     */
    @Override
    public long getCompressionBlockSize()
    {
        throw new UnsupportedOperationException("getCompressionBlockSize is currently not supported");
    }

    /**
     * Get the pixel stride
     *
     * @return pixel stride
     */
    @Override
    public long getPixelStride()
    {
        return this.streamHeader.getPixelStride();
    }

    /**
     * Get the writer's time zone
     *
     * @return time zone
     */
    @Override
    public String getWriterTimeZone()
    {
        return this.streamHeader.getWriterTimezone();
    }

    @Override
    public TypeDescription getFileSchema()
    {
        return this.fileSchema;
    }

    /**
     * Unsupported: In streaming mode, the number of row groups in current stream cannot be determined in advance.
     */
    @Override
    public int getRowGroupNum()
    {
        throw new UnsupportedOperationException("getRowGroupNum is not supported in a stream");
    }

    @Override
    public boolean isPartitioned() { return this.streamHeader.hasPartitioned() && this.streamHeader.getPartitioned(); }

    /**
     * Get file level statistics of each column. Not required in streaming mode
     */
    @Override
    public List<PixelsProto.ColumnStatistic> getColumnStats()
    {
        throw new UnsupportedOperationException("getColumnStats is not supported in a stream");
    }

    /**
     * Get file level statistic of the specified column. Currently unused and unsupported
     */
    @Override
    public PixelsProto.ColumnStatistic getColumnStat(String columnName)
    {
        throw new UnsupportedOperationException("getColumnStat is not supported in a stream");
    }

    /**
     * Row group id is not used in PixelsReaderStreamImpl.
     */
    @Override
    public PixelsProto.RowGroupFooter getRowGroupFooter(int rowGroupId) throws IOException {
        throw new UnsupportedOperationException("getColumnStat is not supported in a stream");
    }

    /**
     * Get information of all row groups. Currently unused and unsupported
     */
    @Override
    public List<PixelsProto.RowGroupInformation> getRowGroupInfos()
    {
        throw new UnsupportedOperationException("getRowGroupInfos is not supported in a stream");
    }

    /**
     * Get information of specified row group. Currently unused and unsupported
     */
    @Override
    public PixelsProto.RowGroupInformation getRowGroupInfo(int rowGroupId)
    {
        throw new UnsupportedOperationException("getRowGroupInfo is not supported in a stream");
    }

    /**
     * Get statistics of the specified row group. Currently unused and unsupported
     */
    @Override
    public PixelsProto.RowGroupStatistic getRowGroupStat(int rowGroupId)
    {
        throw new UnsupportedOperationException("getRowGroupStat is not supported in a stream");
    }

    /**
     * Get statistics of all row groups. Currently unused and unsupported
     */
    @Override
    public List<PixelsProto.RowGroupStatistic> getRowGroupStats()
    {
        throw new UnsupportedOperationException("getRowGroupStats is not supported in a stream");
    }

    @Override
    public PixelsProto.PostScript getPostScript()
    {
        throw new UnsupportedOperationException("getPostScript is not supported in a stream");
    }

    @Override
    public PixelsProto.Footer getFooter()
    {
        throw new UnsupportedOperationException("getFooter is not supported in a stream");
    }

    /**
     * Cleanup and release resources
     *
     * @throws IOException
     */
    @Override
    public void close()
            throws IOException
    {
        recordReader.close();
        physicalReader.close();
        fileSchema = null;
        recordReader = null;
    }
}

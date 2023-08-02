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

import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.Constants;
import io.pixelsdb.pixels.core.exception.PixelsFileMagicInvalidException;
import io.pixelsdb.pixels.core.exception.PixelsFileVersionInvalidException;
import io.pixelsdb.pixels.core.exception.PixelsMetricsCollectProbOutOfRange;
import io.pixelsdb.pixels.core.exception.PixelsReaderException;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.reader.PixelsRecordReaderImpl;
import io.pixelsdb.pixels.core.utils.PixelsCoreConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static java.util.Objects.requireNonNull;

/**
 * Pixels file reader default implementation
 *
 * @author guodong
 * @author hank
 */
@NotThreadSafe
public class PixelsReaderImpl
        implements PixelsReader
{
    private static final Logger LOGGER = LogManager.getLogger(PixelsReaderImpl.class);

    private final TypeDescription fileSchema;
    private final PhysicalReader physicalReader;
    private final PixelsProto.PostScript postScript;
    private final PixelsProto.Footer footer;
    private final List<PixelsRecordReader> recordReaders;
    private final String metricsDir;
    private final float metricsCollectProb;
    private final boolean enableCache;
    private final List<String> cacheOrder;
    private final PixelsCacheReader pixelsCacheReader;
    private final PixelsFooterCache pixelsFooterCache;
    private final Random random;

    private PixelsReaderImpl(TypeDescription fileSchema,
                             PhysicalReader physicalReader,
                             PixelsProto.FileTail fileTail,
                             String metricsDir,
                             float metricsCollectProb,
                             boolean enableCache,
                             List<String> cacheOrder,
                             PixelsCacheReader pixelsCacheReader,
                             PixelsFooterCache pixelsFooterCache)
    {
        this.fileSchema = fileSchema;
        this.physicalReader = physicalReader;
        this.postScript = fileTail.getPostscript();
        this.footer = fileTail.getFooter();
        this.recordReaders = new LinkedList<>();
        this.metricsDir = metricsDir;
        this.metricsCollectProb = metricsCollectProb;
        this.enableCache = enableCache;
        this.cacheOrder = cacheOrder;
        this.pixelsCacheReader = pixelsCacheReader;
        this.pixelsFooterCache = pixelsFooterCache;
        this.random = new Random();
    }

    public static class Builder
    {
        private Storage builderStorage = null;
        private String builderPath = null;
        private List<String> builderCacheOrder = null;
        private TypeDescription builderSchema = null;
        private boolean builderEnableCache = false;
        private PixelsCacheReader builderPixelsCacheReader = null;
        private PixelsFooterCache builderPixelsFooterCache = null;

        private Builder()
        {
        }

        public Builder setStorage(Storage storage)
        {
            this.builderStorage = requireNonNull(storage);
            return this;
        }

        public Builder setPath(String path)
        {
            this.builderPath = requireNonNull(path);
            return this;
        }

        /**
         *
         * @param cacheOrder should not be null.
         * @return
         */
        public Builder setCacheOrder(List<String> cacheOrder)
        {
            this.builderCacheOrder = requireNonNull(cacheOrder);
            return this;
        }

        public Builder setEnableCache(boolean enableCache)
        {
            this.builderEnableCache = enableCache;
            return this;
        }

        public Builder setPixelsCacheReader(PixelsCacheReader pixelsCacheReader)
        {
            this.builderPixelsCacheReader = pixelsCacheReader;
            return this;
        }

        public Builder setPixelsFooterCache(PixelsFooterCache pixelsFooterCache)
        {
            this.builderPixelsFooterCache = pixelsFooterCache;
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
            // get PhysicalReader
            PhysicalReader fsReader = PhysicalReaderUtil.newPhysicalReader(builderStorage, builderPath);
            // try to get file tail from cache
            String filePath = fsReader.getPath();
            PixelsProto.FileTail fileTail = builderPixelsFooterCache.getFileTail(filePath);
            if (fileTail == null)
            {
                if (fsReader == null)
                {
                    LOGGER.error("Failed to create PhysicalReader");
                    throw new PixelsReaderException(
                            "Failed to create PixelsReader due to error of creating PhysicalReader");
                }
                // get FileTail
                long fileLen = fsReader.getFileLength();
                fsReader.seek(fileLen - Long.BYTES);
                long fileTailOffset = fsReader.readLong();
                int fileTailLength = (int) (fileLen - fileTailOffset - Long.BYTES);
                fsReader.seek(fileTailOffset);
                ByteBuffer fileTailBuffer = fsReader.readFully(fileTailLength);
                fileTail = PixelsProto.FileTail.parseFrom(fileTailBuffer);
                builderPixelsFooterCache.putFileTail(filePath, fileTail);
            }

            // check file MAGIC and file version
            PixelsProto.PostScript postScript = fileTail.getPostscript();
            int fileVersion = postScript.getVersion();
            String fileMagic = postScript.getMagic();
            if (!PixelsVersion.matchVersion(fileVersion))
            {
                throw new PixelsFileVersionInvalidException(fileVersion);
            }
            if (!fileMagic.contentEquals(Constants.MAGIC))
            {
                throw new PixelsFileMagicInvalidException(fileMagic);
            }

            builderSchema = TypeDescription.createSchema(fileTail.getFooter().getTypesList());

            // check metrics file
            PixelsCoreConfig coreConfig = new PixelsCoreConfig();
            String metricsDir = coreConfig.getMetricsDir();
//            File file = new File(metricsDir);
//            if (!file.isDirectory() || !file.exists()) {
//                throw new PixelsMetricsDirNotFoundException(metricsDir);
//            }

            // check metrics collect probability
            float metricCollectProb = coreConfig.getMetricsCollectProb();
            if (metricCollectProb > 1.0f || metricCollectProb < 0.0f)
            {
                throw new PixelsMetricsCollectProbOutOfRange(metricCollectProb);
            }

            // create a default PixelsReader
            return new PixelsReaderImpl(builderSchema, fsReader, fileTail, metricsDir, metricCollectProb,
                    builderEnableCache, builderCacheOrder, builderPixelsCacheReader,
                    builderPixelsFooterCache);
        }
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public PixelsProto.RowGroupFooter getRowGroupFooter(int rowGroupId)
            throws IOException
    {
        long footerOffset = footer.getRowGroupInfos(rowGroupId).getFooterOffset();
        int footerLength = footer.getRowGroupInfos(rowGroupId).getFooterLength();
        physicalReader.seek(footerOffset);
        ByteBuffer footer = physicalReader.readFully(footerLength);
        return PixelsProto.RowGroupFooter.parseFrom(footer);
    }

    /**
     * Get a <code>PixelsRecordReader</code>
     *
     * @return record reader
     */
    @Override
    public PixelsRecordReader read(PixelsReaderOption option) throws IOException
    {
        float diceValue = random.nextFloat();
        boolean enableMetrics = false;
        if (diceValue < metricsCollectProb)
        {
            enableMetrics = true;
        }
//        LOGGER.debug("create a recordReader with enableCache as " + enableCache);
        PixelsRecordReader recordReader = new PixelsRecordReaderImpl(physicalReader, postScript, footer, option,
                enableMetrics, metricsDir, enableCache, cacheOrder, pixelsCacheReader, pixelsFooterCache);
        recordReaders.add(recordReader);
        return recordReader;
    }

    /**
     * Get version of the Pixels file
     *
     * @return version number
     */
    @Override
    public PixelsVersion getFileVersion()
    {
        return PixelsVersion.from(this.postScript.getVersion());
    }

    /**
     * Get the number of rows of the file
     *
     * @return num of rows
     */
    @Override
    public long getNumberOfRows()
    {
        return this.postScript.getNumberOfRows();
    }

    /**
     * Get the compression codec used in this file
     *
     * @return compression codec
     */
    @Override
    public PixelsProto.CompressionKind getCompressionKind()
    {
        return this.postScript.getCompression();
    }

    /**
     * Get the compression block size
     *
     * @return compression block size
     */
    @Override
    public long getCompressionBlockSize()
    {
        return this.postScript.getCompressionBlockSize();
    }

    /**
     * Get the pixel stride
     *
     * @return pixel stride
     */
    @Override
    public long getPixelStride()
    {
        return this.postScript.getPixelStride();
    }

    /**
     * Get the writer's time zone
     *
     * @return time zone
     */
    @Override
    public String getWriterTimeZone()
    {
        return this.postScript.getWriterTimezone();
    }

    /**
     * Get schema of this file
     *
     * @return schema
     */
    @Override
    public TypeDescription getFileSchema()
    {
        return this.fileSchema;
    }

    /**
     * Get the number of row groups in this file
     *
     * @return row group num
     */
    @Override
    public int getRowGroupNum()
    {
        return this.footer.getRowGroupInfosCount();
    }

    @Override
    public boolean isPartitioned()
    {
        return this.footer.hasPartitioned() && this.footer.getPartitioned();
    }

    /**
     * Get file level statistics of each column
     *
     * @return array of column stat
     */
    @Override
    public List<PixelsProto.ColumnStatistic> getColumnStats()
    {
        return this.footer.getColumnStatsList();
    }

    /**
     * Get file level statistic of the specified column
     *
     * @param columnName column name
     * @return column stat
     */
    @Override
    public PixelsProto.ColumnStatistic getColumnStat(String columnName)
    {
        List<String> fieldNames = fileSchema.getFieldNames();
        int fieldId = fieldNames.indexOf(columnName);
        if (fieldId == -1)
        {
            return null;
        }
        return this.footer.getColumnStats(fieldId);
    }

    /**
     * Get information of all row groups
     *
     * @return array of row group information
     */
    @Override
    public List<PixelsProto.RowGroupInformation> getRowGroupInfos()
    {
        return this.footer.getRowGroupInfosList();
    }

    /**
     * Get information of specified row group
     *
     * @param rowGroupId row group id
     * @return row group information
     */
    @Override
    public PixelsProto.RowGroupInformation getRowGroupInfo(int rowGroupId)
    {
        if (rowGroupId < 0)
        {
            return null;
        }
        return this.footer.getRowGroupInfos(rowGroupId);
    }

    /**
     * Get statistics of the specified row group
     *
     * @param rowGroupId row group id
     * @return row group statistics
     */
    @Override
    public PixelsProto.RowGroupStatistic getRowGroupStat(int rowGroupId)
    {
        if (rowGroupId < 0)
        {
            return null;
        }
        return this.footer.getRowGroupStats(rowGroupId);
    }

    /**
     * Get statistics of all row groups
     *
     * @return row groups statistics
     */
    @Override
    public List<PixelsProto.RowGroupStatistic> getRowGroupStats()
    {
        return this.footer.getRowGroupStatsList();
    }

    public PixelsProto.PostScript getPostScript()
    {
        return postScript;
    }

    public PixelsProto.Footer getFooter()
    {
        return footer;
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
        for (PixelsRecordReader recordReader : recordReaders)
        {
            recordReader.close();
        }
        this.physicalReader.close();
        /* no need to close the pixelsCacheReader, because it is usually maintained
           as a global singleton instance and shared across different PixelsReaders.
         */

    }
}

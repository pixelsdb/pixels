/*
 * Copyright 2019 PixelsDB.
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
package io.pixelsdb.pixels.hive.common;

import io.pixelsdb.pixels.cache.MemoryMappedFile;
import io.pixelsdb.pixels.cache.PixelsCacheReader;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.*;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Contains factory methods to read or write PIXELS files.
 * refers to {@link org.apache.orc.OrcFile}
 *
 * <p>
 * Created at: 2018-12-12
 * Author: hank, tao
 */
public class PixelsRW
{
    private static Logger log = LogManager.getLogger(PixelsRW.class);
    private static ConfigFactory pixelsConf = ConfigFactory.Instance();
    private static PixelsCacheReader cacheReader = null;
    private static PixelsFooterCache footerCache = new PixelsFooterCache();

    protected PixelsRW()
    {
    }

    public static class ReaderOptions
    {
        private FileSystem fileSystem;
        private PixelsReaderOption option;
        private PixelsSplit split;
        private int batchSize;
        private List<Integer> pixelsIncluded;
        private List<Integer> hiveIncluded;
        private boolean readAllColumns;

        private ReaderOptions(Configuration conf, PixelsSplit split)
        {
            this.split = split;
            try
            {
                this.fileSystem = FileSystem.get(conf);
            } catch (IOException e)
            {
                this.fileSystem = null;
                log.error("failed to get file system.", e);
            }
            this.batchSize = Integer.parseInt(pixelsConf.getProperty("row.batch.size"));
            this.readAllColumns = ColumnProjectionUtils.isReadAllColumns(conf);
            this.option = new PixelsReaderOption();
            this.option.skipCorruptRecords(true);
            this.option.tolerantSchemaEvolution(true);
            this.option.rgRange(split.getRgStart(), split.getRgLen());
            String[] columns = ColumnProjectionUtils.getReadColumnNames(conf);
            this.option.includeCols(columns);

            this.hiveIncluded =  ColumnProjectionUtils.getReadColumnIDs(conf);
            // The column order in hive is not the same as the column order in pixels files.
            // So we have to generate pixelsIncluded from the pixels column order.
            this.pixelsIncluded = new ArrayList<>();
            if (!readAllColumns)
            {
                List<String> columnOrder = split.getOrder();
                Map<String, Integer> nameToOrder = new HashMap<>();
                for (int i = 0; i < columnOrder.size(); ++i)
                {
                    nameToOrder.put(columnOrder.get(i), i);
                }
                for (int i = 0; i < columns.length; ++i)
                {
                    if (nameToOrder.containsKey(columns[i]))
                    {
                        this.pixelsIncluded.add(nameToOrder.get(columns[i]));
                    }
                }
            }

            // if cache is enabled, create cache reader.
            if (split.isCacheEnabled() && cacheReader == null)
            {
                MemoryMappedFile cacheFile;
                MemoryMappedFile indexFile;
                try
                {
                    cacheFile = new MemoryMappedFile(
                            pixelsConf.getProperty("cache.location"),
                            Long.parseLong(pixelsConf.getProperty("cache.size")));
                } catch (Exception e)
                {
                    cacheFile = null;
                    log.error("failed to open pixels cache file.", e);
                }
                try
                {
                    indexFile = new MemoryMappedFile(
                            pixelsConf.getProperty("index.location"),
                            Long.parseLong(pixelsConf.getProperty("index.size")));
                } catch (Exception e)
                {
                    indexFile = null;
                    log.error("failed to open pixels cache index.", e);
                }
                cacheReader = PixelsCacheReader
                        .newBuilder()
                        .setCacheFile(cacheFile)
                        .setIndexFile(indexFile)
                        .build();
            }
        }

        public FileSystem getFileSystem()
        {
            return fileSystem;
        }

        public PixelsReaderOption getReaderOption()
        {
            return option;
        }

        public List<Integer> getPixelsIncluded()
        {
            return pixelsIncluded;
        }

        public List<Integer> getHiveIncluded()
        {
            return hiveIncluded;
        }

        public boolean isCacheEnabled() { return split.isCacheEnabled(); }

        public List<String> getCacheOrder() { return split.getCacheOrder(); }

        public List<String> getOrder() { return split.getOrder(); }

        public boolean isReadAllColumns()
        {
            return readAllColumns;
        }

        public int getBatchSize() { return batchSize; }
    }

    public static ReaderOptions readerOptions(Configuration conf, PixelsSplit split)
    {
        return new ReaderOptions(conf, split);
    }

    public static PixelsReader createReader(Path path,
                                            ReaderOptions options) throws IOException
    {
        boolean isCacheEnabled = options.isCacheEnabled();
        return PixelsReaderImpl.newBuilder()
                .setFS(options.getFileSystem())
                .setPath(path)
                .setEnableCache(isCacheEnabled)
                // cache order should not be null.
                .setCacheOrder(isCacheEnabled ? options.getCacheOrder() : new ArrayList<>(0))
                .setPixelsCacheReader(isCacheEnabled ? cacheReader : null)
                // currently, the footerCache lifetime is hive-cli session wide.
                .setPixelsFooterCache(footerCache)
                .build();
    }

    /**
     * Options for creating PIXELS file writers.
     */
    public static class WriterOptions implements Cloneable
    {
        private final Configuration configuration;
        private FileSystem fileSystemValue = null;
        private TypeDescription schema = null;
        private long stripeSizeValue;
        private long blockSizeValue;
        private int rowIndexStrideValue;
        private short blockReplication;
        private boolean blockPaddingValue;
        private boolean encodingStrategy;
        private int compressionStrategy;

        protected WriterOptions(Properties tableProperties, Configuration conf)
        {
            configuration = conf;
            stripeSizeValue = PixelsConf.STRIPE_SIZE.getLong(tableProperties, conf);
            blockSizeValue = PixelsConf.BLOCK_SIZE.getLong(tableProperties, conf);
            rowIndexStrideValue =
                    (int) PixelsConf.ROW_INDEX_STRIDE.getLong(tableProperties, conf);
            blockReplication = (short) PixelsConf.BLOCK_REPLICATION.getLong(tableProperties,
                    conf);
            blockPaddingValue =
                    PixelsConf.BLOCK_PADDING.getBoolean(tableProperties, conf);
            encodingStrategy = PixelsConf.ENCODING_STRATEGY.getBoolean(tableProperties,
                    conf);
            compressionStrategy = (int) PixelsConf.COMPRESSION_STRATEGY.getLong(tableProperties, conf);
        }

        /**
         * @return a SHALLOW clone
         */
        public WriterOptions clone()
        {
            try
            {
                return (WriterOptions) super.clone();
            } catch (CloneNotSupportedException ex)
            {
                throw new AssertionError("Expected super.clone() to work");
            }
        }

        /**
         * Provide the filesystem for the path, if the client has it available.
         * If it is not provided, it will be found from the path.
         */
        public WriterOptions fileSystem(FileSystem value)
        {
            fileSystemValue = value;
            return this;
        }

        /**
         * Set the stripe size for the file. The writer stores the contents of the
         * stripe in memory until this memory limit is reached and the stripe
         * is flushed to the HDFS file and the next stripe started.
         */
        public WriterOptions stripeSize(long value)
        {
            stripeSizeValue = value;
            return this;
        }

        /**
         * Set the file system block size for the file. For optimal performance,
         * set the block size to be multiple factors of stripe size.
         */
        public WriterOptions blockSize(long value)
        {
            blockSizeValue = value;
            return this;
        }

        /**
         * Set the distance between entries in the row index. The minimum value is
         * 1000 to prevent the index from overwhelming the data. If the stride is
         * set to 0, no indexes will be included in the file.
         */
        public WriterOptions rowIndexStride(int value)
        {
            rowIndexStrideValue = value;
            return this;
        }

        /**
         * The size of the memory buffers used for compressing and storing the
         * stripe in memory. NOTE: PIXELS writer may choose to use smaller buffer
         * size based on stripe size and number of columns for efficient stripe
         * writing and memory utilization. To enforce writer to use the requested
         * buffer size use enforceBufferSize().
         */
        public WriterOptions blockReplication(short value)
        {
            blockReplication = value;
            return this;
        }

        /**
         * Sets whether the HDFS blocks are padded to prevent stripes from
         * straddling blocks. Padding improves locality and thus the speed of
         * reading, but costs space.
         */
        public WriterOptions blockPadding(boolean value)
        {
            blockPaddingValue = value;
            return this;
        }

        /**
         * Sets the encoding strategy that is used to encode the data.
         */
        public WriterOptions encodingStrategy(boolean strategy)
        {
            encodingStrategy = strategy;
            return this;
        }

        public WriterOptions compressionStrategy(int strategy)
        {
            compressionStrategy = strategy;
            return this;
        }

        /**
         * Set the schema for the file. This is a required parameter.
         *
         * @param schema the schema for the file.
         * @return this
         */
        public WriterOptions setSchema(TypeDescription schema)
        {
            this.schema = schema;
            return this;
        }

        public boolean getBlockPadding()
        {
            return blockPaddingValue;
        }

        public long getBlockSize()
        {
            return blockSizeValue;
        }

        public FileSystem getFileSystem()
        {
            return fileSystemValue;
        }

        public Configuration getConfiguration()
        {
            return configuration;
        }

        public TypeDescription getSchema()
        {
            return schema;
        }

        public long getStripeSize()
        {
            return stripeSizeValue;
        }

        public short getBlockReplication()
        {
            return blockReplication;
        }

        public int getRowIndexStride()
        {
            return rowIndexStrideValue;
        }

        public int getCompressionStrategy()
        {
            return compressionStrategy;
        }

        public boolean getEncodingStrategy()
        {
            return encodingStrategy;
        }

    }

    /**
     * Create a set of writer options based on a configuration.
     *
     * @param conf the configuration to use for values
     * @return A WriterOptions object that can be modified
     */
    public static WriterOptions writerOptions(Configuration conf)
    {
        return new WriterOptions(null, conf);
    }

    /**
     * Create a set of write options based on a set of table properties and
     * configuration.
     *
     * @param tableProperties the properties of the table
     * @param conf            the configuration of the query
     * @return a WriterOptions object that can be modified
     */
    public static WriterOptions writerOptions(Properties tableProperties,
                                              Configuration conf)
    {
        return new WriterOptions(tableProperties, conf);
    }

    /**
     * Create an PIXELS file writer. This is the public interface for creating
     * writers going forward and new options will only be added to this method.
     *
     * @param path filename to write to
     * @param opts the options
     * @return a new PIXELS file writer
     * @throws IOException
     */
    public static PixelsWriter createWriter(Path path,
                                            WriterOptions opts
    ) throws IOException
    {
        FileSystem fs = opts.getFileSystem() == null ?
                path.getFileSystem(opts.getConfiguration()) : opts.getFileSystem();
        return
                PixelsWriterImpl.newBuilder()
                        .setSchema(opts.schema)
                        .setPixelStride(opts.getRowIndexStride())
                        .setRowGroupSize(opts.getRowIndexStride())
                        .setFS(fs)
                        .setFilePath(path)
                        .setBlockSize(opts.getBlockSize())
                        .setReplication(opts.getBlockReplication())
                        .setBlockPadding(opts.getBlockPadding())
                        .setEncoding(opts.getEncodingStrategy())
                        .setCompressionBlockSize(opts.getCompressionStrategy())
                        .build();
    }

}

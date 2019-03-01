/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.ruc.iir.pixels.hive.core;

import cn.edu.ruc.iir.pixels.core.*;
import cn.edu.ruc.iir.pixels.core.reader.PixelsReaderOption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Contains factory methods to read or write PIXELS files.
 * refer: [OrcFile](https://github.com/apache/orc/blob/master/java/core/src/java/org/apache/orc/OrcFile.java)
 */
public class PixelsFile {
    private static Logger log = LogManager.getLogger(PixelsFile.class);

    protected PixelsFile() {
    }

    public static class ReaderOptions {
        private final Configuration conf;
        private FileSystem filesystem;
        private PixelsReaderOption option;
        private List<Integer> included;
        private long offset = 0L;
        private long length = 9223372036854775807L;

        // TODO: We can generalize FileMetada interface. Make OrcTail implement FileMetadata interface
        // and remove this class altogether. Both footer caching and llap caching just needs OrcTail.
        // For now keeping this around to avoid complex surgery

        public ReaderOptions(Configuration conf, FileSplit split) {
            this.conf = conf;
            try {
                this.filesystem = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.option = new PixelsReaderOption();
            this.option.skipCorruptRecords(true);
            this.option.tolerantSchemaEvolution(true);
//            this.option.rgRange((int) split.getStart(), (int) split.getLength());
            // todo 'includeCols' be a method
//            this.option.includeCols(new String[]{});
        }

        public ReaderOptions filesystem(FileSystem fs) {
            this.filesystem = fs;
            return this;
        }

        public Configuration getConfiguration() {
            return conf;
        }

        public FileSystem getFilesystem() {
            return filesystem;
        }

        public PixelsReaderOption getOption() {
            return option;
        }

        public ReaderOptions setOption(TypeDescription schema) {
            if (!ColumnProjectionUtils.isReadAllColumns(conf)) {
                included = ColumnProjectionUtils.getReadColumnIDs(conf);
                log.info("genIncludedColumns:" + included.toString());
            } else {
                log.info("genIncludedColumns:null");
            }

            String[] columns = ColumnProjectionUtils.getReadColumnNames(conf);
            System.out.println("Result:" + Arrays.toString(columns));
            System.out.println("ResultIndex:" + included.toString());

            this.option.includeCols(columns);
            return this;
        }

        public ReaderOptions filesystem(JobConf conf) {
            try {
                this.filesystem = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return this;
        }

        public ReaderOptions include(List<Integer> included) {
            this.included = included;
            return this;
        }

        public ReaderOptions range(long offset, long length) {
            this.offset = offset;
            this.length = length;
            return this;
        }
        
        public List<Integer> getIncluded() {
            return included;
        }

    }

    public static ReaderOptions readerOptions(Configuration conf, FileSplit split) {
        return new ReaderOptions(conf, split);
    }

    public static PixelsReader createReader(Path path,
                                            ReaderOptions options) throws IOException {
        FileSystem fs = options.getFilesystem();
        return PixelsReaderImpl.newBuilder()
                .setFS(fs)
                .setPath(path)
                .build();
    }

    /**
     * Options for creating PIXELS file writers.
     */
    public static class WriterOptions implements Cloneable {
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

        protected WriterOptions(Properties tableProperties, Configuration conf) {
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
        public WriterOptions clone() {
            try {
                return (WriterOptions) super.clone();
            } catch (CloneNotSupportedException ex) {
                throw new AssertionError("Expected super.clone() to work");
            }
        }

        /**
         * Provide the filesystem for the path, if the client has it available.
         * If it is not provided, it will be found from the path.
         */
        public WriterOptions fileSystem(FileSystem value) {
            fileSystemValue = value;
            return this;
        }

        /**
         * Set the stripe size for the file. The writer stores the contents of the
         * stripe in memory until this memory limit is reached and the stripe
         * is flushed to the HDFS file and the next stripe started.
         */
        public WriterOptions stripeSize(long value) {
            stripeSizeValue = value;
            return this;
        }

        /**
         * Set the file system block size for the file. For optimal performance,
         * set the block size to be multiple factors of stripe size.
         */
        public WriterOptions blockSize(long value) {
            blockSizeValue = value;
            return this;
        }

        /**
         * Set the distance between entries in the row index. The minimum value is
         * 1000 to prevent the index from overwhelming the data. If the stride is
         * set to 0, no indexes will be included in the file.
         */
        public WriterOptions rowIndexStride(int value) {
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
        public WriterOptions blockReplication(short value) {
            blockReplication = value;
            return this;
        }

        /**
         * Sets whether the HDFS blocks are padded to prevent stripes from
         * straddling blocks. Padding improves locality and thus the speed of
         * reading, but costs space.
         */
        public WriterOptions blockPadding(boolean value) {
            blockPaddingValue = value;
            return this;
        }

        /**
         * Sets the encoding strategy that is used to encode the data.
         */
        public WriterOptions encodingStrategy(boolean strategy) {
            encodingStrategy = strategy;
            return this;
        }

        public WriterOptions compressionStrategy(int strategy) {
            compressionStrategy = strategy;
            return this;
        }

        /**
         * Set the schema for the file. This is a required parameter.
         *
         * @param schema the schema for the file.
         * @return this
         */
        public WriterOptions setSchema(TypeDescription schema) {
            this.schema = schema;
            return this;
        }

        public boolean getBlockPadding() {
            return blockPaddingValue;
        }

        public long getBlockSize() {
            return blockSizeValue;
        }

        public FileSystem getFileSystem() {
            return fileSystemValue;
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public TypeDescription getSchema() {
            return schema;
        }

        public long getStripeSize() {
            return stripeSizeValue;
        }

        public short getBlockReplication() {
            return blockReplication;
        }

        public int getRowIndexStride() {
            return rowIndexStrideValue;
        }

        public int getCompressionStrategy() {
            return compressionStrategy;
        }

        public boolean getEncodingStrategy() {
            return encodingStrategy;
        }

    }

    /**
     * Create a set of writer options based on a configuration.
     *
     * @param conf the configuration to use for values
     * @return A WriterOptions object that can be modified
     */
    public static WriterOptions writerOptions(Configuration conf) {
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
                                              Configuration conf) {
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
    ) throws IOException {
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

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
package io.pixelsdb.pixels.amphi;

import com.google.common.collect.ImmutableList;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.pixelsdb.pixels.core.TypeDescription.SHORT_DECIMAL_MAX_PRECISION;
import static java.util.Objects.requireNonNull;

/**
 * Pixels partial columns downloader, table/directory level
 *
 */
public class PeerDownloader
{
    private static final Logger LOGGER = LogManager.getLogger(PeerDownloader.class);

    // Reader relevant fields
    private final Schema schema; // avro schema
    private final List<Column> columns; // pre-fetched from metadata service
    private final List<String> sourcePaths; // all files prepared in list
    private final Storage inputStorage; // s3
    private final int batchSize; // reader: how many rows in a row batch

    // Writer relevant fields
    private final Storage outputStorage; // file system
    private final String outDirPath; // store all parquet files in one directory (=table level)
    private final int writeRate; // combine how many pixels files into one parquet file
    private final int parquetRowGroupSize; // by default ParquetWriter.DEFAULT_BLOCK_SIZE
    private final int parquetPageSize; // by default ParquetWriter.DEFAULT_PAGE_SIZE
    private final Configuration hadoopConfiguration; // by default empty configuration

    private PeerDownloader(
            Schema schema,
            List<Column> columns,
            List<String> sourcePaths,
            Storage inputStorage,
            int batchSize,
            Storage outputStorage,
            String outDirPath,
            int writeRate,
            int parquetRowGroupSize,
            int parquetPageSize,
            Configuration hadoopConfiguration)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = requireNonNull(columns, "column setting is null");
        checkArgument(columns.size() > 0, "column list is empty");
        this.sourcePaths = requireNonNull(sourcePaths, "source paths is null");
        checkArgument(sourcePaths.size() > 0, "source path list is empty");
        this.inputStorage = requireNonNull(inputStorage, "input storage is null");
        this.batchSize = requireNonNull(batchSize, "batch size is null");
        checkArgument(batchSize > 0, "batch size is not positive");

        this.outputStorage = requireNonNull(outputStorage, "output storage is null");
        this.outDirPath = requireNonNull(outDirPath, "output directory is null");
        this.writeRate = requireNonNull(writeRate, "write rate is null");
        checkArgument(writeRate > 0, "write rate is not positive");
        this.parquetRowGroupSize = parquetRowGroupSize;
        checkArgument(parquetRowGroupSize > 0, "parquet row group size is not positive");
        this.parquetPageSize = parquetPageSize;
        checkArgument(parquetPageSize > 0, "parquet page size is not positive");
        this.hadoopConfiguration = requireNonNull(hadoopConfiguration, "hadoop configuration is null");
    }

    public static class Builder
    {
        private Schema schema = null;
        private List<Column> columns = new LinkedList<>();;
        private List<String> sourcePaths = new LinkedList<>();
        private Storage inputStorage = null;
        private int batchSize = 10000;

        // Writer relevant fields
        private Storage outputStorage = null;
        private String outDirPath = null;
        private int writeRate = 4;
        private int parquetRowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;
        private int parquetPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;
        private Configuration hadoopConfiguration = new Configuration();

        private Builder()
        {
        }

        public PeerDownloader.Builder setSchema(Schema schema)
        {
            this.schema = requireNonNull(schema);
            return this;
        }

        public PeerDownloader.Builder setColumns(List<Column> columns)
        {
            this.columns = ImmutableList.copyOf(requireNonNull(columns));
            return this;
        }

        public PeerDownloader.Builder setSourcePaths(List<String> sourcePaths)
        {
            this.sourcePaths = ImmutableList.copyOf(requireNonNull(sourcePaths));
            return this;
        }

        public PeerDownloader.Builder setInputStorage(Storage storage)
        {
            this.inputStorage = requireNonNull(storage);
            return this;
        }

        public PeerDownloader.Builder setBatchSize(int batchSize)
        {
            this.batchSize = requireNonNull(batchSize);
            return this;
        }

        public PeerDownloader.Builder setOutputStorage(Storage storage)
        {
            this.outputStorage = requireNonNull(storage);
            return this;
        }

        public PeerDownloader.Builder setOutDirPath(String outDirPath)
        {
            this.outDirPath = requireNonNull(outDirPath);
            return this;
        }

        public PeerDownloader.Builder setWriteRate(int writeRate)
        {
            this.writeRate = requireNonNull(writeRate);
            return this;
        }

        public PeerDownloader.Builder setParquetRowGroupSize(int parquetRowGroupSize)
        {
            this.parquetRowGroupSize = requireNonNull(parquetRowGroupSize);
            return this;
        }

        public PeerDownloader.Builder setParquetPageSize(int parquetPageSize)
        {
            this.parquetPageSize = requireNonNull(parquetPageSize);
            return this;
        }

        public PeerDownloader.Builder setHadoopConfiguration(Configuration hadoopConfiguration)
        {
            this.hadoopConfiguration = requireNonNull(hadoopConfiguration);
            return this;
        }

        public PeerDownloader build()
                throws IllegalArgumentException, IOException
        {
            // check arguments
            if (schema == null || inputStorage == null || outputStorage == null ||
                    outDirPath == null || columns.size() == 0 || sourcePaths.size() == 0)
            {
                throw new IllegalArgumentException("Missing argument(s) to build PeerDownloader");
            }

            // create output directory
            if (!outputStorage.exists(outDirPath))
            {
                outputStorage.mkdirs(outDirPath);
            }

            return new PeerDownloader(
                    schema,
                    columns,
                    sourcePaths,
                    inputStorage,
                    batchSize,
                    outputStorage,
                    outDirPath,
                    writeRate,
                    parquetRowGroupSize,
                    parquetPageSize,
                    hadoopConfiguration);
        }
    }


    public static PeerDownloader.Builder newBuilder()
    {
        return new PeerDownloader.Builder();
    }

    /**
     * Write the selected columns into a single parquet file
     */
    public void writeParquetFile() throws IOException
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = this.columns.stream().map(column -> column.getName()).toArray(String[]::new);

        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        // iterate a batch of pixels files of size `writeRate`
        for (int i = 0; i * writeRate < sourcePaths.size(); i++)
        {
            String parquetFileName = outDirPath + "/" + i + ".parquet";

            File outputFile = new File(parquetFileName);
            Path outputPath = new Path(outputFile.getPath());

            ParquetWriter<GenericRecord> parquetWriter = getParquetWriter(outputPath);

            // reader consumes one pixels file each time
            for (int j = i * writeRate; j < sourcePaths.size() && j < (i + 1) * writeRate; j++)
            {
                PixelsReader pixelsReader = getPixelsReader(sourcePaths.get(j));
                LOGGER.info("Reading pixels file: ", sourcePaths.get(j));
                VectorizedRowBatch rowBatch;
                PixelsRecordReader recordReader = pixelsReader.read(option);

                // convert data to column vector and cast to java object
                while (true)
                {
                    rowBatch = recordReader.readBatch(this.batchSize);
                    List<ColumnVector> columnVectors = Arrays.asList(rowBatch.cols);
                    if (rowBatch.endOfFile)
                    {
                        for (int row = 0; row < rowBatch.size; row++)
                        {
                            GenericRecord record = castToGenericRecord(this.schema, this.columns, columnVectors, row);
                            parquetWriter.write(record);
                        }
                        break;
                    }
                    for (int row = 0; row < rowBatch.size; row++)
                    {
                        GenericRecord record = castToGenericRecord(this.schema, this.columns, columnVectors, row);
                        parquetWriter.write(record);
                    }
                }

                recordReader.close();
                pixelsReader.close();
            }
            LOGGER.info("Written parquet file: ", parquetFileName);
            parquetWriter.close();
        }
    }

    private PixelsReader getPixelsReader(String filePath) throws IOException
    {
        PixelsReader pixelsReader = null;
        try
        {
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(this.inputStorage)
                    .setPath(filePath)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
        }
        catch (Exception e)
        {
            throw new IOException("Failed to instantiate the PixelsReader: " + e.getMessage());
        }

        return pixelsReader;
    }

    private ParquetWriter<GenericRecord> getParquetWriter(Path filePath) throws IOException
    {
        ParquetWriter<GenericRecord> parquetWriter = null;
        try
        {
            parquetWriter = AvroParquetWriter
                    .<GenericRecord>builder(filePath)
                    .withRowGroupSize(this.parquetRowGroupSize)
                    .withPageSize(this.parquetPageSize)
                    .withSchema(this.schema)
                    .withConf(this.hadoopConfiguration)
                    .build();
        }
        catch (Exception e)
        {
            throw new IOException("Failed to instantiate the ParquetWriter: " + e.getMessage());
        }

        return parquetWriter;
    }

    // Given the column type, cast column vectors to the generic record, TODO: could be problem
    private static GenericRecord castToGenericRecord(Schema schema, List<Column> columns, List<ColumnVector> columnVectors, int rowIdx)
    {
        GenericRecord record = new GenericData.Record(schema);

        for (int i = 0; i < columns.size(); i++) {
            String typeStr = columns.get(i).getType();
            TypeDescription typeDesc = TypeDescription.fromString(typeStr);
            TypeDescription.Category category = typeDesc.getCategory();

            switch (category) {
                case BOOLEAN:
                case BYTE:
                    ByteColumnVector bcv = (ByteColumnVector) columnVectors.get(i);
                    record.put(columns.get(i).getName(), bcv.vector[rowIdx]);
                    break;
                case SHORT:
                case INT:
                case LONG:
                    LongColumnVector lcv = (LongColumnVector) columnVectors.get(i);
                    record.put(columns.get(i).getName(), lcv.vector[rowIdx]);
                    break;
                case DATE:
                    DateColumnVector dcv = (DateColumnVector) columnVectors.get(i);
                    record.put(columns.get(i).getName(), dcv.dates[rowIdx]);
                    break;
                case TIME:
                    TimeColumnVector tcv = (TimeColumnVector) columnVectors.get(i);
                    record.put(columns.get(i).getName(), tcv.times[rowIdx]);
                    break;
                case TIMESTAMP:
                    TimestampColumnVector tscv = (TimestampColumnVector) columnVectors.get(i);
                    record.put(columns.get(i).getName(), tscv.times[rowIdx]);
                    break;
                case FLOAT:
                case DOUBLE:
                    DoubleColumnVector dbcv = (DoubleColumnVector) columnVectors.get(i);
                    record.put(columns.get(i).getName(), dbcv.vector[rowIdx]);
                    break;
                case DECIMAL:
                    if (typeDesc.getPrecision() <= SHORT_DECIMAL_MAX_PRECISION) {
                        DecimalColumnVector decv = (DecimalColumnVector) columnVectors.get(i);
                        StringBuilder sb = new StringBuilder();
                        decv.stringifyValue(sb, rowIdx);
                        BigDecimal decimalValue = new java.math.BigDecimal(sb.toString());
                        ByteBuffer decimalBytes = ByteBuffer.wrap(decimalValue.unscaledValue().toByteArray());
                        record.put(columns.get(i).getName(), decimalBytes);
                    } else {
                        LongColumnVector longcv = (LongColumnVector) columnVectors.get(i);
                        record.put(columns.get(i).getName(), longcv.vector[rowIdx]);
                    }
                    break;
                case STRING:
                case BINARY:
                case VARBINARY:
                case CHAR:
                case VARCHAR:
                    BinaryColumnVector bincv = (BinaryColumnVector) columnVectors.get(i);
                    record.put(columns.get(i).getName(), new String(bincv.vector[rowIdx], bincv.start[rowIdx], bincv.lens[rowIdx]));
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + category);
            }
        }
        return record;
    }
}

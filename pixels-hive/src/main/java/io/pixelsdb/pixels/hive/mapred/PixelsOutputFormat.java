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
 * License along with Foobar.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.hive.mapred;

import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.hive.common.PixelsConf;
import io.pixelsdb.pixels.hive.common.PixelsRW;
import io.pixelsdb.pixels.hive.PixelsSerDe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * An PIXELS output format that satisfies the org.apache.hadoop.mapred API.
 *
 * This class is not finished, so that write is not supported by pixels-hive.
 *
 * refers to {@link org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat}
 */
public class PixelsOutputFormat
        extends FileOutputFormat<NullWritable, PixelsSerDe.PixelsRow> implements HiveOutputFormat<NullWritable, PixelsSerDe.PixelsRow>
{

    /**
     * This function builds the options for the PIXELS Writer based on the JobConf.
     *
     * @param conf the job configuration
     * @return a new options object
     */
    public static PixelsRW.WriterOptions buildOptions(Configuration conf)
    {
        return PixelsRW.writerOptions(conf)
                .setSchema(TypeDescription.fromString(PixelsConf.MAPRED_OUTPUT_SCHEMA
                        .getString(conf)))
                .rowIndexStride((int) PixelsConf.ROW_INDEX_STRIDE.getLong(conf))
                .stripeSize(PixelsConf.STRIPE_SIZE.getLong(conf))
                .blockSize(PixelsConf.BLOCK_SIZE.getLong(conf))
                .blockPadding(PixelsConf.BLOCK_PADDING.getBoolean(conf))
                .encodingStrategy(PixelsConf.ENCODING_STRATEGY.getBoolean(conf))
                .compressionStrategy((int) PixelsConf.COMPRESSION_STRATEGY.getLong(conf));
    }

    @Override
    public RecordWriter<NullWritable, PixelsSerDe.PixelsRow> getRecordWriter(FileSystem fileSystem,
                                                                             JobConf conf,
                                                                             String name,
                                                                             Progressable progressable
    ) throws IOException
    {
        Path path = getTaskOutputPath(conf, name);
        PixelsWriter writer = PixelsRW.createWriter(path,
                buildOptions(conf).fileSystem(fileSystem));
        return new PixelsMapredRecordWriter(writer);
    }

    /**
     * create the final out file and get some specific settings.
     * In case of empty table location, this method is called, so that it should not
     * return null.
     *
     * @param jobConf
     *          the job configuration file
     * @param finalOutPath
     *          the final output file to be created
     * @param valueClass
     *          the value class used for create
     * @param isCompressed
     *          whether the content is compressed or not
     * @param tableProperties
     *          the table properties of this file's corresponding table
     * @param progress
     *          progress used for status report
     * @return the RecordWriter for the output file
     */
    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Path finalOutPath,
                                                             Class<? extends Writable> valueClass,
                                                             boolean isCompressed, Properties tableProperties,
                                                             Progressable progress) throws IOException
    {
        return new FileSinkOperator.RecordWriter()
        {
            @Override
            public void write(Writable w) throws IOException
            {

            }

            @Override
            public void close(boolean abort) throws IOException
            {

            }
        };
    }
}

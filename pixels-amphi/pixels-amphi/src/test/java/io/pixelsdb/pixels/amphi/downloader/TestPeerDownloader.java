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
package io.pixelsdb.pixels.amphi.downloader;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.*;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class TestPeerDownloader
{
    /* Given the PATH, we can retrieve all pxl uri from s3. */
    @Test
    public void testS3Data()
            throws MetadataException, IOException, InterruptedException
    {
        String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";

        // get layout
        MetadataService metadataService = new MetadataService(hostAddr, 18888);
        List<Layout> layouts = metadataService.getLayouts("tpch_1g", "lineitem");
        System.out.println("existing number of layouts: " + layouts.size());
        System.out.println("layout: " + layouts.get(0));

        // get schema
        List<Column> columns = metadataService.getColumns("tpch_1g", "lineitem", false);
        for (Column column : columns)
        {
            System.out.println(column);
        }

        // get input file paths
        Storage storage = StorageFactory.Instance().getStorage("s3");
        List<Status> statuses = storage.listStatus("s3://pixels-tpch1g/region/v-0-order");
        System.out.println(statuses.size());
        for (Status status : statuses)
        {
            System.out.println(status.getPath());
        }

        metadataService.shutdown();
    }

    /* Read pxl from s3 and retrieve the columns needed */
    @Test
    public void testPixelsReader()
    {
        PixelsReaderOption option = new PixelsReaderOption();
        String[] cols = {"r_name", "r_regionkey"};
        String filePath = "pixels-tpch1g/region/v-0-order/20230529201511_20.pxl";
        int batchSize = 10000;
        option.skipCorruptRecords(true);
        option.tolerantSchemaEvolution(true);
        option.includeCols(cols);

        VectorizedRowBatch rowBatch;
        try (PixelsReader pixelsReader = getReader(filePath);
             PixelsRecordReader recordReader = pixelsReader.read(option))
        {
            while (true)
            {
                rowBatch = recordReader.readBatch(batchSize);
                BinaryColumnVector r_name = (BinaryColumnVector) rowBatch.cols[0];
                LongColumnVector r_regionkey = (LongColumnVector) rowBatch.cols[1];
                if (rowBatch.endOfFile)
                {
                    for (int i = 0; i < rowBatch.size; i++) {
                        String actualStr = new String(r_name.vector[i], r_name.start[i], r_name.lens[i]);
                        System.out.println(r_regionkey.vector[i]);
                        System.out.println(actualStr);
                    }
                    break;
                }
                for (int i = 0; i < rowBatch.size; i++) {
                    String actualStr = new String(r_name.vector[i], r_name.start[i], r_name.lens[i]);
                    System.out.println(r_regionkey.vector[i]);
                    System.out.println(actualStr);
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private PixelsReader getReader(String filePath)
    {
        PixelsReader pixelsReader = null;
        try
        {
            Storage storage = StorageFactory.Instance().getStorage("s3");
            pixelsReader = PixelsReaderImpl.newBuilder()
                    .setStorage(storage)
                    .setPath(filePath)
                    .setPixelsFooterCache(new PixelsFooterCache())
                    .build();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return pixelsReader;
    }

    /* Simply write a parquet file based on avro schema */
    @Test
    public void testParquetWriter() throws IOException
    {
        String avroSchema = "{"
                + "\"type\":\"record\","
                + "\"name\":\"Region\","
                + "\"fields\":["
                    + "{\"name\":\"r_regionkey\",\"type\":\"int\"},"
                    + "{\"name\":\"r_name\",\"type\":\"string\"},"
                    + "{\"name\":\"r_comment\",\"type\":\"string\"}"
                + "]"
                + "}";

        Schema schema = new Schema.Parser().parse(avroSchema);

        File outputFile = new File("region.parquet");
        Path outputPath = new Path(outputFile.getPath());

        List<Integer> regionKeys = Arrays.asList(0, 1, 2, 3, 4);
        List<String> names = Arrays.asList("AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST");
        List<String> comments = Arrays.asList("comment1", "comment2", "comment3", "comment4", "comment5");

        ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(outputPath)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(schema)
                .withConf(new Configuration())
                .build();

        for (int i = 0; i < regionKeys.size(); i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("r_regionkey", regionKeys.get(i));
            record.put("r_name", names.get(i));
            record.put("r_comment", comments.get(i));
            writer.write(record);
        }

        writer.close();
    }

    @Test
    public void testDownloadProcess()
            throws IOException, MetadataException
    {
        URL resourceUrl = getClass().getClassLoader().getResource("config/tpch.json");
        if (resourceUrl != null) {
            String resourcesPath = resourceUrl.getPath();
            String[] args = {resourcesPath};
            DownloadProcess.main(args);
        } else {
            System.out.println("Cannot find configuration file path.");
        }

    }
}
